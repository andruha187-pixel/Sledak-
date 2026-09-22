"""Живые потоки. Все данные хранятся в памяти, быстрый цикл снимков читает только память
(никаких HTTP/SQLite в горячем пути — чтобы не ловить «slow consumer»)."""
import asyncio
import json
import logging
import time
from collections import deque

import aiohttp

from config import ASSETS, BINANCE_SYM, BINANCE_WS, PM_WS, RTDS_WS
from net import session

log = logging.getLogger("streams")


class SecBuckets:
    """1-секундные корзины: [sec, close, buy_usd, sell_usd, n]."""

    def __init__(self, maxlen=1800):
        self.d = deque(maxlen=maxlen)

    def add(self, ts, price, qty, taker_buy):
        sec = int(ts)
        if self.d and sec < self.d[-1][0]:
            return
        if self.d and self.d[-1][0] == sec:
            b = self.d[-1]
        else:
            b = [sec, price, 0.0, 0.0, 0]
            self.d.append(b)
        b[1] = price
        if taker_buy:
            b[2] += qty * price
        else:
            b[3] += qty * price
        b[4] += 1

    def as_list(self, last=400):
        n = len(self.d)
        return [tuple(self.d[i]) for i in range(max(0, n - last), n)]


class BinanceStream:
    def __init__(self):
        self.buckets = {a: SecBuckets() for a in ASSETS}
        self.book = {}
        self.last_msg = 0
        self.sym2a = {BINANCE_SYM[a].lower(): a for a in ASSETS if a in BINANCE_SYM}

    def book_imb(self, a):
        b = self.book.get(a)
        if not b:
            return None
        bs = sum(float(q) for _, q in b["bids"][:10])
        as_ = sum(float(q) for _, q in b["asks"][:10])
        return (bs - as_) / (bs + as_) if bs + as_ > 0 else None

    async def run(self):
        streams = "/".join(f"{s}@aggTrade/{s}@depth20@100ms" for s in self.sym2a)
        url = f"{BINANCE_WS}/stream?streams={streams}"
        while True:
            try:
                s = await session()
                async with s.ws_connect(url, heartbeat=20, max_msg_size=0) as ws:
                    log.info("Binance WS connected")
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            o = json.loads(msg.data)
                            st, d = o["stream"], o["data"]
                        except Exception:
                            continue
                        self.last_msg = time.time()
                        a = self.sym2a.get(st.split("@")[0])
                        if not a:
                            continue
                        if "aggTrade" in st:
                            self.buckets[a].add(d["T"] / 1000, float(d["p"]), float(d["q"]), not d["m"])
                        else:
                            self.book[a] = {"bids": d.get("bids", []), "asks": d.get("asks", [])}
            except Exception as e:
                log.warning("Binance WS error: %s", e)
            await asyncio.sleep(3)


class Chainlink:
    """Цены Chainlink через RTDS Polymarket — именно по ним резолвятся 15m рынки."""

    def __init__(self):
        self.h = {a: deque(maxlen=3600) for a in ASSETS}
        self.last_msg = 0

    def _add(self, a, ts, v):
        sec = int(ts)
        d = self.h[a]
        if d and d[-1][0] == sec:
            d[-1] = (sec, v)
        elif not d or sec > d[-1][0]:
            d.append((sec, v))

    def last(self, a):
        d = self.h.get(a)
        return d[-1][1] if d else None

    def at_or_after(self, a, sec, tol=5):
        for s, v in self.h.get(a, ()):
            if s >= sec:
                return v if s - sec <= tol else None
        return None

    def _handle(self, raw):
        if not raw or raw[0] not in "{[":
            return
        try:
            o = json.loads(raw)
        except Exception:
            return
        for it in (o if isinstance(o, list) else [o]):
            p = it.get("payload") if isinstance(it, dict) else None
            if not isinstance(p, dict):
                continue
            a = str(p.get("symbol", "")).lower().split("/")[0]
            if a not in self.h:
                continue
            if "value" in p:
                ts = p.get("timestamp") or it.get("timestamp") or time.time() * 1000
                self._add(a, float(ts) / 1000, float(p["value"]))
                self.last_msg = time.time()
            for x in p.get("data", []) or []:
                try:
                    self._add(a, float(x["timestamp"]) / 1000, float(x["value"]))
                except Exception:
                    pass

    async def run(self):
        sub = {"action": "subscribe",
               "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": ""}]}
        while True:
            try:
                s = await session()
                async with s.ws_connect(RTDS_WS, max_msg_size=0) as ws:
                    await ws.send_json(sub)
                    log.info("RTDS Chainlink connected")
                    ping = asyncio.create_task(_pinger(ws, 5))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        ping.cancel()
            except Exception as e:
                log.warning("RTDS error: %s", e)
            await asyncio.sleep(3)


async def _pinger(ws, every):
    try:
        while True:
            await asyncio.sleep(every)
            await ws.send_str("PING")
    except Exception:
        pass


class PMBook:
    """Стакан Polymarket по WS. При смене окна — переподключение только с токенами текущих окон
    (старые токены не копятся)."""

    def __init__(self):
        self.books = {}
        self.tokens = set()
        self._changed = asyncio.Event()
        self.trades = deque(maxlen=20000)  # (ts, token, side, price, size)
        self.last_msg = 0

    def set_tokens(self, toks):
        toks = {t for t in toks if t}
        if toks != self.tokens:
            self.tokens = toks
            self._changed.set()

    def _apply(self, tok, side, price, size):
        b = self.books.setdefault(tok, {"b": {}, "a": {}})
        book = b["b"] if str(side).upper() in ("BUY", "BID") else b["a"]
        p, s = float(price), float(size)
        if s <= 0:
            book.pop(p, None)
        else:
            book[p] = s

    def _handle(self, raw):
        if not raw or raw[0] not in "{[":
            return
        try:
            o = json.loads(raw)
        except Exception:
            return
        now = time.time()
        self.last_msg = now
        for it in (o if isinstance(o, list) else [o]):
            if not isinstance(it, dict):
                continue
            et = it.get("event_type")
            if et == "book":
                tok = it.get("asset_id")
                bids = it.get("bids") or it.get("buys") or []
                asks = it.get("asks") or it.get("sells") or []
                self.books[tok] = {"b": {float(x["price"]): float(x["size"]) for x in bids},
                                   "a": {float(x["price"]): float(x["size"]) for x in asks}}
            elif et == "price_change":
                chs = it.get("price_changes")
                if chs:
                    for c in chs:
                        self._apply(c.get("asset_id"), c.get("side"), c.get("price"), c.get("size"))
                else:
                    for c in it.get("changes", []) or []:
                        self._apply(it.get("asset_id"), c.get("side"), c.get("price"), c.get("size"))
            elif et == "last_trade_price":
                try:
                    self.trades.append((now, it.get("asset_id"), str(it.get("side", "")).upper(),
                                        float(it["price"]), float(it.get("size", 0))))
                except Exception:
                    pass

    def top(self, tok):
        b = self.books.get(tok)
        if not b or not b["b"] or not b["a"]:
            return {}
        bids = sorted(b["b"].items(), reverse=True)[:5]
        asks = sorted(b["a"].items())[:5]
        bid, ask = bids[0][0], asks[0][0]
        bs, as_ = sum(s for _, s in bids), sum(s for _, s in asks)
        return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2, "spread": ask - bid,
                "bsz5": bs, "asz5": as_, "imb5": (bs - as_) / (bs + as_) if bs + as_ else None,
                "bsz1": bids[0][1], "asz1": asks[0][1]}

    def flow(self, up, dn, now, w=30):
        f = {"pm_up_flow30": 0.0, "pm_dn_flow30": 0.0, "pm_n30": 0}
        for ts, tok, side, p, s in reversed(self.trades):
            if ts < now - w:
                break
            k = "pm_up_flow30" if tok == up else "pm_dn_flow30" if tok == dn else None
            if k:
                f[k] += p * s * (1 if side == "BUY" else -1)
                f["pm_n30"] += 1
        return f

    async def run(self):
        while True:
            if not self.tokens:
                await asyncio.sleep(1)
                continue
            self._changed.clear()
            toks = list(self.tokens)
            try:
                s = await session()
                async with s.ws_connect(PM_WS, max_msg_size=0) as ws:
                    await ws.send_json({"assets_ids": toks, "type": "market"})
                    log.info("PM WS connected, %d tokens", len(toks))
                    ping = asyncio.create_task(_pinger(ws, 10))
                    try:
                        while not self._changed.is_set():
                            try:
                                msg = await ws.receive(timeout=1.0)
                            except asyncio.TimeoutError:
                                continue
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    self._handle(msg.data)
                                except Exception as e:  # одно кривое сообщение не рвёт соединение
                                    log.debug("pm msg err %s", e)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR,
                                              aiohttp.WSMsgType.CLOSE):
                                break
                    finally:
                        ping.cancel()
                for t in list(self.books):
                    if t not in self.tokens:
                        del self.books[t]
            except Exception as e:
                log.warning("PM WS error: %s", e)
                await asyncio.sleep(2)
