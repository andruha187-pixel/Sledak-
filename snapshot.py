"""Каждую секунду — полный снимок рынка по каждому активу в кольцевой буфер (≈15 мин).
Когда кошелёк совершает сделку, берём снимок ДО неё (время блока − DECISION_LAG_SEC).
Каждые CONTROL_EVERY_SEC снимок пишется в БД как «контроль» — моменты, когда он НЕ входил.
"""
import asyncio
import logging
import time
from collections import deque

import db
from config import (ASSETS, BINANCE_FUT, BINANCE_REST, BINANCE_SYM, BYBIT, COINBASE, COINBASE_SYM,
                    CONTROL_EVERY_SEC)
from features import fair_block, flow_block, hour_of, price_before, ta_all
from net import get_json

log = logging.getLogger("snapshot")


def parse_klines(raw):
    return [(int(k[0]) // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
            for k in raw or []]


class Rest:
    """Медленные REST-данные в фоне; снимок читает только кэш."""

    def __init__(self):
        self.k1 = {}
        self.fut = {}
        self.oi = {a: deque(maxlen=40) for a in ASSETS}
        self.bybit = {}
        self.cb = {}

    def closed_k1(self, a, now):
        return [c for c in self.k1.get(a, []) if c[0] + 60 <= now]

    async def _klines(self):
        while True:
            for a in ASSETS:
                raw = await get_json(f"{BINANCE_REST}/api/v3/klines",
                                     {"symbol": BINANCE_SYM[a], "interval": "1m", "limit": 800}, quiet=True)
                if raw:
                    self.k1[a] = parse_klines(raw)
            await asyncio.sleep(15)

    async def _futures(self):
        while True:
            for a in ASSETS:
                sym = BINANCE_SYM[a]
                pi = await get_json(f"{BINANCE_FUT}/fapi/v1/premiumIndex", {"symbol": sym}, quiet=True)
                oi = await get_json(f"{BINANCE_FUT}/fapi/v1/openInterest", {"symbol": sym}, quiet=True)
                d = {}
                if pi:
                    d["funding"] = float(pi.get("lastFundingRate") or 0)
                    d["mark"] = float(pi.get("markPrice") or 0)
                if oi:
                    self.oi[a].append((time.time(), float(oi["openInterest"])))
                self.fut[a] = d
            await asyncio.sleep(30)

    async def _bybit(self):
        while True:
            for a in ASSETS:
                r = await get_json(f"{BYBIT}/v5/market/tickers",
                                   {"category": "linear", "symbol": BINANCE_SYM[a]}, quiet=True)
                try:
                    x = r["result"]["list"][0]
                    self.bybit[a] = {"px": float(x["lastPrice"]), "funding": float(x.get("fundingRate") or 0)}
                except Exception:
                    pass
            await asyncio.sleep(5)

    async def _coinbase(self):
        while True:
            for a in ASSETS:
                if a in COINBASE_SYM:
                    r = await get_json(f"{COINBASE}/products/{COINBASE_SYM[a]}/ticker", quiet=True)
                    if r and r.get("price"):
                        self.cb[a] = float(r["price"])
            await asyncio.sleep(5)

    def oi_chg5m(self, a):
        h = self.oi.get(a)
        if not h or len(h) < 2:
            return None
        now, cur = h[-1]
        for ts, v in h:
            if ts >= now - 330:
                return (cur / v - 1) * 100 if v else None
        return None

    async def run(self):
        await asyncio.gather(self._klines(), self._futures(), self._bybit(), self._coinbase())


class Snapshotter:
    def __init__(self, markets, pm, bn, cl, rest):
        self.m, self.pm, self.bn, self.cl, self.rest = markets, pm, bn, cl, rest
        self.ring = {a: deque(maxlen=1000) for a in ASSETS}
        self.open_cl = {}
        self._pending = []
        self.built = 0

    def before(self, a, t):
        for s in reversed(self.ring.get(a, ())):
            if s["ts"] <= t:
                return s
        return None

    def build(self, a, now):
        m = self.m.get(a)
        if not m or not (m["start"] <= now < m["start"] + 900):
            return None
        st = m["start"]
        sec_left = st + 900 - now
        f = {"ts": round(now, 3), "slug": m["slug"], "src": "live",
             "sec_into": round(now - st, 2), "sec_left": round(sec_left, 2), "hour": hour_of(now)}
        # --- Polymarket
        up, dn = self.pm.top(m["up"]), self.pm.top(m["dn"])
        for k, t in (("up", up), ("dn", dn)):
            for x, v in t.items():
                f[f"pm_{k}_{x}"] = v
        if up.get("ask") and dn.get("ask"):
            f["pm_sum_ask"] = up["ask"] + dn["ask"]
        if up.get("bid") and dn.get("bid"):
            f["pm_sum_bid"] = up["bid"] + dn["bid"]
        f["pmh_up"] = up.get("mid")
        f["pmh_dn"] = dn.get("mid")
        f.update(self.pm.flow(m["up"], m["dn"], now))
        for lag in (5, 15):
            old = self.before(a, now - lag)
            if old and old.get("slug") == m["slug"] and old.get("pm_up_mid") and up.get("mid"):
                f[f"pm_up_mid_chg{lag}s"] = up["mid"] - old["pm_up_mid"]
        # --- Binance поток/импульс
        b = self.bn.buckets[a].as_list(400)
        fl, bnpx = flow_block(b, now)
        f.update(fl)
        f["bn_px"] = bnpx
        # --- Chainlink (цена резолва) и «цена для победы»
        cl = self.cl.last(a)
        if (a, st) not in self.open_cl:
            v = self.cl.at_or_after(a, st)
            if v:
                self.open_cl[(a, st)] = v
                db.execute("UPDATE windows SET open_cl=? WHERE slug=?", (v, m["slug"]))
        open_cl = self.open_cl.get((a, st))
        open_bn = price_before(b, st + 1)
        if cl and open_cl:
            ref, op, f["ref"] = cl, open_cl, "cl"
        else:
            ref, op, f["ref"] = bnpx, open_bn, "bn"
        f["cl_px"], f["open_px"] = cl, op
        # --- ТА 1m/5m (только закрытые свечи, как в истории)
        ta, sigma = ta_all(self.rest.closed_k1(a, now))
        f.update(ta)
        f.update(fair_block(ref, op, sigma, sec_left))
        if f.get("fair_up") is not None:
            if up.get("ask"):
                f["edge_up"] = f["fair_up"] - up["ask"]
            if dn.get("ask"):
                f["edge_dn"] = (1 - f["fair_up"]) - dn["ask"]
        # --- другие площадки
        if bnpx:
            if cl:
                f["cl_bn_basis_bps"] = (bnpx / cl - 1) * 1e4
            by = self.rest.bybit.get(a)
            if by:
                f["by_basis_bps"] = (by["px"] / bnpx - 1) * 1e4
                f["by_funding"] = by["funding"]
            if a in self.rest.cb:
                f["cb_basis_bps"] = (self.rest.cb[a] / bnpx - 1) * 1e4
        fu = self.rest.fut.get(a, {})
        if "funding" in fu:
            f["bn_funding"] = fu["funding"]
        f["oi_chg5m_pct"] = self.rest.oi_chg5m(a)
        f["bn_book_imb"] = self.bn.book_imb(a)
        return f

    async def loop(self):
        last_ctrl = 0
        last_flush = time.time()
        while True:
            t0 = time.time()
            for a in ASSETS:
                try:
                    s = self.build(a, t0)
                except Exception as e:
                    log.warning("build %s: %s", a, e)
                    s = None
                if s:
                    self.ring[a].append(s)
                    self.built += 1
                    if t0 - last_ctrl >= CONTROL_EVERY_SEC:
                        self._pending.append((s["ts"], a, s["slug"], "live",
                                              {k: v for k, v in s.items() if k not in ("ts", "slug")}))
            if t0 - last_ctrl >= CONTROL_EVERY_SEC:
                last_ctrl = t0
            if t0 - last_flush >= 30 and self._pending:
                rows, self._pending = self._pending, []
                last_flush = t0
                await asyncio.to_thread(db.insert_snapshots, rows)
            await asyncio.sleep(max(0.05, 1.0 - (time.time() - t0)))
