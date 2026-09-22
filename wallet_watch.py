import asyncio
import logging
import time
from collections import defaultdict

import db
from config import ALERT_EACH_TRADE, ASSETS, DATA_API, DECISION_LAG_SEC, POLYGON_RPC, WALLET
from markets import gamma_market, parse_slug
from net import get_json, post_json

log = logging.getLogger("wallet")
_rpc_sem = asyncio.Semaphore(2)


def activity_key(it):
    return ":".join(str(it.get(k, "")) for k in
                    ("transactionHash", "asset", "side", "size", "price", "timestamp", "type"))


def activity_to_row(it, source):
    slug = it.get("slug") or ""
    ps = parse_slug(slug)
    return {"key": activity_key(it), "ts": int(it.get("timestamp") or 0), "source": source,
            "type": it.get("type"), "asset": ps[0] if ps else None, "slug": slug,
            "token": it.get("asset"), "outcome": str(it.get("outcome") or "").lower() or None,
            "side": it.get("side"), "price": _fl(it.get("price")), "size": _fl(it.get("size")),
            "usdc": _fl(it.get("usdcSize")), "tx": it.get("transactionHash"), "role": None,
            "detected_ts": time.time(), "feat": {}}


def _fl(x):
    try:
        return float(x)
    except Exception:
        return None


async def tx_role(tx):
    """maker / taker по событиям OrderFilled в квитанции транзакции Polygon.
    В matchOrders у ордера-тейкера поле taker == адрес самой биржи (log.address)."""
    if not tx:
        return None
    async with _rpc_sem:
        r = await post_json(POLYGON_RPC, {"jsonrpc": "2.0", "id": 1,
                                          "method": "eth_getTransactionReceipt", "params": [tx]})
    try:
        logs = r["result"]["logs"]
    except Exception:
        return None
    w = WALLET[2:].rjust(64, "0")
    roles = set()
    for lg in logs:
        t = [x[2:].lower() for x in lg.get("topics", [])]
        if len(t) == 4 and t[2] == w:
            exch = lg["address"][2:].lower().rjust(64, "0")
            roles.add("taker" if t[3] == exch else "maker")
    return "+".join(sorted(roles)) if roles else "unknown"


class WalletWatch:
    def __init__(self, snap, tg):
        self.snap, self.tg = snap, tg
        self.seen = {r[0] for r in db.query("SELECT key FROM trades WHERE ts > ?", (time.time() - 3 * 86400,))}
        self.pos = defaultdict(lambda: {"up": 0.0, "down": 0.0, "fills": 0, "last_ts": None, "cost": 0.0})
        self.last_trade_ts = None
        self.polls_ok = 0

    async def run(self):
        first = True
        while True:
            items = await get_json(f"{DATA_API}/activity",
                                   {"user": WALLET, "limit": 100, "offset": 0,
                                    "sortBy": "TIMESTAMP", "sortDirection": "DESC"}, quiet=True)
            if items is not None:
                self.polls_ok += 1
                if first and not items:
                    await self.tg.send("⚠️ По адресу нет активности в data-api. Проверь, что это proxy-адрес "
                                       "из профиля Polymarket (polymarket.com/profile/0x...).")
                first = False
                for it in reversed(items):
                    try:
                        await self._on(it)
                    except Exception as e:
                        log.warning("on item: %s", e)
            await asyncio.sleep(2)

    async def _on(self, it):
        key = activity_key(it)
        if key in self.seen:
            return
        self.seen.add(key)
        row = activity_to_row(it, "live")
        now = time.time()
        ps = parse_slug(row["slug"])
        feat = {"detect_lag": round(now - row["ts"], 2)}
        if row["type"] == "TRADE" and ps and ps[0] in ASSETS and now - row["ts"] < 120:
            a, st = ps
            s = self.snap.before(a, row["ts"] - DECISION_LAG_SEC)
            if s:
                fs = {k: v for k, v in s.items() if k not in ("ts", "slug")}
                if s["slug"] != row["slug"]:  # торгует не текущее окно (например, следующее заранее)
                    fs = {k: v for k, v in fs.items() if not k.startswith(("pm_", "edge_", "pmh_"))}
                    feat["window_offset"] = (st - int(s["ts"]) // 900 * 900) // 900
                feat.update(fs)
                feat["snap_age"] = round(row["ts"] - s["ts"], 2)
            p = self.pos[row["slug"]]
            feat.update(pos_up_before=p["up"], pos_dn_before=p["down"], fills_before=p["fills"],
                        since_prev_fill=(row["ts"] - p["last_ts"]) if p["last_ts"] else None)
        if row["type"] == "TRADE" and row["outcome"] in ("up", "down"):
            p = self.pos[row["slug"]]
            sg = 1 if row["side"] == "BUY" else -1
            p[row["outcome"]] += sg * (row["size"] or 0)
            p["cost"] += sg * (row["usdc"] or 0)
            p["fills"] += 1
            p["last_ts"] = row["ts"]
            self.last_trade_ts = row["ts"]
        row["feat"] = feat
        row["source"] = "live" if "sec_left" in feat else "live_nofeat"
        await asyncio.to_thread(db.insert_trades, [row])
        if ps:
            await asyncio.to_thread(db.upsert_window, row["slug"], ps[0], ps[1])
        if row["type"] == "TRADE":
            asyncio.create_task(self._role(key, row["tx"]))
        if ALERT_EACH_TRADE and row["type"] == "TRADE":
            await self.tg.send(f"👁 {row['side']} {row['outcome']} {row['slug']} @ {row['price']} × {row['size']}"
                               f" (${row['usdc']:.2f}), осталось {feat.get('sec_left', '?')}с")

    async def _role(self, key, tx):
        r = await tx_role(tx)
        if r:
            await asyncio.to_thread(db.execute, "UPDATE trades SET role=? WHERE key=?", (r, key))


async def role_backfill(limit):
    rows = db.query("SELECT key, tx FROM trades WHERE type='TRADE' AND role IS NULL AND tx IS NOT NULL "
                    "ORDER BY ts DESC LIMIT ?", (limit,))
    cache = {}
    for key, tx in rows:
        if tx not in cache:
            cache[tx] = await tx_role(tx)
        if cache[tx]:
            await asyncio.to_thread(db.execute, "UPDATE trades SET role=? WHERE key=?", (cache[tx], key))


async def resolver(cl):
    while True:
        try:
            rows = db.query("SELECT slug, asset, end FROM windows WHERE winner IS NULL AND end < ? "
                            "ORDER BY end DESC LIMIT 40", (time.time() - 180,))
            for slug, a, end in rows:
                m = await gamma_market(slug)
                if m and m["winner"]:
                    db.execute("UPDATE windows SET winner=? WHERE slug=?", (m["winner"], slug))
                    v = cl.at_or_after(a, end) if a in cl.h else None
                    if v:
                        db.execute("UPDATE windows SET close_cl=? WHERE slug=?", (v, slug))
                await asyncio.sleep(0.3)
        except Exception as e:
            log.warning("resolver: %s", e)
        await asyncio.sleep(60)
