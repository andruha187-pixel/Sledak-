import asyncio
import json
import logging
import time

import db
from config import ASSETS, CLOB, GAMMA, WINDOW_SEC
from net import get_json

log = logging.getLogger("markets")


def wstart(ts):
    return int(ts) // WINDOW_SEC * WINDOW_SEC


def make_slug(asset, start):
    return f"{asset}-updown-15m-{start}"


def parse_slug(slug):
    p = (slug or "").split("-")
    if len(p) == 4 and p[1] == "updown" and p[2] == "15m" and p[3].isdigit():
        return p[0], int(p[3])
    return None


def _jl(x):
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return []
    return x or []


async def gamma_market(slug):
    d = await get_json(f"{GAMMA}/markets", {"slug": slug}, quiet=True)
    if not d:
        return None
    m = d[0] if isinstance(d, list) else d
    toks = _jl(m.get("clobTokenIds"))
    outs = [str(o).lower() for o in _jl(m.get("outcomes"))]
    prices = _jl(m.get("outcomePrices"))
    up = dn = None
    for o, t in zip(outs, toks):
        if o == "up":
            up = t
        elif o == "down":
            dn = t
    winner = None
    if m.get("closed") and prices:
        for o, p in zip(outs, prices):
            try:
                if float(p) >= 0.99:
                    winner = o
            except Exception:
                pass
    if up and up == dn:  # известный баг API (видели на HYPE)
        up = dn = None
    return {"slug": slug, "up": up, "dn": dn, "closed": bool(m.get("closed")), "winner": winner}


async def prices_history(token, start, end):
    if not token:
        return []
    d = await get_json(f"{CLOB}/prices-history",
                       {"market": token, "startTs": int(start), "endTs": int(end), "fidelity": 1}, quiet=True)
    h = (d or {}).get("history") or []
    return sorted((int(x["t"]), float(x["p"])) for x in h if "t" in x and "p" in x)


class MarketCache:
    """Текущее окно по каждому активу; Gamma дёргается один раз на окно."""

    def __init__(self):
        self.cur = {}

    def get(self, a):
        return self.cur.get(a)

    async def loop(self, pm):
        while True:
            now = time.time()
            s = wstart(now)
            for a in ASSETS:
                slug = make_slug(a, s)
                c = self.cur.get(a)
                if c and c["slug"] == slug:
                    continue
                m = await gamma_market(slug)
                if m and m["up"] and m["dn"]:
                    self.cur[a] = {"slug": slug, "asset": a, "start": s, "up": m["up"], "dn": m["dn"]}
                    await asyncio.to_thread(db.upsert_window, slug, a, s, m["up"], m["dn"])
                    log.info("window %s", slug)
            pm.set_tokens([t for c in self.cur.values() if c["start"] == s for t in (c["up"], c["dn"])])
            await asyncio.sleep(1)
