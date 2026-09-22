"""Выкачивает ВСЮ историю кошелька с BACKFILL_SINCE и восстанавливает рыночный контекст
каждой сделки по 1-секундным свечам Binance + истории цен Polymarket.
Плюс «контрольные» точки: те же окна каждые HIST_CONTROL_STEP сек и окна, где он НЕ торговал.
Возобновляется после рестарта (прогресс в meta)."""
import asyncio
import logging
import random
import time
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone

import db
from config import (ASSETS, BACKFILL_CONCURRENCY, BACKFILL_MAX_WINDOWS, BACKFILL_NOTRADE_WINDOWS,
                    BACKFILL_SINCE, BINANCE_REST, BINANCE_SYM, DATA_API, DECISION_LAG_SEC,
                    HIST_CONTROL_STEP, ROLE_BACKFILL_LAST, WALLET)
from features import fair_block, flow_block, hour_of, price_before, ta_all
from markets import gamma_market, make_slug, parse_slug, prices_history
from net import get_json
from snapshot import parse_klines
from wallet_watch import activity_to_row, role_backfill

log = logging.getLogger("backfill")
OFFSET_CAP = 2500


async def fetch_range(a, b):
    out, off = [], 0
    while True:
        d = await get_json(f"{DATA_API}/activity", {"user": WALLET, "limit": 500, "offset": off, "start": a,
                                                     "end": b, "sortBy": "TIMESTAMP", "sortDirection": "ASC"},
                           quiet=True)
        if d is None and off == 0:
            await asyncio.sleep(3)
            d = await get_json(f"{DATA_API}/activity", {"user": WALLET, "limit": 500, "offset": 0, "start": a,
                                                         "end": b, "sortBy": "TIMESTAMP", "sortDirection": "ASC"})
            if d is None:
                log.warning("activity %s-%s failed", a, b)
                return out
        if d is None or (off + 500 > OFFSET_CAP and len(d) == 500):
            if b - a <= 30:
                return out + (d or [])
            mid = (a + b) // 2
            return await fetch_range(a, mid) + await fetch_range(mid + 1, b)
        out += d
        if len(d) < 500:
            return out
        off += 500


async def bulk_klines(asset, interval, start, end):
    out, t = [], start * 1000
    step = {"1m": 60, "1s": 1}[interval]
    while t < end * 1000:
        raw = await get_json(f"{BINANCE_REST}/api/v3/klines",
                             {"symbol": BINANCE_SYM[asset], "interval": interval, "startTime": t,
                              "endTime": end * 1000, "limit": 1000}, quiet=True)
        if not raw:
            break
        out += raw
        t = int(raw[-1][0]) + step * 1000
        if len(raw) < 1000:
            break
    return out


def buckets_from_klines(raw):
    """1s-свечи → корзины (sec, close, buy_usd, sell_usd, n) — тот же формат, что в живом потоке."""
    out = []
    for k in raw:
        q = float(k[7])
        tb = float(k[10])
        out.append((int(k[0]) // 1000, float(k[4]), tb, max(q - tb, 0.0), int(k[8])))
    return out


def hist_price(h, t):
    i = bisect_right(h, (t, 9e9)) - 1
    return h[i][1] if i >= 0 else None


def hist_feat(start, t, b, k1t, k1, h_up, h_dn, res):
    sec_left = start + 900 - t
    f = {"src": "hist", "res": res, "sec_into": t - start, "sec_left": sec_left, "hour": hour_of(t)}
    fl, px = flow_block(b, t)
    f.update(fl)
    f["bn_px"] = px
    op = price_before(b, start + 1)
    f["open_px"], f["ref"] = op, "bn"
    i = bisect_right(k1t, t - 60)
    ta, sigma = ta_all(k1[max(0, i - 800):i])
    f.update(ta)
    f.update(fair_block(px, op, sigma, sec_left))
    f["pmh_up"] = hist_price(h_up, t)
    f["pmh_dn"] = hist_price(h_dn, t)
    return f


async def process_window(slug, k1t, k1, traded):
    asset, start = parse_slug(slug)
    gm = await gamma_market(slug)
    if not gm or not gm["up"]:
        db.execute("UPDATE windows SET hist_done=2 WHERE slug=?", (slug,))
        return
    db.upsert_window(slug, asset, start, gm["up"], gm["dn"], gm["winner"])
    raw = await bulk_klines(asset, "1s", start - 200, start + 900)
    res = "1s"
    if len(raw) < 300:  # 1s недоступны — грубый фоллбек на 1m
        res = "1m"
        i0, i1 = bisect_left(k1t, start - 600), bisect_left(k1t, start + 900)
        b = [(t + 59, c, v * c / 2, v * c / 2, 0) for t, o, h, l, c, v in k1[i0:i1]]
    else:
        b = buckets_from_klines(raw)
    h_up = await prices_history(gm["up"], start - 120, start + 960)
    h_dn = await prices_history(gm["dn"], start - 120, start + 960)
    # CPU и SQLite — в потоке, чтобы не тормозить живые WS
    await asyncio.to_thread(_compute_store, slug, asset, start, b, k1t, k1, h_up, h_dn, res, traded)


def _compute_store(slug, asset, start, b, k1t, k1, h_up, h_dn, res, traded):
    if traded:
        rows = db.query("SELECT key, ts FROM trades WHERE slug=? AND type='TRADE' AND source='backfill'", (slug,))
        upd = [(_js(hist_feat(start, ts - DECISION_LAG_SEC, b, k1t, k1, h_up, h_dn, res)), key) for key, ts in rows]
        db.executemany("UPDATE trades SET feat=? WHERE key=?", upd)
    snaps = []
    for t in range(start + 10, start + 900, HIST_CONTROL_STEP):
        f = hist_feat(start, t, b, k1t, k1, h_up, h_dn, res)
        f["traded_window"] = int(traded)
        snaps.append((t, asset, slug, "hist", f))
    db.insert_snapshots(snaps)
    db.execute("UPDATE windows SET hist_done=1 WHERE slug=?", (slug,))


def _js(f):
    import json
    return json.dumps(f, separators=(",", ":"), default=float)


async def run_backfill(tg):
    since = int(datetime.strptime(BACKFILL_SINCE, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    now = int(time.time())
    # 1) события кошелька
    day = int(db.meta_get("bf_until") or since)
    if day < now - 3600:
        await tg.send(f"⏳ Качаю историю кошелька с {BACKFILL_SINCE}…")
        total = 0
        while day < now:
            items = await fetch_range(day, min(day + 86399, now))
            db.insert_trades([activity_to_row(it, "backfill") for it in items])
            total += len(items)
            day += 86400
            db.meta_set("bf_until", day)
        n = db.query("SELECT COUNT(*) FROM trades")[0][0]
        await tg.send(f"✅ История событий загружена: {n} записей (+{total} за этот проход)")
    # 2) окна
    rows = db.query("SELECT DISTINCT slug FROM trades WHERE slug LIKE '%-updown-15m-%'")
    traded = set()
    for (slug,) in rows:
        ps = parse_slug(slug)
        if ps and ps[0] in ASSETS:
            traded.add(slug)
            db.upsert_window(slug, ps[0], ps[1])
    todo_t = [s for (s,) in db.query("SELECT slug FROM windows WHERE hist_done=0 ORDER BY start DESC")
              if s in traded][:BACKFILL_MAX_WINDOWS]
    # окна без его сделок — случайная выборка из того же периода
    nt = []
    if db.meta_get("bf_nt_seeded") != "1" and traded:
        starts = [parse_slug(s)[1] for s in traded]
        lo, hi = min(starts), max(starts)
        pool = [make_slug(a, t) for a in ASSETS for t in range(lo, hi + 1, 900) if make_slug(a, t) not in traded]
        random.seed(1)
        nt = random.sample(pool, min(BACKFILL_NOTRADE_WINDOWS, len(pool)))
        for s in nt:
            a, t = parse_slug(s)
            db.upsert_window(s, a, t)
        db.meta_set("bf_nt_seeded", "1")
    todo_nt = [s for (s,) in db.query("SELECT slug FROM windows WHERE hist_done=0") if s not in traded]
    todo_nt = todo_nt[:BACKFILL_NOTRADE_WINDOWS]
    todo = [(s, True) for s in todo_t] + [(s, False) for s in todo_nt]
    if not todo:
        await role_backfill(ROLE_BACKFILL_LAST)
        return
    await tg.send(f"⏳ Восстанавливаю рыночный контекст: {len(todo_t)} окон с его сделками + "
                  f"{len(todo_nt)} контрольных. Это долго (десятки минут), бот при этом уже следит вживую.")
    # 3) 1m свечи оптом для ТА
    k1d = {}
    for a in ASSETS:
        k1d[a] = parse_klines(await bulk_klines(a, "1m", since - 86400, now))
    k1t = {a: [c[0] for c in k1d[a]] for a in ASSETS}
    sem = asyncio.Semaphore(BACKFILL_CONCURRENCY)
    done = 0

    async def one(slug, tr):
        nonlocal done
        async with sem:
            a = parse_slug(slug)[0]
            try:
                await process_window(slug, k1t[a], k1d[a], tr)
            except Exception as e:
                log.warning("window %s: %s", slug, e)
            done += 1
            if done % 500 == 0:
                await tg.send(f"… контекст: {done}/{len(todo)} окон")

    for i in range(0, len(todo), 100):
        await asyncio.gather(*(one(s, tr) for s, tr in todo[i:i + 100]))
    await role_backfill(ROLE_BACKFILL_LAST)
    db.meta_set("bf_done_ts", int(time.time()))
    await tg.send("✅ Исторический контекст готов. Жми /report — первый полный разбор стратегии.")
