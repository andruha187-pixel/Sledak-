import asyncio
import logging
import time

import analyze
import db
from backfill import run_backfill
from config import ASSETS, REPORT_HOURS, WALLET
from markets import MarketCache
from snapshot import Rest, Snapshotter
from streams import BinanceStream, Chainlink, PMBook
from telegram import TG
from wallet_watch import WalletWatch, resolver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("main")


async def main():
    tg = TG()
    markets, pm, bn, cl, rest = MarketCache(), PMBook(), BinanceStream(), Chainlink(), Rest()
    snap = Snapshotter(markets, pm, bn, cl, rest)
    ww = WalletWatch(snap, tg)
    report_lock = asyncio.Lock()
    state = {"next_report": time.time() + REPORT_HOURS * 3600}

    async def do_report():
        if report_lock.locked():
            await tg.send("Отчёт уже собирается…")
            return
        async with report_lock:
            await tg.send("🧮 Собираю отчёт…")
            try:
                path, caption, _ = await asyncio.to_thread(analyze.build_report)  # CPU — не в event loop
                await tg.send_file(path, caption)
            except Exception as e:
                log.exception("report")
                await tg.send(f"⚠️ Ошибка отчёта: {e}")
            state["next_report"] = time.time() + REPORT_HOURS * 3600

    async def reporter():
        while True:
            await asyncio.sleep(30)
            if time.time() >= state["next_report"]:
                await do_report()

    async def status():
        now = time.time()
        n_tr = db.query("SELECT COUNT(*) FROM trades")[0][0]
        n_live = db.query("SELECT COUNT(*) FROM trades WHERE source='live'")[0][0]
        n_sn = db.query("SELECT src, COUNT(*) FROM snapshots GROUP BY src")
        hist = db.query("SELECT hist_done, COUNT(*) FROM windows GROUP BY hist_done")
        age = lambda t: f"{now - t:.0f}с назад" if t else "нет"
        last = ww.last_trade_ts
        await tg.send(
            f"📊 Статус\nКошелёк: {WALLET}\nАктивы: {', '.join(ASSETS)}\n"
            f"Событий в БД: {n_tr} (живых с контекстом: {n_live})\nСнимков: {dict(n_sn)}\n"
            f"Окна (0=ждёт,1=готово,2=нет данных): {dict(hist)}\n"
            f"Последняя его сделка: {age(last)}\n"
            f"Потоки: Binance {age(bn.last_msg)}, Chainlink {age(cl.last_msg)}, стакан PM {age(pm.last_msg)}\n"
            f"Окна: {', '.join(m['slug'] for m in markets.cur.values())}\n"
            f"Следующий отчёт через {(state['next_report'] - now) / 60:.0f} мин")

    async def last10():
        rows = db.query("SELECT ts, side, outcome, slug, price, size, usdc, role FROM trades "
                        "WHERE type='TRADE' ORDER BY ts DESC LIMIT 10")
        txt = "\n".join(f"{time.strftime('%H:%M:%S', time.gmtime(r[0]))} {r[1]} {r[2]} {r[3]} "
                        f"@{r[4]} ×{r[5]} ${r[6] or 0:.2f} {r[7] or ''}" for r in rows)
        await tg.send("Последние 10 филлов (UTC):\n" + (txt or "нет"))

    async def backfill_task():
        await asyncio.sleep(10)
        try:
            await run_backfill(tg)
        except Exception as e:
            log.exception("backfill")
            await tg.send(f"⚠️ Backfill упал: {e}. Перезапуск продолжит с того же места.")

    await tg.send(f"🔎 Профайлер запущен\nЦель: {WALLET}\nАктивы: {', '.join(ASSETS)}\n"
                  f"Команды: /report /status /last")
    await asyncio.gather(
        bn.run(), cl.run(), pm.run(), rest.run(), markets.loop(pm), snap.loop(), ww.run(),
        resolver(cl), reporter(), backfill_task(),
        tg.commands({"/report": do_report, "/status": status, "/last": last10}),
    )


if __name__ == "__main__":
    asyncio.run(main())
