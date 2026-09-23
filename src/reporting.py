"""
Периодический отчёт для этого персонального бота — раз в
REPORT_INTERVAL_HOURS формирует CSV-файлы из накопленных данных и шлёт
их в Telegram. Только momentum + hedge — у этого бота нет основной
сигнальной стратегии, поэтому таблицы signals/trades всегда пусты
(намеренно, не баг) и в отчёт не включаются.

Момент последнего отчёта хранится в bot_settings (переживает рестарт) —
чтобы при перезапуске не задваивать период и не терять данные между ним.
"""
from __future__ import annotations
import asyncio
import csv
import os
import time

from config import settings
from src import storage, telegram_notify, hedge_bot

_LAST_REPORT_KEY = "last_report_ts"


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


def _get_last_report_ts() -> int:
    saved = storage.get_all_settings().get(_LAST_REPORT_KEY)
    try:
        return int(saved)
    except (TypeError, ValueError):
        return int(time.time() - settings.REPORT_INTERVAL_HOURS * 3600)


def _set_last_report_ts(ts: int) -> None:
    storage.set_setting(_LAST_REPORT_KEY, ts)


async def build_and_send_report() -> None:
    since_ts = _get_last_report_ts()
    now_ts = int(time.time())

    momentum = storage.get_momentum_since(since_ts) if settings.MOMENTUM_TRACKER_ENABLED else []
    hedge_positions = storage.get_hedge_positions_since(since_ts)
    skip_counts = hedge_bot.get_and_reset_skip_counts()

    if not momentum and not hedge_positions and not skip_counts:
        _set_last_report_ts(now_ts)
        return

    from_label = time.strftime("%Y%m%d-%H%M", time.gmtime(since_ts))
    to_label = time.strftime("%Y%m%d-%H%M", time.gmtime(now_ts))
    base = os.path.join(settings.REPORTS_DIR, f"{from_label}_to_{to_label}")

    if momentum:
        momentum_path = f"{base}_momentum.csv"
        _write_csv(momentum_path, storage.MOMENTUM_COLUMNS, momentum)
        reached_by_cp: dict[float, int] = {}
        for row in momentum:
            cp = row[storage.MOMENTUM_COLUMNS.index("checkpoint_price")]
            reached_by_cp[cp] = reached_by_cp.get(cp, 0) + 1
        cp_lines = "\n".join(f"  {cp:.2f}: {n} раз" for cp, n in sorted(reached_by_cp.items()))
        momentum_caption = (
            f"🔬 Momentum-отчёт {from_label} → {to_label}\n"
            f"Контрольных точек зафиксировано: {len(momentum)}\n\n"
            f"По уровням:\n{cp_lines}"
        )
        await telegram_notify.send_document(momentum_path, momentum_caption)

    if hedge_positions:
        hedge_path = f"{base}_hedge.csv"
        _write_csv(hedge_path, storage.HEDGE_COLUMNS, hedge_positions)
        closed = [row for row in hedge_positions if row[storage.HEDGE_COLUMNS.index("status")] == "closed"]
        hedged_count = sum(1 for row in closed if row[storage.HEDGE_COLUMNS.index("hedge_price")] is not None)
        pnl_sum = sum(row[storage.HEDGE_COLUMNS.index("pnl_usdc")] or 0 for row in closed)
        hedge_caption = (
            f"🔒 Хедж-бот {from_label} → {to_label}\n"
            f"Позиций закрыто: {len(closed)} (с прибылью продано: {hedged_count}, "
            f"держали до резолюции: {len(closed) - hedged_count}) | PnL: {pnl_sum:+.2f} USDC (без учёта комиссии)"
        )
        await telegram_notify.send_document(hedge_path, hedge_caption)

    # Причины пропуска каждого рынка — счётчики накапливались в памяти
    # (hedge_bot._skip_counts), не на каждый тик в БД (иначе вернули бы
    # проблему со "slow consumer" из-за блокировки event loop). Показывает,
    # почему конкретный актив/таймфрейм не входил: ждёт цену, упёрся в
    # лимит, проскочил окно и т.д. — то, чего раньше не было видно вообще.
    if skip_counts:
        skip_rows = []
        for (asset, timeframe_label), reasons in sorted(skip_counts.items()):
            for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
                skip_rows.append((asset, timeframe_label, hedge_bot.SKIP_REASON_LABELS.get(reason, reason), count))
        skip_path = f"{base}_skip_reasons.csv"
        _write_csv(skip_path, ["asset", "timeframe", "reason", "count"], skip_rows)

        # В подпись — только самое частое НЕ-тривиальное по каждому активу/
        # таймфрейму (просто "ждём цену входа" не показываем — это норма,
        # не диагностически интересно; интересны лимиты/пропуски/сработки).
        interesting = {"missed_entry_window", "daily_loss_limit", "max_open_positions",
                       "hedge_leg_too_small", "no_price", "entered", "hedged", "already_touched_other_side",
                       "outside_entry_time_window", "timeframe_disabled"}
        lines = []
        for (asset, timeframe_label), reasons in sorted(skip_counts.items()):
            notable = {r: c for r, c in reasons.items() if r in interesting and c > 0}
            if notable:
                parts = ", ".join(f"{hedge_bot.SKIP_REASON_LABELS[r]}: {c}" for r, c in
                                   sorted(notable.items(), key=lambda x: -x[1]))
                lines.append(f"  {asset.upper()} {timeframe_label}: {parts}")
        skip_caption = (
            f"📋 Причины (не)входа {from_label} → {to_label}\n" +
            ("\n".join(lines) if lines else "  Ничего примечательного — либо везде тихо ждём цену, либо не было тиков.")
        )
        await telegram_notify.send_document(skip_path, skip_caption)

    _set_last_report_ts(now_ts)


async def report_loop() -> None:
    while True:
        try:
            await build_and_send_report()
        except Exception:
            pass
        await asyncio.sleep(max(60, settings.REPORT_INTERVAL_HOURS * 3600))
