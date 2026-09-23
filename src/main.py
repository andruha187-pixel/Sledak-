"""
Точка входа для ПЕРСОНАЛЬНОГО бота под конкретную идею — момент-хедж
стратегию, без основной сигнальной торговли (strategy.py/executor.py
здесь физически нет, это не форк-переключатель, а осознанно урезанный
проект). Работает сразу на двух таймфреймах (5m и 15m по умолчанию) —
на каждую пару (актив, таймфрейм) заведено ДВА независимых цикла:

1. "Медленный" (_instance_loop, раз в timeframe.poll_interval_seconds —
   3-5с): находит активный рынок через Gamma API, подписывает WS-стакан,
   считает индикаторы с Binance для momentum_tracker. Внешние запросы —
   намеренно не чаще этого интервала, иначе рискуем упереться в лимиты
   Gamma/Binance API без реальной пользы.
2. "Быстрый" (_price_watch_loop, раз в HEDGE_POLL_SECONDS — по умолчанию
   1с): читает уже живой WS-стакан (book_stream — обновляется в реальном
   времени независимо от нашего интервала опроса) и проверяет условия
   входа/хеджа для hedge_bot. НЕ делает внешних запросов вообще — только
   память и локальная SQLite, поэтому частить его почти бесплатно.

Раньше это была ОДНА проверка раз в 3-5 секунд, из-за чего цена могла
"проскочить" вход на 0.70 и попасться боту уже на 0.80-0.90 (реальный
случай из отчёта 2026-09-20: средняя цена входа была 0.839 вместо 0.70).
Разделение решает это по-настоящему, а не через допуск HEDGE_ENTRY_TOLERANCE
(тот остаётся как подстраховка на случай совсем резких скачков).

Общие фоновые задачи (не привязаны к конкретному потоку):
- settlement_loop: резолюция хедж-позиций и разметка исходов momentum-
  контрольных точек по всем активам/таймфреймам разом.
- reporting.report_loop: периодический CSV-отчёт (momentum + hedge).
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import market_discovery
from src import polymarket_client, storage, telegram_notify, book_stream, runtime_state, reporting, momentum_tracker, hedge_bot
from src.market_discovery import ActiveMarket
from src.timeframes import TIMEFRAMES, TimeframeProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("hedge-bot")

# Слаг сейчас активного рынка по каждому потоку — общий словарь, читает
# settlement_loop, чтобы не спрашивать Gamma API про рынки, которые
# заведомо ещё не могли зарезолвиться.
_active_slugs: dict[str, str] = {}

# Кэш последнего найденного активного рынка на пару (актив, таймфрейм) —
# читает быстрый _price_watch_loop, чтобы не дёргать Gamma API сам.
# Валиден, пока не истёк market.end_time — обновляется медленным циклом.
_active_markets: dict[str, ActiveMarket] = {}

# Последнее состояние каждого потока — для общего статуса в /menu.
_instance_state: dict[str, dict] = {}


async def _instance_tick(asset: str, timeframe: TimeframeProfile) -> None:
    key = f"{asset}:{timeframe.label}"

    if not runtime_state.is_asset_enabled(asset):
        _instance_state[key] = {
            "market_slug": "—", "asset": asset, "timeframe": timeframe.label,
            "direction": "выкл", "current_price": "—", "strike_price": "—",
            "minutes_left": "—", "safety_score": "—",
            "up_token_id": None, "down_token_id": None,
        }
        telegram_notify.set_state_ref(_instance_state)
        return

    # Рынок на протяжении всего своего окна не меняется (тот же condition_id,
    # токены, время начала/конца) — кэшируем и спрашиваем Gamma API заново
    # только когда окно истекло, а не на каждом тике медленного цикла (было:
    # один и тот же рынок запрашивался каждые 3-5 секунд, до ~100 лишних
    # запросов за один 5-минутный рынок).
    cached = _active_markets.get(key)
    if cached is not None and time.time() < cached.end_time:
        market = cached
    else:
        # Окно истекло (или это первый тик) — отписываемся от токенов
        # СТАРОГО рынка для этого же потока прежде, чем подписаться на новый.
        # Без этого _subscribed растёт неограниченно с каждым новым окном
        # (реальный случай, 2026-09-21: "нет цены в стакане" почти всегда
        # после ~часа работы — вероятно, упёрлись в лимит подписки).
        if cached is not None and settings.USE_LIVE_BOOK_STREAM:
            book_stream.unsubscribe([cached.up_token_id, cached.down_token_id])
        market = await market_discovery.get_active_market(asset, timeframe)
        _active_slugs[key] = market.slug
        _active_markets[key] = market
        if settings.USE_LIVE_BOOK_STREAM:
            book_stream.subscribe([market.up_token_id, market.down_token_id])

    if not runtime_state.get("dry_run"):
        asyncio.create_task(polymarket_client.prewarm_transport())

    minutes_left = max(0.0, (market.end_time - time.time()) / 60)

    _instance_state[key] = {
        "market_slug": market.slug,
        "asset": asset,
        "timeframe": timeframe.label,
        "direction": "—",
        "current_price": round(market.strike_price, 2),
        "strike_price": round(market.strike_price, 2),
        "minutes_left": round(minutes_left, 2),
        "safety_score": "—",
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
    }
    telegram_notify.set_state_ref(_instance_state)

    if settings.MOMENTUM_TRACKER_ENABLED:
        try:
            await momentum_tracker.check_market(market, timeframe)
        except Exception as exc:  # noqa: BLE001 — исследовательский модуль не должен ронять хедж-бота
            log.warning("Ошибка momentum_tracker для %s: %s", market.slug, exc)


async def _instance_loop(asset: str, timeframe: TimeframeProfile) -> None:
    key = f"{asset}:{timeframe.label}"
    consecutive_failures = 0
    notified_dead = False
    while True:
        try:
            await _instance_tick(asset, timeframe)
            consecutive_failures = 0
            notified_dead = False
        except Exception as exc:  # noqa: BLE001 — один сломанный поток не должен ронять остальные
            consecutive_failures += 1
            log.exception("Ошибка в потоке %s (%d подряд): %s", key, consecutive_failures, exc)
            if consecutive_failures == 10 and not notified_dead:
                notified_dead = True
                await telegram_notify.notify(
                    f"⚠️ Поток {key} не может найти рынок уже {consecutive_failures} попыток подряд "
                    f"({exc}). Похоже, этого рынка не существует для данного актива/таймфрейма. "
                    f"Перехожу на редкий опрос (раз в 10 минут), остальные потоки не затронуты."
                )

        sleep_for = timeframe.poll_interval_seconds if consecutive_failures < 10 else 600
        await asyncio.sleep(sleep_for)


async def _price_watch_loop(asset: str, timeframe: TimeframeProfile) -> None:
    """Быстрый цикл только для hedge_bot — без внешних запросов, только
    чтение уже живого WS-стакана и локальной БД, поэтому частить его почти
    бесплатно (в отличие от _instance_loop, который дёргает Gamma/Binance)."""
    key = f"{asset}:{timeframe.label}"
    while True:
        market = _active_markets.get(key)
        if market is not None and runtime_state.is_asset_enabled(asset) and time.time() < market.end_time:
            try:
                await hedge_bot.check_market(market, timeframe)
            except Exception as exc:  # noqa: BLE001
                log.warning("Ошибка hedge_bot (быстрый цикл) для %s: %s", market.slug, exc)
        await asyncio.sleep(settings.HEDGE_POLL_SECONDS)


async def settlement_loop() -> None:
    """Общая (не привязанная к конкретному активу) фоновая задача:
    резолюция хедж-позиций и разметка исходов momentum-точек разом."""
    while True:
        try:
            active = set(_active_slugs.values())
            if settings.MOMENTUM_TRACKER_ENABLED:
                await momentum_tracker.label_resolved(exclude_slugs=active)
                momentum_tracker.cleanup_old_sessions(active)
            await hedge_bot.settle_resolved()
        except Exception as exc:  # noqa: BLE001
            log.exception("Ошибка в settlement_loop: %s", exc)
        await asyncio.sleep(10)


async def main():
    storage.init_db()
    runtime_state.init_from_db()

    forced_back_to_dry_run = False
    if not runtime_state.get("dry_run") and not settings.POLY_PRIVATE_KEY:
        runtime_state.set("dry_run", True)
        forced_back_to_dry_run = True

    app = telegram_notify.build_app()
    async with app:
        await app.start()
        await app.updater.start_polling()

        await telegram_notify.clear_legacy_keyboard()
        dry_run = runtime_state.get("dry_run")
        assets_line = ", ".join(a.upper() for a in settings.ASSETS)
        timeframes_line = ", ".join(tf.label for tf in TIMEFRAMES)
        forced_note = (
            "\n⚠️ Был сохранён LIVE-режим с прошлого раза, но POLY_PRIVATE_KEY сейчас не задан — "
            "принудительно откатил в DRY RUN."
            if forced_back_to_dry_run else ""
        )
        await telegram_notify.notify(
            f"🔒 Хедж-бот запущен (персональный, момент-хедж идея, БЕЗ основной сигнальной стратегии).\n"
            f"Режим: {'DRY RUN' if dry_run else 'LIVE — реальные сделки!'}\n"
            f"Активы: {assets_line}\nТаймфреймы: {timeframes_line}\n"
            f"Открой /menu для управления."
            f"{forced_note}"
        )

        book_stream_task = None
        if settings.USE_LIVE_BOOK_STREAM:
            book_stream_task = asyncio.create_task(book_stream.run_forever())

        report_task = asyncio.create_task(reporting.report_loop())
        settlement_task = asyncio.create_task(settlement_loop())

        instance_tasks = [
            asyncio.create_task(_instance_loop(asset, timeframe))
            for asset in settings.ASSETS
            for timeframe in TIMEFRAMES
        ]
        price_watch_tasks = [
            asyncio.create_task(_price_watch_loop(asset, timeframe))
            for asset in settings.ASSETS
            for timeframe in TIMEFRAMES
        ]

        try:
            await asyncio.gather(*instance_tasks, *price_watch_tasks)
        finally:
            if book_stream_task:
                book_stream_task.cancel()
            report_task.cancel()
            settlement_task.cancel()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
