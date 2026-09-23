"""
Take-profit стратегия (переход с хеджа на продажу — 2026-09-22):
1. Как только цена стороны рынка впервые достигает HEDGE_ENTRY_PRICE
   (по умолчанию 0.70) — покупаем эту сторону.
2. Если цена продолжает расти и достигает HEDGE_TRIGGER_PRICE (0.90) до
   конца окна — ПРОДАЁМ уже купленную позицию (тот же токен) по текущей
   рыночной цене, фиксируя прибыль. Позиция закрывается сразу, ждать
   резолюции рынка не нужно.
3. Если цена НЕ доходит до HEDGE_TRIGGER_PRICE — остаёмся с открытой
   позицией до резолюции рынка. Это не мелочь: по нашим данным именно
   такие случаи почти гарантированно проигрывают (~4.5% винрейт) —
   учитывай это в размере ставки.

Раньше (до 2026-09-22) на шаге 2 докупалась ПРОТИВОПОЛОЖНАЯ сторона в
таком же количестве акций (хедж) — математически это даёт РОВНО ТУ ЖЕ
прибыль (проверено на 1126 momentum-сессиях, числа идентичны с точностью
до цента, включая комиссию — она тоже симметрична: price*(1-price) не
меняется от замены p на 1-p). Перешли на продажу по двум причинам:
- Физически исключает целый класс багов с перепутанным токеном —
  продать можно только то, что реально держишь, перепутать нечего
  (реальный случай: у HYPE иногда up_token_id совпадал с down_token_id,
  и "хедж" покупал ту же сторону повторно — реальные убытки, 2026-09-21).
- Проще: не нужно отслеживать вторую ногу, считать "равные акции",
  ждать резолюции для финального расчёта — прибыль известна сразу же
  при продаже.

Порог 0.90 (не 0.70-0.85) выбран по факту бэктеста на реальных отчётах:
чем позже фиксируешь прибыль, тем больше гарантированная маржа с каждой
успешной сделки, и это перевешивает потери от возросшего числа случаев,
где цена вообще не доходит до порога. См. обсуждение в чате от
2026-09-20 — на четырёх отчётах (366 сессий) порог 0.90 дал +$29.45, на
0.70-0.85 — убыток, несмотря на то что зафиксированная сделка каждый раз
безубыточна по построению.

ВАЖНО про комиссию: pnl_usdc здесь считается БЕЗ вычета комиссии тейкера
(7% для крипторынков на КАЖДУЮ ногу — вход и продажа отдельно, хотя
эффективная комиссия от суммы сделки заметно ниже 7% у краёв диапазона
цены — ближе к 2% на входе ~0.70, ~0.7% на выходе ~0.90; ставка 7% —
это коэффициент в формуле fee=shares×0.07×price×(1-price), а не доля от
суммы сделки напрямую). Реальный итог на кошельке будет чуть хуже, чем
показывают отчёты. См. обсуждение комиссий в README.
"""
from __future__ import annotations
import logging
import time

from config import settings
from src import book_stream, market_discovery, polymarket_client, runtime_state, storage, telegram_notify
from src.market_discovery import ActiveMarket
from src.timeframes import TimeframeProfile

log = logging.getLogger("hedge_bot")

_last_warned_config: tuple | None = None  # чтобы не спамить одним и тем же предупреждением каждый тик

# (market_slug, side) -> unix-время последней неудачной попытки продажи —
# защита от бесконечного ретрая каждую секунду при повторяющейся ошибке.
_last_sell_failure: dict[tuple[str, str], float] = {}

# То же самое, но для входа — реальный случай, 2026-09-22: FOK-ордер на
# HYPE не мог исполниться целиком (недостаточно ликвидности по нужной
# цене), а позиция так и не создавалась (ордер падал ДО записи в
# _open_positions/_touched_markets) — значит на следующем тике условие
# входа снова выполнялось, и бот пытался заново каждую секунду, спамя
# одной и той же ошибкой.
_last_entry_failure: dict[tuple[str, str], float] = {}

# Зеркало открытых позиций В ПАМЯТИ — (market_slug, side) -> dict с полями
# position_id/entry_shares/entry_cost/status. Быстрый цикл (check_market,
# раз в HEDGE_POLL_SECONDS x 12 потоков) читает ТОЛЬКО это, ни разу не
# трогая SQLite — иначе синхронные (блокирующие) вызовы к диску на каждом
# тике останавливают весь event loop, включая чтение WS-сокета, и сервер
# отключает нас как "slow consumer" (реальный случай, 2026-09-21). SQLite
# остаётся источником истины и пишется при каждом реальном изменении
# состояния (вход/хедж/резолюция), просто не читается на каждый тик.
_open_positions: dict[tuple[str, str], dict] = {}
_positions_loaded = False

# Рынки, в которые уже входили ХОТЯ БЫ ОДНОЙ стороной — включая уже
# закрытые позиции, не только открытые. Без этого бот мог входить в ОБЕ
# стороны одного и того же рынка в разное время (реальный случай,
# 2026-09-21: DOWN по 0.71 проиграл -$10, позже туда же UP по 0.71-0.73
# выиграл +$2 — но по сумме всё равно в минус, а самое главное, это
# бессмысленно: исход у рынка один, вторая ставка гарантированно не может
# и выиграть, и не быть при этом просто дублирующим риском). Растёт на
# протяжении жизни процесса, но это лёгкие строки-слаги, для практических
# масштабов работы бота (дни-недели) не проблема.
_touched_markets: set[str] = set()

# Счётчики причин пропуска в памяти — (asset, timeframe) -> {причина: счёт}.
# НЕ пишутся в БД на каждый тик (это и вызвало проблему со "slow consumer"
# в прошлый раз) — только читаются и сбрасываются раз в REPORT_INTERVAL_HOURS
# из reporting.py, чтобы попасть в 4-часовой отчёт.
_skip_counts: dict[tuple[str, str], dict[str, int]] = {}

SKIP_REASON_LABELS = {
    "no_price": "нет цены в стакане",
    "waiting_for_entry": "ждём цену входа",
    "missed_entry_window": "цена проскочила мимо входа",
    "daily_loss_limit": "дневной лимит убытка",
    "max_open_positions": "потолок открытых позиций",
    "hedge_leg_too_small": "нога хеджа меньше минимума ордера",
    "waiting_for_hedge": "ждём цену хеджа",
    "already_touched_other_side": "уже входили в этот рынок другой стороной",
    "timeframe_disabled": "таймфрейм выключен для входов",
    "outside_entry_time_window": "вне окна входа по времени",
    "entered": "вход выполнен",
    "hedged": "хедж выполнен",
}


def _count(asset: str, timeframe_label: str, reason: str) -> None:
    key = (asset, timeframe_label)
    bucket = _skip_counts.setdefault(key, {})
    bucket[reason] = bucket.get(reason, 0) + 1


def get_and_reset_skip_counts() -> dict[tuple[str, str], dict[str, int]]:
    """Вызывается из reporting.py раз в REPORT_INTERVAL_HOURS — забирает
    накопленное и обнуляет счётчики для следующего периода."""
    global _skip_counts
    snapshot = _skip_counts
    _skip_counts = {}
    return snapshot


def _load_open_positions_from_db() -> None:
    """Разово при старте (и один раз после падения) — восстанавливаем
    зеркало из БД на случай, если бот перезапустился с открытыми позициями."""
    global _positions_loaded
    for pos_id, market_slug, side, entry_shares, entry_cost, hedge_shares, hedge_cost, status, dry_run in \
            storage.get_unsettled_hedge_positions():
        _open_positions[(market_slug, side)] = {
            "position_id": pos_id, "entry_shares": entry_shares, "entry_cost": entry_cost, "status": status,
            # После рестарта историю "минимума с начала жизни позиции" мы
            # теряем — берём цену входа как отправную точку (пессимистично
            # недооценит просадку, если она уже была ДО рестарта, это
            # известное и приемлемое ограничение исследовательского учёта).
            "min_price_seen": entry_cost / entry_shares if entry_shares else None,
        }
        _touched_markets.add(market_slug)
    _positions_loaded = True


def _daily_loss_exceeded() -> bool:
    """Полночь UTC — тот же принцип, что и у основного бота: разово в
    сутки сбрасывается счётчик, чтобы не копить убыток бесконечно."""
    midnight_ts = int(time.time() // 86400) * 86400
    pnl_today = storage.get_hedge_pnl_since(midnight_ts)
    return pnl_today <= -abs(runtime_state.get("daily_loss_limit_usdc"))


async def _execute_entry(market: ActiveMarket, side: str, token_id: str, price_hint: float,
                          timeframe: TimeframeProfile) -> None:
    key = (market.slug, side)
    last_fail = _last_entry_failure.get(key)
    if last_fail is not None and time.time() - last_fail < 15:
        return

    dry_run = runtime_state.get("dry_run")
    stake = runtime_state.get("hedge_stake_usdc")

    book = await polymarket_client.get_orderbook_cached(token_id, depth_levels=10)
    available = book.ask_liquidity_usdc
    if available < settings.MIN_VIABLE_TRADE_USDC:
        return
    if available < stake:
        stake = round(available * 0.9, 2)

    tick = book.tick_size or book_stream.tick_size(token_id)
    reference_price = book.best_ask or price_hint
    if reference_price is None:
        return
    price_cap = polymarket_client.round_price_for_buy(
        min(reference_price + settings.LIVE_ENTRY_MAX_SLIPPAGE, 0.99), tick,
    )

    order_id = "dry-run"
    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            await telegram_notify.notify("❌ Хедж-бот: LIVE включён, но POLY_PRIVATE_KEY не задан — вход пропущен.")
            return
        try:
            resp = await polymarket_client.place_buy_order(token_id, price_cap, stake, tick)
        except Exception as exc:  # noqa: BLE001
            _last_entry_failure[key] = time.time()
            await telegram_notify.notify(f"❌ Хедж-бот: ошибка входа ({market.slug}): {exc}\nПовторю через 15с.")
            return
        order_id = polymarket_client.response_field(resp, "order_id") or str(resp)

    _last_entry_failure.pop(key, None)
    shares = stake / price_cap
    position_id = storage.create_hedge_position(
        market.slug, market.asset, timeframe.label, side, price_cap, shares, stake, token_id, dry_run,
    )
    _open_positions[(market.slug, side)] = {
        "position_id": position_id, "entry_shares": shares, "entry_cost": stake, "status": "open_unhedged",
        "min_price_seen": price_cap,  # исследовательское наблюдение — не торгуем на этом, только считаем
    }
    _touched_markets.add(market.slug)
    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else ''}🔷 Хедж-бот: вход {side} по {market.slug}\n"
        f"Цена: {price_cap:.3f} | Размер: {stake:.2f} USDC | "
        + (f"продажа при {runtime_state.get('hedge_trigger_price'):.2f}"
           if runtime_state.get("hedge_take_profit_enabled") else "держим до резолюции")
    )


async def _execute_take_profit(market: ActiveMarket, side: str, position_id: int, entry_shares: float,
                                token_id: str) -> None:
    """
    Продаём УЖЕ КУПЛЕННУЮ позицию (тот же token_id, что при входе) по текущей
    рыночной цене — не докупаем противоположную сторону. Математически это
    даёт РОВНО ТУ ЖЕ прибыль, что и хедж (проверено на 1126 momentum-сессиях
    — числа идентичны), но проще и физически исключает целый класс багов
    (перепутанный токен противоположной стороны — реальный случай с HYPE,
    2026-09-21): продать можно только то, что реально держишь, перепутать
    нечего. Плюс позиция закрывается СРАЗУ, не нужно ждать резолюции рынка.
    """
    dry_run = runtime_state.get("dry_run")

    # Если недавно уже не удалось продать эту позицию — не долбим API и не
    # спамим одной и той же ошибкой каждую секунду, даём паузу перед
    # повторной попыткой (реальный случай, 2026-09-21: округление цены до
    # 3 знаков вместо 2 роняло ордер на LIVE, и бот ретраил его каждую
    # секунду бесконечно — сотни одинаковых сообщений об ошибке).
    key = (market.slug, side)
    last_fail = _last_sell_failure.get(key)
    if last_fail is not None and time.time() - last_fail < 15:
        return

    book = await polymarket_client.get_orderbook_cached(token_id, depth_levels=10)
    ref_price = book.best_bid
    if ref_price is None:
        return  # нет биды прямо сейчас (некому продать) — попробуем на следующем тике

    tick = book.tick_size or book_stream.tick_size(token_id)
    # Защита от проскальзывания на продаже — не продаём дешевле этой цены.
    # Выравниваем по шагу тика ВНИЗ (округление до N знаков без привязки к
    # тику — та самая причина бага выше: Polymarket требует цену, кратную
    # tick_size, не просто "не больше 2 знаков после запятой").
    min_price = polymarket_client.round_price_for_buy(
        max(ref_price - settings.HEDGE_LEG_MAX_SLIPPAGE, 0.01), tick,
    )

    order_id = "dry-run"
    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            await telegram_notify.notify("❌ Хедж-бот: LIVE включён, но POLY_PRIVATE_KEY не задан — продажа пропущена.")
            return
        try:
            resp = await polymarket_client.place_sell_order(token_id, entry_shares, min_price)
        except Exception as exc:  # noqa: BLE001
            _last_sell_failure[key] = time.time()
            await telegram_notify.notify(f"❌ Хедж-бот: ошибка продажи ({market.slug}): {exc}\nПовторю через 15с.")
            return
        order_id = polymarket_client.response_field(resp, "order_id") or str(resp)

    _last_sell_failure.pop(key, None)
    proceeds = entry_shares * ref_price
    pos_state = _open_positions.get((market.slug, side), {})
    entry_cost = pos_state.get("entry_cost", 0.0)
    min_price_seen = pos_state.get("min_price_seen")
    pnl = proceeds - entry_cost

    storage.mark_hedged(position_id, ref_price, entry_shares, proceeds, token_id)
    storage.settle_hedge_position(position_id, "SOLD", pnl, min_price_seen)
    _open_positions.pop((market.slug, side), None)  # закрыта сразу — убираем из зеркала

    emoji = "🟢" if pnl > 0 else "🔴"
    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else ''}{emoji} Хедж-бот: закрыта позиция {market.slug} ({side})\n"
        f"Продали по {ref_price:.3f}, выручка {proceeds:.2f} USDC (вход был {entry_cost:.2f}) — "
        f"PnL: {pnl:+.2f} USDC, зафиксировано, ждать резолюции не нужно."
    )


async def check_market(market: ActiveMarket, timeframe: TimeframeProfile) -> None:
    global _last_warned_config, _positions_loaded
    if not runtime_state.get("hedge_bot_enabled"):
        return

    if not _positions_loaded:
        _load_open_positions_from_db()  # разово при первом тике после старта

    entry_price = runtime_state.get("hedge_entry_price")
    take_profit_price = runtime_state.get("hedge_trigger_price")
    take_profit_on = runtime_state.get("hedge_take_profit_enabled")
    stake = runtime_state.get("hedge_stake_usdc")

    # Разрешены ли НОВЫЕ входы на этом таймфрейме и в этот момент окна.
    # Уже открытые позиции продолжают сопровождаться в любом случае.
    allowed_tfs = {x.strip() for x in str(runtime_state.get("hedge_timeframes")).split(",") if x.strip()}
    window_len = max(1, market.end_time - market.start_time)
    elapsed_frac = (time.time() - market.start_time) / window_len
    tf_allowed = timeframe.label in allowed_tfs
    time_allowed = (runtime_state.get("hedge_entry_min_elapsed") <= elapsed_frac
                    <= runtime_state.get("hedge_entry_max_elapsed"))

    sides = [
        ("UP", market.up_token_id, market.down_token_id),
        ("DOWN", market.down_token_id, market.up_token_id),
    ]
    for side, token_id, opposite_token_id in sides:
        price = book_stream.best_ask(token_id)
        if price is None:
            _count(market.asset, timeframe.label, "no_price")
            continue

        # Читаем ТОЛЬКО зеркало в памяти — ни одного обращения к SQLite на
        # этом (горячем, раз в секунду x 12 потоков) пути.
        existing = _open_positions.get((market.slug, side))
        if existing is None:
            if market.slug in _touched_markets:
                # Уже входили в этот рынок другой стороной (возможно, уже
                # закрытой) — вторая сторона того же рынка гарантированно
                # конфликтует с первой (исход один), не входим ещё раз.
                _count(market.asset, timeframe.label, "already_touched_other_side")
                continue
            entry_tolerance = runtime_state.get("hedge_entry_tolerance")
            if entry_price <= price <= entry_price + entry_tolerance:
                if not tf_allowed:
                    _count(market.asset, timeframe.label, "timeframe_disabled")
                    continue
                if not time_allowed:
                    _count(market.asset, timeframe.label, "outside_entry_time_window")
                    continue
                if _daily_loss_exceeded():
                    _count(market.asset, timeframe.label, "daily_loss_limit")
                    continue  # дневной лимит убытка сработал — новых входов не открываем
                if len(_open_positions) >= runtime_state.get("max_open_positions"):
                    _count(market.asset, timeframe.label, "max_open_positions")
                    continue  # общий потолок одновременно открытых позиций
                _count(market.asset, timeframe.label, "entered")
                await _execute_entry(market, side, token_id, price, timeframe)
            elif price > entry_price + entry_tolerance:
                # Цена уже проскочила мимо входа за один тик (типично на 5m) —
                # не гонимся за ней, экономика рассчитана именно на вход
                # около entry_price, не на любую цену выше него (баг, найденный
                # на реальных данных 2026-09-20: средняя цена входа была 0.839
                # вместо 0.70).
                _count(market.asset, timeframe.label, "missed_entry_window")
            else:
                _count(market.asset, timeframe.label, "waiting_for_entry")
        elif existing["status"] == "open_unhedged":
            # Для решения "продавать" используем best_bid (что реально
            # получим), а не best_ask (что заплатили бы за покупку) — иначе
            # при широком спреде триггер срабатывает раньше, чем цена
            # реальной продажи туда доходит (реальный случай, 2026-09-22:
            # цель 0.95, а продажи срабатывали в диапазоне 0.84-0.97 —
            # средняя маржа 29.8% вместо ожидаемых 35.7%).
            sell_price = book_stream.best_bid(token_id)

            # Исследовательское наблюдение (не торгуем на этом): держим
            # минимальную цену, которую видела позиция за всё время жизни —
            # чтобы потом честно посчитать, помог бы стоп-лосс на каком-то
            # уровне или только резал бы позиции, которые в итоге
            # отыгрались бы обратно. Чтение из памяти, не пишем в БД на
            # каждый тик — запись случится один раз при закрытии позиции.
            if sell_price is not None:
                prev_min = existing.get("min_price_seen")
                if prev_min is None or sell_price < prev_min:
                    existing["min_price_seen"] = sell_price

            if take_profit_on and sell_price is not None and sell_price >= take_profit_price:
                _count(market.asset, timeframe.label, "hedged")
                await _execute_take_profit(market, side, existing["position_id"], existing["entry_shares"], token_id)
            else:
                _count(market.asset, timeframe.label, "waiting_for_hedge")


async def settle_resolved() -> None:
    """Общая фоновая задача (как executor.settle_resolved_trades) — узнаём
    исход рынков с открытыми хедж-позициями и фиксируем итоговый PnL."""
    for pos_id, market_slug, side, entry_shares, entry_cost, hedge_shares, hedge_cost, status, dry_run in \
            storage.get_unsettled_hedge_positions():
        try:
            outcome = await market_discovery.get_resolution(market_slug)
        except Exception as exc:  # noqa: BLE001
            log.warning("Не удалось узнать исход %s: %s", market_slug, exc)
            continue
        if not outcome:
            continue

        won = outcome == side
        if status == "hedged":
            realized_shares = entry_shares if won else hedge_shares
            total_cost = entry_cost + (hedge_cost or 0)
            pnl = realized_shares * 1.0 - total_cost
        else:  # open_unhedged — не успели захеджировать до резолюции
            pnl = (entry_shares * 1.0 - entry_cost) if won else -entry_cost

        min_price_seen = _open_positions.get((market_slug, side), {}).get("min_price_seen")
        storage.settle_hedge_position(pos_id, outcome, pnl, min_price_seen)
        _open_positions.pop((market_slug, side), None)  # больше не открыта — убираем из зеркала в памяти
        emoji = "🟢" if pnl > 0 else "🔴"
        await telegram_notify.notify(
            f"{emoji} Хедж-бот: {market_slug} ({side}) зарезолвился {outcome}. "
            f"{'Хедж сработал' if status == 'hedged' else 'Без хеджа (не дошло до порога)'}. "
            f"PnL: {pnl:+.2f} USDC" + (" (dry run)" if dry_run else "")
        )
