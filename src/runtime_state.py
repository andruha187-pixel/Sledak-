"""
Настройки, которые можно менять на лету из Telegram (без передеплоя):
активы, параметры хедж-бота (вход/хедж/ставка), дневной стоп-лосс, режим
DRY_RUN/LIVE.

Живут в памяти для быстрого доступа из hedge_bot на каждом тике, но
каждое изменение сразу пишется в SQLite (`bot_settings`) — переживает
рестарт процесса (важно на Render: контейнер может перезапуститься сам
по себе, не только по твоей команде).
"""
from __future__ import annotations
import os

from config import settings
from src import storage


def _get_float_env(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _get_bool_env(name: str, default: bool) -> bool:
    val = os.getenv(name)
    return val.lower() == "true" if val else default


_DEFAULTS = {
    "paused": False,
    "dry_run": settings.DRY_RUN,
    "trade_size_usdc": settings.TRADE_SIZE_USDC,
    "daily_loss_limit_usdc": settings.DAILY_LOSS_LIMIT_USDC,
    "safety_score_threshold": settings.SAFETY_SCORE_THRESHOLD,
    "min_entry_price": settings.MIN_ENTRY_PRICE,
    "max_entry_price": settings.MAX_ENTRY_PRICE,
    # По умолчанию выключено: каждая прошедшая порог сделка идёт полным
    # TRADE_SIZE_USDC, без урезания по пограничности score.
    "size_scaling_enabled": False,
    # Стоп-лосс ОТДЕЛЬНОЙ позиции в процентах (не дневной!): если текущая
    # стоимость позиции (по best bid в стакане) упала настолько от суммы
    # входа — закрываем досрочно продажей, не дожидаясь резолюции рынка.
    "position_stop_loss_enabled": False,
    "position_stop_loss_pct": 50.0,
    # Какие активы сейчас реально торгуются — можно включать/выключать
    # по одному через Telegram, не трогая остальные и не передеплоя.
    # Хранится как строка через запятую (см. get/set_enabled_assets ниже).
    "enabled_assets": ",".join(settings.ASSETS),
    # Режим размера ставки: "fixed" (константа в USDC, trade_size_usdc) или
    # "percent" (доля от ТЕКУЩЕГО банка — starting_bankroll_usdc + вся
    # реализованная прибыль/убыток с начала). Percent-режим сам сжимается
    # при просадке и растёт при выигрышах — в отличие от fixed, который на
    # похудевшем банке становится относительно только агрессивнее.
    "sizing_mode": "fixed",
    "bankroll_pct": 5.0,
    "starting_bankroll_usdc": 60.0,
    # Отслеживание чужого кошелька: уведомления всегда можно включить
    # отдельно от реального копирования сделок (copytrade) — по умолчанию
    # только уведомляем, ничего не покупаем автоматически.
    "wallet_notify_enabled": True,
    "wallet_copytrade_enabled": False,
    "copytrade_size_usdc": 5.0,
    # Хедж-бот: вход по ENTRY_PRICE, докупка противоположной стороны при
    # достижении HEDGE_PRICE — см. src/hedge_bot.py. Пороги пришли из
    # анализа реальных momentum-отчётов (2026-09-20): хедж на 0.90 дал
    # положительный PnL на бэктесте, хедж на 0.70-0.85 — отрицательный,
    # несмотря на то, что сам хедж всегда безубыточен по построению —
    # разница в том, сколько сессий вообще НЕ доходит до точки хеджа и
    # остаётся неприкрытой позицией (см. обсуждение в чате).
    "hedge_bot_enabled": _get_bool_env("HEDGE_BOT_ENABLED", True),
    "hedge_entry_price": _get_float_env("HEDGE_ENTRY_PRICE", 0.70),
    # Насколько выше hedge_entry_price ещё можно входить — если цена уже
    # проскочила дальше (типично на 5m между тиками), пропускаем это окно,
    # не гонимся: экономика хеджа рассчитана на вход БЛИЗКО к 0.70, не на
    # произвольную цену выше (найдено на реальных данных 2026-09-20:
    # средняя цена входа была 0.839 вместо 0.70 без этого ограничения).
    # Сужено с 0.03 до 0.015 (2026-09-21): реальная маржа на захеджированную
    # сделку была только 57% от теоретической — вход дальше от идеальных
    # 0.70 напрямую съедает гарантированную маржу хеджа (формула прибыли —
    # stake*(hedge_price/entry_price - 1), выше entry_price = меньше маржа).
    "hedge_entry_tolerance": _get_float_env("HEDGE_ENTRY_TOLERANCE", 0.015),
    # 0.95, не 0.90 (2026-09-22): полный перебор всех пар вход/выход на
    # 2438 сессиях показал 0.70→0.95 лучшей комбинацией — $0.545/сделку
    # против $0.465 у 0.70→0.90 (на 17% больше). Закономерность чёткая:
    # самый ранний вход + самый поздний выход выигрывает почти всегда,
    # промежуточные комбинации (например 0.75→0.80, 0.80→0.85) чаще в
    # минусе — комиссия и спред съедают непропорционально много при
    # маленькой целевой прибыли.
    "hedge_trigger_price": _get_float_env("HEDGE_TRIGGER_PRICE", 0.95),
    # $10, а не $5 — при входе 0.70 и хедже на 0.90 нога хеджа стоит
    # stake*(1-0.90)/0.70 = stake*0.143; при $5 это $0.71 (ниже минимума
    # ордера Polymarket в $1!), хедж физически не мог бы исполниться.
    # При $10 — уже $1.43, с запасом выше минимума.
    "hedge_stake_usdc": _get_float_env("HEDGE_STAKE_USDC", 10.0),
    # Потолок ОДНОВРЕМЕННО открытых позиций по всем активам/таймфреймам —
    # без него все 12 потоков могли бы войти разом. Дефолт берём из
    # MAX_OPEN_POSITIONS в .env, но теперь можно менять кнопкой без
    # передеплоя.
    "max_open_positions": settings.MAX_OPEN_POSITIONS,
    # --- Правила по анализу 2026-09-23 (3296 momentum-сессий + 265 сделок бота) ---
    # Продажа на пороге ВЫКЛ по умолчанию: из 187 позиций, проданных по 0.95,
    # 183 выиграли бы и так — продажа отдавала ~5 центов с акции + вторую
    # комиссию. Держать до резолюции оказалось выгоднее на тех же сделках.
    "hedge_take_profit_enabled": _get_bool_env("HEDGE_TAKE_PROFIT_ENABLED", False),
    # Окно входа по времени — доля прошедшего окна рынка. На 5m вход при
    # 30-70% прошедшего времени: winrate 79.9%, +10.6% ROI (n=1231), стабильно
    # в обеих половинах выборки и по всем 6 активам; ранний вход (0-30%)
    # стабильно в минусе.
    "hedge_entry_min_elapsed": _get_float_env("HEDGE_ENTRY_MIN_ELAPSED", 0.30),
    "hedge_entry_max_elapsed": _get_float_env("HEDGE_ENTRY_MAX_ELAPSED", 0.70),
    # На каких таймфреймах хедж-бот ВХОДИТ (momentum-наблюдение идёт на всех).
    # 15m по данным около нуля и шумный — выключен для торговли по умолчанию.
    "hedge_timeframes": os.getenv("HEDGE_TIMEFRAMES", "5m"),
}

# Типы приведения при чтении из SQLite (там всё хранится как TEXT)
_CASTERS = {
    "paused": lambda v: str(v).lower() == "true",
    "dry_run": lambda v: str(v).lower() == "true",
    "trade_size_usdc": float,
    "daily_loss_limit_usdc": float,
    "safety_score_threshold": float,
    "min_entry_price": float,
    "max_entry_price": float,
    "size_scaling_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_pct": float,
    "enabled_assets": str,
    "sizing_mode": str,
    "bankroll_pct": float,
    "starting_bankroll_usdc": float,
    "wallet_notify_enabled": lambda v: str(v).lower() == "true",
    "wallet_copytrade_enabled": lambda v: str(v).lower() == "true",
    "copytrade_size_usdc": float,
    "hedge_bot_enabled": lambda v: str(v).lower() == "true",
    "hedge_entry_price": float,
    "hedge_entry_tolerance": float,
    "hedge_trigger_price": float,
    "hedge_stake_usdc": float,
    "max_open_positions": int,
    "hedge_take_profit_enabled": lambda v: str(v).lower() == "true",
    "hedge_entry_min_elapsed": float,
    "hedge_entry_max_elapsed": float,
    "hedge_timeframes": str,
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db()."""
    saved = storage.get_all_settings()
    for key, raw in saved.items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass


def get(key: str):
    return _state[key]


def set(key: str, value) -> None:
    _state[key] = value
    storage.set_setting(key, value)


def snapshot() -> dict:
    return dict(_state)


# --- Включение/выключение отдельных активов ---

def get_enabled_assets() -> set[str]:
    raw = _state.get("enabled_assets", "") or ""
    return {a for a in raw.split(",") if a}


def is_asset_enabled(asset: str) -> bool:
    return asset.lower() in get_enabled_assets()


def set_enabled_assets(assets: set[str]) -> None:
    set("enabled_assets", ",".join(sorted(assets)))


def toggle_asset(asset: str) -> bool:
    """Переключает состояние актива и возвращает новое (True = включён)."""
    asset = asset.lower()
    enabled = get_enabled_assets()
    if asset in enabled:
        enabled.discard(asset)
    else:
        enabled.add(asset)
    set_enabled_assets(enabled)
    return asset in enabled


# --- Размер ставки: fixed или % от текущего банка ---

def current_bankroll() -> float:
    """starting_bankroll_usdc + реализованная прибыль/убыток с начала.
    Считаем ТОЛЬКО реальные (не dry-run) сделки — иначе виртуальный PnL из
    периодов тестового прогона исказил бы размер реальных ставок (баг,
    найденный на реальных отчётах: банк считался завышенным на сумму
    прошлого dry-run PnL). Если сейчас DRY_RUN, наоборот, честнее было бы
    видеть, как рос бы виртуальный банк — но раз sizing реальных денег и
    dry-run использует один и тот же расчёт, отдаём предпочтение
    безопасности реальных ставок."""
    pnl = storage.get_pnl_summary(0, live_only=True)["pnl_usdc"]
    return get("starting_bankroll_usdc") + pnl


def compute_trade_size() -> float:
    """Базовый размер ставки ДО масштабирования по score (см.
    executor._scale_trade_size) — либо константа, либо доля от банка."""
    if get("sizing_mode") == "percent":
        bankroll = max(0.0, current_bankroll())
        return round(bankroll * get("bankroll_pct") / 100, 2)
    return get("trade_size_usdc")
