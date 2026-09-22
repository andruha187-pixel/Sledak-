"""Все настройки через переменные окружения."""
import os


def _f(k, d):
    return float(os.getenv(k, d))


def _i(k, d):
    return int(os.getenv(k, d))


# Кого профилируем (proxy-кошелёк Polymarket, как в адресе профиля)
WALLET = os.getenv("TARGET_WALLET", "0xb945945d5bcaf7b56834d4da8cdf8f8f94b2db68").lower()
ASSETS = [a.strip().lower() for a in os.getenv("ASSETS", "btc,eth").split(",") if a.strip()]
WINDOW_SEC = 900

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
REPORT_HOURS = _f("REPORT_HOURS", 4)
ALERT_EACH_TRADE = os.getenv("ALERT_EACH_TRADE", "0") == "1"

# Живой сбор
WALLET_POLL_SEC = _f("WALLET_POLL_SEC", 2)
CONTROL_EVERY_SEC = _i("CONTROL_EVERY_SEC", 5)      # как часто сохранять «контрольный» снимок рынка
DECISION_LAG_SEC = _f("DECISION_LAG_SEC", 2)        # решение принято ~за N сек до времени блока сделки

# История
BACKFILL_SINCE = os.getenv("BACKFILL_SINCE", "2026-03-01")
BACKFILL_MAX_WINDOWS = _i("BACKFILL_MAX_WINDOWS", 8000)      # окна, где он торговал
BACKFILL_NOTRADE_WINDOWS = _i("BACKFILL_NOTRADE_WINDOWS", 1500)  # окна, где НЕ торговал (контроль)
BACKFILL_CONCURRENCY = _i("BACKFILL_CONCURRENCY", 4)
HIST_CONTROL_STEP = _i("HIST_CONTROL_STEP", 30)
ROLE_BACKFILL_LAST = _i("ROLE_BACKFILL_LAST", 1500)          # скольким последним сделкам определить maker/taker

# Анализ
ANALYSIS_MAX_CONTROLS = _i("ANALYSIS_MAX_CONTROLS", 80000)

DB_PATH = os.getenv("DB_PATH", "data/profiler.db")
OUT_DIR = os.getenv("OUT_DIR", "data/reports")

# Эндпоинты
DATA_API = "https://data-api.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
PM_WS = os.getenv("PM_WS", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
RTDS_WS = os.getenv("RTDS_WS", "wss://ws-live-data.polymarket.com")
BINANCE_REST = os.getenv("BINANCE_REST", "https://api.binance.com")          # если 451: https://data-api.binance.vision
BINANCE_WS = os.getenv("BINANCE_WS", "wss://stream.binance.com:9443")        # если блок: wss://data-stream.binance.vision
BINANCE_FUT = os.getenv("BINANCE_FUT", "https://fapi.binance.com")
BYBIT = "https://api.bybit.com"
COINBASE = "https://api.exchange.coinbase.com"
POLYGON_RPC = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")

BINANCE_SYM = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT",
               "xrp": "XRPUSDT", "bnb": "BNBUSDT", "hype": "HYPEUSDT"}
COINBASE_SYM = {"btc": "BTC-USD", "eth": "ETH-USD", "sol": "SOL-USD", "xrp": "XRP-USD"}
