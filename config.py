from __future__ import annotations
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _get_float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


@dataclass
class Settings:
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")

    # tag_id категорий Polymarket: 2=Politics, 120=Finance
    TAGS: dict = field(default_factory=lambda: {"politics": 2, "finance": 120})

    MIN_PRICE: float = _get_float("MIN_PRICE", 0.80)
    MAX_PRICE: float = _get_float("MAX_PRICE", 0.95)
    MIN_VOLUME_USD: float = _get_float("MIN_VOLUME_USD", 20000)
    MAX_DAYS_OUT: float = _get_float("MAX_DAYS_OUT", 180)

    SCAN_INTERVAL_HOURS: float = _get_float("SCAN_INTERVAL_HOURS", 6.0)

    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    DB_PATH: str = os.getenv("DB_PATH", "data/scanner.db")


settings = Settings()
