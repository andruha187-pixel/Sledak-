"""
Хранит, о каких кандидатах уже уведомляли — чтобы слать в Telegram только
НОВЫЕ (появившиеся в диапазоне цены впервые, либо вернувшиеся туда после
того, как выходили из диапазона).
"""
from __future__ import annotations
import os
import sqlite3
import time
from contextlib import contextmanager

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_candidates (
    key TEXT PRIMARY KEY,
    first_seen_ts INTEGER,
    last_seen_ts INTEGER,
    last_price REAL
);
"""


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _conn() as conn:
        conn.executescript(_SCHEMA)


def get_seen_keys() -> set[str]:
    with _conn() as conn:
        cur = conn.execute("SELECT key FROM seen_candidates")
        return {row[0] for row in cur.fetchall()}


def mark_seen(key: str, price: float) -> None:
    now = int(time.time())
    with _conn() as conn:
        conn.execute(
            """INSERT INTO seen_candidates (key, first_seen_ts, last_seen_ts, last_price)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET last_seen_ts = excluded.last_seen_ts,
                                               last_price = excluded.last_price""",
            (key, now, now, price),
        )


def forget_stale(current_keys: set[str]) -> None:
    """Убираем из трекинга кандидатов, которых больше нет в диапазоне
    (закрылись или цена ушла) — если вернутся позже, уведомим заново."""
    seen = get_seen_keys()
    stale = seen - current_keys
    if not stale:
        return
    with _conn() as conn:
        conn.executemany("DELETE FROM seen_candidates WHERE key = ?", [(k,) for k in stale])
