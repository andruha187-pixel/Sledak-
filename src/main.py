"""
Точка входа: раз в SCAN_INTERVAL_HOURS сканирует Polymarket на кандидатов
в Политике/Финансах с ценой в диапазоне [MIN_PRICE, MAX_PRICE], шлёт в
Telegram только НОВЫЕ (не уведомлённые ранее), убирает из отслеживания
те, что вышли из диапазона (закрылись или цена ушла) — если появятся
снова, уведомим повторно.

Это НЕ трейдинг-бот — ничего не покупает и не продаёт, только ищет и
уведомляет. Решение "заходить или нет" — за человеком (или за отдельным
ИИ-анализом, которому пересылают найденное).
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import scanner, storage, notify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("opportunity-scanner")


async def run_once() -> None:
    log.info("Сканирую Polymarket (Политика/Финансы, цена %.2f-%.2f)...",
              settings.MIN_PRICE, settings.MAX_PRICE)
    rows = await scanner.scan()
    log.info("Всего кандидатов сейчас: %d", len(rows))

    current_keys = {r["key"] for r in rows}
    seen_keys = storage.get_seen_keys()
    new_rows = [r for r in rows if r["key"] not in seen_keys]

    if new_rows:
        log.info("Новых кандидатов: %d", len(new_rows))
        await notify.notify_new_candidates(new_rows)
        for r in new_rows:
            storage.mark_seen(r["key"], r["price"])
    else:
        log.info("Новых кандидатов нет.")

    storage.forget_stale(current_keys)


async def main() -> None:
    storage.init_db()
    while True:
        try:
            await run_once()
        except Exception as exc:  # noqa: BLE001 — не роняем цикл из-за одной ошибки
            log.exception("Ошибка в цикле сканирования: %s", exc)
        await asyncio.sleep(settings.SCAN_INTERVAL_HOURS * 3600)


if __name__ == "__main__":
    asyncio.run(main())
