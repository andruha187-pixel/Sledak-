"""
Поиск кандидатов на "почти уверенные" сделки в Политике/Финансах на
Polymarket — механическая часть (цена + объём + срок). Саму надёжность
кандидата (насколько цена реально отражает актуальные новости) этот
модуль не оценивает — это должен делать человек или отдельный ИИ-анализ,
см. README.

Публичный Gamma API, авторизация не нужна.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone

import httpx

from config import settings


async def _fetch_events(client: httpx.AsyncClient, tag_id: int, limit: int = 100) -> list[dict]:
    events = []
    offset = 0
    while True:
        resp = await client.get(f"{settings.GAMMA_HOST}/events", params={
            "tag_id": tag_id, "closed": "false", "active": "true",
            "limit": limit, "offset": offset, "order": "volume24hr", "ascending": "false",
        })
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        events.extend(page)
        if len(page) < limit:
            break
        offset += limit
        if offset >= 1000:
            break
    return events


def _extract_candidates(events: list[dict], category: str) -> list[dict]:
    now = datetime.now(timezone.utc)
    rows = []
    for ev in events:
        markets = ev.get("markets") or [ev]
        for market in markets:
            outcomes_raw = market.get("outcomes")
            prices_raw = market.get("outcomePrices")
            try:
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
            except (ValueError, TypeError):
                continue
            if not outcomes or not prices or len(outcomes) != len(prices):
                continue

            volume = float(market.get("volume") or ev.get("volume") or 0)
            if volume < settings.MIN_VOLUME_USD:
                continue

            end_date_raw = market.get("endDate") or ev.get("endDate")
            days_left = None
            if end_date_raw:
                try:
                    end_dt = datetime.fromisoformat(str(end_date_raw).replace("Z", "+00:00"))
                    days_left = (end_dt - now).total_seconds() / 86400
                except ValueError:
                    pass
            if settings.MAX_DAYS_OUT and days_left is not None and days_left > settings.MAX_DAYS_OUT:
                continue

            slug = ev.get("slug") or market.get("slug") or ""
            for outcome, price_str in zip(outcomes, prices):
                try:
                    price = float(price_str)
                except (ValueError, TypeError):
                    continue
                if settings.MIN_PRICE <= price <= settings.MAX_PRICE:
                    rows.append({
                        "key": f"{slug}:{outcome}",
                        "category": category,
                        "slug": slug,
                        "question": market.get("question") or ev.get("title") or "",
                        "outcome": outcome,
                        "price": price,
                        "volume": volume,
                        "days_left": round(days_left, 1) if days_left is not None else None,
                        "url": f"https://polymarket.com/event/{slug}",
                    })
    return rows


async def scan() -> list[dict]:
    all_rows = []
    async with httpx.AsyncClient(timeout=20) as client:
        for category, tag_id in settings.TAGS.items():
            events = await _fetch_events(client, tag_id)
            all_rows.extend(_extract_candidates(events, category))
    all_rows.sort(key=lambda r: r["volume"], reverse=True)
    return all_rows
