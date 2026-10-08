from __future__ import annotations
from telegram import Bot

from config import settings


async def notify_new_candidates(rows: list[dict]) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID не заданы — печатаю в консоль вместо отправки.")
        for r in rows:
            print(r)
        return

    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)

    if len(rows) == 1:
        r = rows[0]
        text = (
            f"🆕 Новый кандидат [{r['category']}]\n"
            f"{r['question']}\n"
            f"Исход: {r['outcome']} @ {r['price']:.2f}\n"
            f"Объём: ${r['volume']:,.0f} | до резолюции {r['days_left']} дн.\n"
            f"{r['url']}"
        )
        await bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text)
        return

    # Несколько новых сразу — одним сообщением, компактно
    lines = [f"🆕 Новых кандидатов: {len(rows)}\n"]
    for r in rows[:20]:
        lines.append(
            f"[{r['category']}] {r['question']} — {r['outcome']} @ {r['price']:.2f} "
            f"(${r['volume']:,.0f}, {r['days_left']} дн.)\n{r['url']}"
        )
    text = "\n\n".join(lines)
    # Telegram режет сообщения на 4096 символов — разобьём, если что
    for i in range(0, len(text), 4000):
        await bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text[i:i + 4000])
