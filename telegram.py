import asyncio
import logging
import os

import aiohttp

from config import TG_CHAT, TG_TOKEN
from net import get_json, session

log = logging.getLogger("tg")


def _split_caption(text, limit=1000):
    """Подпись к файлу в Telegram — максимум 1024 символа; остаток уйдёт отдельным сообщением."""
    if len(text) <= limit:
        return text, ""
    cut = text.rfind("\n", 0, limit)
    cut = cut if cut > 0 else limit
    return text[:cut], text[cut:].lstrip("\n")


class TG:
    def __init__(self):
        self.base = f"https://api.telegram.org/bot{TG_TOKEN}"
        self.on = bool(TG_TOKEN and TG_CHAT)

    async def send(self, text):
        if not self.on:
            log.info("TG: %s", text)
            return
        s = await session()
        for i in range(0, len(text), 3900):
            try:
                async with s.post(f"{self.base}/sendMessage",
                                  json={"chat_id": TG_CHAT, "text": text[i:i + 3900],
                                        "disable_web_page_preview": True}) as r:
                    if r.status != 200:
                        log.warning("sendMessage %s %s", r.status, await r.text())
            except Exception as e:
                log.warning("sendMessage err %s", e)

    async def send_file(self, path, caption=""):
        cap, rest = _split_caption(caption)
        if not self.on:
            log.info("TG file %s\n%s", path, caption)
            return
        s = await session()
        with open(path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("chat_id", str(TG_CHAT))
            data.add_field("caption", cap)
            data.add_field("document", f.read(), filename=os.path.basename(path))
        try:
            async with s.post(f"{self.base}/sendDocument", data=data,
                              timeout=aiohttp.ClientTimeout(total=300)) as r:
                if r.status != 200:
                    log.warning("sendDocument %s %s", r.status, await r.text())
                    await self.send(f"⚠️ Не смог отправить файл ({r.status}). Он лежит на сервере: {path}")
        except Exception as e:
            log.warning("sendDocument err %s", e)
        if rest:
            await self.send(rest)

    async def commands(self, handlers):
        if not self.on:
            return
        off = 0
        while True:
            d = await get_json(f"{self.base}/getUpdates", {"timeout": 15, "offset": off}, quiet=True)
            for u in (d or {}).get("result", []):
                off = u["update_id"] + 1
                m = u.get("message") or {}
                if str(m.get("chat", {}).get("id")) != str(TG_CHAT):
                    continue
                cmd = (m.get("text") or "").split(" ")[0].split("@")[0].lower()
                h = handlers.get(cmd)
                if h:
                    asyncio.create_task(h())
            if d is None:
                await asyncio.sleep(3)
