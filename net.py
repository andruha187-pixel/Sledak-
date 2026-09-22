import asyncio
import logging

import aiohttp

log = logging.getLogger("net")
_s = None


async def session():
    global _s
    if _s is None or _s.closed:
        _s = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25),
                                   headers={"User-Agent": "Mozilla/5.0 wallet-profiler/1.0"})
    return _s


def _p(params):
    return {k: str(v) for k, v in (params or {}).items() if v is not None}


async def get_json(url, params=None, retries=3, quiet=False):
    for i in range(retries):
        try:
            s = await session()
            async with s.get(url, params=_p(params)) as r:
                if r.status == 429 or r.status >= 500:
                    await asyncio.sleep(2 * (i + 1))
                    continue
                if r.status >= 400:
                    if not quiet:
                        log.warning("GET %s %s -> %s", url, params, r.status)
                    return None
                return await r.json(content_type=None)
        except Exception as e:  # noqa
            if not quiet:
                log.debug("GET %s err %s", url, e)
            await asyncio.sleep(1 + i)
    return None


async def post_json(url, payload, retries=3):
    for i in range(retries):
        try:
            s = await session()
            async with s.post(url, json=payload) as r:
                if r.status == 429 or r.status >= 500:
                    await asyncio.sleep(2 * (i + 1))
                    continue
                if r.status >= 400:
                    return None
                return await r.json(content_type=None)
        except Exception:
            await asyncio.sleep(1 + i)
    return None
