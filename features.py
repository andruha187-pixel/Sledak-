"""Индикаторы и признаки. ОДИН и тот же код для живых снимков и для истории,
чтобы признаки были сопоставимы.

Форматы:
  buckets: список (sec, close, buy_usd, sell_usd, n) по 1-секундным корзинам, по возрастанию sec
  candles: список (t_open, o, h, l, c, v) закрытых 1m свечей, по возрастанию
"""
import math
import time
from bisect import bisect_left


# ---------- базовые индикаторы ----------
def ema(v, n):
    if len(v) < n:
        return None
    k = 2 / (n + 1)
    e = sum(v[:n]) / n
    for x in v[n:]:
        e = x * k + e * (1 - k)
    return e


def ema_series(v, n):
    if len(v) < n:
        return []
    k = 2 / (n + 1)
    e = sum(v[:n]) / n
    out = [e]
    for x in v[n:]:
        e = x * k + e * (1 - k)
        out.append(e)
    return out


def rsi(c, n=14):
    if len(c) < n + 1:
        return None
    g = l = 0.0
    for i in range(1, n + 1):
        d = c[i] - c[i - 1]
        g += max(d, 0)
        l += max(-d, 0)
    g /= n
    l /= n
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]
        g = (g * (n - 1) + max(d, 0)) / n
        l = (l * (n - 1) + max(-d, 0)) / n
    return 100.0 if l == 0 else 100 - 100 / (1 + g / l)


def macd_hist(c, f=12, s=26, sig=9):
    if len(c) < s + sig:
        return None
    es = ema_series(c, s)
    ef = ema_series(c, f)[-len(es):]
    m = [a - b for a, b in zip(ef, es)]
    sg = ema(m, sig)
    return None if sg is None else m[-1] - sg


def atr(h, l, c, n=14):
    if len(c) < n + 1:
        return None
    tr = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
    a = sum(tr[:n]) / n
    for x in tr[n:]:
        a = (a * (n - 1) + x) / n
    return a


def bb_pctb(c, n=20, k=2.0):
    if len(c) < n:
        return None
    w = c[-n:]
    m = sum(w) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in w) / n)
    return 0.5 if sd == 0 else (c[-1] - (m - k * sd)) / (2 * k * sd)


def logret_std(c):
    r = [math.log(c[i] / c[i - 1]) for i in range(1, len(c)) if c[i - 1] > 0 and c[i] > 0]
    if len(r) < 5:
        return None
    m = sum(r) / len(r)
    return math.sqrt(sum((x - m) ** 2 for x in r) / len(r))


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _bp(a, b):
    return (a / b - 1) * 1e4 if a and b else None


# ---------- блоки признаков ----------
def ta_block(c, p):
    out = {}
    if len(c) < 30:
        return out
    cl = [x[4] for x in c]
    hi = [x[2] for x in c]
    lo = [x[3] for x in c]
    vo = [x[5] for x in c]
    px = cl[-1]
    e9, e21, e50 = ema(cl, 9), ema(cl, 21), ema(cl, 50)
    out[f"{p}_px_ema9"] = _bp(px, e9)
    out[f"{p}_px_ema21"] = _bp(px, e21)
    out[f"{p}_px_ema50"] = _bp(px, e50)
    out[f"{p}_ema9_21"] = _bp(e9, e21)
    out[f"{p}_rsi"] = rsi(cl, 14)
    mh = macd_hist(cl)
    out[f"{p}_macdh_bps"] = mh / px * 1e4 if mh is not None else None
    out[f"{p}_bb"] = bb_pctb(cl, 20)
    a = atr(hi, lo, cl, 14)
    out[f"{p}_atr_bps"] = a / px * 1e4 if a else None
    if len(vo) >= 21:
        w = vo[-21:-1]
        m = sum(w) / 20
        sd = math.sqrt(sum((x - m) ** 2 for x in w) / 20)
        out[f"{p}_vol_z"] = (vo[-1] - m) / sd if sd > 0 else 0.0
    out[f"{p}_ret1_bps"] = _bp(cl[-1], cl[-2])
    return out


def agg5(c1):
    out, cur = [], None
    for t, o, h, l, c, v in c1:
        g = int(t) // 300 * 300
        if cur is None or cur[0] != g:
            if cur is not None and cur[6] == 5:
                out.append(tuple(cur[:6]))
            cur = [g, o, h, l, c, v, 1]
        else:
            cur[2] = max(cur[2], h)
            cur[3] = min(cur[3], l)
            cur[4] = c
            cur[5] += v
            cur[6] += 1
    if cur is not None and cur[6] == 5:
        out.append(tuple(cur[:6]))
    return out


def ta_all(c1):
    """c1 — закрытые 1m свечи. Возвращает (dict признаков, sigma_1m как доля)."""
    d = ta_block(c1[-200:], "m1")
    d.update(ta_block(agg5(c1[-800:]), "m5"))
    sigma = logret_std([x[4] for x in c1[-61:]]) if len(c1) >= 20 else None
    if sigma:
        d["sigma1m_bps"] = sigma * 1e4
    return d, sigma


def price_before(b, t, secs=None):
    """Цена закрытия последней корзины с sec < t."""
    if secs is None:
        secs = [x[0] for x in b]
    i = bisect_left(secs, t) - 1
    return b[i][1] if i >= 0 else None


def flow_block(b, t):
    """Импульс и поток ордеров Binance на момент t (только корзины sec < t)."""
    out = {}
    if not b:
        return out, None
    secs = [x[0] for x in b]
    i = bisect_left(secs, t) - 1
    if i < 0:
        return out, None
    px = b[i][1]
    for w in (5, 15, 30, 60, 180):
        p0 = price_before(b, t - w, secs)
        out[f"ret{w}s_bps"] = _bp(px, p0) if p0 else None
    for w in (15, 60):
        j = bisect_left(secs, t - w)
        bv = sum(x[2] for x in b[j:i + 1])
        sv = sum(x[3] for x in b[j:i + 1])
        out[f"cvd{w}s"] = (bv - sv) / 1e3
        out[f"buy_ratio{w}s"] = bv / (bv + sv) if bv + sv > 0 else None
        if w == 60:
            out["usd60s"] = (bv + sv) / 1e3
            out["n60s"] = sum(x[4] for x in b[j:i + 1])
    j = bisect_left(secs, t - 61)
    cl = [x[1] for x in b[j:i + 1]]
    s = logret_std(cl) if len(cl) > 10 else None
    out["rv60s_bps"] = s * 1e4 if s else None
    return out, px


def fair_block(px, open_px, sigma1m, sec_left):
    """Справедливая вероятность UP по броуновской модели: Φ(ln(P/P0) / (σ·√t))."""
    if not (px and open_px and sigma1m):
        return {}
    d = math.log(px / open_px)
    tl = max(sec_left, 1) / 60
    z = d / (sigma1m * math.sqrt(tl)) if sigma1m > 0 else 0.0
    return {"delta_bps": d * 1e4, "z": z, "fair_up": norm_cdf(z)}


def hour_of(t):
    return time.gmtime(t).tm_hour
