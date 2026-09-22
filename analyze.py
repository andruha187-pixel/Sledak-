"""Разбор стратегии кошелька. Запускается в отдельном потоке.
Выход: zip (report.md + CSV) и короткая подпись для Telegram."""
import json
import os
import sqlite3
import time
import zipfile

import numpy as np
import pandas as pd

from config import ANALYSIS_MAX_CONTROLS, DB_PATH, DECISION_LAG_SEC, OUT_DIR, WALLET

SIGNED = ["delta_bps", "z", "ret5s_bps", "ret15s_bps", "ret30s_bps", "ret60s_bps", "ret180s_bps",
          "cvd15s", "cvd60s", "m1_px_ema9", "m1_px_ema21", "m1_px_ema50", "m1_ema9_21", "m1_macdh_bps",
          "m1_ret1_bps", "m5_px_ema9", "m5_px_ema21", "m5_px_ema50", "m5_ema9_21", "m5_macdh_bps",
          "m5_ret1_bps", "bn_book_imb", "cl_bn_basis_bps", "by_basis_bps", "cb_basis_bps"]
FLIP100 = ["m1_rsi", "m5_rsi"]
FLIP1 = ["m1_bb", "m5_bb", "buy_ratio15s", "buy_ratio60s"]
UNSIGNED = ["sec_left", "sec_into", "hour", "sigma1m_bps", "m1_atr_bps", "m5_atr_bps", "m1_vol_z", "m5_vol_z",
            "rv60s_bps", "usd60s", "n60s", "pm_sum_ask", "pm_sum_bid", "bn_funding", "oi_chg5m_pct", "pm_n30"]
PM_FIELDS = ["ask", "bid", "mid", "spread", "bsz5", "asz5", "imb5", "bsz1", "asz1"]


# ---------------- загрузка ----------------
def _expand(df):
    if df.empty:
        return df
    feats = [json.loads(x) if isinstance(x, str) and x else {} for x in df["feat"]]
    f = pd.DataFrame(feats, index=df.index)
    f = f.drop(columns=[c for c in f.columns if c in df.columns], errors="ignore")
    return pd.concat([df.drop(columns=["feat"]), f], axis=1)


def load():
    con = sqlite3.connect(DB_PATH)
    tr = _expand(pd.read_sql("SELECT * FROM trades", con))
    wn = pd.read_sql("SELECT * FROM windows", con)
    parts = []
    for src in ("live", "hist"):
        n = con.execute("SELECT COUNT(*) FROM snapshots WHERE src=?", (src,)).fetchone()[0]
        if n > ANALYSIS_MAX_CONTROLS:
            q = "SELECT * FROM snapshots WHERE src=? AND abs(random()) % ? = 0"
            parts.append(pd.read_sql(q, con, params=(src, int(np.ceil(n / ANALYSIS_MAX_CONTROLS)))))
        else:
            parts.append(pd.read_sql("SELECT * FROM snapshots WHERE src=?", con, params=(src,)))
    con.close()
    sn = pd.concat(parts, ignore_index=True)
    sn = _expand(sn)
    if "ts" in sn:
        sn["ts"] = pd.to_numeric(sn["ts"], errors="coerce")
    for c in ("price", "size", "usdc", "ts"):
        if c in tr:
            tr[c] = pd.to_numeric(tr[c], errors="coerce")
    return tr, wn, sn


# ---------------- нормализация «в сторону выбранного исхода» ----------------
def _g(df, c):
    return pd.to_numeric(df[c], errors="coerce") if c in df else pd.Series(np.nan, index=df.index)


def sidefy(df, is_up):
    up = np.asarray(is_up, dtype=bool)
    s = np.where(up, 1.0, -1.0)
    o = pd.DataFrame(index=df.index)
    for c in SIGNED:
        if c in df:
            o["my_" + c] = _g(df, c) * s
    for c in FLIP100:
        if c in df:
            o["my_" + c] = np.where(up, _g(df, c), 100 - _g(df, c))
    for c in FLIP1:
        if c in df:
            o["my_" + c] = np.where(up, _g(df, c), 1 - _g(df, c))
    if "fair_up" in df:
        o["my_fair"] = np.where(up, _g(df, "fair_up"), 1 - _g(df, "fair_up"))
    for x in PM_FIELDS:
        a, b = f"pm_up_{x}", f"pm_dn_{x}"
        if a in df or b in df:
            o["my_" + x] = np.where(up, _g(df, a), _g(df, b))
            o["opp_" + x] = np.where(up, _g(df, b), _g(df, a))
    if "pmh_up" in df or "pmh_dn" in df:
        o["my_pmh"] = np.where(up, _g(df, "pmh_up"), _g(df, "pmh_dn"))
    if "pm_up_flow30" in df:
        o["my_flow30"] = np.where(up, _g(df, "pm_up_flow30"), _g(df, "pm_dn_flow30"))
        o["opp_flow30"] = np.where(up, _g(df, "pm_dn_flow30"), _g(df, "pm_up_flow30"))
    if "pm_up_mid_chg5s" in df:
        o["my_pm_chg5s"] = _g(df, "pm_up_mid_chg5s") * s
    if "my_fair" in o and "my_ask" in o:
        o["my_edge"] = o["my_fair"] - o["my_ask"]
    if "my_fair" in o and "my_pmh" in o:
        o["my_edge_h"] = o["my_fair"] - o["my_pmh"]
    for c in UNSIGNED:
        if c in df:
            o[c] = _g(df, c)
    return o


# ---------------- PnL по окнам ----------------
def window_pnl(tr, wn):
    t = tr[tr["slug"].fillna("").str.contains("-updown-15m-")].sort_values("ts")
    rows = []
    for slug, g in t.groupby("slug"):
        cash, sh, buy_usd, nb, ns = 0.0, {"up": 0.0, "down": 0.0}, 0.0, 0, 0
        cost = {"up": 0.0, "down": 0.0}
        qty = {"up": 0.0, "down": 0.0}
        for r in g.itertuples(index=False):
            sz, us, oc = (r.size or 0.0), (r.usdc or 0.0), (r.outcome or "")
            if r.type == "TRADE" and oc in sh:
                if r.side == "BUY":
                    cash -= us
                    sh[oc] += sz
                    buy_usd += us
                    nb += 1
                    cost[oc] += us
                    qty[oc] += sz
                elif r.side == "SELL":
                    cash += us
                    sh[oc] -= sz
                    ns += 1
            elif r.type == "MERGE":
                cash += us
                sh["up"] -= sz
                sh["down"] -= sz
            elif r.type == "SPLIT":
                cash -= us
                sh["up"] += sz
                sh["down"] += sz
        rows.append({"slug": slug, "cash": cash, "sh_up": sh["up"], "sh_down": sh["down"], "buy_usd": buy_usd,
                     "n_buys": nb, "n_sells": ns, "avg_up": cost["up"] / qty["up"] if qty["up"] else np.nan,
                     "avg_dn": cost["down"] / qty["down"] if qty["down"] else np.nan,
                     "qty_up": qty["up"], "qty_dn": qty["down"],
                     "merged": int((g["type"] == "MERGE").any())})
    pw = pd.DataFrame(rows)
    if pw.empty:
        return pw
    pw = pw.merge(wn[["slug", "asset", "start", "winner"]], on="slug", how="left")
    pw["payout"] = np.where(pw["winner"] == "up", pw["sh_up"],
                            np.where(pw["winner"] == "down", pw["sh_down"], np.nan))
    pw["pnl"] = pw["cash"] + pw["payout"]
    pw["both_sides"] = (pw["qty_up"] > 0) & (pw["qty_dn"] > 0)
    pw["pair_cost"] = pw["avg_up"] + pw["avg_dn"]
    return pw


# ---------------- статистика ----------------
def auc(pos, neg):
    pos, neg = pos.dropna(), neg.dropna()
    if len(pos) < 10 or len(neg) < 10:
        return np.nan
    r = pd.concat([pos, neg], ignore_index=True).rank()
    rp = r.iloc[:len(pos)].sum()
    return (rp - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg))


def feature_table(E, C):
    rows = []
    for c in E.columns:
        if c not in C.columns:
            continue
        e, k = E[c].dropna(), C[c].dropna()
        if len(e) < 10 or len(k) < 10:
            continue
        a = auc(e, k)
        rows.append({"feature": c, "auc": a, "sep": abs(a - 0.5), "n_entry": len(e),
                     "entry_p10": e.quantile(.1), "entry_med": e.median(), "entry_p90": e.quantile(.9),
                     "ctrl_p10": k.quantile(.1), "ctrl_med": k.median(), "ctrl_p90": k.quantile(.9)})
    return pd.DataFrame(rows).sort_values("sep", ascending=False) if rows else pd.DataFrame()


def _leaf_rules(dt, cols):
    t = dt.tree_
    out = {}

    def walk(n, conds):
        if t.children_left[n] == -1:
            out[n] = conds
            return
        f, th = cols[t.feature[n]], t.threshold[n]
        walk(t.children_left[n], conds + [f"{f} <= {th:.4g}"])
        walk(t.children_right[n], conds + [f"{f} > {th:.4g}"])

    walk(0, [])
    return out


def tree_rules(E, C, title):
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.tree import DecisionTreeClassifier
    except ImportError:
        return f"### {title}\nsklearn не установлен\n", pd.DataFrame()
    if len(E) < 30 or len(C) < 100:
        return f"### {title}\nМало данных: входов {len(E)}, контролей {len(C)} — нужно ≥30 и ≥100.\n", pd.DataFrame()
    cols = [c for c in E.columns if c in C.columns and E[c].notna().mean() > 0.6 and C[c].notna().mean() > 0.6
            and c not in ("hour",)]
    if not cols:
        return f"### {title}\nНет общих признаков.\n", pd.DataFrame()
    X = pd.concat([E[cols], C[cols]], ignore_index=True).astype(float)
    y = np.r_[np.ones(len(E)), np.zeros(len(C))]
    X = X.fillna(X.median())
    base = y.mean()
    lines = [f"### {title}", f"Входов: {len(E)}, контрольных моментов: {len(C)}, базовая доля {base:.4f}", ""]
    dt = DecisionTreeClassifier(max_depth=4, min_samples_leaf=max(10, len(E) // 30),
                                class_weight="balanced", random_state=0).fit(X, y)
    leaves = dt.apply(X)
    st = pd.DataFrame({"leaf": leaves, "y": y}).groupby("leaf")["y"].agg(["sum", "count"])
    st["rate"] = st["sum"] / st["count"]
    st["lift"] = st["rate"] / base
    st["recall"] = st["sum"] / y.sum()
    rules = _leaf_rules(dt, cols)
    st = st.sort_values("lift", ascending=False)
    lines.append("**Правила-кандидаты (листья дерева, отсортированы по lift):**")
    rr = []
    for leaf, r in st.iterrows():
        if r["recall"] < 0.03:
            continue
        cond = " И ".join(rules.get(leaf, []))
        lines.append(f"- lift ×{r['lift']:.1f}, покрывает {r['recall']*100:.0f}% его входов: {cond}")
        rr.append({"lift": r["lift"], "recall": r["recall"], "entries": r["sum"], "moments": r["count"], "rule": cond})
    rf = RandomForestClassifier(n_estimators=200, max_depth=7, min_samples_leaf=10, class_weight="balanced",
                                n_jobs=1, random_state=0).fit(X, y)
    imp = sorted(zip(rf.feature_importances_, cols), reverse=True)[:15]
    lines.append("\n**Важность признаков (случайный лес):** " + ", ".join(f"{c} {v:.3f}" for v, c in imp))
    return "\n".join(lines) + "\n", pd.DataFrame(rr)


def entries_controls(tr, sn, src):
    buys = tr[(tr["type"] == "TRADE") & (tr["side"] == "BUY") & tr["outcome"].isin(["up", "down"])]
    if "src" not in buys:
        return pd.DataFrame(), pd.DataFrame(), buys.iloc[:0]
    e = buys[buys["src"] == src].sort_values("ts").drop_duplicates(["slug", "outcome"], keep="first")
    E = sidefy(e, e["outcome"] == "up")
    c = sn[sn["src"] == src] if "src" in sn else sn.iloc[:0]
    if c.empty:
        return E, pd.DataFrame(), e
    first = buys.groupby(["slug", "outcome"])["ts"].min().to_dict()
    Cs = []
    for side in ("up", "down"):
        fe = pd.Series([first.get((s, side), np.nan) for s in c["slug"]], index=c.index, dtype=float)
        keep = fe.isna() | (pd.to_numeric(c["ts"]) < fe - DECISION_LAG_SEC - 10)
        cc = c[keep]
        Cs.append(sidefy(cc, np.full(len(cc), side == "up")))
    return E, pd.concat(Cs, ignore_index=True), e


def _pct(x):
    return f"{x*100:.1f}%" if pd.notna(x) else "—"


# ---------------- отчёт ----------------
def build_report():
    t0 = time.time()
    tr, wn, sn = load()
    L = [f"# Профиль кошелька {WALLET}", f"Сгенерировано: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}", ""]
    csvs = {}
    if tr.empty:
        return _write(L + ["Сделок пока нет."], csvs, "Сделок пока нет.")

    tr["dt"] = pd.to_datetime(tr["ts"], unit="s", utc=True)
    L += ["## 1. Обзор",
          f"Период: {tr['dt'].min():%Y-%m-%d} → {tr['dt'].max():%Y-%m-%d}",
          "События по типам: " + ", ".join(f"{k}={v}" for k, v in tr["type"].value_counts().items()), ""]
    ud = tr[tr["slug"].fillna("").str.contains("-updown-15m-")]
    other = tr[(tr["type"] == "TRADE") & ~tr.index.isin(ud.index)]
    L.append(f"Сделок в 15m Up/Down: {int((ud['type']=='TRADE').sum())}; в других рынках: {len(other)}")
    if len(other):
        L.append("Другие рынки (топ): " + ", ".join(f"{k}({v})" for k, v in other["slug"].value_counts().head(8).items()))
    L.append("По активам: " + ", ".join(f"{k}={v}" for k, v in ud[ud["type"] == "TRADE"]["asset"].value_counts().items()))

    # --- PnL
    pw = window_pnl(tr, wn)
    res = pw.dropna(subset=["pnl"]) if not pw.empty else pw
    L += ["", "## 2. Результат (пересчёт по окнам, до резолва)"]
    if not res.empty:
        L.append(f"Окон с резолвом: {len(res)} из {len(pw)}. PnL: ${res['pnl'].sum():,.0f}. "
                 f"Плюсовых окон: {_pct((res['pnl'] > 0).mean())}. Оборот покупок: ${res['buy_usd'].sum():,.0f}. "
                 f"ROI на оборот: {_pct(res['pnl'].sum() / max(res['buy_usd'].sum(), 1))}")
        by = res.groupby("asset")["pnl"].agg(["sum", "count", lambda s: (s > 0).mean()])
        for a, r in by.iterrows():
            L.append(f"- {a}: ${r['sum']:,.0f} за {int(r['count'])} окон, плюсовых {_pct(r.iloc[2])}")
        res2 = res.copy()
        res2["month"] = pd.to_datetime(res2["start"], unit="s").dt.strftime("%Y-%m")
        L.append("По месяцам: " + ", ".join(f"{m}: ${v:,.0f}" for m, v in res2.groupby("month")["pnl"].sum().items()))
        csvs["windows_pnl.csv"] = pw

    # --- Почерк
    t = ud[ud["type"] == "TRADE"].copy()
    b = t[t["side"] == "BUY"]
    L += ["", "## 3. Почерк (как он торгует технически)"]
    verdict = []
    if not pw.empty:
        bs = pw["both_sides"].mean()
        pc = pw.loc[pw["both_sides"], "pair_cost"]
        L.append(f"Окон, где покупал ОБЕ стороны: {_pct(bs)}; средняя стоимость пары Up+Down: "
                 f"{pc.mean():.3f} (медиана {pc.median():.3f})" if len(pc) else f"Окон с обеими сторонами: {_pct(bs)}")
        L.append(f"MERGE (сливает пары в $1): {int((tr['type']=='MERGE').sum())}, REDEEM: {int((tr['type']=='REDEEM').sum())}, "
                 f"окон с продажами до конца: {_pct((pw['n_sells'] > 0).mean())}")
        if bs > 0.5 and len(pc) and pc.median() < 1:
            verdict.append("АРБИТРАЖ ПОЛНОГО СЕТА: покупает Up+Down суммарно дешевле $1")
    if "role" in t and t["role"].notna().any():
        rv = t["role"].value_counts(normalize=True)
        L.append("Роль в сделках (on-chain): " + ", ".join(f"{k} {_pct(v)}" for k, v in rv.items()))
        if rv.get("maker", 0) > 0.6:
            verdict.append("МАРКЕТ-МЕЙКЕР: в основном исполняются его лимитки")
    if len(t):
        fpw = t.groupby("slug").size()
        L.append(f"Филлов на окно: медиана {fpw.median():.0f}, p90 {fpw.quantile(.9):.0f}, макс {fpw.max()}")
        gaps = t.sort_values("ts").groupby("slug")["ts"].diff().dropna()
        if len(gaps):
            L.append(f"Интервал между филлами внутри окна: медиана {gaps.median():.0f}с; в ту же секунду: {_pct((gaps == 0).mean())}")
        sz = t["size"].round(2).value_counts(normalize=True).head(6)
        L.append("Самые частые размеры (шт): " + ", ".join(f"{k:g} ({_pct(v)})" for k, v in sz.items()))
        L.append(f"Сумма филла: медиана ${t['usdc'].median():.2f}, p90 ${t['usdc'].quantile(.9):.2f}, макс ${t['usdc'].max():.0f}")
        pb = pd.cut(b["price"], [0, .1, .2, .3, .4, .5, .6, .7, .8, .85, .9, .95, .97, .99, 1.0])
        L.append("")
        L.append("**Цена покупки → доля объёма и винрейт филла (исход = победитель):**")
        bb = b.merge(wn[["slug", "winner"]], on="slug", how="left")
        bb["won"] = np.where(bb["winner"].isna(), np.nan, (bb["outcome"] == bb["winner"]).astype(float))
        bb["bucket"] = pd.cut(bb["price"], pb.cat.categories)
        g = bb.groupby("bucket", observed=True).agg(n=("price", "size"), usd=("usdc", "sum"),
                                                     win=("won", "mean"), avgp=("price", "mean"))
        g["edge"] = g["win"] - g["avgp"]
        for k, r in g.iterrows():
            L.append(f"- {k}: {int(r['n'])} филлов, ${r['usd']:,.0f}, винрейт {_pct(r['win'])}, "
                     f"ср.цена {r['avgp']:.3f}, реальный edge {r['edge']*100:+.1f}пп" if pd.notna(r['win']) else
                     f"- {k}: {int(r['n'])} филлов, ${r['usd']:,.0f}")
        csvs["price_buckets.csv"] = g.reset_index()
        if "sec_left" in b:
            sl = pd.to_numeric(b["sec_left"], errors="coerce")
            cut = pd.cut(sl, [-1e9, 0, 30, 60, 120, 180, 300, 450, 600, 900],
                         labels=["до старта", "0-30", "30-60", "60-120", "120-180", "180-300", "300-450",
                                 "450-600", "600-900"])
            L.append("Когда покупает (сек до конца окна): " +
                     ", ".join(f"{k}: {_pct(v)}" for k, v in cut.value_counts(normalize=True).sort_index().items()))
        if "window_offset" in b:
            L.append(f"Покупки НЕ в текущем окне (заранее): {_pct(b['window_offset'].notna().mean())}")

    # --- Направление
    L += ["", "## 4. Логика направления (какую сторону берёт)"]
    for src in ("live", "hist"):
        bs_ = b[b["src"] == src] if "src" in b else b.iloc[:0]
        if len(bs_) < 10:
            continue
        S = sidefy(bs_, bs_["outcome"] == "up")
        parts = [f"[{src}, {len(bs_)} покупок]"]
        for c, name in (("my_delta_bps", "сторона, которая СЕЙЧАС выигрывает"),
                        ("my_ret5s_bps", "по импульсу 5с"), ("my_ret30s_bps", "по импульсу 30с"),
                        ("my_ret180s_bps", "по импульсу 3м"), ("my_m1_ema9_21", "по тренду EMA 1m"),
                        ("my_m5_macdh_bps", "по MACD 5m"), ("my_cvd60s", "по потоку CVD 60с")):
            if c in S and S[c].notna().sum() > 10:
                parts.append(f"{name}: {_pct((S[c] > 0).mean())}")
        if "my_edge" in S and S["my_edge"].notna().sum() > 10:
            parts.append(f"edge модели (fair−ask) медиана {S['my_edge'].median()*100:+.1f}пп, >0 в {_pct((S['my_edge'] > 0).mean())}")
        if "my_edge_h" in S and S["my_edge_h"].notna().sum() > 10:
            parts.append(f"edge к цене PM медиана {S['my_edge_h'].median()*100:+.1f}пп")
        L.append("- " + "; ".join(parts))
        if "my_delta_bps" in S and not any(v.startswith("ПОЗДНИЙ") for v in verdict):
            al = (S["my_delta_bps"] > 0).mean()
            if al > 0.85 and b["price"].median() > 0.75:
                verdict.append("ПОЗДНИЙ ФАВОРИТ: докупает уже выигрывающую сторону по высокой цене")
            m5 = (S.get("my_ret5s_bps", pd.Series(dtype=float)) > 0).mean()
            if m5 > 0.7 and not any(v.startswith("ЛАТЕНТ") for v in verdict):
                verdict.append("ЛАТЕНТНЫЙ АРБИТРАЖ: входит сразу после рывка Binance, пока стакан PM не догнал")

    # --- Признаки и правила
    L += ["", "## 5. Чем моменты его входов отличаются от всех остальных"]
    L.append("AUC: 0.5 = не отличается, >0.7 или <0.3 = сильный фильтр. Признаки my_* развёрнуты в сторону "
             "выбранного исхода (my_delta_bps>0 = цена на стороне его ставки).")
    for src, title in (("live", "Живые данные (полный набор: стакан PM, Chainlink, биржи)"),
                       ("hist", "История (Binance 1s + цены PM)")):
        E, C, _ = entries_controls(tr, sn, src)
        if E.empty or C.empty:
            L.append(f"\n### {title}\nПока нет данных.")
            continue
        ft = feature_table(E, C)
        csvs[f"features_{src}.csv"] = ft
        L.append(f"\n### {title}: топ-15 отличающих признаков")
        for r in ft.head(15).itertuples():
            L.append(f"- {r.feature}: AUC {r.auc:.2f} | у него медиана {r.entry_med:.4g} "
                     f"[{r.entry_p10:.4g}…{r.entry_p90:.4g}] | обычно {r.ctrl_med:.4g}")
        txt, rules = tree_rules(E, C, f"Дерево решений — {title}")
        L += ["", txt]
        if not rules.empty:
            csvs[f"rules_{src}.csv"] = rules

    # --- Вывод
    L += ["", "## 6. Гипотеза стиля (автоматически)"]
    L += [f"- {v}" for v in verdict] or ["- Однозначного паттерна нет — нужна ручная разборка CSV."]

    # --- CSV входов
    keep = [c for c in ["dt", "ts", "source", "type", "asset", "slug", "outcome", "side", "price", "size", "usdc",
                        "role", "tx"] if c in tr]
    ent = tr[keep].copy()
    if len(b):
        S = sidefy(b, b["outcome"] == "up")
        ent = ent.join(S, how="left")
    raw = [c for c in tr.columns if c not in ent.columns and c not in ("key", "detected_ts")]
    ent = ent.join(tr[raw], how="left").sort_values("ts")
    csvs["trades_enriched.csv"] = ent
    csvs["trades_last_4h.csv"] = ent[ent["ts"] > time.time() - 4 * 3600]
    L.append(f"\n_Анализ занял {time.time()-t0:.0f}с_")

    head = [f"🧬 Профиль {WALLET[:8]}…", L[4] if len(L) > 4 else ""]
    if not res.empty:
        head.append(f"PnL по окнам: ${res['pnl'].sum():,.0f}, окон {len(res)}, плюсовых {_pct((res['pnl'] > 0).mean())}")
    head += [f"• {v}" for v in verdict] or ["• Стиль пока не определён"]
    head.append(f"Сделок за 4ч: {len(csvs['trades_last_4h.csv'])}")
    return _write(L, csvs, "\n".join(head))


def _write(lines, csvs, caption):
    os.makedirs(OUT_DIR, exist_ok=True)
    name = time.strftime("profile_%Y%m%d_%H%M", time.gmtime())
    path = os.path.join(OUT_DIR, name + ".zip")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("report.md", "\n".join(lines))
        for n, df in csvs.items():
            z.writestr(n, df.to_csv(index=False))
    return path, caption, "\n".join(lines)
