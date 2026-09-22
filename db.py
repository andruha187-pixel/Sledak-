import json
import os
import sqlite3
import threading

from config import DB_PATH

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
_conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
_lock = threading.Lock()
_conn.execute("PRAGMA journal_mode=WAL")
_conn.executescript("""
CREATE TABLE IF NOT EXISTS trades(
  key TEXT PRIMARY KEY, ts INTEGER, source TEXT, type TEXT, asset TEXT, slug TEXT, token TEXT,
  outcome TEXT, side TEXT, price REAL, size REAL, usdc REAL, tx TEXT, role TEXT,
  detected_ts REAL, feat TEXT);
CREATE INDEX IF NOT EXISTS ix_tr_ts ON trades(ts);
CREATE INDEX IF NOT EXISTS ix_tr_slug ON trades(slug);
CREATE TABLE IF NOT EXISTS snapshots(ts REAL, asset TEXT, slug TEXT, src TEXT, feat TEXT);
CREATE INDEX IF NOT EXISTS ix_sn_ts ON snapshots(ts);
CREATE TABLE IF NOT EXISTS windows(slug TEXT PRIMARY KEY, asset TEXT, start INTEGER, end INTEGER,
  up_token TEXT, dn_token TEXT, open_cl REAL, close_cl REAL, winner TEXT, hist_done INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
""")

TRADE_COLS = ["key", "ts", "source", "type", "asset", "slug", "token", "outcome", "side",
              "price", "size", "usdc", "tx", "role", "detected_ts", "feat"]


def execute(sql, args=()):
    with _lock:
        return _conn.execute(sql, args)


def executemany(sql, rows):
    if not rows:
        return
    with _lock:
        _conn.execute("BEGIN")
        try:
            _conn.executemany(sql, rows)
            _conn.execute("COMMIT")
        except Exception:
            _conn.execute("ROLLBACK")
            raise


def query(sql, args=()):
    with _lock:
        return _conn.execute(sql, args).fetchall()


def meta_get(k, default=None):
    r = query("SELECT v FROM meta WHERE k=?", (k,))
    return r[0][0] if r else default


def meta_set(k, v):
    execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


def insert_trades(rows):
    """rows: list of dict. INSERT OR IGNORE — повторы безопасны."""
    data = []
    for r in rows:
        feat = r.get("feat")
        if isinstance(feat, dict):
            feat = json.dumps(feat, separators=(",", ":"), default=float)
        data.append(tuple(r.get(c) if c != "feat" else feat for c in TRADE_COLS))
    executemany(f"INSERT OR IGNORE INTO trades({','.join(TRADE_COLS)}) VALUES({','.join('?'*len(TRADE_COLS))})", data)


def set_trade_feat(key, feat, source=None):
    js = json.dumps(feat, separators=(",", ":"), default=float)
    if source:
        execute("UPDATE trades SET feat=?, source=? WHERE key=?", (js, source, key))
    else:
        execute("UPDATE trades SET feat=? WHERE key=?", (js, key))


def insert_snapshots(rows):
    """rows: list of (ts, asset, slug, src, feat_dict)"""
    executemany("INSERT INTO snapshots(ts,asset,slug,src,feat) VALUES(?,?,?,?,?)",
                [(a, b, c, d, json.dumps(e, separators=(",", ":"), default=float)) for a, b, c, d, e in rows])


def upsert_window(slug, asset, start, up=None, dn=None, winner=None):
    execute("INSERT OR IGNORE INTO windows(slug,asset,start,end) VALUES(?,?,?,?)", (slug, asset, start, start + 900))
    if up and dn:
        execute("UPDATE windows SET up_token=?, dn_token=? WHERE slug=?", (up, dn, slug))
    if winner:
        execute("UPDATE windows SET winner=? WHERE slug=?", (winner, slug))
