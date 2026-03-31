#database_organiser/db_init.py
import sqlite3
import pandas as pd
import logging
# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
DB_RAW     = "data/raw_html.db"
DB_FUND    = "data/fundamentals.db"
DB_DERIVED = "data/derived_ratios.db"

# ── Initialize Databases ───────────────────────────────────────────────────────
def _init_raw_db(db_path: str = DB_RAW):
    con = sqlite3.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_html (
            symbol      TEXT PRIMARY KEY,
            html        TEXT NOT NULL,
            scraped_at  TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

def melt_df(df, symbol):
    df_long = df.melt(
        id_vars=["metric"],
        var_name="period",
        value_name="value"
    )
    df_long["symbol"] = symbol
    df_long["scraped_at"] = pd.Timestamp.now().isoformat()
    return df_long

def _upsert_dict(con: sqlite3.Connection, table: str, data: dict):
    """Insert or replace a single dict row into table"""
    # Flatten lists to string
    flat = {k: (", ".join(v) if isinstance(v, list) else v) for k, v in data.items()}
    cols = ", ".join(flat.keys())
    placeholders = ", ".join(["?"] * len(flat))
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} (symbol TEXT)", )
    # Add missing columns
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    for col in flat.keys():
        if col not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
    con.execute(
        f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})",
        list(flat.values()),
    )


def _upsert_df(con, table, df, symbol):
    if df.empty:
        return

    df = melt_df(df, symbol)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            symbol     TEXT,
            metric     TEXT,
            period     TEXT,
            value      TEXT,
            scraped_at TEXT,
            PRIMARY KEY (symbol, metric, period)
        )
    """)

    rows = df.to_dict('records')
    con.executemany(f"""
        INSERT OR REPLACE INTO {table} (symbol, metric, period, value, scraped_at)
        VALUES (:symbol, :metric, :period, :value, :scraped_at)
    """, rows)

def store_all(symbol: str, data: dict, db_path: str = DB_FUND):
    con = sqlite3.connect(db_path)
    try:
        _upsert_dict(con, "company_info",    data["company_info"])
        _upsert_dict(con, "top_ratios",      data["top_ratios"])
        _upsert_dict(con, "analysis",        data["analysis"])
        _upsert_dict(con, "peers_meta",      data["peers_meta"])
        _upsert_df(con,   "quarterly",       data["quarterly"],       symbol)
        _upsert_df(con,   "profit_loss",     data["profit_loss"],     symbol)
        _upsert_df(con,   "balance_sheet",   data["balance_sheet"],   symbol)
        _upsert_df(con,   "cash_flow",       data["cash_flow"],       symbol)
        _upsert_df(con,   "ratios_screener", data["ratios_screener"], symbol)
        _upsert_df(con,   "shareholding",    data["shareholding"],    symbol)
        con.commit()
        log.info(f"[{symbol}] Stored to {db_path}")
    except Exception as e:
        con.rollback()
        log.error(f"[{symbol}] DB error: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    pass