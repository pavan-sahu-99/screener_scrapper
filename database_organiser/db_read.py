import sqlite3
import pandas as pd

# ── Constants ──────────────────────────────────────────────────────────────────
DB_RAW     = "data/raw_html.db"
DB_FUND    = "data/fundamentals.db"
DB_DERIVED = "data/derived_ratios.db"

# ── Read Databases ───────────────────────────────────────────────────────
def _read_raw_db(db_path: str = DB_RAW):
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM raw_html", con)
    con.close()
    return df
