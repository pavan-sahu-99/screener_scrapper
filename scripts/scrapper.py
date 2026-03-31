# scripts/scrapper.py
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import random
from datetime import datetime
import requests
import pandas as pd
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from database_organiser.db_init import _init_raw_db, DB_RAW
from threading import Lock
import threading

db_lock = Lock()
thread_local = threading.local()



# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/scrapper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# =========================
# CONSTANTS
# =========================
BASE_URL = "https://www.screener.in"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://www.screener.in/",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

RETRY_COUNT = 3
SESSION_REFRESH_EVERY = 30   
MAX_WORKERS = 3          
MIN_DELAY = 2.5
MAX_DELAY = 4.0

# =========================
# SESSION FACTORY
# =========================
def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(BASE_URL, timeout=10)
        time.sleep(1.0)
    except Exception:
        pass
    return s

session = make_session()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = make_session()
    return thread_local.session
# =========================
# INIT DB
# =========================
_init_raw_db(DB_RAW)


# =========================
# CHECK IF ALREADY FETCHED
# =========================
def already_fetched(symbol):
    con = sqlite3.connect(DB_RAW)
    cur = con.execute("SELECT 1 FROM raw_html WHERE symbol=?", (symbol,))
    exists = cur.fetchone() is not None
    con.close()
    return exists


# =========================
# FETCH WITH BACKOFF
# =========================
def fetch_soup(symbol: str):
    if already_fetched(symbol):
        logging.info(f"[{symbol}] Skipped (cached)")
        return "skipped"

    sess = get_session()
    url = f"{BASE_URL}/company/{symbol}/"
    resp = None

    for attempt in range(RETRY_COUNT):
        try:
            resp = sess.get(url, timeout=15)
            if resp.status_code == 429:
                backoff = (2 ** attempt) * 10 + random.uniform(0, 5)
                logging.warning(f"[{symbol}] 429. Sleeping {backoff:.1f}s")
                time.sleep(backoff)
                continue
            if resp.status_code != 200:
                time.sleep(2 ** attempt)
                continue
            break
        except requests.RequestException as e:
            backoff = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"[{symbol}] Error attempt {attempt+1}: {e}")
            time.sleep(backoff)

    if resp is None or resp.status_code != 200:
        logging.error(f"[{symbol}] Failed")
        return None

    if "Page Not Found" in resp.text:
        return "invalid"

    html = resp.text

    with db_lock:                 
        con = sqlite3.connect(DB_RAW, timeout=10)
        con.execute(
            "INSERT OR REPLACE INTO raw_html VALUES (?,?,?)",
            (symbol, html, datetime.now().isoformat()),
        )
        con.commit()
        con.close()

    logging.info(f"[{symbol}] Stored ({len(html)//1024} KB)")

    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))  
    return "success"

# =========================
# Parallel EXECUTION
# =========================
def fetch_data_universe(symbols):
    failed = []
    invalid = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_soup, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            result = future.result()
            if result is None:
                failed.append(sym)
            elif result == "invalid":
                invalid.append(sym)

    if failed:
        pd.DataFrame(failed, columns=["failed_symbols"]).to_csv("data/failed_symbols.csv", index=False)
    if invalid:
        pd.DataFrame(invalid, columns=["invalid_symbols"]).to_csv("data/invalid_symbols.csv", index=False)
    logging.info("Scraping completed.")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    df = pd.read_csv("data/instruments.csv")
    symbols = df['tradingsymbol'].dropna().unique().tolist()
    logging.info(f"Total symbols: {len(symbols)}")
    fetch_data_universe(symbols)