# scripts/extractor.py
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import sqlite3
import numpy as np
import re
from typing import Optional
from database_organiser.db_read import _read_raw_db
from database_organiser.db_init import store_all
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging

MAX_WORKERS = 8
db_lock = Lock()

# ============== #
# Helper Functions
# ============== #

def _clean(text: str) -> str:
    """Strip whitespace and non-breaking spaces."""
    return text.replace("\xa0", " ").strip()

def _parse_number(text: str) -> Optional[float]:
    """Convert '1,23,456.78' or '45.6%' → float. Returns None if unparseable."""
    cleaned = re.sub(r"[₹,%\s]", "", text.replace(",", ""))
    try:
        return float(cleaned)
    except ValueError:
        return None

def _extract_generic_table(soup: BeautifulSoup, section_id: str) -> pd.DataFrame:
    """
    Generic table extractor for any section.
    Returns DataFrame with:
        - First column = row label (metric name)
        - Remaining columns = period headers (e.g. 'Mar 2024')
    """
    section = soup.find("section", {"id": section_id})
    if not section:
        return pd.DataFrame()

    holder = section.find("div", {"data-result-table": ""})
    table  = (holder or section).find("table")
    if not table:
        return pd.DataFrame()

    thead = table.find("thead")
    tbody = table.find("tbody")
    if not thead or not tbody:
        return pd.DataFrame()

    raw_headers = []
    for th in thead.find_all("th"):
        date_key = th.get("data-date-key")
        raw_headers.append(date_key if date_key else _clean(th.get_text()))

    rows = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        label = _clean(cells[0].get_text())
        values = [_clean(td.get_text()) for td in cells[1:]]
        rows.append([label] + values)

    col_count = max(len(r) for r in rows)
    if len(raw_headers) == col_count:
        cols = raw_headers
    elif len(raw_headers) == col_count + 1:
        cols = raw_headers[1:]          
    else:
        cols = raw_headers[:col_count]

    cols[0] = "metric"                  
    df = pd.DataFrame(rows, columns=cols[:len(rows[0])])
    return df


# ==================================#
# Individual Extraction Functions:
# ==================================#

def extract_company_info(soup: BeautifulSoup, symbol: str) -> dict:
    """
    Extract metadata:
        symbol, name, bse_code, nse_symbol, website,
        sector, industry, about, current_price, scraped_at,
        screener_company_id, screener_warehouse_id
    """
    info: dict = {"symbol": symbol, "scraped_at": datetime.now().isoformat()}

    # Internal IDs (useful for future API calls)
    meta_div = soup.find("div", {"id": "company-info"})
    if meta_div:
        info["screener_company_id"]   = meta_div.get("data-company-id", "")
        info["screener_warehouse_id"] = meta_div.get("data-warehouse-id", "")

    # Company name
    h1 = soup.find("h1", class_="margin-0")
    info["name"] = _clean(h1.get_text()) if h1 else ""

    # Current price
    price_div = soup.find("div", class_="font-size-18")
    if price_div:
        span = price_div.find("span")
        info["current_price"] = _clean(span.get_text()) if span else ""

    # BSE / NSE codes and website from company-links
    links_div = soup.find("div", class_="company-links")
    if links_div:
        for a in links_div.find_all("a"):
            href = a.get("href", "")
            text = _clean(a.get_text())
            if "bseindia.com" in href:
                # BSE code is at end of URL path
                info["bse_code"]    = href.rstrip("/").split("/")[-1]
                info["bse_url"]     = href
            elif "nseindia.com" in href:
                info["nse_symbol"]  = href.split("symbol=")[-1]
                info["nse_url"]     = href
            elif "bse" not in text.lower() and "nse" not in text.lower():
                info["website"]     = href

    # Sector / Industry from peers section breadcrumb
    peers = soup.find("section", {"id": "peers"})
    if peers:
        breadcrumb = peers.find("p", class_="sub")
        if breadcrumb:
            links = breadcrumb.find_all("a")
            labels = [a.get("title", "") for a in links]
            texts  = [_clean(a.get_text()) for a in links]
            for label, text in zip(labels, texts):
                if label == "Sector":
                    info["sector"] = text
                elif label == "Broad Sector":
                    info["broad_sector"] = text
                elif label == "Industry":
                    info["industry"] = text
                elif label == "Broad Industry":
                    info["broad_industry"] = text

    # About text
    about_div = soup.find("div", class_="about")
    if about_div:
        p = about_div.find("p")
        info["about"] = _clean(p.get_text()) if p else ""

    # Indices / Benchmarks 
    benchmarks_p = soup.find("p", {"id": "benchmarks"})
    if benchmarks_p:
        indices = [
            _clean(a.get_text())
            for a in benchmarks_p.find_all("a")
            if "hidden" not in (a.get("class") or [])
        ]
        info["indices"] = ", ".join(indices)

    return info

def extract_top_ratios(soup: BeautifulSoup, symbol: str) -> dict:
    """
    Extract sidebar key ratios:
        Market Cap, Current Price, High/Low, Stock P/E,
        Book Value, Dividend Yield, ROCE, ROE, Face Value
    """
    ratios: dict = {"symbol": symbol, "scraped_at": datetime.now().isoformat()}

    ul = soup.find("ul", {"id": "top-ratios"})
    if not ul:
        return ratios

    for li in ul.find_all("li"):
        name_span  = li.find("span", class_="name")
        value_span = li.find("span", class_="value")
        if not name_span or not value_span:
            continue

        key = _clean(name_span.get_text())
        # High / Low has two number spans — join with '/'
        numbers = [_clean(s.get_text()) for s in value_span.find_all("span", class_="number")]
        if len(numbers) > 1:
            raw_val = " / ".join(numbers)
        elif numbers:
            raw_val = numbers[0]
        else:
            raw_val = _clean(value_span.get_text())

        # Normalise key to snake_case column name
        col = key.lower().replace(" ", "_").replace("/", "_").replace(".", "")
        ratios[col]            = raw_val
        ratios[f"{col}_num"]   = _parse_number(numbers[0]) if numbers else None

    return ratios

def extract_analysis(soup: BeautifulSoup, symbol: str) -> dict:
    """
    Extract Pros & Cons.
    """
    result = {"symbol": symbol, "scraped_at": datetime.now().isoformat(),
              "pros": [], "cons": []}

    section = soup.find("section", {"id": "analysis"})
    if not section:
        return result

    pros_div = section.find("div", class_="pros")
    cons_div = section.find("div", class_="cons")

    if pros_div:
        result["pros"] = [_clean(li.get_text()) for li in pros_div.find_all("li")]
    if cons_div:
        result["cons"] = [_clean(li.get_text()) for li in cons_div.find_all("li")]

    result["pros_text"] = " | ".join(result["pros"])
    result["cons_text"] = " | ".join(result["cons"])
    result["pros_count"] = len(result["pros"])
    result["cons_count"] = len(result["cons"])
    return result


def extract_quarterly(soup: BeautifulSoup, symbol: str) -> pd.DataFrame:
    """
    Quarterly P&L results.
    Columns: metric, <date_keys...>, symbol, scraped_at
    Key metrics: Sales, Expenses, Operating Profit, OPM%, Net Profit, EPS
    """
    df = _extract_generic_table(soup, "quarters")
    if df.empty:
        return df
    df["symbol"]     = symbol
    df["scraped_at"] = datetime.now().isoformat()
    return df


def extract_profit_loss(soup: BeautifulSoup, symbol: str) -> pd.DataFrame:
    """
    Annual P&L: Sales, Expenses, OPM%, Other Income, Interest,
                Depreciation, Profit before tax, Tax, Net Profit, EPS, Dividend Payout
    """
    df = _extract_generic_table(soup, "profit-loss")
    if df.empty:
        return df
    df["symbol"]     = symbol
    df["scraped_at"] = datetime.now().isoformat()
    return df


def extract_balance_sheet(soup: BeautifulSoup, symbol: str) -> pd.DataFrame:
    """
    Annual Balance Sheet:
    Equity Capital, Reserves, Borrowings, Other Liabilities,
    Fixed Assets, CWIP, Investments, Other Assets, Total Assets
    """
    df = _extract_generic_table(soup, "balance-sheet")
    if df.empty:
        return df
    df["symbol"]     = symbol
    df["scraped_at"] = datetime.now().isoformat()
    return df


def extract_cash_flow(soup: BeautifulSoup, symbol: str) -> pd.DataFrame:
    """
    Annual Cash Flow:
    Cash from Operating Activity, Investing Activity,
    Financing Activity, Net Cash Flow
    """
    df = _extract_generic_table(soup, "cash-flow")
    if df.empty:
        return df
    df["symbol"]     = symbol
    df["scraped_at"] = datetime.now().isoformat()
    return df


def extract_ratios_table(soup: BeautifulSoup, symbol: str) -> pd.DataFrame:
    """
    Annual efficiency / valuation ratios pre-calculated by Screener:
    Debtor Days, Inventory Days, Days Payable, Cash Conversion Cycle,
    Working Capital Days, ROCE%, ROE%
    """
    df = _extract_generic_table(soup, "ratios")
    if df.empty:
        return df
    df["symbol"]     = symbol
    df["scraped_at"] = datetime.now().isoformat()
    return df


def extract_shareholding(soup: BeautifulSoup, symbol: str) -> pd.DataFrame:
    """
    Quarterly shareholding pattern:
    Promoters, FIIs, DIIs, Government, Public, No. of Shareholders
    Critical signal: promoter pledge / FII trend is a key institutional flow indicator.
    """
    section = soup.find("section", {"id": "shareholding"})
    if not section:
        return pd.DataFrame()

    # Use the quarterly tab (first table in #quarterly-shp)
    quarterly_div = section.find("div", {"id": "quarterly-shp"})
    if not quarterly_div:
        return pd.DataFrame()

    table = quarterly_div.find("table")
    if not table:
        return pd.DataFrame()

    thead = table.find("thead")
    tbody = table.find("tbody")
    headers = ["metric"] + [_clean(th.get_text()) for th in thead.find_all("th")[1:]]

    rows = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        label  = _clean(cells[0].get_text())
        values = [_clean(td.get_text()) for td in cells[1:]]
        rows.append([label] + values)

    df = pd.DataFrame(rows, columns=headers[:len(rows[0])] if rows else headers)
    df["symbol"]     = symbol
    df["scraped_at"] = datetime.now().isoformat()
    return df


def extract_peers(soup: BeautifulSoup, symbol: str) -> dict:
    """
    Extract sector / industry / index classification metadata from peers section.
    Peer table itself loads dynamically (JS), so we capture the static metadata only.
    Returns dict with broad_sector, sector, broad_industry, industry, indices.
    """
    info = {"symbol": symbol, "scraped_at": datetime.now().isoformat()}
    peers = soup.find("section", {"id": "peers"})
    if not peers:
        return info

    breadcrumb = peers.find("p", class_="sub")
    if breadcrumb:
        for a in breadcrumb.find_all("a"):
            title = a.get("title", "")
            text  = _clean(a.get_text())
            if title:
                key = title.lower().replace(" ", "_")
                info[key] = text

    benchmarks_p = soup.find("p", {"id": "benchmarks"})
    if benchmarks_p:
        all_tags = [_clean(a.get_text()) for a in benchmarks_p.find_all("a")]
        info["indices"] = ", ".join(all_tags)

    return info


def process_symbol(row):
    symbol = row['symbol']
    html = row['html']
    try:
        soup = BeautifulSoup(html, 'html.parser')
        data = {
            "company_info":    extract_company_info(soup, symbol),
            "top_ratios":      extract_top_ratios(soup, symbol),
            "analysis":        extract_analysis(soup, symbol),
            "peers_meta":      extract_peers(soup, symbol),
            "quarterly":       extract_quarterly(soup, symbol),
            "profit_loss":     extract_profit_loss(soup, symbol),
            "balance_sheet":   extract_balance_sheet(soup, symbol),
            "cash_flow":       extract_cash_flow(soup, symbol),
            "ratios_screener": extract_ratios_table(soup, symbol),
            "shareholding":    extract_shareholding(soup, symbol),
        }
        with db_lock:
            store_all(symbol, data)

        return symbol, "success"

    except Exception as e:
        logging.error(f"[{symbol}] Failed: {e}")
        return symbol, "failed"


def run_parallel_extraction(max_workers=MAX_WORKERS):
    df = _read_raw_db()
    rows = df.to_dict('records')
    logging.info(f"Extracting {len(rows)} symbols with {max_workers} workers")

    failed = []
    success = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_symbol, row): row['symbol'] for row in rows}

        for i, future in enumerate(as_completed(futures), 1):
            symbol, status = future.result()
            if status == "failed":
                failed.append(symbol)
            else:
                success += 1

            if i % 100 == 0:
                logging.info(f"Progress: {i}/{len(rows)} | Success: {success} | Failed: {len(failed)}")

    if failed:
        pd.DataFrame(failed, columns=["failed_symbols"]).to_csv(
            "data/extraction_failed.csv", index=False
        )
        logging.info(f"Failed: {len(failed)} symbols saved to extraction_failed.csv")

    logging.info(f"Done. {success}/{len(rows)} extracted successfully.")


if __name__ == "__main__":
    run_parallel_extraction()