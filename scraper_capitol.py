"""
Capitol Trades Scraper
Scraped alle pagina's van capitoltrades.com (2912+ pagina's).
Parst de embedded RSC (React Server Components) data.
Slaat op als JSON in data/capitol_trades.json.

Gebruik:
    python scraper_capitol.py                  # alles (duurt ~30 min)
    python scraper_capitol.py --pages 50       # eerste 50 pagina's
    python scraper_capitol.py --pages 10 --test  # test run
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("capitol_scraper")

BASE_URL   = "https://www.capitoltrades.com/trades"
OUTPUT_DIR = Path(__file__).parent / "data"
HEADERS    = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}


BSLASH = "\\"
BSLASH_QUOTE = BSLASH + '"'
BSLASH_N = BSLASH + "n"
SEARCH_1 = '"data":[{"_issuerId'
SEARCH_2 = '"data":[{"_txId'


def parse_page(html: str) -> list[dict]:
    """Parse trades uit Capitol Trades HTML (Next.js RSC format)."""
    scripts = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    merged = "".join(scripts)

    unescaped = merged.replace(BSLASH_QUOTE, '"').replace(BSLASH_N, "\n")

    idx = unescaped.find(SEARCH_1)
    if idx < 0:
        idx = unescaped.find(SEARCH_2)
    if idx < 0:
        return []

    try:
        arr_start = unescaped.index("[", idx + 6)
        bracket_count = 0
        result = ""
        for i in range(arr_start, min(arr_start + 200000, len(unescaped))):
            c = unescaped[i]
            result += c
            if c == "[":
                bracket_count += 1
            elif c == "]":
                bracket_count -= 1
            if bracket_count == 0:
                break
        trades_raw = json.loads(result)
    except (json.JSONDecodeError, ValueError):
        return []

    if not isinstance(trades_raw, list) or not trades_raw:
        return []

    if not isinstance(trades_raw[0], dict) or "txDate" not in trades_raw[0]:
        return []

    trades = []
    for t in trades_raw:
        issuer = t.get("issuer") or {}
        pol = t.get("politician") or {}
        ticker_raw = issuer.get("issuerTicker") or ""
        ticker = ticker_raw.split(":")[0] if ticker_raw and ":" in ticker_raw else (ticker_raw or "")

        trades.append({
            "ticker":         ticker,
            "asset_name":     issuer.get("issuerName", ""),
            "sector":         issuer.get("sector", ""),
            "country":        issuer.get("country", ""),
            "member":         f"{pol.get('firstName', '')} {pol.get('lastName', '')}".strip(),
            "party":          (pol.get("party", "") or "")[:1].upper(),
            "chamber":        t.get("chamber", ""),
            "state":          pol.get("_stateId", ""),
            "politician_id":  t.get("_politicianId", ""),
            "owner":          t.get("owner", ""),
            "type":           "BUY" if t.get("txType") == "buy" else "SELL" if t.get("txType") == "sell" else t.get("txType", "").upper(),
            "tx_type_ext":    t.get("txTypeExtended", ""),
            "transaction_date": t.get("txDate", ""),
            "pub_date":       t.get("pubDate", ""),
            "reporting_gap":  t.get("reportingGap"),
            "value":          t.get("value"),
            "price":          t.get("price"),
            "source":         "capitol_trades",
        })
    return trades


def get_total_pages(html: str) -> int:
    """Probeer het totaal aantal pagina's te achterhalen."""
    # Zoek naar pagination info
    matches = re.findall(r'"totalPages"\s*:\s*(\d+)', html)
    if matches:
        return int(matches[0])
    # Zoek naar "page X of Y" patronen
    page_of = re.findall(r'(\d{3,4})\s*(?:pages|pagina)', html, re.I)
    if page_of:
        return int(page_of[0])
    return 0


def scrape(max_pages: int = 0, delay: float = 1.0, sort: str = "-txDate") -> list[dict]:
    """Scrape alle pagina's van Capitol Trades."""
    all_trades = []
    page = 1
    consecutive_empty = 0
    total_pages = max_pages or 3000  # fallback

    while page <= total_pages:
        if max_pages > 0 and page > max_pages:
            break

        url = f"{BASE_URL}?sortBy={sort}&page={page}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                log.warning("Page %d: HTTP %d", page, resp.status_code)
                consecutive_empty += 1
                if consecutive_empty >= 5:
                    log.warning("5 consecutive failures, stopping")
                    break
                time.sleep(delay * 3)
                page += 1
                continue

            # Op pagina 1: probeer totaal pagina's te achterhalen
            if page == 1:
                tp = get_total_pages(resp.text)
                if tp > 0:
                    total_pages = tp if max_pages == 0 else min(tp, max_pages)
                    log.info("Total pages detected: %d (scraping %d)", tp, total_pages)

            trades = parse_page(resp.text)
            log.debug("Page %d: %d chars HTML, %d trades parsed", page, len(resp.text), len(trades))
            if trades:
                all_trades.extend(trades)
                consecutive_empty = 0
            else:
                consecutive_empty += 1
                if consecutive_empty >= 5:
                    log.warning("5 consecutive empty pages at page %d, stopping", page)
                    break

        except Exception as e:
            log.warning("Page %d error: %s", page, str(e)[:80])
            consecutive_empty += 1

        if page % 25 == 0:
            log.info("Progress: page %d/%d | %d trades total", page, total_pages, len(all_trades))

        page += 1
        time.sleep(delay)

    log.info("Done: %d pages -> %d trades", page - 1, len(all_trades))
    return all_trades


def save(trades: list[dict]) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / "capitol_trades.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2, ensure_ascii=False)
    log.info("Saved %d trades to %s", len(trades), path)
    return path


def main():
    parser = argparse.ArgumentParser(description="Scrape Capitol Trades")
    parser.add_argument("--pages", type=int, default=0, help="Max pages (0=all)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (sec)")
    parser.add_argument("--test", action="store_true", help="Test run (5 pages)")
    args = parser.parse_args()

    pages = 5 if args.test else args.pages
    trades = scrape(max_pages=pages, delay=args.delay)
    if trades:
        save(trades)
        # Toon samenvatting
        tickers = set(t["ticker"] for t in trades if t["ticker"])
        members = set(t["member"] for t in trades if t["member"])
        buys = sum(1 for t in trades if t["type"] == "BUY")
        sells = sum(1 for t in trades if t["type"] == "SELL")
        print(f"\nSamenvatting:")
        print(f"  Trades:  {len(trades)}")
        print(f"  BUY:     {buys}")
        print(f"  SELL:    {sells}")
        print(f"  Tickers: {len(tickers)}")
        print(f"  Leden:   {len(members)}")


if __name__ == "__main__":
    main()
