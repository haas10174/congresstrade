"""
InsiderFinance Scraper
Haalt alle congress trades op in 1 request (Senate + House).
6200+ trades, geen paginering nodig.
Slaat op als JSON in data/insider_finance.json.

Gebruik:
    python scraper_insider.py
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("insider_scraper")

URL = "https://www.insiderfinance.io/congress-trades"
OUTPUT_DIR = Path(__file__).parent / "data"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def scrape() -> list[dict]:
    log.info("Fetching InsiderFinance congress trades...")
    resp = requests.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    match = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL
    )
    if not match:
        log.error("__NEXT_DATA__ not found")
        return []

    raw = json.loads(match.group(1))
    props = raw.get("props", {}).get("pageProps", {})

    senate_raw = props.get("data", [])
    house_raw = props.get("hdata", [])
    party_map = {
        p["name"]: p["party"] for p in props.get("politicalPartyForSenator", [])
    }

    trades = []

    # Senate trades
    for t in senate_raw:
        ticker = (t.get("symbol") or "").strip().upper()
        tx_type_raw = (t.get("type") or "").upper()
        if "PURCHASE" in tx_type_raw or "BUY" in tx_type_raw:
            tx_type = "BUY"
        elif "SALE" in tx_type_raw or "SELL" in tx_type_raw:
            tx_type = "SELL"
        elif "EXCHANGE" in tx_type_raw:
            tx_type = "EXCHANGE"
        else:
            tx_type = tx_type_raw

        name = f"{t.get('firstName', '')} {t.get('lastName', '')}".strip()
        party = t.get("party", "") or party_map.get(name, "")

        trades.append({
            "ticker": ticker,
            "asset_name": t.get("assetDescription", ""),
            "asset_type": t.get("assetType", ""),
            "member": name,
            "party": party[:1].upper() if party else "",
            "party_full": party,
            "chamber": "Senate",
            "owner": t.get("owner", ""),
            "type": tx_type,
            "transaction_date": t.get("transactionDate", ""),
            "disclosure_date": t.get("dateRecieved", ""),
            "amount": t.get("amount", ""),
            "comment": t.get("comment", ""),
            "disclosure_link": t.get("link", ""),
            "source": "insider_finance",
        })

    # House trades
    for t in house_raw:
        ticker = (t.get("ticker") or "").strip().upper()
        if ticker in ("--", "N/A", ""):
            ticker = ""
        tx_type_raw = (t.get("type") or "").upper()
        if "PURCHASE" in tx_type_raw or "BUY" in tx_type_raw:
            tx_type = "BUY"
        elif "SALE" in tx_type_raw or "SELL" in tx_type_raw:
            tx_type = "SELL"
        elif "EXCHANGE" in tx_type_raw:
            tx_type = "EXCHANGE"
        else:
            tx_type = tx_type_raw

        party = (t.get("party") or "")

        trades.append({
            "ticker": ticker,
            "asset_name": t.get("assetDescription", ""),
            "asset_type": "",
            "member": t.get("representative", ""),
            "party": party[:1].upper() if party else "",
            "party_full": party,
            "chamber": "House",
            "district": t.get("district", ""),
            "owner": t.get("owner", ""),
            "type": tx_type,
            "transaction_date": t.get("transactionDate", ""),
            "disclosure_date": t.get("disclosureDate", ""),
            "disclosure_year": t.get("disclosureYear", ""),
            "amount": t.get("amount", ""),
            "capital_gains_over_200": t.get("capitalGainsOver200USD", ""),
            "disclosure_link": t.get("link", ""),
            "source": "insider_finance",
        })

    log.info("Parsed %d Senate + %d House = %d total trades",
             len(senate_raw), len(house_raw), len(trades))
    return trades


def save(trades: list[dict]) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / "insider_finance.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2, ensure_ascii=False)
    log.info("Saved to %s", path)
    return path


def main():
    trades = scrape()
    if trades:
        save(trades)

        tickers = set(t["ticker"] for t in trades if t["ticker"])
        members = set(t["member"] for t in trades)
        buys = sum(1 for t in trades if t["type"] == "BUY")
        sells = sum(1 for t in trades if t["type"] == "SELL")

        print(f"\nSamenvatting:")
        print(f"  Totaal:   {len(trades)}")
        print(f"  BUY:      {buys}")
        print(f"  SELL:     {sells}")
        print(f"  Tickers:  {len(tickers)}")
        print(f"  Leden:    {len(members)}")
        print(f"  Senate:   {sum(1 for t in trades if t['chamber'] == 'Senate')}")
        print(f"  House:    {sum(1 for t in trades if t['chamber'] == 'House')}")


if __name__ == "__main__":
    main()
