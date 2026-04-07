"""
Capitol Trades v2 Scraper — per politicus alle trades ophalen.
Gebruikt de sitemap voor 201 politici-IDs, dan per politicus alle pagina's.

Gebruik:
    python scraper_capitol_v2.py              # alle politici
    python scraper_capitol_v2.py --max 10     # eerste 10
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
log = logging.getLogger("capitol_v2")

SITEMAP_URL = "https://www.capitoltrades.com/politicians/sitemap.xml"
BASE_URL    = "https://www.capitoltrades.com/politicians"
OUTPUT_DIR  = Path(__file__).parent / "data"
HEADERS     = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

BSLASH_QUOTE = chr(92) + '"'
BSLASH_N = chr(92) + "n"


def get_politician_ids() -> list[str]:
    r = requests.get(SITEMAP_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    ids = re.findall(r'/politicians/([A-Z]\d{5,6})', r.text)
    log.info("Found %d politician IDs in sitemap", len(ids))
    return sorted(set(ids))


def parse_page_trades(html: str) -> tuple[list[dict], str, int]:
    """Parse trades + politician name + total count from HTML."""
    scripts = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    merged = "".join(scripts)
    unescaped = merged.replace(BSLASH_QUOTE, '"').replace(BSLASH_N, '\n')

    # Naam
    name_m = re.search(r'"firstName":"([^"]+)","gender":"[^"]*","lastName":"([^"]+)"', unescaped)
    name = f"{name_m.group(1)} {name_m.group(2)}" if name_m else ""

    # Party
    party_m = re.search(r'"party":"([^"]+)"', unescaped)
    party = party_m.group(1) if party_m else ""

    # Total count
    total_m = re.search(r'"totalCount":(\d+)', unescaped)
    total = int(total_m.group(1)) if total_m else 0

    # Trades array
    idx = unescaped.find('"data":[{"_issuerId')
    if idx < 0:
        return [], name, total

    try:
        arr_start = unescaped.index('[', idx + 6)
        bc = 0
        result = ""
        for i in range(arr_start, min(arr_start + 500000, len(unescaped))):
            c = unescaped[i]
            result += c
            if c == '[': bc += 1
            elif c == ']': bc -= 1
            if bc == 0: break
        raw = json.loads(result)
    except (json.JSONDecodeError, ValueError):
        return [], name, total

    if not raw or not isinstance(raw, list):
        return [], name, total

    trades = []
    for t in raw:
        if not isinstance(t, dict) or "txDate" not in t:
            continue
        issuer = t.get("issuer") or {}
        pol = t.get("politician") or {}
        ticker_raw = issuer.get("issuerTicker") or ""
        ticker = ticker_raw.split(":")[0] if ticker_raw and ":" in ticker_raw else (ticker_raw or "")

        trades.append({
            "ticker": ticker,
            "asset_name": issuer.get("issuerName", ""),
            "sector": issuer.get("sector", ""),
            "member": name or f"{pol.get('firstName','')} {pol.get('lastName','')}".strip(),
            "party": (pol.get("party") or party or "")[:1].upper(),
            "chamber": t.get("chamber", ""),
            "state": pol.get("_stateId", ""),
            "politician_id": t.get("_politicianId", ""),
            "owner": t.get("owner", ""),
            "type": "BUY" if t.get("txType") == "buy" else "SELL" if t.get("txType") == "sell" else (t.get("txType") or "").upper(),
            "transaction_date": t.get("txDate", ""),
            "pub_date": t.get("pubDate", ""),
            "reporting_gap": t.get("reportingGap"),
            "value": t.get("value"),
            "price": t.get("price"),
            "source": "capitol_trades",
        })

    return trades, name, total


def scrape_politician(pol_id: str, delay: float = 1.5) -> list[dict]:
    """Scrape alle trades voor 1 politicus (alle pagina's)."""
    all_trades = []
    page = 1

    while True:
        url = f"{BASE_URL}/{pol_id}?page={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                break
        except Exception:
            break

        trades, name, total = parse_page_trades(r.text)
        if not trades:
            break

        all_trades.extend(trades)

        if len(all_trades) >= total or page > (total // 15 + 2):
            break

        page += 1
        time.sleep(delay)

    return all_trades


def scrape_all(max_politicians: int = 0, delay: float = 1.5) -> list[dict]:
    pol_ids = get_politician_ids()
    if max_politicians > 0:
        pol_ids = pol_ids[:max_politicians]

    # Tussentijds opslaan zodat we niet alles verliezen bij crash
    all_trades = []
    ua_list = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    ]
    import random

    for i, pid in enumerate(pol_ids):
        HEADERS["User-Agent"] = random.choice(ua_list)
        trades = scrape_politician(pid, delay=delay)
        if trades:
            name = trades[0]["member"]
            all_trades.extend(trades)
            log.info("[%d/%d] %s (%s): %d trades", i + 1, len(pol_ids), name, pid, len(trades))
        else:
            log.debug("[%d/%d] %s: no trades", i + 1, len(pol_ids), pid)

        # Tussentijds opslaan elke 10 politici
        if (i + 1) % 10 == 0:
            save(all_trades)
            log.info("Progress: %d/%d politicians | %d total trades (saved)", i + 1, len(pol_ids), len(all_trades))
            time.sleep(delay * 5)  # extra pauze na batch

        time.sleep(delay + random.uniform(0.5, 2.0))

    log.info("Done: %d politicians -> %d trades", len(pol_ids), len(all_trades))
    return all_trades


def save(trades: list[dict]) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / "capitol_trades.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2, ensure_ascii=False)
    log.info("Saved %d trades to %s", len(trades), path)
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=0, help="Max politicians (0=all)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay per request")
    args = parser.parse_args()

    trades = scrape_all(args.max, args.delay)
    if trades:
        save(trades)
        tickers = set(t["ticker"] for t in trades if t["ticker"])
        members = set(t["member"] for t in trades)
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
