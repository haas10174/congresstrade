"""
Congressional Disclosure Scraper
Downloadt PTR (Periodic Transaction Report) PDFs van de officiele House Clerk
en parset de stock trades eruit. Slaat op als JSON.

Gebruik:
    python scraper.py              # scrape 2025
    python scraper.py --year 2024  # scrape 2024
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import pdfplumber
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("scraper")

BASE_URL    = "https://disclosures-clerk.house.gov/public_disc"
INDEX_URL   = BASE_URL + "/financial-pdfs/{year}FD.zip"
PTR_PDF_URL = BASE_URL + "/ptr-pdfs/{year}/{docid}.pdf"
OUTPUT_DIR  = Path(__file__).parent / "data"
HEADERS     = {"User-Agent": "Mozilla/5.0 (CongressTracker research/educational)"}

# Regex voor trade regels in PDFs
# Patroon: [SP prefix] ASSET NAME  P/S  MM/DD/YYYY  MM/DD/YYYY  $AMOUNT
DATE_PAT    = re.compile(r"(\d{2}/\d{2}/\d{4})")
TRADE_PAT   = re.compile(
    r"^(.+?)\s+(P|S|PE|SE)\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(.+)$"
)
AMOUNT_PAT  = re.compile(r"\$([\d,]+)")
TICKER_PAT  = re.compile(r"\(([A-Z]{1,5})\)")
NAME_PAT    = re.compile(r"Name:\s*(.+)")
STATE_PAT   = re.compile(r"State/District:\s*(\w+)")


def fetch_index(year: int) -> list[dict]:
    """Download de FD ZIP en parse de XML index. Return lijst van PTR filings."""
    url = INDEX_URL.format(year=year)
    log.info("Downloading index: %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(resp.content))
    xml_name = f"{year}FD.xml"
    root = ET.fromstring(z.read(xml_name))

    ptrs = []
    for member in root:
        filing_type = (member.find("FilingType").text or "").strip()
        if filing_type != "P":  # P = Periodic Transaction Report
            continue
        ptrs.append({
            "name":     f"{(member.find('First').text or '').strip()} {(member.find('Last').text or '').strip()}",
            "state":    (member.find("StateDst").text or "").strip(),
            "date":     (member.find("FilingDate").text or "").strip(),
            "docid":    (member.find("DocID").text or "").strip(),
            "prefix":   (member.find("Prefix").text or "").strip(),
        })

    log.info("Found %d PTR filings for %d", len(ptrs), year)
    return ptrs


def download_pdf(year: int, docid: str) -> bytes | None:
    """Download een PTR PDF. Returns bytes of None bij fout."""
    url = PTR_PDF_URL.format(year=year, docid=docid)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.content
    except Exception as e:
        log.debug("Failed %s: %s", docid, e)
    return None


def parse_pdf(pdf_bytes: bytes, filing: dict) -> list[dict]:
    """Parse een PTR PDF en extract alle stock trades."""
    trades = []
    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
    except Exception as e:
        log.debug("PDF open failed: %s", e)
        return []

    for page in pdf.pages:
        text = page.extract_text() or ""
        lines = text.split("\n")

        # Parse naam en state indien niet al in filing
        name_match = NAME_PAT.search(text)
        state_match = STATE_PAT.search(text)

        for i, line in enumerate(lines):
            line = line.strip()
            if not line or "Digitally Signed" in line:
                continue

            # Kijk of de regel datums bevat (trade regel)
            dates = DATE_PAT.findall(line)
            if len(dates) < 2:
                continue

            # Probeer gestructureerde match
            # Soms staat "SP" prefix (voor Spouse) aan het begin
            clean = line
            owner = "Self"
            if clean.startswith("SP "):
                clean = clean[3:]
                owner = "Spouse"
            elif clean.startswith("JT "):
                clean = clean[3:]
                owner = "Joint"
            elif clean.startswith("DC "):
                clean = clean[3:]
                owner = "Dependent"

            m = TRADE_PAT.match(clean)
            if m:
                asset_raw   = m.group(1).strip()
                tx_type_raw = m.group(2).strip()
                tx_date     = m.group(3)
                report_date = m.group(4)
                amount_raw  = m.group(5).strip()
            else:
                # Fallback: zoek de twee datums en splits daaromheen
                parts = re.split(r"(\d{2}/\d{2}/\d{4})", clean)
                if len(parts) < 5:
                    continue
                asset_raw = parts[0].strip()
                # Zoek P of S vlak voor de eerste datum
                if asset_raw.endswith(" P") or asset_raw.endswith(" PE"):
                    tx_type_raw = "P"
                    asset_raw = asset_raw[:-2].strip()
                elif asset_raw.endswith(" S") or asset_raw.endswith(" SE"):
                    tx_type_raw = "S"
                    asset_raw = asset_raw[:-2].strip()
                else:
                    tx_type_raw = "?"
                tx_date     = parts[1]
                report_date = parts[3] if len(parts) > 3 else ""
                amount_raw  = parts[4].strip() if len(parts) > 4 else ""

            # Extract ticker uit asset naam
            ticker_match = TICKER_PAT.search(asset_raw)
            ticker = ticker_match.group(1) if ticker_match else ""

            # Clean asset name
            asset_name = TICKER_PAT.sub("", asset_raw).strip()
            asset_name = re.sub(r"\[ST\]|\[OP\]|\[OI\]|\[EF\]|\[CS\]", "", asset_name).strip()
            asset_name = re.sub(r"\s+", " ", asset_name).strip()

            # Transaction type
            tx_type = "BUY" if tx_type_raw in ("P", "PE") else "SELL" if tx_type_raw in ("S", "SE") else "UNKNOWN"

            # Amount
            amounts = AMOUNT_PAT.findall(amount_raw)
            amount_str = amount_raw.strip()

            # Parse dates
            try:
                tx_dt = datetime.strptime(tx_date, "%m/%d/%Y").strftime("%Y-%m-%d")
            except Exception:
                tx_dt = tx_date
            try:
                rp_dt = datetime.strptime(report_date, "%m/%d/%Y").strftime("%Y-%m-%d")
            except Exception:
                rp_dt = report_date

            # Bereken delay
            delay = None
            try:
                d1 = datetime.strptime(tx_date, "%m/%d/%Y")
                d2 = datetime.strptime(report_date, "%m/%d/%Y")
                delay = max(0, (d2 - d1).days)
            except Exception:
                pass

            trades.append({
                "member":           filing.get("name", name_match.group(1).strip() if name_match else "Unknown"),
                "state":            filing.get("state", state_match.group(1) if state_match else ""),
                "docid":            filing.get("docid", ""),
                "owner":            owner,
                "asset":            asset_name,
                "ticker":           ticker,
                "type":             tx_type,
                "transaction_date": tx_dt,
                "report_date":      rp_dt,
                "amount":           amount_str,
                "delay_days":       delay,
                "source":           "house_clerk_ptr",
            })

    pdf.close()
    return trades


def scrape(year: int, max_filings: int = 0, delay_sec: float = 0.3) -> list[dict]:
    """Volledige scrape pipeline voor een jaar."""
    ptrs = fetch_index(year)
    if max_filings > 0:
        ptrs = ptrs[:max_filings]

    all_trades = []
    failed = 0

    for i, filing in enumerate(ptrs):
        docid = filing["docid"]
        pdf_bytes = download_pdf(year, docid)
        if not pdf_bytes:
            failed += 1
            continue

        trades = parse_pdf(pdf_bytes, filing)
        all_trades.extend(trades)

        if (i + 1) % 25 == 0:
            log.info("  Progress: %d/%d filings | %d trades | %d failed",
                     i + 1, len(ptrs), len(all_trades), failed)

        time.sleep(delay_sec)  # respecteer de server

    log.info("Done: %d filings -> %d trades (%d failed)", len(ptrs), len(all_trades), failed)
    return all_trades


def save(trades: list[dict], year: int) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / f"congress_trades_{year}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2, ensure_ascii=False)
    log.info("Saved %d trades to %s", len(trades), path)
    return path


def main():
    parser = argparse.ArgumentParser(description="Scrape House Clerk PTR disclosures")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--max", type=int, default=0, help="Max filings (0=all)")
    parser.add_argument("--delay", type=float, default=0.3, help="Delay between requests")
    args = parser.parse_args()

    trades = scrape(args.year, args.max, args.delay)
    if trades:
        save(trades, args.year)


if __name__ == "__main__":
    main()
