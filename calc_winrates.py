"""
Bereken win rates per congreslid.
Voor elke BUY trade: vergelijk koers op trade-datum met koers 30 dagen later.
Als koers steeg → win. Als daalde → loss. Win rate = wins / totaal.

Gebruik:
    python calc_winrates.py
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("winrates")

DATA_DIR = Path(__file__).parent / "data"
OUTPUT   = DATA_DIR / "winrates.json"
HOLD_DAYS = 30  # vergelijk koers na 30 dagen


def load_all_trades() -> pd.DataFrame:
    """Laad alle trades uit alle bronnen."""
    rows = []
    for pattern in ["insider_finance.json", "congress_trades_*.json", "capitol_trades.json"]:
        for f in DATA_DIR.glob(pattern):
            try:
                data = json.load(open(f, encoding="utf-8"))
                rows.extend(data)
            except Exception:
                pass

    if not rows:
        return pd.DataFrame()

    trades = []
    for item in rows:
        ticker = (item.get("ticker") or item.get("symbol") or "").strip().upper()
        if not ticker or not ticker.isalpha() or len(ticker) > 5:
            continue
        tx = (item.get("type") or "").upper()
        if "PURCHASE" in tx or "BUY" in tx:
            tx = "BUY"
        elif "SALE" in tx or "SELL" in tx:
            tx = "SELL"
        else:
            continue
        date_str = item.get("transaction_date") or item.get("transactionDate") or ""
        try:
            dt = pd.to_datetime(date_str)
        except Exception:
            continue
        member = item.get("member") or item.get("representative") or ""
        if not member:
            continue
        trades.append({"member": member, "ticker": ticker, "type": tx, "date": dt})

    df = pd.DataFrame(trades)
    df = df.drop_duplicates(subset=["member", "ticker", "date"])
    return df


def download_prices(tickers: list[str]) -> pd.DataFrame:
    """Download prijsdata voor alle tickers in 1 batch."""
    log.info("Downloading prices for %d tickers...", len(tickers))
    start = "2014-01-01"
    end = pd.Timestamp.now().strftime("%Y-%m-%d")

    try:
        raw = yf.download(tickers, start=start, end=end, progress=False, auto_adjust=True)
        if isinstance(raw.columns, pd.MultiIndex):
            prices = raw["Close"]
        else:
            prices = raw
        prices.index = pd.to_datetime(prices.index).tz_localize(None)
        log.info("Got prices: %d days x %d tickers", len(prices), len(prices.columns))
        return prices
    except Exception as e:
        log.error("Price download failed: %s", e)
        return pd.DataFrame()


def calc_trade_outcome(prices: pd.DataFrame, ticker: str, trade_date: pd.Timestamp) -> str | None:
    """Return 'win', 'loss', of None als data niet beschikbaar."""
    if ticker not in prices.columns:
        return None

    col = prices[ticker].dropna()
    # Zoek dichtstbijzijnde handelsdag op of na trade_date
    future = col[col.index >= trade_date]
    if len(future) < 2:
        return None

    entry_date = future.index[0]
    entry_price = future.iloc[0]

    # Zoek prijs na HOLD_DAYS
    target_date = entry_date + pd.Timedelta(days=HOLD_DAYS)
    after = col[col.index >= target_date]

    if len(after) == 0:
        # Nog geen 30 dagen verstreken — gebruik laatste beschikbare prijs
        exit_price = col.iloc[-1]
    else:
        exit_price = after.iloc[0]

    if pd.isna(entry_price) or pd.isna(exit_price) or entry_price == 0:
        return None

    return "win" if exit_price > entry_price else "loss"


def main():
    df = load_all_trades()
    if df.empty:
        log.error("No trades found")
        return

    # Focus op BUY trades (win rate gaat over koop-beslissingen)
    buys = df[df["type"] == "BUY"].copy()
    log.info("Total BUY trades: %d", len(buys))

    # Unieke tickers
    tickers = sorted(buys["ticker"].unique().tolist())
    log.info("Unique tickers to price: %d", len(tickers))

    # Download in batches van 100 (yfinance limiet)
    all_prices = pd.DataFrame()
    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        log.info("  Batch %d-%d / %d", i + 1, min(i + batch_size, len(tickers)), len(tickers))
        prices = download_prices(batch)
        if not prices.empty:
            all_prices = pd.concat([all_prices, prices], axis=1)
        time.sleep(1)

    if all_prices.empty:
        log.error("No price data downloaded")
        return

    log.info("Calculating outcomes for %d trades...", len(buys))

    # Bereken outcome per trade
    results = []
    for _, trade in buys.iterrows():
        outcome = calc_trade_outcome(all_prices, trade["ticker"], trade["date"])
        if outcome:
            results.append({
                "member": trade["member"],
                "ticker": trade["ticker"],
                "date": str(trade["date"].date()),
                "outcome": outcome,
            })

    log.info("Outcomes calculated: %d / %d trades", len(results), len(buys))

    # Aggregeer per lid
    rdf = pd.DataFrame(results)
    winrates = {}

    for member, grp in rdf.groupby("member"):
        wins = (grp["outcome"] == "win").sum()
        losses = (grp["outcome"] == "loss").sum()
        total = wins + losses
        if total < 3:  # minimaal 3 trades voor betrouwbare win rate
            continue
        winrates[member] = {
            "wins": int(wins),
            "losses": int(losses),
            "total": int(total),
            "win_rate": round(wins / total * 100, 1),
        }

    # Sorteer op win rate
    winrates = dict(sorted(winrates.items(), key=lambda x: x[1]["win_rate"], reverse=True))

    # Save
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(winrates, f, indent=2, ensure_ascii=False)

    log.info("Saved %d member win rates to %s", len(winrates), OUTPUT)

    # Top 10
    print(f"\nTop 10 Win Rates (min 3 trades):")
    for i, (name, stats) in enumerate(list(winrates.items())[:10]):
        print(f"  {i+1:2d}. {name:30s} | {stats['win_rate']:5.1f}% | {stats['wins']}W/{stats['losses']}L ({stats['total']} trades)")

    print(f"\nBottom 5:")
    for name, stats in list(winrates.items())[-5:]:
        print(f"      {name:30s} | {stats['win_rate']:5.1f}% | {stats['wins']}W/{stats['losses']}L")


if __name__ == "__main__":
    main()
