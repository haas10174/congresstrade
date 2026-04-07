"""
Congressional Trading Tracker — Flask website
Poort 8051 | Data: Quiver Quantitative (gratis)
Foto's: theunitedstates.io/images/congress (via BioGuideID)
"""
from __future__ import annotations

import time
from datetime import datetime

from pathlib import Path

import pandas as pd
import requests
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────

QUIVER_URL = "https://api.quiverquant.com/beta/live/congresstrading"
PHOTO_URL  = "https://theunitedstates.io/images/congress/225x275/{bio_id}.jpg"

HIGH_ALPHA = {
    "Nancy Pelosi", "Paul Pelosi", "Dan Crenshaw", "Tommy Tuberville",
    "Josh Gottheimer", "Ro Khanna", "Michael McCaul", "Marjorie Taylor Greene",
    "Brian Mast", "David Rouzer",
}

AMOUNT_MAP = {
    "$1,001 - $15,000": 1_001, "$15,001 - $50,000": 15_001,
    "$50,001 - $100,000": 50_001, "$100,001 - $250,000": 100_001,
    "$250,001 - $500,000": 250_001, "$500,001 - $1,000,000": 500_001,
    "$1,000,001 - $5,000,000": 1_000_001, "Over $5,000,000": 5_000_001,
}

AMOUNT_BUCKETS = {
    "1-100k":  (1_001, 99_999),
    "100k-1m": (100_001, 999_999),
    "1m+":     (1_000_001, 999_999_999),
}

# ─── Data cache ───────────────────────────────────────────────────

_CACHE: dict = {"raw": None, "ts": 0.0}
_DF_CACHE: dict = {"df": None, "members": None, "ts": 0.0}


def fetch_raw() -> list[dict]:
    now = time.time()
    if _CACHE["raw"] and (now - _CACHE["ts"]) < 3600:
        return _CACHE["raw"]
    try:
        r = requests.get(QUIVER_URL, headers={"Accept": "application/json"}, timeout=15)
        data = r.json() if r.status_code == 200 else []
    except Exception:
        data = []
    _CACHE["raw"] = data
    _CACHE["ts"]  = now
    return data


def _load_scraped() -> list[dict]:
    """Laad alle lokaal gescrapete data (House Clerk, Capitol Trades, InsiderFinance)."""
    import json as _json
    data_dir = Path(__file__).parent / "data"
    all_trades = []
    for pattern in ["congress_trades_*.json", "capitol_trades.json", "insider_finance.json"]:
        for f in sorted(data_dir.glob(pattern)):
            try:
                with open(f, encoding="utf-8") as fh:
                    all_trades.extend(_json.load(fh))
            except Exception:
                pass
    return all_trades


def get_trades(lookback: int = 9999) -> pd.DataFrame:
    raw = fetch_raw()
    if not raw:
        rows = []
    else:
        cutoff = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=lookback)
        rows = []
        for item in raw:
            ticker = (item.get("Ticker") or "").strip().upper()
            if not ticker or not ticker.isalpha() or len(ticker) > 5:
                continue
            tx_raw = (item.get("Transaction") or "").upper()
            if "PURCHASE" in tx_raw or "BUY" in tx_raw:
                tx = "BUY"
            elif "SALE" in tx_raw or "SELL" in tx_raw:
                tx = "SELL"
            else:
                continue
            tx_str = item.get("TransactionDate") or ""
            rp_str = item.get("ReportDate") or ""
            try:
                tx_date = pd.to_datetime(tx_str) if tx_str else None
            except Exception:
                tx_date = None
            try:
                rp_date = pd.to_datetime(rp_str) if rp_str else None
            except Exception:
                rp_date = None
            dt = tx_date or rp_date
            if dt is None:
                continue
            dt_naive = pd.Timestamp(dt).tz_localize(None) if pd.Timestamp(dt).tzinfo else pd.Timestamp(dt)
            if dt_naive < cutoff:
                continue
            delay = max(0, (rp_date - tx_date).days) if tx_date and rp_date else None
            amount_str = (item.get("Range") or "").strip()
            amount_min = AMOUNT_MAP.get(amount_str, 0)

            member = item.get("Representative", "Unknown")
            rows.append({
                "ticker": ticker, "member": member,
                "party": item.get("Party", ""),
                "chamber": item.get("House", ""),
                "bio_id": item.get("BioGuideID", ""),
                "type": tx, "date": dt_naive.normalize(),
                "report_date": rp_date,
                "amount_str": amount_str, "amount_min": amount_min,
                "delay": delay,
                "high_alpha": member in HIGH_ALPHA,
                "asset_name": "",
            })

    # Merge met lokaal gescrapete data (House Clerk, Capitol Trades, InsiderFinance)
    scraped = _load_scraped()
    if rows:
        existing = {(r["member"], r["ticker"], str(r["date"].date())) for r in rows}
        cutoff2 = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=lookback)
    else:
        existing = set()
        cutoff2 = pd.Timestamp("2000-01-01")

    for item in scraped:
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
        dt2 = dt.tz_localize(None) if dt.tzinfo else dt
        if dt2 < cutoff2:
            continue
        member = item.get("member") or item.get("representative") or "Unknown"
        key = (member, ticker, str(dt2.date()))
        if key in existing:
            continue
        existing.add(key)
        rp_str = item.get("report_date") or item.get("disclosure_date") or item.get("pub_date") or ""
        try:
            rp = pd.to_datetime(rp_str) if rp_str else None
        except Exception:
            rp = None
        delay = item.get("delay_days") or item.get("reporting_gap")
        if delay is None and rp and dt2:
            try:
                delay = max(0, (rp.tz_localize(None) if rp.tzinfo else rp - dt2).days)
            except Exception:
                delay = None
        amount_str = item.get("amount") or item.get("amount_str") or ""
        party_raw = (item.get("party") or "")
        party = party_raw[:1].upper() if len(party_raw) == 1 else (
            "D" if "dem" in party_raw.lower() else
            "R" if "rep" in party_raw.lower() else
            party_raw[:1].upper()
        )
        rows.append({
            "ticker": ticker, "member": member,
            "party": party,
            "chamber": item.get("chamber") or "House",
            "bio_id": item.get("politician_id") or "",
            "type": tx, "date": dt2.normalize(),
            "report_date": rp,
            "amount_str": amount_str,
            "amount_min": item.get("value") or AMOUNT_MAP.get(amount_str, 0),
            "delay": delay,
            "high_alpha": member in HIGH_ALPHA,
            "asset_name": item.get("asset_name") or item.get("asset") or "",
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _load_member_db() -> dict[str, dict]:
    """Laad de volledige ledenlijst (538 leden) met foto URLs."""
    path = Path(__file__).parent / "data" / "members.json"
    if not path.exists():
        return {}
    try:
        import json as _json
        with open(path, encoding="utf-8") as f:
            data = _json.load(f)
        db = {}
        for m in data:
            db[m["name"].lower()] = m
            key2 = f"{m['first_name']} {m['last_name']}".lower()
            db[key2] = m
        return db
    except Exception:
        return {}


def _load_member_list() -> list[dict]:
    """Laad ruwe ledenlijst voor senate map."""
    path = Path(__file__).parent / "data" / "members.json"
    if not path.exists():
        return []
    try:
        import json as _json
        with open(path, encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return []


def _load_winrates() -> dict[str, dict]:
    """Laad voorberekende win rates uit data/winrates.json."""
    path = Path(__file__).parent / "data" / "winrates.json"
    if not path.exists():
        return {}
    try:
        import json as _json
        with open(path, encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return {}


def get_cached_data() -> tuple[pd.DataFrame, list[dict]]:
    """Cached versie — herberekent alleen elke 5 minuten."""
    now = time.time()
    if _DF_CACHE["df"] is not None and (now - _DF_CACHE["ts"]) < 300:
        return _DF_CACHE["df"], _DF_CACHE["members"]
    df = get_trades(9999)
    members = build_members(df)

    # Merge win rates
    winrates = _load_winrates()
    for m in members:
        wr = winrates.get(m["name"], {})
        m["win_rate"] = wr.get("win_rate")
        m["wins"] = wr.get("wins", 0)
        m["losses"] = wr.get("losses", 0)

    _DF_CACHE["df"] = df
    _DF_CACHE["members"] = members
    _DF_CACHE["ts"] = now
    return df, members


def build_members(df: pd.DataFrame) -> list[dict]:
    """Bouw per-lid statistieken. Voegt alle 538 leden toe (ook zonder trades)."""
    member_db = _load_member_db()
    seen_names = set()

    members = []

    # Eerst: leden met trades
    if not df.empty:
        for member, grp in df.groupby("member"):
            buys  = grp[grp["type"] == "BUY"]
            sells = grp[grp["type"] == "SELL"]
            bio_id = grp["bio_id"].iloc[0] if "bio_id" in grp.columns else ""
            party  = grp["party"].iloc[0]
            chamber = grp["chamber"].iloc[0]
            top_tickers = grp["ticker"].value_counts().head(5).to_dict()
            avg_delay = grp["delay"].mean() if grp["delay"].notna().any() else None
            last_trade = grp["date"].max()
            biggest = grp["amount_min"].max()

            slug = str(member).lower().replace(" ", "-").replace("(", "").replace(")", "").replace(",", "").replace(".", "")

            db_entry = member_db.get(str(member).lower(), {})
            photo = db_entry.get("photo_url", "")
            if not photo and bio_id:
                photo = PHOTO_URL.format(bio_id=bio_id)
            if not party and db_entry:
                party = db_entry.get("party", "")[:1].upper()
            if not chamber or chamber == "House":
                chamber = db_entry.get("chamber", chamber)

            members.append({
                "name": member,
                "slug": db_entry.get("slug", slug),
                "bio_id": db_entry.get("bio_id", bio_id),
                "photo": photo,
                "party": party,
                "party_label": {"R": "Republican", "D": "Democrat", "I": "Independent"}.get(party, party),
                "chamber": chamber,
                "state": db_entry.get("state", ""),
                "district": db_entry.get("district", ""),
                "total_trades": len(grp),
                "buys": len(buys),
                "sells": len(sells),
                "buy_pct": round(len(buys) / max(len(grp), 1) * 100),
                "top_tickers": top_tickers,
                "avg_delay": round(avg_delay, 1) if avg_delay else None,
                "last_trade": last_trade.strftime("%Y-%m-%d") if pd.notna(last_trade) else "",
                "last_trade_days": (pd.Timestamp.utcnow().tz_localize(None).normalize() - last_trade).days if pd.notna(last_trade) else 999,
                "biggest_trade": biggest,
                "high_alpha": str(member) in HIGH_ALPHA,
            })
            seen_names.add(str(member).lower())

    # Dan: alle leden zonder trades (uit members.json)
    for key, db_entry in member_db.items():
        if key in seen_names or db_entry["name"].lower() in seen_names:
            continue
        seen_names.add(db_entry["name"].lower())
        p_raw = db_entry.get("party", "")
        party = p_raw[:1].upper() if len(p_raw) == 1 else (
            "D" if "dem" in p_raw.lower() else
            "R" if "rep" in p_raw.lower() else
            p_raw[:1].upper()
        )
        members.append({
            "name": db_entry["name"],
            "slug": db_entry["slug"],
            "bio_id": db_entry.get("bio_id", ""),
            "photo": db_entry.get("photo_url", ""),
            "party": party,
            "party_label": {"R": "Republican", "D": "Democrat", "I": "Independent"}.get(party, p_raw),
            "chamber": db_entry.get("chamber", ""),
            "state": db_entry.get("state", ""),
            "district": db_entry.get("district", ""),
            "total_trades": 0,
            "buys": 0, "sells": 0, "buy_pct": 0,
            "top_tickers": {},
            "avg_delay": None,
            "last_trade": "",
            "last_trade_days": 9999,
            "biggest_trade": 0,
            "high_alpha": False,
        })

    members.sort(key=lambda m: m["total_trades"], reverse=True)
    return members


def get_member_trades(df: pd.DataFrame, slug: str) -> tuple[dict | None, list[dict]]:
    """Haal member info + trades op via slug."""
    members = build_members(df)
    member = next((m for m in members if m["slug"] == slug), None)
    if not member:
        return None, []
    if not df.empty and member["name"] in df["member"].values:
        trades = df[df["member"] == member["name"]].sort_values("date", ascending=False)
    else:
        trades = pd.DataFrame()
    trade_list = []
    for _, r in trades.iterrows():
        trade_list.append({
            "ticker": r["ticker"],
            "asset_name": r.get("asset_name", ""),
            "type": r["type"],
            "date": r["date"].strftime("%Y-%m-%d"),
            "amount": r["amount_str"],
            "amount_min": int(r.get("amount_min", 0) or 0),
            "delay": int(r["delay"]) if pd.notna(r.get("delay")) else None,
            "chamber": r.get("chamber", ""),
        })
    return member, trade_list


def build_all_trades(df: pd.DataFrame, member_db: dict) -> list[dict]:
    """Bouw een platte lijst van alle trades voor de Trades tab."""
    if df.empty:
        return []
    result = []
    df_sorted = df.sort_values("date", ascending=False)
    for _, r in df_sorted.iterrows():
        member_name = r["member"]
        db_entry = member_db.get(str(member_name).lower(), {})
        slug = db_entry.get("slug") or str(member_name).lower().replace(" ", "-").replace("(","").replace(")","").replace(",","").replace(".","")
        photo = db_entry.get("photo_url", "")
        party = r["party"] or db_entry.get("party", "")[:1].upper()
        result.append({
            "ticker": r["ticker"],
            "asset_name": r.get("asset_name", ""),
            "member": member_name,
            "slug": slug,
            "photo": photo,
            "party": party,
            "chamber": r.get("chamber", ""),
            "type": r["type"],
            "date": r["date"].strftime("%Y-%m-%d"),
            "amount": r["amount_str"],
            "amount_min": int(r.get("amount_min", 0) or 0),
        })
    return result


# ─── Routes ───────────────────────────────────────────────────────

@app.route("/")
def index():
    tab = request.args.get("tab", "politicians")
    days = int(request.args.get("days", 9999))
    sort = request.args.get("sort", "trades")
    party_filter = request.args.get("party", "all")
    chamber_filter = request.args.get("chamber", "all")
    winrate_filter = request.args.get("winrate", "all")
    search = (request.args.get("q") or "").strip()

    df, members = get_cached_data()

    # Filter for politicians tab
    members_filtered = list(members)
    if party_filter != "all":
        members_filtered = [m for m in members_filtered if m["party"] == party_filter]
    if chamber_filter != "all":
        members_filtered = [m for m in members_filtered if m["chamber"].lower().startswith(chamber_filter.lower())]
    if winrate_filter != "all":
        if winrate_filter == "70+":
            members_filtered = [m for m in members_filtered if m.get("win_rate") is not None and m["win_rate"] >= 70]
        elif winrate_filter == "50-70":
            members_filtered = [m for m in members_filtered if m.get("win_rate") is not None and 50 <= m["win_rate"] < 70]
        elif winrate_filter == "50-":
            members_filtered = [m for m in members_filtered if m.get("win_rate") is not None and m["win_rate"] < 50]
    if search:
        q = search.lower()
        members_filtered = [m for m in members_filtered if q in m["name"].lower() or any(q.upper() == t for t in m["top_tickers"])]

    if sort == "recent":
        members_filtered.sort(key=lambda m: m["last_trade_days"])
    elif sort == "alpha":
        members_filtered = [m for m in members_filtered if m["high_alpha"]] + [m for m in members_filtered if not m["high_alpha"]]
    elif sort == "buys":
        members_filtered.sort(key=lambda m: m["buys"], reverse=True)
    elif sort == "sells":
        members_filtered.sort(key=lambda m: m["sells"], reverse=True)
    elif sort == "volume":
        members_filtered.sort(key=lambda m: m["biggest_trade"], reverse=True)
    elif sort == "winrate":
        members_filtered.sort(key=lambda m: m.get("win_rate") or 0, reverse=True)

    top_performers = sorted(members, key=lambda m: m["total_trades"], reverse=True)[:5]

    total_trades = sum(m["total_trades"] for m in members)
    total_buys   = sum(m["buys"] for m in members)
    total_sells  = sum(m["sells"] for m in members)

    # Build senate map data
    all_members_raw = _load_member_list()
    senate_members = [m for m in all_members_raw if m.get("chamber", "").lower() == "senate"]
    # Enrich with trade counts
    trade_counts = {}
    if not df.empty:
        for name, cnt in df.groupby("member").size().items():
            trade_counts[str(name).lower()] = cnt
    for sm in senate_members:
        sm["trade_count"] = trade_counts.get(sm["name"].lower(), 0)
        p_raw = sm.get("party", "")
        sm["party_code"] = (
            "D" if "dem" in p_raw.lower() else
            "R" if "rep" in p_raw.lower() else
            "I"
        )

    return render_template("index.html",
        tab=tab,
        members=members_filtered,
        all_members_count=len(members),
        days=days, sort=sort, party=party_filter,
        chamber=chamber_filter, winrate=winrate_filter, search=search,
        top_performers=top_performers,
        total_trades=total_trades, total_buys=total_buys,
        total_sells=total_sells, total_members=len(members),
        senate_members=senate_members,
        now=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )


@app.route("/politician/<slug>")
def politician(slug: str):
    df, _ = get_cached_data()
    member, trades = get_member_trades(df, slug)
    if not member:
        return render_template("404.html"), 404

    # Calculate volume stats
    total_volume = sum(t["amount_min"] for t in trades if t["amount_min"])
    last_trade = trades[0]["date"] if trades else ""

    # Win rate placeholder (requires price data — show from pelosi_full if available)
    win_rate = None

    return render_template("politician.html",
        member=member, trades=trades,
        total_volume=total_volume,
        last_trade=last_trade,
        win_rate=win_rate,
    )


@app.route("/api/trades")
def api_trades():
    df, _ = get_cached_data()
    if df.empty:
        return jsonify([])
    result = df.sort_values("date", ascending=False).head(200)
    return jsonify(result.assign(date=result["date"].astype(str)).to_dict(orient="records"))


@app.route("/api/trades-filter")
def api_trades_filter():
    """AJAX endpoint for the Trades tab with all filters."""
    days = int(request.args.get("days", 9999))
    ticker_q = (request.args.get("ticker") or "").strip().upper()
    member_q = (request.args.get("member") or "").strip().lower()
    chamber_f = (request.args.get("chamber") or "all").lower()
    party_f = (request.args.get("party") or "all").upper()
    amount_f = (request.args.get("amount") or "all").lower()
    type_f = (request.args.get("type") or "all").upper()
    page = int(request.args.get("page", 1))
    per_page = 50

    df, _ = get_cached_data()
    if df.empty:
        return jsonify({"trades": [], "total": 0, "pages": 0})

    member_db = _load_member_db()

    # Apply filters
    if ticker_q:
        df = df[df["ticker"].str.contains(ticker_q, na=False)]
    if member_q:
        df = df[df["member"].str.lower().str.contains(member_q, na=False)]
    if chamber_f != "all":
        df = df[df["chamber"].str.lower().str.startswith(chamber_f)]
    if party_f != "ALL":
        df = df[df["party"] == party_f]
    if type_f != "ALL":
        df = df[df["type"] == type_f]
    if amount_f != "all" and amount_f in AMOUNT_BUCKETS:
        lo, hi = AMOUNT_BUCKETS[amount_f]
        df = df[(df["amount_min"] >= lo) & (df["amount_min"] <= hi)]

    df = df.sort_values("date", ascending=False)
    total = len(df)
    pages = max(1, (total + per_page - 1) // per_page)
    slice_df = df.iloc[(page - 1) * per_page: page * per_page]

    trades_out = []
    for _, r in slice_df.iterrows():
        member_name = r["member"]
        db_entry = member_db.get(str(member_name).lower(), {})
        slug = db_entry.get("slug") or str(member_name).lower().replace(" ", "-").replace("(","").replace(")","").replace(",","").replace(".","")
        photo = db_entry.get("photo_url", "")
        trades_out.append({
            "ticker": r["ticker"],
            "asset_name": r.get("asset_name", ""),
            "member": member_name,
            "slug": slug,
            "photo": photo,
            "party": r["party"],
            "chamber": r.get("chamber", ""),
            "type": r["type"],
            "date": r["date"].strftime("%Y-%m-%d"),
            "amount": r["amount_str"],
            "amount_min": int(r.get("amount_min", 0) or 0),
        })

    return jsonify({"trades": trades_out, "total": total, "pages": pages, "page": page})


# ─── Run ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Congress Tracker - http://127.0.0.1:8051")
    app.run(debug=False, host="127.0.0.1", port=8051)
