"""
Congressional Trading Tracker
Standalone dashboard — STOCK Act disclosures van Senaat + House of Representatives
Data: Quiver Quantitative live endpoint (gratis, geen key nodig)
Poort: 8051
"""
from __future__ import annotations

import time
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import yfinance as yf
from dash import Input, Output, dcc, html

# ─── Config ───────────────────────────────────────────────────────

QUIVER_URL = "https://api.quiverquant.com/beta/live/congresstrading"

COLORS = {
    "bg":     "#08090f",
    "card":   "#0f1117",
    "border": "#1e2330",
    "accent": "#3b82f6",
    "green":  "#10b981",
    "red":    "#ef4444",
    "yellow": "#f59e0b",
    "purple": "#8b5cf6",
    "text":   "#f1f5f9",
    "muted":  "#64748b",
    "gold":   "#d4a017",
}

# High-alpha leden (historisch hoge excess returns na trade)
HIGH_ALPHA = {
    "Nancy Pelosi", "Paul Pelosi", "Dan Crenshaw", "Brian Mast",
    "Marjorie Taylor Greene", "Tommy Tuberville", "Josh Gottheimer",
    "Ro Khanna", "Michael McCaul", "David Rouzer",
}

# Commissie → sector tickers (inside knowledge boost)
COMMITTEE_SECTORS: dict[str, list[str]] = {
    "Armed Services":  ["LMT", "RTX", "NOC", "GD", "BA", "HII", "LDOS", "SAIC", "CACI", "BAH"],
    "Intelligence":    ["PANW", "CRWD", "FTNT", "CACI", "SAIC", "LDOS", "BAH", "PLTR"],
    "Energy":          ["XOM", "CVX", "COP", "SLB", "HAL", "MPC", "VLO", "PSX", "EOG"],
    "Finance":         ["JPM", "BAC", "GS", "MS", "WFC", "C", "BLK", "SCHW", "V", "MA"],
    "Health":          ["UNH", "CVS", "MCK", "HUM", "ELV", "CI", "LLY", "ABBV", "MRK"],
    "Technology":      ["AAPL", "MSFT", "GOOGL", "META", "AMZN", "NVDA", "AMD", "INTC"],
}

# Bedrag range → minimum waarde in dollars
AMOUNT_MAP: dict[str, int] = {
    "$1,001 - $15,000":        1_001,
    "$15,001 - $50,000":      15_001,
    "$50,001 - $100,000":     50_001,
    "$100,001 - $250,000":   100_001,
    "$250,001 - $500,000":   250_001,
    "$500,001 - $1,000,000": 500_001,
    "$1,000,001 - $5,000,000": 1_000_001,
    "Over $5,000,000":       5_000_001,
}

# ─── Data ─────────────────────────────────────────────────────────

_CACHE: dict = {"data": None, "ts": 0.0}


def _fetch(lookback: int = 180) -> pd.DataFrame:
    now = time.time()
    if _CACHE["data"] is not None and (now - _CACHE["ts"]) < 3600:
        raw = _CACHE["data"]
    else:
        try:
            r = requests.get(QUIVER_URL, headers={"Accept": "application/json"}, timeout=15)
            raw = r.json() if r.status_code == 200 else []
        except Exception:
            raw = []
        _CACHE["data"] = raw
        _CACHE["ts"] = now

    if not raw:
        return pd.DataFrame()

    cutoff = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=lookback)
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
        # Datums: transaction date (wanneer gehandeld) en report date (wanneer gemeld)
        tx_date_str     = item.get("TransactionDate") or ""
        report_date_str = item.get("ReportDate") or ""
        try:
            tx_date = pd.to_datetime(tx_date_str) if tx_date_str else None
        except Exception:
            tx_date = None
        try:
            report_date = pd.to_datetime(report_date_str) if report_date_str else None
        except Exception:
            report_date = None
        dt = tx_date or report_date
        if dt is None or pd.Timestamp(dt) < cutoff:
            continue
        # Filing delay in dagen
        delay = None
        if tx_date and report_date:
            delay = max(0, (report_date - tx_date).days)
        # Amount parsing
        amount_str = item.get("Range") or item.get("Amount") or ""
        amount_min = AMOUNT_MAP.get(amount_str.strip(), 0)
        if amount_min == 0:
            try:
                amount_min = int(float(item.get("Amount", 0)))
            except Exception:
                amount_min = 0
        member = item.get("Representative", "")
        rows.append({
            "ticker":      ticker,
            "member":      member,
            "party":       item.get("Party", ""),
            "chamber":     item.get("House", ""),
            "type":        tx,
            "date":        pd.Timestamp(dt).normalize(),
            "report_date": report_date,
            "amount_str":  amount_str,
            "amount_min":  amount_min,
            "delay_days":  delay,
            "high_alpha":  member in HIGH_ALPHA,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ─── App ──────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.SLATE],
    title="Congress Tracker",
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

_TAB   = {"backgroundColor": COLORS["card"], "color": COLORS["muted"],
          "border": "none", "padding": "10px 22px", "fontSize": "0.88rem"}
_TABON = {**_TAB, "color": COLORS["accent"], "borderBottom": f"2px solid {COLORS['accent']}"}

_CHART = dict(
    template="plotly_dark",
    paper_bgcolor=COLORS["card"],
    plot_bgcolor=COLORS["card"],
    font={"color": COLORS["text"], "family": "Inter, sans-serif"},
    margin={"l": 10, "r": 10, "t": 36, "b": 10},
    legend={"bgcolor": "rgba(0,0,0,0)", "font": {"size": 11}},
)
_AXIS = dict(gridcolor=COLORS["border"], color=COLORS["muted"], showgrid=True)

app.layout = html.Div([
    # Header
    dbc.Navbar(dbc.Container([
        html.Div([
            html.Span("🏛️", style={"fontSize": "1.4rem"}),
            html.Span(" Congressional Trading Tracker",
                      style={"color": COLORS["text"], "fontWeight": "700",
                             "fontSize": "1.1rem", "marginLeft": "8px"}),
            html.Span(" — STOCK Act disclosures",
                      style={"color": COLORS["muted"], "fontSize": "0.8rem", "marginLeft": "8px"}),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div([
            html.Span(id="last-update", style={"color": COLORS["muted"], "fontSize": "0.78rem"}),
            dbc.Badge("LIVE DATA", color="success", className="ms-3",
                      style={"fontSize": "0.7rem"}),
        ], style={"display": "flex", "alignItems": "center"}),
    ], fluid=True, className="d-flex justify-content-between align-items-center"),
    color=COLORS["card"], dark=True,
    style={"borderBottom": f"2px solid {COLORS['border']}", "marginBottom": "0"}),

    # Controls bar
    html.Div([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.Label("Periode", style={"color": COLORS["muted"], "fontSize": "0.75rem", "marginBottom": "4px"}),
                    dbc.Select(id="lookback",
                        options=[
                            {"label": "30 dagen",  "value": "30"},
                            {"label": "60 dagen",  "value": "60"},
                            {"label": "90 dagen",  "value": "90"},
                            {"label": "180 dagen", "value": "180"},
                        ],
                        value="90",
                        style={"backgroundColor": COLORS["border"], "color": COLORS["text"],
                               "border": "none", "borderRadius": "6px", "fontSize": "0.85rem"},
                    ),
                ], width=2),
                dbc.Col([
                    html.Label("Filter type", style={"color": COLORS["muted"], "fontSize": "0.75rem", "marginBottom": "4px"}),
                    dbc.Select(id="tx-filter",
                        options=[
                            {"label": "Alles",        "value": "all"},
                            {"label": "Alleen BUY",   "value": "BUY"},
                            {"label": "Alleen SELL",  "value": "SELL"},
                            {"label": "High-alpha leden", "value": "alpha"},
                        ],
                        value="all",
                        style={"backgroundColor": COLORS["border"], "color": COLORS["text"],
                               "border": "none", "borderRadius": "6px", "fontSize": "0.85rem"},
                    ),
                ], width=2),
                dbc.Col([
                    html.Label("Ticker zoeken", style={"color": COLORS["muted"], "fontSize": "0.75rem", "marginBottom": "4px"}),
                    dbc.Input(id="ticker-search", placeholder="bijv. NVDA",
                        style={"backgroundColor": COLORS["border"], "color": COLORS["text"],
                               "border": "none", "borderRadius": "6px", "fontSize": "0.85rem"},
                    ),
                ], width=2),
                dbc.Col([
                    html.Label("\u00a0", style={"fontSize": "0.75rem", "marginBottom": "4px", "display": "block"}),
                    dbc.Button("Vernieuwen", id="refresh-btn", color="primary", size="sm",
                               style={"width": "100%"}),
                ], width=1),
                dbc.Col([
                    html.Div(id="stat-bar",
                             style={"display": "flex", "gap": "24px", "alignItems": "center",
                                    "paddingTop": "20px"}),
                ], width=5),
            ], align="end"),
        ], fluid=True),
    ], style={"backgroundColor": COLORS["bg"], "padding": "14px 0 10px 0",
              "borderBottom": f"1px solid {COLORS['border']}"}),

    # Tabs
    dcc.Tabs(id="tabs", value="signals", children=[
        dcc.Tab(label="Signal Feed",       value="signals",  style=_TAB, selected_style=_TABON),
        dcc.Tab(label="Grafieken",         value="charts",   style=_TAB, selected_style=_TABON),
        dcc.Tab(label="Leden",             value="members",  style=_TAB, selected_style=_TABON),
        dcc.Tab(label="Recente trades",    value="trades",   style=_TAB, selected_style=_TABON),
        dcc.Tab(label="Nieuws",            value="news",        style=_TAB, selected_style=_TABON),
        dcc.Tab(label="Performance",       value="performance", style=_TAB, selected_style=_TABON),
        dcc.Tab(label="Edge Signals",      value="edges",       style=_TAB, selected_style=_TABON),
    ], style={"backgroundColor": COLORS["card"],
              "borderBottom": f"1px solid {COLORS['border']}"}),

    html.Div(id="tab-content",
             style={"backgroundColor": COLORS["bg"], "minHeight": "90vh", "padding": "24px"}),

    dcc.Interval(id="auto-refresh", interval=3600_000, n_intervals=0),
], style={"backgroundColor": COLORS["bg"], "fontFamily": "'Inter', sans-serif"})


# ─── Helpers ──────────────────────────────────────────────────────

def _card(title: str, content, width: int = 12, height: int | None = None) -> dbc.Col:
    style = {"backgroundColor": COLORS["card"], "border": f"1px solid {COLORS['border']}",
             "borderRadius": "8px"}
    body_style = {}
    if height:
        body_style["height"] = f"{height}px"
        body_style["overflowY"] = "auto"
    return dbc.Col(dbc.Card([
        dbc.CardHeader(title, style={
            "backgroundColor": COLORS["card"], "color": COLORS["muted"],
            "fontSize": "0.8rem", "fontWeight": "600", "letterSpacing": "0.05em",
            "textTransform": "uppercase", "borderBottom": f"1px solid {COLORS['border']}",
        }),
        dbc.CardBody(content, style=body_style),
    ], style=style), width=width)


def _stat(label: str, val: str, color: str):
    return html.Div([
        html.Div(val, style={"color": color, "fontWeight": "700", "fontSize": "1.25rem",
                             "lineHeight": "1"}),
        html.Div(label, style={"color": COLORS["muted"], "fontSize": "0.7rem", "marginTop": "2px"}),
    ])


def _empty_fig() -> go.Figure:
    fig = go.Figure()
    fig.update_layout(**_CHART)
    return fig


def _apply_filters(df: pd.DataFrame, tx_filter: str, ticker_search: str) -> pd.DataFrame:
    if df.empty:
        return df
    if tx_filter == "BUY":
        df = df[df["type"] == "BUY"]
    elif tx_filter == "SELL":
        df = df[df["type"] == "SELL"]
    elif tx_filter == "alpha":
        df = df[df["high_alpha"]]
    if ticker_search:
        df = df[df["ticker"] == ticker_search.strip().upper()]
    return df


# ─── Stat bar + last update ───────────────────────────────────────

@app.callback(
    Output("stat-bar",     "children"),
    Output("last-update",  "children"),
    Input("auto-refresh",  "n_intervals"),
    Input("refresh-btn",   "n_clicks"),
    Input("lookback",      "value"),
    Input("tx-filter",     "value"),
    Input("ticker-search", "value"),
)
def update_stats(_, __, lookback, tx_filter, ticker_search):
    df = _fetch(int(lookback or 90))
    df = _apply_filters(df, tx_filter or "all", ticker_search or "")
    ts = datetime.utcnow().strftime("Bijgewerkt %H:%M UTC")

    if df.empty:
        return [_stat("Geen data", "—", COLORS["muted"])], ts

    buys  = df[df["type"] == "BUY"]
    sells = df[df["type"] == "SELL"]

    return [
        _stat("BUY trades",      str(len(buys)),             COLORS["green"]),
        _stat("SELL trades",     str(len(sells)),            COLORS["red"]),
        _stat("Unieke leden",    str(df["member"].nunique()),COLORS["accent"]),
        _stat("Unieke tickers",  str(df["ticker"].nunique()),COLORS["yellow"]),
        _stat("High-alpha",      str(df["high_alpha"].sum()),COLORS["purple"]),
    ], ts


# ─── Tab routing ──────────────────────────────────────────────────

@app.callback(
    Output("tab-content", "children"),
    Input("tabs",          "value"),
    Input("lookback",      "value"),
    Input("tx-filter",     "value"),
    Input("ticker-search", "value"),
    Input("refresh-btn",   "n_clicks"),
    Input("auto-refresh",  "n_intervals"),
)
def render_tab(tab, lookback, tx_filter, ticker_search, _, __):
    df = _fetch(int(lookback or 90))
    df = _apply_filters(df, tx_filter or "all", ticker_search or "")
    if tab == "signals":  return tab_signals(df)
    if tab == "charts":   return tab_charts(df, int(lookback or 90))
    if tab == "members":  return tab_members(df)
    if tab == "trades":   return tab_trades(df)
    if tab == "news":        return tab_news(df)
    if tab == "performance": return tab_performance(df)
    if tab == "edges":       return tab_edges(df, int(lookback or 90))
    return html.Div()


# ─── Tab 1: Signal Feed ───────────────────────────────────────────

def tab_signals(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.Div(html.P("Geen data. Controleer verbinding.",
                               style={"color": COLORS["muted"]}), style={"padding": "40px"})

    buys  = df[df["type"] == "BUY"]
    sells = df[df["type"] == "SELL"]

    ticker_buys  = buys["ticker"].value_counts().rename("BUY")
    ticker_sells = sells["ticker"].value_counts().rename("SELL")
    tdf = pd.concat([ticker_buys, ticker_sells], axis=1).fillna(0).astype(int)
    tdf["total"]   = tdf["BUY"] + tdf["SELL"]
    tdf["buy_pct"] = (tdf["BUY"] / tdf["total"] * 100).round(0)
    tdf = tdf.sort_values("total", ascending=False).head(30)

    rows = []
    for ticker, row in tdf.iterrows():
        members_buy  = set(buys[buys["ticker"] == ticker]["member"])
        members_sell = set(sells[sells["ticker"] == ticker]["member"])

        if row["BUY"] >= row["SELL"] and len(members_buy) >= 2:
            action, action_col = "KOPEN",   COLORS["green"]
            members = members_buy
        elif row["SELL"] > row["BUY"] and len(members_sell) >= 2:
            action, action_col = "VERKOPEN", COLORS["red"]
            members = members_sell
        elif len(members_buy | members_sell) >= 1 and (members_buy | members_sell) & HIGH_ALPHA:
            action, action_col = "KOPEN" if row["BUY"] >= row["SELL"] else "VERKOPEN", \
                                 COLORS["green"] if row["BUY"] >= row["SELL"] else COLORS["red"]
            members = members_buy | members_sell
        else:
            action, action_col = "WACHTEN", COLORS["muted"]
            members = members_buy | members_sell

        strength = int(row["buy_pct"]) if action == "KOPEN" else int(100 - row["buy_pct"])
        is_alpha  = bool(members & HIGH_ALPHA)

        names = ", ".join(sorted(m.split("(")[0].strip()[:20] for m in list(members)[:3]))
        if len(members) > 3:
            names += f" +{len(members)-3}"

        # Recent date
        ticker_df_  = df[df["ticker"] == ticker]
        last_date   = ticker_df_["date"].max()
        days_ago    = (pd.Timestamp.utcnow().normalize() - last_date).days

        rows.append(html.Tr([
            html.Td([
                html.Strong(ticker, style={"color": COLORS["accent"], "fontSize": "0.95rem"}),
                html.Span(f" ★" if is_alpha else "",
                          style={"color": COLORS["gold"], "fontSize": "0.75rem"}),
            ], style={"width": "75px", "verticalAlign": "middle"}),

            html.Td(html.Span(action, style={
                "color": action_col, "fontWeight": "700", "fontSize": "0.82rem",
                "padding": "3px 10px", "border": f"1px solid {action_col}",
                "borderRadius": "5px",
            }), style={"width": "100px", "verticalAlign": "middle"}),

            html.Td([
                html.Div(f"{strength}%", style={"color": action_col, "fontSize": "0.75rem",
                                                "fontWeight": "600"}),
                html.Div(style={"backgroundColor": COLORS["border"], "borderRadius": "3px",
                                "height": "5px", "width": "100px", "overflow": "hidden",
                                "marginTop": "3px"},
                         children=html.Div(style={
                             "width": f"{strength}%", "height": "100%",
                             "backgroundColor": action_col, "borderRadius": "3px",
                         })),
            ], style={"width": "120px", "verticalAlign": "middle"}),

            html.Td([
                html.Span(f"{int(row['BUY'])}↑", style={"color": COLORS["green"], "fontSize": "0.82rem"}),
                html.Span(f"  {int(row['SELL'])}↓", style={"color": COLORS["red"], "fontSize": "0.82rem"}),
            ], style={"width": "80px", "verticalAlign": "middle"}),

            html.Td(f"{days_ago}d geleden",
                    style={"color": COLORS["muted"], "fontSize": "0.75rem",
                           "width": "90px", "verticalAlign": "middle"}),

            html.Td(names,
                    style={"color": COLORS["muted"], "fontSize": "0.78rem",
                           "verticalAlign": "middle"}),

        ], style={"borderBottom": f"1px solid {COLORS['border']}",
                  "transition": "background 0.15s"}))

    header = html.Thead(html.Tr([
        html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500",
                          "fontSize": "0.72rem", "textTransform": "uppercase",
                          "letterSpacing": "0.06em",
                          "borderBottom": f"2px solid {COLORS['border']}",
                          "paddingBottom": "10px"})
        for c in ["Ticker", "Signaal", "Sterkte", "B / S", "Laatste trade", "Leden"]
    ]))

    legend = html.Div([
        html.Span("★ = High-alpha lid (historisch hoge excess return na trade)",
                  style={"color": COLORS["gold"], "fontSize": "0.73rem"}),
        html.Span("  |  Minimaal 2 leden voor geldig signaal",
                  style={"color": COLORS["muted"], "fontSize": "0.73rem"}),
    ], style={"marginBottom": "14px"})

    return html.Div([
        legend,
        dbc.Card(dbc.CardBody(
            dbc.Table([header, html.Tbody(rows)],
                      bordered=False, hover=True, size="sm",
                      style={"color": COLORS["text"], "marginBottom": 0}),
            style={"padding": "0 4px", "maxHeight": "75vh", "overflowY": "auto"},
        ), style={"backgroundColor": COLORS["card"],
                  "border": f"1px solid {COLORS['border']}",
                  "borderRadius": "8px"}),
    ])


# ─── Tab 2: Grafieken ─────────────────────────────────────────────

def tab_charts(df: pd.DataFrame, lookback: int) -> html.Div:
    if df.empty:
        return html.P("Geen data.", style={"color": COLORS["muted"]})

    buys  = df[df["type"] == "BUY"]
    sells = df[df["type"] == "SELL"]

    # Chart 1: Butterfly — top tickers buy vs sell
    ticker_buys  = buys["ticker"].value_counts().rename("BUY")
    ticker_sells = sells["ticker"].value_counts().rename("SELL")
    tdf = pd.concat([ticker_buys, ticker_sells], axis=1).fillna(0).astype(int)
    tdf["total"] = tdf["BUY"] + tdf["SELL"]
    tdf = tdf.nlargest(20, "total").sort_values("BUY")

    fig_butterfly = go.Figure()
    fig_butterfly.add_trace(go.Bar(
        y=tdf.index, x=tdf["BUY"], name="BUY",
        orientation="h", marker_color=COLORS["green"],
        text=tdf["BUY"].astype(str), textposition="auto",
        textfont={"size": 10, "color": "#fff"},
    ))
    fig_butterfly.add_trace(go.Bar(
        y=tdf.index, x=-tdf["SELL"], name="SELL",
        orientation="h", marker_color=COLORS["red"],
        text=tdf["SELL"].astype(str), textposition="auto",
        textfont={"size": 10, "color": "#fff"},
    ))
    fig_butterfly.update_layout(
        **_CHART, barmode="relative", height=460, title="Top Tickers — Buy vs Sell",
        xaxis={**_AXIS, "title": "← SELL  |  BUY →", "tickvals": []},
        yaxis={**_AXIS, "tickfont": {"size": 11}},
    )

    # Chart 2: Timeline
    df2 = df.copy()
    df2["date"] = pd.to_datetime(df2["date"])
    tl_buy  = buys.groupby("date").size().rename("BUY")
    tl_sell = sells.groupby("date").size().rename("SELL")
    tl = pd.concat([tl_buy, tl_sell], axis=1).fillna(0).astype(int).sort_index()
    if lookback > 60:
        tl = tl.resample("W").sum()

    fig_tl = go.Figure()
    fig_tl.add_trace(go.Bar(x=tl.index, y=tl["BUY"],  name="BUY",  marker_color=COLORS["green"]))
    fig_tl.add_trace(go.Bar(x=tl.index, y=tl["SELL"], name="SELL", marker_color=COLORS["red"]))
    fig_tl.update_layout(
        **_CHART, barmode="stack", height=300, title="Dagelijkse activiteit",
        xaxis=_AXIS, yaxis={**_AXIS, "title": "# trades"},
    )

    # Chart 3: Buy dominance
    tdf2 = tdf.copy()
    tdf2["pct"] = (tdf2["BUY"] / tdf2["total"] * 100).round(1)
    tdf2 = tdf2.sort_values("pct")
    colors_dom = [
        COLORS["green"] if v >= 70 else COLORS["yellow"] if v >= 40 else COLORS["red"]
        for v in tdf2["pct"]
    ]
    fig_dom = go.Figure(go.Bar(
        y=tdf2.index, x=tdf2["pct"], orientation="h",
        marker_color=colors_dom,
        text=[f"{v}%" for v in tdf2["pct"]],
        textposition="auto", textfont={"size": 10},
    ))
    fig_dom.add_vline(x=50, line_dash="dash", line_color=COLORS["muted"], line_width=1)
    fig_dom.update_layout(
        **_CHART, height=460, title="Buy dominantie %",
        xaxis={**_AXIS, "range": [0, 110], "ticksuffix": "%"},
        yaxis={**_AXIS, "tickfont": {"size": 10}},
        showlegend=False,
    )

    return html.Div([
        dbc.Row([
            _card("Top Tickers", dcc.Graph(figure=fig_butterfly, config={"displayModeBar": False}), width=7),
            _card("Buy Dominantie", dcc.Graph(figure=fig_dom, config={"displayModeBar": False}), width=5),
        ], className="mb-4"),
        dbc.Row([
            _card("Activiteit over tijd", dcc.Graph(figure=fig_tl, config={"displayModeBar": False}), width=12),
        ]),
    ])


# ─── Tab 3: Leden ─────────────────────────────────────────────────

def tab_members(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.P("Geen data.", style={"color": COLORS["muted"]})

    buys  = df[df["type"] == "BUY"]
    sells = df[df["type"] == "SELL"]

    # Top leden chart
    mb  = buys["member"].value_counts().rename("BUY").head(15)
    ms  = sells["member"].value_counts().rename("SELL").head(15)
    mdf = pd.concat([mb, ms], axis=1).fillna(0).astype(int)
    mdf["total"] = mdf["BUY"] + mdf["SELL"]
    mdf = mdf.nlargest(15, "total").sort_values("total")
    mdf.index = [n.split("(")[0].strip()[:25] for n in mdf.index]

    fig_members = go.Figure()
    fig_members.add_trace(go.Bar(
        y=mdf.index, x=mdf["BUY"],  name="BUY",  orientation="h",
        marker_color=COLORS["green"],
    ))
    fig_members.add_trace(go.Bar(
        y=mdf.index, x=mdf["SELL"], name="SELL", orientation="h",
        marker_color=COLORS["red"],
    ))
    fig_members.update_layout(
        **_CHART, barmode="stack", height=520,
        title="Meest actieve congresleden",
        xaxis={**_AXIS, "title": "Aantal trades"},
        yaxis={**_AXIS, "tickfont": {"size": 10}},
    )

    # Party breakdown donut
    party_counts = df["party"].value_counts()
    party_colors = {"R": "#ef4444", "D": COLORS["accent"], "I": COLORS["yellow"]}
    fig_party = go.Figure(go.Pie(
        labels=party_counts.index,
        values=party_counts.values,
        hole=0.55,
        marker_colors=[party_colors.get(p, COLORS["muted"]) for p in party_counts.index],
        textfont={"size": 12},
    ))
    fig_party.update_layout(
        **_CHART, height=300, title="Partij verdeling",
        showlegend=True,
    )

    # Chamber breakdown
    chamber_counts = df["chamber"].value_counts()
    fig_chamber = go.Figure(go.Pie(
        labels=chamber_counts.index,
        values=chamber_counts.values,
        hole=0.55,
        marker_colors=[COLORS["accent"], COLORS["purple"]],
        textfont={"size": 12},
    ))
    fig_chamber.update_layout(
        **_CHART, height=300, title="Senate vs House",
        showlegend=True,
    )

    # High-alpha leden tabel
    alpha_rows = []
    for member in sorted(HIGH_ALPHA):
        mdata = df[df["member"].str.contains(member.split()[-1], case=False, na=False)]
        if mdata.empty:
            continue
        b = len(mdata[mdata["type"] == "BUY"])
        s = len(mdata[mdata["type"] == "SELL"])
        tickers = ", ".join(mdata["ticker"].value_counts().head(3).index.tolist())
        alpha_rows.append(html.Tr([
            html.Td([html.Span("★ ", style={"color": COLORS["gold"]}),
                     html.Strong(member, style={"color": COLORS["text"], "fontSize": "0.82rem"})]),
            html.Td(f"{b}↑  {s}↓", style={"color": COLORS["muted"], "fontSize": "0.8rem"}),
            html.Td(tickers, style={"color": COLORS["accent"], "fontSize": "0.8rem"}),
        ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

    alpha_table = dbc.Table(
        [html.Thead(html.Tr([
            html.Th("Lid", style={"color": COLORS["muted"], "fontSize": "0.72rem", "fontWeight": "500"}),
            html.Th("B/S", style={"color": COLORS["muted"], "fontSize": "0.72rem", "fontWeight": "500"}),
            html.Th("Top tickers", style={"color": COLORS["muted"], "fontSize": "0.72rem", "fontWeight": "500"}),
        ])),
         html.Tbody(alpha_rows or [html.Tr(html.Td("Geen data", colSpan=3))])],
        bordered=False, size="sm", style={"color": COLORS["text"], "marginBottom": 0},
    )

    return html.Div([
        dbc.Row([
            _card("Activiteit per lid", dcc.Graph(figure=fig_members, config={"displayModeBar": False}), width=8),
            dbc.Col([
                dbc.Row([_card("Partij", dcc.Graph(figure=fig_party,   config={"displayModeBar": False}), width=12)], className="mb-3"),
                dbc.Row([_card("Kamer",  dcc.Graph(figure=fig_chamber, config={"displayModeBar": False}), width=12)]),
            ], width=4),
        ], className="mb-4"),
        dbc.Row([
            _card("★ High-Alpha Leden", alpha_table, width=12, height=300),
        ]),
    ])


# ─── Tab 4: Recente Trades ────────────────────────────────────────

def tab_trades(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.P("Geen data.", style={"color": COLORS["muted"]})

    recent = df.sort_values("date", ascending=False).head(100)

    header = html.Thead(html.Tr([
        html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500",
                          "fontSize": "0.72rem", "textTransform": "uppercase",
                          "borderBottom": f"2px solid {COLORS['border']}"})
        for c in ["Datum", "Lid", "Kamer", "Ticker", "Type", "Bedrag", "★"]
    ]))

    rows = []
    for _, r in recent.iterrows():
        tc = COLORS["green"] if r["type"] == "BUY" else COLORS["red"]
        rows.append(html.Tr([
            html.Td(str(r["date"].date()), style={"color": COLORS["muted"], "fontSize": "0.8rem"}),
            html.Td(r["member"][:30],      style={"color": COLORS["text"],  "fontSize": "0.8rem"}),
            html.Td(r["chamber"],          style={"color": COLORS["muted"], "fontSize": "0.75rem"}),
            html.Td(html.Strong(r["ticker"]), style={"color": COLORS["accent"]}),
            html.Td(r["type"], style={"color": tc, "fontWeight": "700", "fontSize": "0.8rem"}),
            html.Td(r["amount_str"],       style={"color": COLORS["muted"], "fontSize": "0.75rem"}),
            html.Td("★" if r["high_alpha"] else "",
                    style={"color": COLORS["gold"]}),
        ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

    return dbc.Card(dbc.CardBody(
        dbc.Table([header, html.Tbody(rows)],
                  bordered=False, hover=True, size="sm",
                  style={"color": COLORS["text"], "marginBottom": 0}),
        style={"padding": "4px", "maxHeight": "78vh", "overflowY": "auto"},
    ), style={"backgroundColor": COLORS["card"],
              "border": f"1px solid {COLORS['border']}",
              "borderRadius": "8px"})


# ─── Tab 5: Nieuws ────────────────────────────────────────────────

def tab_news(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.P("Geen data.", style={"color": COLORS["muted"]})

    top_buy = (
        df[df["type"] == "BUY"]["ticker"]
        .value_counts()
        .head(8)
        .index.tolist()
    )

    blocks = []
    for ticker in top_buy:
        try:
            raw_news = yf.Ticker(ticker).news or []
        except Exception:
            raw_news = []
        if not raw_news:
            continue

        buy_members = df[(df["ticker"] == ticker) & (df["type"] == "BUY")]["member"].nunique()

        articles = []
        for item in raw_news[:4]:
            title     = item.get("title", "")
            link      = item.get("link", "#")
            publisher = item.get("publisher", "")
            ts        = item.get("providerPublishTime", 0)
            age       = ""
            if ts:
                d = (pd.Timestamp.utcnow() - pd.Timestamp(ts, unit="s", tz="UTC")).days
                age = f"{d}d geleden" if d > 0 else "vandaag"

            articles.append(html.Div([
                html.A(title, href=link, target="_blank", style={
                    "color": COLORS["text"], "fontSize": "0.84rem",
                    "textDecoration": "none", "display": "block",
                    "lineHeight": "1.4",
                }),
                html.Span(f"{publisher}  ·  {age}",
                          style={"color": COLORS["muted"], "fontSize": "0.72rem"}),
            ], style={"marginBottom": "10px", "paddingBottom": "10px",
                      "borderBottom": f"1px solid {COLORS['border']}55"}))

        if articles:
            blocks.append(dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Strong(ticker, style={"color": COLORS["accent"], "fontSize": "0.95rem"}),
                    html.Span(f"  {buy_members} leden kopen",
                              style={"color": COLORS["muted"], "fontSize": "0.75rem"}),
                ], style={"backgroundColor": COLORS["card"],
                          "borderBottom": f"1px solid {COLORS['border']}"}),
                dbc.CardBody(articles,
                             style={"maxHeight": "260px", "overflowY": "auto",
                                    "padding": "12px"}),
            ], style={"backgroundColor": COLORS["card"],
                      "border": f"1px solid {COLORS['border']}",
                      "borderRadius": "8px",
                      "height": "100%"}), width=4, className="mb-4"))

    if not blocks:
        return html.P("Geen nieuws gevonden.", style={"color": COLORS["muted"]})

    return dbc.Row(blocks)


# ─── Tab 6: Performance ──────────────────────────────────────────

_PERF_CACHE: dict = {"data": None, "ts": 0.0}


def _calc_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per congreslid + per trade: haal koers op op transactiedatum en 30/60 dagen later.
    Bereken excess return vs SPY. Cached 2 uur want yfinance calls zijn traag.
    """
    now = time.time()
    if _PERF_CACHE["data"] is not None and (now - _PERF_CACHE["ts"]) < 7200:
        return _PERF_CACHE["data"]

    if df.empty:
        return pd.DataFrame()

    # Beperk tot laatste 90 dagen en max 80 trades (anders duurt het te lang)
    cutoff = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=90)
    subset = df[df["date"] >= cutoff].copy()
    subset = subset.sort_values("date", ascending=False).head(80)

    tickers_needed = list(subset["ticker"].unique()) + ["SPY"]
    start_str = (pd.Timestamp.utcnow() - pd.Timedelta(days=100)).strftime("%Y-%m-%d")
    end_str   = pd.Timestamp.utcnow().strftime("%Y-%m-%d")

    try:
        raw = yf.download(tickers_needed, start=start_str, end=end_str,
                          progress=False, auto_adjust=True)
        if isinstance(raw.columns, pd.MultiIndex):
            prices = raw["Close"]
        else:
            prices = raw
    except Exception:
        return pd.DataFrame()

    prices.index = pd.to_datetime(prices.index).tz_localize(None)

    rows = []
    for _, trade in subset.iterrows():
        ticker = trade["ticker"]
        tx_date = pd.Timestamp(trade["date"])
        if ticker not in prices.columns or "SPY" not in prices.columns:
            continue

        # Zoek dichtstbijzijnde handelsdag
        future_idx = prices.index[prices.index >= tx_date]
        if len(future_idx) < 2:
            continue
        entry_date = future_idx[0]
        entry_px   = prices.loc[entry_date, ticker]
        spy_entry  = prices.loc[entry_date, "SPY"]

        results_30 = results_60 = None
        idx_30 = prices.index[prices.index >= entry_date + pd.Timedelta(days=30)]
        if len(idx_30) >= 1:
            px_30  = prices.loc[idx_30[0], ticker]
            spy_30 = prices.loc[idx_30[0], "SPY"]
            ret_30    = (px_30 - entry_px) / entry_px * 100
            spy_ret30 = (spy_30 - spy_entry) / spy_entry * 100
            results_30 = round(ret_30 - spy_ret30, 2)

        idx_60 = prices.index[prices.index >= entry_date + pd.Timedelta(days=60)]
        if len(idx_60) >= 1:
            px_60  = prices.loc[idx_60[0], ticker]
            spy_60 = prices.loc[idx_60[0], "SPY"]
            ret_60    = (px_60 - entry_px) / entry_px * 100
            spy_ret60 = (spy_60 - spy_entry) / spy_entry * 100
            results_60 = round(ret_60 - spy_ret60, 2)

        rows.append({
            "member":    trade["member"],
            "ticker":    ticker,
            "type":      trade["type"],
            "date":      tx_date.date(),
            "alpha_30d": results_30,
            "alpha_60d": results_60,
            "high_alpha": trade["high_alpha"],
        })

    result = pd.DataFrame(rows) if rows else pd.DataFrame()
    _PERF_CACHE["data"] = result
    _PERF_CACHE["ts"]   = now
    return result


def tab_performance(df: pd.DataFrame) -> html.Div:
    if df.empty:
        return html.P("Geen data.", style={"color": COLORS["muted"]})

    perf = _calc_performance(df)

    if perf.empty:
        return html.Div([
            html.P("Performance berekenen...", style={"color": COLORS["muted"]}),
            html.P("Eerste keer laden duurt ~20 seconden (yfinance koersen ophalen).",
                   style={"color": COLORS["muted"], "fontSize": "0.8rem"}),
        ])

    buy_perf = perf[perf["type"] == "BUY"].dropna(subset=["alpha_30d"])

    # ── Per lid: gemiddelde alpha na BUY trades ───────────────────
    member_perf = (
        buy_perf.groupby("member")
        .agg(
            trades=("ticker", "count"),
            avg_alpha_30d=("alpha_30d", "mean"),
            avg_alpha_60d=("alpha_60d", "mean"),
        )
        .query("trades >= 2")
        .sort_values("avg_alpha_30d", ascending=False)
        .head(20)
    )
    member_perf.index = [n.split("(")[0].strip()[:25] for n in member_perf.index]
    member_perf["avg_alpha_30d"] = member_perf["avg_alpha_30d"].round(2)
    member_perf["avg_alpha_60d"] = member_perf["avg_alpha_60d"].round(2)

    colors_bar = [
        COLORS["green"] if v > 0 else COLORS["red"]
        for v in member_perf["avg_alpha_30d"]
    ]
    fig_members = go.Figure(go.Bar(
        y=member_perf.index,
        x=member_perf["avg_alpha_30d"],
        orientation="h",
        marker_color=colors_bar,
        text=[f"{v:+.1f}%" for v in member_perf["avg_alpha_30d"]],
        textposition="auto",
        textfont={"size": 10},
        name="Alpha 30d",
    ))
    fig_members.add_vline(x=0, line_color=COLORS["muted"], line_width=1)
    fig_members.update_layout(
        **_CHART, height=520,
        title="Gemiddelde excess return vs SPY na BUY — 30 dagen (min. 2 trades)",
        xaxis={**_AXIS, "ticksuffix": "%", "title": "Excess return vs SPY"},
        yaxis={**_AXIS, "tickfont": {"size": 10}},
        showlegend=False,
    )

    # ── Per ticker: gemiddelde alpha na congress BUY ──────────────
    ticker_perf = (
        buy_perf.groupby("ticker")
        .agg(trades=("member", "count"), avg_alpha=("alpha_30d", "mean"))
        .query("trades >= 2")
        .sort_values("avg_alpha", ascending=False)
        .head(15)
    )
    t_colors = [COLORS["green"] if v > 0 else COLORS["red"] for v in ticker_perf["avg_alpha"]]
    fig_tickers = go.Figure(go.Bar(
        x=ticker_perf.index,
        y=ticker_perf["avg_alpha"].round(2),
        marker_color=t_colors,
        text=[f"{v:+.1f}%" for v in ticker_perf["avg_alpha"]],
        textposition="outside",
        textfont={"size": 10},
    ))
    fig_tickers.add_hline(y=0, line_color=COLORS["muted"], line_width=1)
    fig_tickers.update_layout(
        **_CHART, height=300,
        title="Alpha per ticker na congress BUY (30d, min. 2 trades)",
        xaxis={**_AXIS},
        yaxis={**_AXIS, "ticksuffix": "%"},
        showlegend=False,
    )

    # ── Detail tabel ──────────────────────────────────────────────
    detail = perf.sort_values("alpha_30d", ascending=False, na_position="last").head(50)
    header = html.Thead(html.Tr([
        html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500",
                          "fontSize": "0.72rem", "textTransform": "uppercase",
                          "borderBottom": f"2px solid {COLORS['border']}"})
        for c in ["Lid", "Ticker", "Type", "Datum", "Alpha 30d", "Alpha 60d", "★"]
    ]))
    trows = []
    for _, r in detail.iterrows():
        a30 = r["alpha_30d"]
        a60 = r["alpha_60d"]
        c30 = COLORS["green"] if (a30 or 0) > 0 else COLORS["red"]
        c60 = COLORS["green"] if (a60 or 0) > 0 else COLORS["red"]
        trows.append(html.Tr([
            html.Td(str(r["member"])[:28], style={"color": COLORS["text"],  "fontSize": "0.8rem"}),
            html.Td(html.Strong(r["ticker"]), style={"color": COLORS["accent"]}),
            html.Td(r["type"], style={"color": COLORS["green"] if r["type"] == "BUY" else COLORS["red"],
                                      "fontWeight": "700", "fontSize": "0.8rem"}),
            html.Td(str(r["date"]), style={"color": COLORS["muted"], "fontSize": "0.78rem"}),
            html.Td(f"{a30:+.2f}%" if a30 is not None else "—",
                    style={"color": c30, "fontWeight": "600", "fontSize": "0.82rem"}),
            html.Td(f"{a60:+.2f}%" if a60 is not None else "—",
                    style={"color": c60, "fontWeight": "600", "fontSize": "0.82rem"}),
            html.Td("★" if r["high_alpha"] else "",
                    style={"color": COLORS["gold"]}),
        ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

    detail_table = dbc.Card(dbc.CardBody(
        dbc.Table([header, html.Tbody(trows)],
                  bordered=False, hover=True, size="sm",
                  style={"color": COLORS["text"], "marginBottom": 0}),
        style={"padding": "4px", "maxHeight": "380px", "overflowY": "auto"},
    ), style={"backgroundColor": COLORS["card"],
              "border": f"1px solid {COLORS['border']}",
              "borderRadius": "8px"})

    return html.Div([
        html.P(
            "Excess return = aandeelrendement na trade - SPY rendement in dezelfde periode. "
            "Positief = congreslid versloeg de markt na deze trade.",
            style={"color": COLORS["muted"], "fontSize": "0.78rem", "marginBottom": "20px"},
        ),
        dbc.Row([
            _card("Alpha per lid (BUY trades)",
                  dcc.Graph(figure=fig_members, config={"displayModeBar": False}), width=7),
            _card("Alpha per ticker",
                  dcc.Graph(figure=fig_tickers, config={"displayModeBar": False}), width=5),
        ], className="mb-4"),
        dbc.Row([_card("Trade detail — excess return", detail_table, width=12)]),
    ])


# ─── Tab 7: Edge Signals ─────────────────────────────────────────

def tab_edges(df: pd.DataFrame, lookback: int) -> html.Div:
    if df.empty:
        return html.P("Geen data.", style={"color": COLORS["muted"]})

    buys  = df[df["type"] == "BUY"]
    sells = df[df["type"] == "SELL"]
    sections = []

    # ─────────────────────────────────────────────────────────────
    # EDGE 1: Filing Delay Analysis
    # ─────────────────────────────────────────────────────────────
    delay_df = df[df["delay_days"].notna()].copy()
    if not delay_df.empty:
        # Gemiddelde vertraging per lid
        member_delay = (
            delay_df.groupby("member")["delay_days"]
            .agg(["mean", "count"])
            .query("count >= 2")
            .sort_values("mean", ascending=False)
            .head(20)
        )
        member_delay.index = [n.split("(")[0].strip()[:22] for n in member_delay.index]
        member_delay["mean"] = member_delay["mean"].round(1)

        colors_delay = [
            COLORS["red"] if v >= 40 else COLORS["yellow"] if v >= 25 else COLORS["green"]
            for v in member_delay["mean"]
        ]
        fig_delay = go.Figure(go.Bar(
            y=member_delay.index, x=member_delay["mean"],
            orientation="h", marker_color=colors_delay,
            text=[f"{v}d" for v in member_delay["mean"]],
            textposition="auto", textfont={"size": 10},
        ))
        fig_delay.add_vline(x=45, line_dash="dash", line_color=COLORS["red"],
                            line_width=1, annotation_text="45d limiet",
                            annotation_font_color=COLORS["red"])
        fig_delay.update_layout(
            **_CHART, height=450,
            title="Gemiddelde filing delay per lid (dagen)",
            xaxis={**_AXIS, "title": "Dagen tussen trade en melding"},
            yaxis={**_AXIS, "tickfont": {"size": 10}},
            showlegend=False,
        )

        # Delay distributie histogram
        fig_hist = go.Figure(go.Histogram(
            x=delay_df["delay_days"], nbinsx=30,
            marker_color=COLORS["accent"],
            opacity=0.85,
        ))
        fig_hist.add_vline(x=45, line_dash="dash", line_color=COLORS["red"], line_width=2)
        fig_hist.update_layout(
            **_CHART, height=250,
            title="Verdeling filing delay (alle trades)",
            xaxis={**_AXIS, "title": "Dagen"},
            yaxis={**_AXIS, "title": "# trades"},
        )

        # Late filers tabel (> 40 dagen)
        late = delay_df[delay_df["delay_days"] >= 35].sort_values("delay_days", ascending=False).head(20)
        late_rows = []
        for _, r in late.iterrows():
            late_rows.append(html.Tr([
                html.Td(str(r["member"])[:25], style={"color": COLORS["text"], "fontSize": "0.8rem"}),
                html.Td(html.Strong(r["ticker"]), style={"color": COLORS["accent"]}),
                html.Td(r["type"], style={
                    "color": COLORS["green"] if r["type"] == "BUY" else COLORS["red"],
                    "fontWeight": "700", "fontSize": "0.8rem",
                }),
                html.Td(str(r["date"].date()), style={"color": COLORS["muted"], "fontSize": "0.78rem"}),
                html.Td(f"{int(r['delay_days'])}d", style={
                    "color": COLORS["red"] if r["delay_days"] >= 40 else COLORS["yellow"],
                    "fontWeight": "700",
                }),
                html.Td(r["amount_str"], style={"color": COLORS["muted"], "fontSize": "0.75rem"}),
            ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

        late_header = html.Thead(html.Tr([
            html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500", "fontSize": "0.72rem",
                              "borderBottom": f"2px solid {COLORS['border']}"})
            for c in ["Lid", "Ticker", "Type", "Trade datum", "Delay", "Bedrag"]
        ]))
        late_table = dbc.Table([late_header, html.Tbody(late_rows)],
                               bordered=False, hover=True, size="sm",
                               style={"color": COLORS["text"], "marginBottom": 0})

        sections.append(html.Div([
            html.H5("1. Filing Delay", style={"color": COLORS["gold"], "marginBottom": "6px"}),
            html.P("Leden die consequent laat indienen maximaliseren hun informatievoorsprong. "
                   "Rood = 40+ dagen, geel = 25-40, groen = <25.",
                   style={"color": COLORS["muted"], "fontSize": "0.78rem", "marginBottom": "16px"}),
            dbc.Row([
                _card("Delay per lid", dcc.Graph(figure=fig_delay, config={"displayModeBar": False}), width=7),
                dbc.Col([
                    dbc.Row([_card("Distributie", dcc.Graph(figure=fig_hist, config={"displayModeBar": False}), width=12)]),
                ], width=5),
            ], className="mb-3"),
            dbc.Row([_card("Late Filers (35+ dagen)", late_table, width=12, height=300)]),
        ], className="mb-5"))

    # ─────────────────────────────────────────────────────────────
    # EDGE 2: Bipartisan Convergence
    # ─────────────────────────────────────────────────────────────
    buy_parties = buys.groupby(["ticker", "party"]).size().unstack(fill_value=0)
    bipartisan = pd.DataFrame()
    if "R" in buy_parties.columns and "D" in buy_parties.columns:
        bp = buy_parties[(buy_parties["R"] >= 1) & (buy_parties["D"] >= 1)].copy()
        bp["total"] = bp.sum(axis=1)
        bp = bp.sort_values("total", ascending=False).head(15)
        bipartisan = bp

    if not bipartisan.empty:
        fig_bp = go.Figure()
        fig_bp.add_trace(go.Bar(
            x=bipartisan.index, y=bipartisan["R"],
            name="Republican", marker_color=COLORS["red"],
        ))
        fig_bp.add_trace(go.Bar(
            x=bipartisan.index, y=bipartisan["D"],
            name="Democrat", marker_color=COLORS["accent"],
        ))
        if "I" in bipartisan.columns:
            fig_bp.add_trace(go.Bar(
                x=bipartisan.index, y=bipartisan.get("I", 0),
                name="Independent", marker_color=COLORS["yellow"],
            ))
        fig_bp.update_layout(
            **_CHART, barmode="stack", height=320,
            title="Bipartisane BUY convergentie (R + D kopen allebei)",
            xaxis=_AXIS, yaxis={**_AXIS, "title": "# BUY trades"},
        )

        # Welke leden per bipartisan ticker
        bp_detail = []
        for ticker in bipartisan.index[:8]:
            t_buys = buys[buys["ticker"] == ticker]
            r_members = ", ".join(t_buys[t_buys["party"] == "R"]["member"].str.split("(").str[0].str.strip().unique()[:3])
            d_members = ", ".join(t_buys[t_buys["party"] == "D"]["member"].str.split("(").str[0].str.strip().unique()[:3])
            bp_detail.append(html.Tr([
                html.Td(html.Strong(ticker), style={"color": COLORS["accent"]}),
                html.Td(f"{int(bipartisan.loc[ticker, 'R'])}R + {int(bipartisan.loc[ticker, 'D'])}D",
                        style={"color": COLORS["text"], "fontSize": "0.82rem"}),
                html.Td(r_members, style={"color": COLORS["red"], "fontSize": "0.75rem"}),
                html.Td(d_members, style={"color": COLORS["accent"], "fontSize": "0.75rem"}),
            ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

        bp_table = dbc.Table([
            html.Thead(html.Tr([
                html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500", "fontSize": "0.72rem",
                                  "borderBottom": f"2px solid {COLORS['border']}"})
                for c in ["Ticker", "Mix", "Republicans", "Democrats"]
            ])),
            html.Tbody(bp_detail),
        ], bordered=False, hover=True, size="sm",
           style={"color": COLORS["text"], "marginBottom": 0})

        sections.append(html.Div([
            html.H5("2. Bipartisane Convergentie", style={"color": COLORS["gold"], "marginBottom": "6px"}),
            html.P("Wanneer zowel R als D hetzelfde aandeel kopen is er waarschijnlijk "
                   "beleids-gedreven informatie die beide kampen kennen. Sterkste signaal.",
                   style={"color": COLORS["muted"], "fontSize": "0.78rem", "marginBottom": "16px"}),
            dbc.Row([
                _card("Bipartisan BUY tickers",
                      dcc.Graph(figure=fig_bp, config={"displayModeBar": False}), width=7),
                _card("Detail", bp_table, width=5, height=310),
            ]),
        ], className="mb-5"))

    # ─────────────────────────────────────────────────────────────
    # EDGE 3: Amount Escalation
    # ─────────────────────────────────────────────────────────────
    big_trades = df[df["amount_min"] >= 100_001].sort_values("amount_min", ascending=False).head(25)
    if not big_trades.empty:
        esc_rows = []
        for _, r in big_trades.iterrows():
            # Check of dit lid normaal kleiner handelt
            member_median = df[df["member"] == r["member"]]["amount_min"].median()
            escalation = r["amount_min"] / max(member_median, 1)
            is_escalated = escalation >= 3

            tc = COLORS["green"] if r["type"] == "BUY" else COLORS["red"]
            esc_rows.append(html.Tr([
                html.Td(str(r["member"])[:25], style={"color": COLORS["text"], "fontSize": "0.8rem"}),
                html.Td(html.Strong(r["ticker"]), style={"color": COLORS["accent"]}),
                html.Td(r["type"], style={"color": tc, "fontWeight": "700", "fontSize": "0.8rem"}),
                html.Td(r["amount_str"], style={"color": COLORS["yellow"], "fontWeight": "600", "fontSize": "0.82rem"}),
                html.Td([
                    html.Span(f"{escalation:.1f}x",
                              style={"color": COLORS["red"] if is_escalated else COLORS["muted"],
                                     "fontWeight": "700" if is_escalated else "400"}),
                    html.Span(" vs normaal", style={"color": COLORS["muted"], "fontSize": "0.7rem"}),
                ]),
                html.Td(str(r["date"].date()), style={"color": COLORS["muted"], "fontSize": "0.78rem"}),
            ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

        esc_header = html.Thead(html.Tr([
            html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500", "fontSize": "0.72rem",
                              "borderBottom": f"2px solid {COLORS['border']}"})
            for c in ["Lid", "Ticker", "Type", "Bedrag", "Escalatie", "Datum"]
        ]))

        sections.append(html.Div([
            html.H5("3. Bedrag Escalatie", style={"color": COLORS["gold"], "marginBottom": "6px"}),
            html.P("Trades boven $100k, gesorteerd op grootte. "
                   "'Escalatie' toont hoeveel groter dan het normale bedrag van dit lid. "
                   "3x+ = rood (ongewoon hoog vertrouwen).",
                   style={"color": COLORS["muted"], "fontSize": "0.78rem", "marginBottom": "16px"}),
            dbc.Row([_card("Grote trades ($100k+)",
                          dbc.Table([esc_header, html.Tbody(esc_rows)],
                                    bordered=False, hover=True, size="sm",
                                    style={"color": COLORS["text"]}),
                          width=12, height=400)]),
        ], className="mb-5"))

    # ─────────────────────────────────────────────────────────────
    # EDGE 4: Cluster Velocity
    # ─────────────────────────────────────────────────────────────
    cluster_rows = []
    buy_tickers = buys["ticker"].value_counts()
    for ticker in buy_tickers[buy_tickers >= 2].index[:20]:
        t_buys = buys[buys["ticker"] == ticker].sort_values("date")
        if len(t_buys) < 2:
            continue
        dates = t_buys["date"].values
        # Bereken het venster: verschil tussen eerste en laatste buy
        window_days = (pd.Timestamp(dates[-1]) - pd.Timestamp(dates[0])).days
        n_members = t_buys["member"].nunique()
        if n_members < 2:
            continue
        velocity = n_members / max(window_days, 1) * 7  # leden per week

        cluster_rows.append({
            "ticker": ticker,
            "n_buys": len(t_buys),
            "n_members": n_members,
            "window": window_days,
            "velocity": round(velocity, 2),
            "members": ", ".join(t_buys["member"].str.split("(").str[0].str.strip().unique()[:3]),
        })

    if cluster_rows:
        cdf = pd.DataFrame(cluster_rows).sort_values("velocity", ascending=False).head(15)

        vel_colors = [
            COLORS["red"] if v >= 3 else COLORS["yellow"] if v >= 1.5 else COLORS["green"]
            for v in cdf["velocity"]
        ]
        fig_vel = go.Figure(go.Bar(
            x=cdf["ticker"], y=cdf["velocity"],
            marker_color=vel_colors,
            text=[f"{v}/wk" for v in cdf["velocity"]],
            textposition="outside", textfont={"size": 10},
        ))
        fig_vel.update_layout(
            **_CHART, height=320,
            title="Cluster velocity (unieke leden per week die kopen)",
            xaxis=_AXIS, yaxis={**_AXIS, "title": "Leden/week"},
            showlegend=False,
        )

        # Detail tabel
        cl_rows_html = []
        for _, r in cdf.iterrows():
            cl_rows_html.append(html.Tr([
                html.Td(html.Strong(r["ticker"]), style={"color": COLORS["accent"]}),
                html.Td(f"{r['n_members']} leden", style={"color": COLORS["text"], "fontSize": "0.82rem"}),
                html.Td(f"{r['n_buys']} buys", style={"color": COLORS["green"], "fontSize": "0.82rem"}),
                html.Td(f"{r['window']}d venster", style={"color": COLORS["muted"], "fontSize": "0.82rem"}),
                html.Td(f"{r['velocity']}/wk", style={
                    "color": COLORS["red"] if r["velocity"] >= 3 else COLORS["yellow"],
                    "fontWeight": "700",
                }),
                html.Td(r["members"], style={"color": COLORS["muted"], "fontSize": "0.75rem"}),
            ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

        cl_header = html.Thead(html.Tr([
            html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500", "fontSize": "0.72rem",
                              "borderBottom": f"2px solid {COLORS['border']}"})
            for c in ["Ticker", "Leden", "BUY trades", "Venster", "Velocity", "Wie"]
        ]))

        sections.append(html.Div([
            html.H5("4. Cluster Velocity", style={"color": COLORS["gold"], "marginBottom": "6px"}),
            html.P("Hoe snel meerdere leden hetzelfde aandeel kopen. "
                   "3+ leden/week = rood (snelle clustering = sterkst signaal).",
                   style={"color": COLORS["muted"], "fontSize": "0.78rem", "marginBottom": "16px"}),
            dbc.Row([
                _card("Velocity per ticker",
                      dcc.Graph(figure=fig_vel, config={"displayModeBar": False}), width=6),
                _card("Cluster detail", dbc.Table([cl_header, html.Tbody(cl_rows_html)],
                      bordered=False, hover=True, size="sm",
                      style={"color": COLORS["text"]}), width=6, height=310),
            ]),
        ], className="mb-5"))

    # ─────────────────────────────────────────────────────────────
    # EDGE 5: Committee x Sector Match
    # ─────────────────────────────────────────────────────────────
    sector_match_rows = []
    for _, r in buys.iterrows():
        for committee, tickers in COMMITTEE_SECTORS.items():
            if r["ticker"] in tickers:
                sector_match_rows.append({
                    "member": r["member"],
                    "ticker": r["ticker"],
                    "committee": committee,
                    "date": r["date"],
                    "amount": r["amount_str"],
                    "high_alpha": r["high_alpha"],
                })

    if sector_match_rows:
        smdf = pd.DataFrame(sector_match_rows)

        # Tel per ticker hoeveel matches
        sm_counts = smdf.groupby(["ticker", "committee"]).agg(
            trades=("member", "count"),
            members=("member", "nunique"),
        ).reset_index().sort_values("trades", ascending=False).head(15)

        fig_sm = go.Figure(go.Bar(
            x=sm_counts["ticker"],
            y=sm_counts["trades"],
            marker_color=COLORS["purple"],
            text=sm_counts["committee"],
            textposition="outside", textfont={"size": 9, "color": COLORS["muted"]},
        ))
        fig_sm.update_layout(
            **_CHART, height=300,
            title="Commissie-relevante trades (sector match)",
            xaxis=_AXIS, yaxis={**_AXIS, "title": "# trades"},
            showlegend=False,
        )

        sm_html_rows = []
        for _, r in smdf.sort_values("date", ascending=False).head(20).iterrows():
            sm_html_rows.append(html.Tr([
                html.Td(str(r["member"])[:25], style={"color": COLORS["text"], "fontSize": "0.8rem"}),
                html.Td(html.Strong(r["ticker"]), style={"color": COLORS["accent"]}),
                html.Td(r["committee"], style={"color": COLORS["purple"], "fontSize": "0.8rem", "fontWeight": "600"}),
                html.Td(str(r["date"].date()), style={"color": COLORS["muted"], "fontSize": "0.78rem"}),
                html.Td(r["amount"], style={"color": COLORS["muted"], "fontSize": "0.75rem"}),
                html.Td("★" if r["high_alpha"] else "", style={"color": COLORS["gold"]}),
            ], style={"borderBottom": f"1px solid {COLORS['border']}22"}))

        sm_header = html.Thead(html.Tr([
            html.Th(c, style={"color": COLORS["muted"], "fontWeight": "500", "fontSize": "0.72rem",
                              "borderBottom": f"2px solid {COLORS['border']}"})
            for c in ["Lid", "Ticker", "Relevante commissie", "Datum", "Bedrag", "★"]
        ]))

        sections.append(html.Div([
            html.H5("5. Commissie x Sector Match", style={"color": COLORS["gold"], "marginBottom": "6px"}),
            html.P("Trades waarbij het aandeel in de sector valt van een relevante congrescommissie. "
                   "Bijv. Armed Services lid koopt defensieaandeel = potentieel inside knowledge.",
                   style={"color": COLORS["muted"], "fontSize": "0.78rem", "marginBottom": "16px"}),
            dbc.Row([
                _card("Sector matches",
                      dcc.Graph(figure=fig_sm, config={"displayModeBar": False}), width=5),
                _card("Detail", dbc.Table([sm_header, html.Tbody(sm_html_rows)],
                      bordered=False, hover=True, size="sm",
                      style={"color": COLORS["text"]}), width=7, height=300),
            ]),
        ], className="mb-5"))

    if not sections:
        return html.P("Onvoldoende data voor edge analyse. Probeer een langere periode.",
                      style={"color": COLORS["muted"]})

    return html.Div([
        html.Div([
            html.P("5 data-driven edges uit STOCK Act disclosures. "
                   "Elke sectie toont een andere manier om informatie-asymmetrie te detecteren.",
                   style={"color": COLORS["muted"], "fontSize": "0.82rem", "marginBottom": "24px"}),
        ]),
        *sections,
    ])


# ─── Run ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Congressional Trading Tracker - http://127.0.0.1:8051")
    app.run(debug=False, host="127.0.0.1", port=8051)
