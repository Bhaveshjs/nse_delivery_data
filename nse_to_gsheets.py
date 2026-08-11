"""
NSE Delivery + Price Data → Google Sheets
==========================================
Writes to TWO sheets every run:

  Tab 1 "Raw Data"        — every fetch appended chronologically (full history)
  Tab 2 "Latest Snapshot" — one row per symbol, overwritten each run + Signal column

Signal logic:
    STRONG BUY     : Price ABOVE VWAP + Delivery > 60% + % change positive
    ACCUMULATION   : Price BELOW VWAP + Delivery > 60% (buying despite weakness)
    DISTRIBUTION   : Price ABOVE VWAP + Delivery > 60% + % change negative
    SPECULATIVE    : Price ABOVE VWAP + Delivery < 40% (no real buying conviction)
    WEAK / AVOID   : Price BELOW VWAP + Delivery < 40% + % change negative
    NEUTRAL        : Everything else

Setup:
    pip install "nse[local,server]" gspread google-auth
    # installing both extras means one install works whether you run this
    # locally or on GitHub Actions — the script auto-detects which to use.

Credentials (either works, checked in this order):
    1. Environment variables (used automatically on GitHub Actions via secrets):
         GOOGLE_CREDENTIALS_JSON  — service account JSON key (full contents)
         GOOGLE_SHEET_ID          — ID from Google Sheet URL (/d/<THIS>/edit)
    2. Local files sitting next to this script (used automatically for local runs
       if the env vars above are not set):
         credentials.json  — the service account JSON key file, saved as-is
         sheet_id.txt      — a plain text file containing just the Sheet ID

GitHub Secrets (for the Actions workflow):
    GOOGLE_CREDENTIALS_JSON  — service account JSON key (full contents)
    GOOGLE_SHEET_ID          — ID from Google Sheet URL (/d/<THIS>/edit)
"""

import os, json, time, gspread
from nse import NSE  # actively-maintained NSE client that handles Akamai bot-protection cookies internally
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# NIFTY 50 + BANK NIFTY
# ─────────────────────────────────────────────────────────────────────────────

# NIFTY50 = {
#     "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
#     "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BHARTIARTL", "BPCL",
#     "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
#     "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
#     "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
#     "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
#     "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
#     "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
#     "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS",
#     "TECHM", "TITAN", "ULTRACEMCO", "WIPRO", "ZOMATO",
# }

# BANKNIFTY = {
#     "AUBANK", "AXISBANK", "BANDHANBNK", "BANKBARODA", "CANBK",
#     "FEDERALBNK", "HDFCBANK", "ICICIBANK", "IDFCFIRSTB", "INDUSINDBK",
#     "KOTAKBANK", "PNB", "SBIN",
# }

NIFTY50 = {
    "ADANIENT", "ADANIPORTS","RELIANCE", "SBIN", 
}

BANKNIFTY = {
    "ICICIBANK", 
}

def index_label(symbol):
    in_n, in_b = symbol in NIFTY50, symbol in BANKNIFTY
    if in_n and in_b: return "Nifty50 + BankNifty"
    if in_n:          return "Nifty50"
    if in_b:          return "BankNifty"
    return "Other"

SCRIPS = sorted(NIFTY50) + sorted(BANKNIFTY - NIFTY50)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

IST              = ZoneInfo("Asia/Kolkata")
MARKET_START     = dtime(9, 15)
MARKET_END       = dtime(15, 45)

RAW_SHEET        = "Raw Data"
SNAPSHOT_SHEET   = "Latest Snapshot"

# Delivery % thresholds for signals
HIGH_DELIVERY    = 60.0
LOW_DELIVERY     = 40.0

RAW_HEADERS = [
    "Timestamp (IST)", "Symbol", "Index",
    "LTP (Rs.)", "% Change", "VWAP (Rs.)", "Price vs VWAP",
    "Quantity Traded", "Deliverable Quantity (Gross)",
    "% Deliverable to Traded Qty", "As Of Date (NSE)", "Remarks",
]

SNAPSHOT_HEADERS = [
    "Symbol", "Index", "Signal",
    "LTP (Rs.)", "% Change", "VWAP (Rs.)", "Price vs VWAP",
    "Quantity Traded", "Deliverable Quantity (Gross)",
    "% Deliverable to Traded Qty", "As Of Date (NSE)",
    "Last Updated (IST)",
]

# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def compute_signal(d: dict) -> str:
    """
    Returns one of: STRONG BUY | ACCUMULATION | DISTRIBUTION |
                    SPECULATIVE RALLY | WEAK - AVOID | NEUTRAL
    based on VWAP position, delivery %, and % change.
    """
    try:
        vs      = d.get("vs_vwap", "")
        pct_del = d.get("pct_deliverable_raw", None)   # raw float
        pct_chg = d.get("pct_change_raw", None)        # raw float

        if pct_del is None or vs == "N/A":
            return "NEUTRAL"

        above   = "ABOVE" in vs
        below   = "BELOW" in vs
        high_d  = pct_del >= HIGH_DELIVERY
        low_d   = pct_del <  LOW_DELIVERY
        up      = pct_chg is not None and pct_chg >= 0
        down    = pct_chg is not None and pct_chg <  0

        if above and high_d and up:   return "STRONG BUY"
        if above and high_d and down: return "DISTRIBUTION"
        if below and high_d:          return "ACCUMULATION"
        if above and low_d:           return "SPECULATIVE RALLY"
        if below and low_d and down:  return "WEAK - AVOID"
        return "NEUTRAL"

    except Exception:
        return "NEUTRAL"


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Signal → background colour (RGB 0-1 for Sheets API)
SIGNAL_COLORS = {
    "STRONG BUY":       {"red": 0.60, "green": 0.88, "blue": 0.60},  # green
    "ACCUMULATION":     {"red": 0.67, "green": 0.88, "blue": 1.00},  # blue
    "DISTRIBUTION":     {"red": 1.00, "green": 0.73, "blue": 0.40},  # orange
    "SPECULATIVE RALLY":{"red": 1.00, "green": 0.97, "blue": 0.60},  # yellow
    "WEAK - AVOID":     {"red": 1.00, "green": 0.60, "blue": 0.60},  # red
    "NEUTRAL":          {"red": 0.95, "green": 0.95, "blue": 0.95},  # light gray
}

CREDENTIALS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.json")
SHEET_ID_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sheet_id.txt")

def get_gsheet_client():
    """
    Loads Google service account credentials from (in priority order):
      1. GOOGLE_CREDENTIALS_JSON env var (used on GitHub Actions via secrets)
      2. A local credentials.json file sitting next to this script (for local runs)
    """
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        creds_dict = json.loads(creds_json)
    elif os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
            creds_dict = json.load(f)
    else:
        raise EnvironmentError(
            "No Google credentials found. Either set the GOOGLE_CREDENTIALS_JSON "
            f"env var, or place your service account key file at:\n  {CREDENTIALS_FILE}"
        )

    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_sheet_id():
    """
    Loads the Google Sheet ID from (in priority order):
      1. GOOGLE_SHEET_ID env var (used on GitHub Actions via secrets)
      2. A local sheet_id.txt file sitting next to this script (for local runs)
    """
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if sheet_id:
        return sheet_id.strip()
    if os.path.exists(SHEET_ID_FILE):
        with open(SHEET_ID_FILE, "r", encoding="utf-8") as f:
            sheet_id = f.read().strip()
        if sheet_id:
            return sheet_id
    raise EnvironmentError(
        "No Google Sheet ID found. Either set the GOOGLE_SHEET_ID env var, "
        f"or place the sheet ID as plain text in:\n  {SHEET_ID_FILE}"
    )


def get_or_create_ws(spreadsheet, title, headers):
    """Get or create a worksheet, writing headers if brand new."""
    try:
        ws = spreadsheet.worksheet(title)
        if not ws.row_values(1):
            ws.insert_row(headers, 1)
            _fmt_header(spreadsheet, ws, len(headers))
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=50000, cols=len(headers) + 2)
        ws.insert_row(headers, 1)
        _fmt_header(spreadsheet, ws, len(headers))
    return ws


def _fmt_header(spreadsheet, ws, num_cols):
    """Dark blue header, white bold text."""
    spreadsheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                      "startColumnIndex": 0, "endColumnIndex": num_cols},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.12, "green": 0.31, "blue": 0.47},
                "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}, "fontSize": 10},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }
    }]})


def colour_snapshot_rows(spreadsheet, ws, data_by_symbol):
    """
    Colour each row in the Snapshot sheet based on its signal.
    Reads current row positions by matching Symbol column.
    """
    try:
        all_values = ws.get_all_values()
        requests   = []
        for row_idx, row in enumerate(all_values[1:], start=1):   # skip header
            if not row: continue
            symbol = row[0]
            signal = data_by_symbol.get(symbol, {}).get("signal", "NEUTRAL")
            color  = SIGNAL_COLORS.get(signal, SIGNAL_COLORS["NEUTRAL"])
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId":          ws.id,
                        "startRowIndex":    row_idx,
                        "endRowIndex":      row_idx + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex":   len(SNAPSHOT_HEADERS),
                    },
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": color,
                        "horizontalAlignment": "CENTER",
                    }},
                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment)"
                }
            })
        if requests:
            spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"   Warning: could not colour snapshot rows: {e}")


def update_snapshot(spreadsheet, ws, snapshot_rows, data_by_symbol):
    """
    Overwrite snapshot sheet: clear data rows, rewrite all symbols sorted by signal priority.
    """
    signal_order = ["STRONG BUY", "ACCUMULATION", "DISTRIBUTION",
                    "SPECULATIVE RALLY", "WEAK - AVOID", "NEUTRAL"]

    snapshot_rows.sort(key=lambda r: (
        signal_order.index(r[2]) if r[2] in signal_order else 99,
        r[0]   # then alphabetically by symbol
    ))

    # Clear everything below header
    ws.batch_clear(["A2:Z50000"])
    time.sleep(1)

    if snapshot_rows:
        ws.append_rows(snapshot_rows, value_input_option="USER_ENTERED")
        time.sleep(1)
        colour_snapshot_rows(spreadsheet, ws, data_by_symbol)


# ─────────────────────────────────────────────────────────────────────────────
# NSE SESSION + FETCH
# ─────────────────────────────────────────────────────────────────────────────

# Cookies get cached here between runs (the `nse` library handles the Akamai
# bot-protection handshake and stores/reuses cookies from this folder).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# GitHub Actions always sets GITHUB_ACTIONS=true on its runners, so we can
# auto-detect whether we're running "on a server" (cloud) vs locally, and use
# the transport the `nse` library recommends for each (httpx/http2 for cloud,
# requests for local) without you having to configure anything.
IS_SERVER = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def get_nse_client():
    return NSE(download_folder=SCRIPT_DIR, server=IS_SERVER)


def fetch_data(symbol, nse_client):
    result = {
        "ltp": "N/A", "pct_change": "N/A", "vwap": "N/A", "vs_vwap": "N/A",
        "qty_traded": "N/A", "deliverable_qty": "N/A",
        "pct_deliverable": "N/A", "as_of": "",
        # raw floats for signal engine
        "pct_change_raw": None, "pct_deliverable_raw": None,
        "error": None,
    }

    try:
        q = nse_client.quote(symbol)

        meta  = q.get("metaData", {})
        trade = q.get("tradeInfo", {})

        ltp        = trade.get("lastPrice")
        pct_change = meta.get("pChange")
        vwap_nse   = meta.get("averagePrice")   # NSE's day VWAP-equivalent
        qt         = trade.get("quantitytraded")
        dq         = trade.get("deliveryquantity")
        pct        = trade.get("deliveryToTradedQuantity")

        result["as_of"] = trade.get("secwisedelposdate", "")

        ltp_f  = float(ltp)      if ltp      is not None else None
        vwap_f = float(vwap_nse) if vwap_nse is not None else None

        if ltp_f is not None:      result["ltp"]  = f"{ltp_f:,.2f}"
        if pct_change is not None:
            pcf = float(pct_change)
            result["pct_change"]     = f"{'+' if pcf>=0 else ''}{pcf:.2f}%"
            result["pct_change_raw"] = pcf
        if vwap_f is not None:      result["vwap"] = f"{vwap_f:.2f}"

        result["qty_traded"]      = f"{int(qt):,}" if qt is not None else "N/A"
        result["deliverable_qty"] = f"{int(dq):,}" if dq is not None else "N/A"
        if pct is not None:
            pctf = float(pct)
            result["pct_deliverable"]     = f"{pctf:.2f}%"
            result["pct_deliverable_raw"] = pctf

        # Above / Below VWAP
        if ltp_f is not None and vwap_f is not None:
            diff = ltp_f - vwap_f
            result["vs_vwap"] = (
                f"ABOVE (+{diff:.2f})"      if diff > 0 else
                f"BELOW (-{abs(diff):.2f})" if diff < 0 else
                "AT VWAP"
            )

    except Exception as e:
        result["error"] = str(e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def is_market_hours():
    return MARKET_START <= datetime.now(IST).time() <= MARKET_END


def run():
    if not is_market_hours():
        print(f"Skipping — outside market hours ({datetime.now(IST).strftime('%H:%M')} IST).")
        return

    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"\n{'='*62}")
    print(f"Fetching {len(SCRIPS)} scrips at {timestamp}")
    print(f"{'='*62}")

    gc           = get_gsheet_client()
    spreadsheet  = gc.open_by_key(get_sheet_id())
    ws_raw       = get_or_create_ws(spreadsheet, RAW_SHEET,      RAW_HEADERS)
    ws_snap      = get_or_create_ws(spreadsheet, SNAPSHOT_SHEET, SNAPSHOT_HEADERS)
    nse_client   = get_nse_client()

    raw_rows      = []
    snapshot_rows = []
    data_by_symbol = {}

    try:
        for symbol in SCRIPS:
            print(f"  {symbol:<15} ", end="", flush=True)
            d      = fetch_data(symbol, nse_client)
            signal = compute_signal(d)
            data_by_symbol[symbol] = {**d, "signal": signal}

            # Row for Raw Data tab
            raw_rows.append([
                timestamp, symbol, index_label(symbol),
                d["ltp"], d["pct_change"], d["vwap"], d["vs_vwap"],
                d["qty_traded"], d["deliverable_qty"], d["pct_deliverable"],
                d["as_of"], d.get("error") or "OK",
            ])

            # Row for Latest Snapshot tab
            snapshot_rows.append([
                symbol, index_label(symbol), signal,
                d["ltp"], d["pct_change"], d["vwap"], d["vs_vwap"],
                d["qty_traded"], d["deliverable_qty"], d["pct_deliverable"],
                d["as_of"], timestamp,
            ])

            print(f"[{signal:<18}]  LTP={d['ltp']:>10}  {d['pct_change']:>8}  "
                  f"Deliv%={d['pct_deliverable']:>7}  {d['vs_vwap']}")
            time.sleep(0.5)
    finally:
        nse_client.exit()  # always close the NSE session, even if a fetch raised

    print(f"\nWriting to Google Sheets...")

    # Tab 1: append to Raw Data
    ws_raw.append_rows(raw_rows, value_input_option="USER_ENTERED")
    print(f"  Raw Data      : {len(raw_rows)} rows appended")

    # Tab 2: overwrite Snapshot with sorted + coloured data
    update_snapshot(spreadsheet, ws_snap, snapshot_rows, data_by_symbol)
    print(f"  Latest Snapshot: {len(snapshot_rows)} rows updated + colour coded")

    # Print signal summary
    from collections import Counter
    counts = Counter(data_by_symbol[s]["signal"] for s in SCRIPS)
    print(f"\nSignal summary:")
    for sig in ["STRONG BUY","ACCUMULATION","DISTRIBUTION","SPECULATIVE RALLY","WEAK - AVOID","NEUTRAL"]:
        if counts.get(sig, 0) > 0:
            print(f"  {sig:<20} : {counts[sig]} stocks")

    print(f"\nDone at {datetime.now(IST).strftime('%H:%M:%S IST')}")


if __name__ == "__main__":
    run()
