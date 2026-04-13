"""
NSE Delivery + Price Data → Google Sheets
==========================================
Fetches all Nifty 50 + Bank Nifty stocks every hour during market hours.
All data goes into ONE sheet with Symbol and Index columns.

Two API calls per symbol:
    Call 1: /api/quote-equity?symbol=X              → priceInfo (LTP, %change, VWAP)
    Call 2: /api/quote-equity?symbol=X&section=trade_info → securityWiseDP (delivery)

Columns:
    Timestamp | Symbol | Index | LTP | % Change | VWAP | Price vs VWAP |
    Qty Traded | Deliverable Qty | % Deliverable | As Of Date | Remarks

Setup:
    pip install requests gspread google-auth

GitHub Secrets needed:
    GOOGLE_CREDENTIALS_JSON  — full contents of service account JSON key
    GOOGLE_SHEET_ID          — ID from Google Sheet URL (/d/<THIS>/edit)
"""

import os, json, time, requests, gspread
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from urllib.parse import quote
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# NIFTY 50 + BANK NIFTY
# ─────────────────────────────────────────────────────────────────────────────

NIFTY50 = {
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BHARTIARTL", "BPCL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
    "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
    "SUNPHARMA", "TATACONSUM", "TMPV", "TCS",
    "TECHM", "TITAN", "ULTRACEMCO", "WIPRO", "ETERNAL","TATASTEEL"
}

BANKNIFTY = {
    "AUBANK", "AXISBANK", "BANDHANBNK", "BANKBARODA", "CANBK",
    "FEDERALBNK", "HDFCBANK", "ICICIBANK", "IDFCFIRSTB", "INDUSINDBK",
    "KOTAKBANK", "PNB", "SBIN",
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

IST          = ZoneInfo("Asia/Kolkata")
MARKET_START = dtime(9, 15)
MARKET_END   = dtime(15, 45)
SHEET_NAME   = "NSE Data"

HEADERS = [
    "Timestamp (IST)",
    "Symbol",
    "Index",
    "LTP (Rs.)",
    "% Change",
    "VWAP (Rs.)",
    "Price vs VWAP",
    "Quantity Traded",
    "Deliverable Quantity (Gross)",
    "% Deliverable to Traded Qty",
    "As Of Date (NSE)",
    "Remarks",
]

# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_gsheet_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise EnvironmentError("GOOGLE_CREDENTIALS_JSON env var not set.")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json), scopes=SCOPES
    )
    return gspread.authorize(creds)


def get_or_create_sheet(spreadsheet):
    """Return the single NSE Data worksheet, creating it with headers if needed."""
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
        if not ws.row_values(1):
            ws.insert_row(HEADERS, 1)
            _format_header(spreadsheet, ws)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=50000, cols=15)
        ws.insert_row(HEADERS, 1)
        _format_header(spreadsheet, ws)
    return ws


def _format_header(spreadsheet, ws):
    spreadsheet.batch_update({"requests": [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 0.12, "green": 0.31, "blue": 0.47},
                    "textFormat": {
                        "bold": True,
                        "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                        "fontSize": 10,
                    },
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }
    }]})


# ─────────────────────────────────────────────────────────────────────────────
# NSE SESSION
# ─────────────────────────────────────────────────────────────────────────────

NSE_HDRS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}

def get_nse_session():
    s = requests.Session()
    s.headers.update(NSE_HDRS)
    s.get("https://www.nseindia.com/", timeout=15);          time.sleep(2)
    s.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15); time.sleep(1)
    return s


# ─────────────────────────────────────────────────────────────────────────────
# NSE FETCH — 2 calls per symbol
#   Call 1 → /api/quote-equity?symbol=X              → priceInfo (LTP, %change, VWAP)
#   Call 2 → /api/quote-equity?symbol=X&section=trade_info → securityWiseDP (delivery)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_data(symbol, session):
    result = {
        "ltp": "N/A", "pct_change": "N/A", "vwap": "N/A", "vs_vwap": "N/A",
        "qty_traded": "N/A", "deliverable_qty": "N/A",
        "pct_deliverable": "N/A", "as_of": "", "error": None,
    }
    sym_enc = quote(symbol, safe="")

    try:
        # ── Call 1: price data ────────────────────────────────────────────────
        r1 = session.get(
            f"https://www.nseindia.com/api/quote-equity?symbol={sym_enc}",
            timeout=15
        )
        if r1.status_code != 200:
            result["error"] = f"Call1 HTTP {r1.status_code}"; return result

        pi         = r1.json().get("priceInfo", {})
        ltp        = pi.get("lastPrice")
        pct_change = pi.get("pChange")
        vwap_nse   = pi.get("vwap")

        ltp_f  = float(ltp)      if ltp      is not None else None
        vwap_f = float(vwap_nse) if vwap_nse is not None else None

        if ltp_f is not None:
            result["ltp"] = f"{ltp_f:,.2f}"
        if pct_change is not None:
            s = "+" if float(pct_change) >= 0 else ""
            result["pct_change"] = f"{s}{float(pct_change):.2f}%"
        if vwap_f is not None:
            result["vwap"] = f"{vwap_f:.2f}"

        time.sleep(0.4)

        # ── Call 2: delivery data ─────────────────────────────────────────────
        r2 = session.get(
            f"https://www.nseindia.com/api/quote-equity?symbol={sym_enc}&section=trade_info",
            timeout=15
        )
        if r2.status_code != 200:
            result["error"] = f"Call2 HTTP {r2.status_code}"; return result

        data2 = r2.json()
        dp    = data2.get("securityWiseDP", {})
        ti    = data2.get("marketDeptOrderBook", {}).get("tradeInfo", {})

        qt  = dp.get("quantityTraded")
        dq  = dp.get("deliveryQuantity")
        pct = dp.get("deliveryToTradedQuantity")
        result["as_of"]           = dp.get("secWiseDelPosDate", "")
        result["qty_traded"]      = f"{int(qt):,}"       if qt  is not None else "N/A"
        result["deliverable_qty"] = f"{int(dq):,}"       if dq  is not None else "N/A"
        result["pct_deliverable"] = f"{float(pct):.2f}%" if pct is not None else "N/A"

        # VWAP fallback from tradeInfo if priceInfo didn't have it
        if vwap_f is None:
            vol = ti.get("totalTradedVolume")
            val = ti.get("totalTradedValue")
            if vol and val and float(vol) > 0:
                vwap_f         = (float(val) * 1e7) / (float(vol) * 1e5)
                result["vwap"] = f"{vwap_f:.2f}"

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

    timestamp   = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"\n{'='*60}")
    print(f"Fetching {len(SCRIPS)} scrips at {timestamp}")
    print(f"{'='*60}")

    gc          = get_gsheet_client()
    spreadsheet = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])
    ws          = get_or_create_sheet(spreadsheet)
    nse         = get_nse_session()

    rows = []
    for symbol in SCRIPS:
        print(f"  {symbol:<15} ", end="", flush=True)
        d   = fetch_data(symbol, nse)
        row = [
            timestamp, symbol, index_label(symbol),
            d["ltp"], d["pct_change"], d["vwap"], d["vs_vwap"],
            d["qty_traded"], d["deliverable_qty"], d["pct_deliverable"],
            d["as_of"], d.get("error") or "OK",
        ]
        rows.append(row)

        if d["error"]:
            print(f"ERROR — {d['error']}")
        else:
            print(f"LTP={d['ltp']:>10}  {d['pct_change']:>8}  "
                  f"VWAP={d['vwap']:>10}  {d['vs_vwap']}")
        time.sleep(0.5)

    # Batch append all rows in one Sheets API call
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"\n{len(rows)} rows written to '{SHEET_NAME}' tab.")
    print(f"Done at {datetime.now(IST).strftime('%H:%M:%S IST')}")


if __name__ == "__main__":
    run()
