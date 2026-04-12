"""
NSE Delivery + Price Data → Google Sheets  (Single Sheet, Single API Call)
===========================================================================
Fetches all Nifty 50 + Bank Nifty stocks every hour during market hours.
All data goes into ONE sheet with Symbol and Index columns.

Single API call per symbol:
    /api/quote-equity?symbol=X
    Returns: priceInfo (LTP, %change, VWAP) + securityWiseDP (delivery)
             + marketDeptOrderBook.tradeInfo (volume/value for VWAP fallback)

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
from google.oauth2.service_account import Credentials
from urllib.parse import quote

# ─────────────────────────────────────────────────────────────────────────────
# NIFTY 50 + BANK NIFTY  (deduplicated, with index labels)
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
    "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "ULTRACEMCO", "WIPRO", "ZOMATO",
}

BANKNIFTY = {
    "AUBANK", "AXISBANK", "BANDHANBNK", "BANKBARODA", "CANBK",
    "FEDERALBNK", "HDFCBANK", "ICICIBANK", "IDFCFIRSTB", "INDUSINDBK",
    "KOTAKBANK", "PNB", "SBIN",
}

def index_label(symbol: str) -> str:
    in_n = symbol in NIFTY50
    in_b = symbol in BANKNIFTY
    if in_n and in_b: return "Nifty50 + BankNifty"
    if in_n:          return "Nifty50"
    if in_b:          return "BankNifty"
    return "Other"

# All unique symbols, Nifty50 first then BankNifty-only
SCRIPS = sorted(NIFTY50) + sorted(BANKNIFTY - NIFTY50)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

IST          = ZoneInfo("Asia/Kolkata")
MARKET_START = dtime(9, 15)
MARKET_END   = dtime(15, 45)
SHEET_NAME   = "NSE Data"          # Single sheet name — change if you like

SHEET_HEADERS = [
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
    """Return the single 'NSE Data' worksheet, creating it if needed."""
    try:
        ws = spreadsheet.worksheet(SHEET_NAME)
        # Add header if sheet is blank
        if not ws.row_values(1):
            ws.insert_row(SHEET_HEADERS, 1)
            _format_header(spreadsheet, ws)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_NAME, rows=50000, cols=15)
        ws.insert_row(SHEET_HEADERS, 1)
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

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
}

def get_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    session.get("https://www.nseindia.com/", timeout=15)
    time.sleep(2)
    session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
    time.sleep(1)
    return session


# ─────────────────────────────────────────────────────────────────────────────
# NSE DATA FETCH — single API call per symbol
#
# Endpoint: /api/quote-equity?symbol=X  (no section param)
# Returns everything in one response:
#   priceInfo.lastPrice          → LTP
#   priceInfo.pChange            → % change
#   priceInfo.vwap               → VWAP (direct from NSE — most accurate)
#   securityWiseDP.*             → delivery data
#   marketDeptOrderBook.tradeInfo→ volume/value (VWAP fallback if priceInfo.vwap missing)
#
# VWAP fallback formula (if NSE doesn't return it directly):
#   NSE gives totalTradedVolume in LAKH shares, totalTradedValue in CRORES
#   VWAP = (totalTradedValue_crore * 1,00,00,000) / (totalTradedVolume_lakh * 1,00,000)
#         = (totalTradedValue / totalTradedVolume) * 100
# ─────────────────────────────────────────────────────────────────────────────

def fetch_data(symbol: str, session: requests.Session) -> dict:
    result = {
        "ltp":             "N/A",
        "pct_change":      "N/A",
        "vwap":            "N/A",
        "vs_vwap":         "N/A",
        "qty_traded":      "N/A",
        "deliverable_qty": "N/A",
        "pct_deliverable": "N/A",
        "as_of":           "",
        "error":           None,
    }
    try:
        # URL-encode symbol (handles M&M → M%26M)
        encoded = quote(symbol, safe="")
        url     = f"https://www.nseindia.com/api/quote-equity?symbol={encoded}"
        resp    = session.get(url, timeout=15)

        if resp.status_code != 200:
            result["error"] = f"HTTP {resp.status_code}"
            return result

        data = resp.json()

        # ── Price info ────────────────────────────────────────────────────────
        price_info = data.get("priceInfo", {})
        ltp        = price_info.get("lastPrice")
        pct_change = price_info.get("pChange")
        vwap_nse   = price_info.get("vwap")         # NSE provides VWAP directly

        ltp_float  = float(ltp)  if ltp        is not None else None
        vwap_float = float(vwap_nse) if vwap_nse is not None else None

        if ltp_float is not None:
            result["ltp"] = f"{ltp_float:,.2f}"

        if pct_change is not None:
            sign = "+" if float(pct_change) >= 0 else ""
            result["pct_change"] = f"{sign}{float(pct_change):.2f}%"

        # ── VWAP: use NSE value; calculate as fallback ─────────────────────
        if vwap_float is not None:
            result["vwap"] = f"{vwap_float:.2f}"
        else:
            # Fallback: calculate from totalTradedValue / totalTradedVolume
            ti        = data.get("marketDeptOrderBook", {}).get("tradeInfo", {})
            vol_lakh  = ti.get("totalTradedVolume")
            val_crore = ti.get("totalTradedValue")
            if vol_lakh and val_crore and float(vol_lakh) > 0:
                vwap_float     = (float(val_crore) * 1e7) / (float(vol_lakh) * 1e5)
                result["vwap"] = f"{vwap_float:.2f}"

        # ── Above / Below VWAP ────────────────────────────────────────────────
        if ltp_float is not None and vwap_float is not None:
            diff = ltp_float - vwap_float
            if diff > 0:
                result["vs_vwap"] = f"ABOVE (+{diff:.2f})"
            elif diff < 0:
                result["vs_vwap"] = f"BELOW (-{abs(diff):.2f})"
            else:
                result["vs_vwap"] = "AT VWAP"

        # ── Delivery info ─────────────────────────────────────────────────────
        dp = data.get("securityWiseDP", {})
        qty_traded      = dp.get("quantityTraded")
        deliverable_qty = dp.get("deliveryQuantity")
        pct_deliverable = dp.get("deliveryToTradedQuantity")
        result["as_of"] = dp.get("secWiseDelPosDate", "")

        result["qty_traded"]      = f"{int(qty_traded):,}"      if qty_traded      is not None else "N/A"
        result["deliverable_qty"] = f"{int(deliverable_qty):,}" if deliverable_qty is not None else "N/A"
        result["pct_deliverable"] = f"{float(pct_deliverable):.2f}%" if pct_deliverable is not None else "N/A"

    except Exception as e:
        result["error"] = str(e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def is_market_hours() -> bool:
    return MARKET_START <= datetime.now(IST).time() <= MARKET_END


def run():
    # if not is_market_hours():
    #     print(f"Skipping — outside market hours ({datetime.now(IST).strftime('%H:%M')} IST).")
    #     return

    timestamp   = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"Fetching {len(SCRIPS)} scrips at {timestamp}")
    print(f"Nifty50={len(NIFTY50)}  BankNifty={len(BANKNIFTY)}  "
          f"BankNifty-only={len(BANKNIFTY - NIFTY50)}")

    gc          = get_gsheet_client()
    spreadsheet = gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])
    ws          = get_or_create_sheet(spreadsheet)
    nse_session = get_nse_session()

    rows_to_append = []

    for symbol in SCRIPS:
        print(f"   {symbol:<15} ... ", end="", flush=True)
        data = fetch_data(symbol, nse_session)

        row = [
            timestamp,
            symbol,
            index_label(symbol),
            data["ltp"],
            data["pct_change"],
            data["vwap"],
            data["vs_vwap"],
            data["qty_traded"],
            data["deliverable_qty"],
            data["pct_deliverable"],
            data["as_of"],
            data.get("error") or "OK",
        ]
        rows_to_append.append(row)

        if data["error"]:
            print(f"ERROR — {data['error']}")
        else:
            print(
                f"LTP={data['ltp']}  ({data['pct_change']})  "
                f"VWAP={data['vwap']}  {data['vs_vwap']}"
            )
        time.sleep(0.5)   # be gentle with NSE

    # Batch append all rows in one Sheets API call — faster + fewer quota hits
    ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    print(f"\nDone. {len(rows_to_append)} rows written to '{SHEET_NAME}' tab.")


if __name__ == "__main__":
    run()
