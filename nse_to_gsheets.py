"""
NSE Delivery Data → Google Sheets
===================================
Runs on GitHub Actions every hour during market hours.
Writes one tab per scrip into a shared Google Sheet.

Setup:
    pip install requests gspread google-auth

Environment variables needed (set as GitHub Secrets):
    GOOGLE_CREDENTIALS_JSON  — full contents of your service account JSON key
    GOOGLE_SHEET_ID          — the ID from your Google Sheet URL
                               https://docs.google.com/spreadsheets/d/<THIS_PART>/edit
"""

import os
import json
import time
import requests
import gspread
from datetime import datetime, time as dtime
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo


# ─────────────────────────────────────────────────────────────────────────────
# ✏️  CONFIGURE YOUR SCRIPS HERE
# ─────────────────────────────────────────────────────────────────────────────
SCRIPS = [
    "RELIANCE",
    "TCS",
    "INFY",
    "HDFCBANK",
    "WIPRO",
    # Add any NSE symbol here ↑
]

MARKET_START = dtime(9, 15)
MARKET_END   = dtime(23, 45)

# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS AUTH
# ─────────────────────────────────────────────────────────────────────────────

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def get_gsheet_client():
    """Authenticate using service account JSON from environment variable."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise EnvironmentError(
            "GOOGLE_CREDENTIALS_JSON env var not set.\n"
            "Set it to the full contents of your service account JSON key."
        )
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_sheet(spreadsheet, symbol: str):
    """Get worksheet for symbol, create it with headers if it doesn't exist."""
    headers = [
        "Timestamp",
        "Quantity Traded",
        "Deliverable Quantity (Gross)",
        "% Deliverable to Traded Qty",
        "As Of Date (NSE)",
        "Remarks",
    ]
    try:
        ws = spreadsheet.worksheet(symbol)
        # If sheet exists but has no header, write it
        if ws.row_count == 0 or ws.cell(1, 1).value != "Timestamp":
            ws.insert_row(headers, 1)
    except gspread.exceptions.WorksheetNotFound:
        # Create new sheet and write headers
        ws = spreadsheet.add_worksheet(title=symbol, rows=5000, cols=10)
        ws.insert_row(headers, 1)
        # Bold + color the header row using Sheets API formatting
        spreadsheet.batch_update({
            "requests": [{
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor":  {"red": 0.12, "green": 0.31, "blue": 0.47},
                            "textFormat":       {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            }]
        })
    return ws


# ─────────────────────────────────────────────────────────────────────────────
# NSE DATA FETCH  (same proven logic as nse_delivery_tracker.py)
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
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
    session.headers.update(HEADERS)
    session.get("https://www.nseindia.com/", timeout=15)
    time.sleep(2)
    session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
    time.sleep(1)
    return session


def fetch_delivery_data(symbol: str, session: requests.Session) -> dict:
    result = {
        "symbol":          symbol,
        "qty_traded":      "N/A",
        "deliverable_qty": "N/A",
        "pct_deliverable": "N/A",
        "as_of":           "",
        "error":           None,
    }
    try:
        url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}&section=trade_info"
        resp = session.get(url, timeout=15)

        if resp.status_code != 200:
            result["error"] = f"HTTP {resp.status_code}"
            return result

        dp = resp.json().get("securityWiseDP", {})

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
    now = datetime.now().time()
    return MARKET_START <= now <= MARKET_END


def run():
    # GitHub Actions always runs when triggered — skip if outside market hours
    if not is_market_hours():
        print(f"⏸  Outside market hours ({datetime.now().strftime('%H:%M')} IST). Exiting.")
        return

    #timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST")
    print(f"🕐 Fetching at {timestamp}")

    # Connect to Google Sheets
    gc           = get_gsheet_client()
    sheet_id     = os.environ["GOOGLE_SHEET_ID"]
    spreadsheet  = gc.open_by_key(sheet_id)

    # Create a fresh NSE session
    nse_session  = get_nse_session()

    for symbol in SCRIPS:
        print(f"   📈 {symbol} ... ", end="", flush=True)
        data = fetch_delivery_data(symbol, nse_session)

        ws = get_or_create_sheet(spreadsheet, symbol)

        row = [
            timestamp,
            data["qty_traded"],
            data["deliverable_qty"],
            data["pct_deliverable"],
            data["as_of"],
            data.get("error") or "OK",
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")

        if data["error"]:
            print(f"❌ {data['error']}")
        else:
            print(
                f"✅  Traded={data['qty_traded']} | "
                f"Deliv={data['deliverable_qty']} | "
                f"%={data['pct_deliverable']}  [{data['as_of']}]"
            )

        time.sleep(1)  # avoid Google Sheets rate limits

    print(f"\n✅ All done. Data written to Google Sheet ID: {sheet_id}")


if __name__ == "__main__":
    run()
