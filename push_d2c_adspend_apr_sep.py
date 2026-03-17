#!/usr/bin/env python3
"""Push D2C Ad Spend totals into column S Grand Total row for Apr-Sep 2025."""

import time
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

# Ad spend values from screenshot (these are total monthly D2C ad spend)
AD_SPEND = {
    "April 2025 MIS": 4000091,
    "May 2025 MIS": 1712733,
    "June 2025 MIS": 878270,
    "July 2025 MIS": 2582040,
    "August 2025 MIS": 4187079,
    "September 2025 MIS": 4024227,
}


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for ws_title, spend in AD_SPEND.items():
        print(f"\n  Updating: {ws_title} — Ad Spend: ₹{spend:,}")
        try:
            ws = sh.worksheet(ws_title)
        except gspread.exceptions.WorksheetNotFound:
            print(f"    ⚠️  Not found, skipping")
            continue

        # Find GRAND TOTAL row
        col_a = ws.col_values(1)
        grand_total_row = None
        for i, val in enumerate(col_a):
            if val and "GRAND TOTAL" in val.upper():
                grand_total_row = i + 1
                break

        if not grand_total_row:
            print(f"    ⚠️  No GRAND TOTAL row found, skipping")
            continue

        # Put ad spend in Grand Total S cell
        ws.update(range_name=f"S{grand_total_row}", values=[[spend]], value_input_option="USER_ENTERED")
        print(f"    ✓ Written ₹{spend:,} to S{grand_total_row}")
        time.sleep(2)

    print(f"\n✅ Done! Ad Spend updated for Apr-Sep 2025")
    print(f"   Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
