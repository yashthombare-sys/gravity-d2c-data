#!/usr/bin/env python3
"""Add 'Ad Spent' column (S) to all 5 MIS sheets in Google Sheets."""

import time
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

WORKSHEETS = [
    "April 2025 MIS",
    "May 2025 MIS",
    "June 2025 MIS",
    "July 2025 MIS",
    "August 2025 MIS",
    "September 2025 MIS",
    "October 2025 MIS",
    "November 2025 MIS",
    "December 2025 MIS",
    "January 2026 MIS",
    "February 2026 MIS",
]


def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for ws_title in WORKSHEETS:
        print(f"\nUpdating: {ws_title}")
        try:
            ws = sh.worksheet(ws_title)
        except gspread.exceptions.WorksheetNotFound:
            print(f"  Sheet not found, skipping")
            continue

        # Expand columns if needed (18 -> 19)
        if ws.col_count < 19:
            ws.resize(cols=19)
            time.sleep(1)

        # Add header in S1
        ws.update(range_name="S1", values=[["Ad Spent"]], value_input_option="USER_ENTERED")

        # Format S1 like other headers
        ws.format("S1", {
            "backgroundColor": {"red": 0.157, "green": 0.255, "blue": 0.459},
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        })

        # Get all values in column A to find GRAND TOTAL row
        col_a = ws.col_values(1)
        grand_total_row = None
        for i, val in enumerate(col_a):
            if val and "GRAND TOTAL" in val.upper():
                grand_total_row = i + 1
                break

        if grand_total_row:
            # Add SUM formula for Ad Spent in Grand Total row
            # Find subtotal rows
            subtotal_rows = []
            for i, val in enumerate(col_a):
                if val and "Subtotal" in val:
                    subtotal_rows.append(i + 1)

            # Format Grand Total S cell
            ws.format(f"S{grand_total_row}", {
                "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
                "textFormat": {"bold": True, "fontSize": 11,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            })

        # Format currency for S column
        last_row = grand_total_row or len(col_a)
        ws.format(f"S2:S{last_row}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })

        print(f"  Added 'Ad Spent' column (S)")
        time.sleep(2)

    print(f"\nDone! All 5 sheets updated with Ad Spent column.")
    print(f"Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
