#!/usr/bin/env python3
"""Clean up stray ad spend values written to Amazon section rows, then re-push correct D2C values."""

import time
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

WORKSHEETS = [
    "October 2025 MIS",
    "November 2025 MIS",
    "December 2025 MIS",
    "January 2026 MIS",
    "February 2026 MIS",
]


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for ws_title in WORKSHEETS:
        print(f"\nCleaning: {ws_title}")
        ws = sh.worksheet(ws_title)
        time.sleep(1)

        col_a = ws.col_values(1)
        time.sleep(1)

        # Find first GRAND TOTAL row (end of Shiprocket section)
        first_grand_total = None
        for i, val in enumerate(col_a):
            if val and "GRAND TOTAL" in val.upper():
                first_grand_total = i + 1
                break

        if not first_grand_total:
            print("  No GRAND TOTAL found, skipping")
            continue

        # Clear all S cells AFTER the first GRAND TOTAL (Amazon section)
        last_row = len(col_a)
        if last_row > first_grand_total:
            clear_range = f"S{first_grand_total + 1}:S{last_row}"
            ws.batch_clear([clear_range])
            print(f"  Cleared {clear_range} (Amazon section)")
            time.sleep(2)

    print("\n✅ Cleanup done!")


if __name__ == "__main__":
    main()
