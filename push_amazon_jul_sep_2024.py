#!/usr/bin/env python3
"""
Push Amazon MIS for Jul-Sep 2024 to Google Sheets.
Uses the same build_amazon_section logic from push_amazon_mis.py.
"""

import json, time, os, sys

# Add path so we can import from push_amazon_mis
BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
sys.path.insert(0, BASE)

from push_amazon_mis import (
    build_amazon_section, push_month, SHEET_URL, CREDS_FILE
)

import gspread
from google.oauth2.service_account import Credentials

MONTHS_TO_PUSH = {
    "Jul 2024": ("July 2024 MIS", "amazon_jul_2024_mis_data.json"),
    "Aug 2024": ("August 2024 MIS", "amazon_aug_2024_mis_data.json"),
    "Sep 2024": ("September 2024 MIS", "amazon_sep_2024_mis_data.json"),
}


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    print("\nPushing Amazon MIS (Jul-Sep 2024) to Google Sheets\n")

    for month_key, (ws_title, data_file) in MONTHS_TO_PUSH.items():
        push_month(sh, ws_title, data_file)
        time.sleep(15)

    print(f"\nDone! Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
