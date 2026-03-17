#!/usr/bin/env python3
"""
Create a new Google Spreadsheet dedicated to Daily MIS.
Shares it with the user and pushes all historical data.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gspread
from google.oauth2.service_account import Credentials
from config import BASE_DIR

CREDS_FILE = os.path.join(BASE_DIR, "shiproket-mis-70c28ae6e7fb.json")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

USER_EMAIL = "hmthombare121@gmail.com"

def create_spreadsheet():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    # Create new spreadsheet
    spreadsheet = gc.create("Clapstore Daily MIS")
    spreadsheet.share(USER_EMAIL, perm_type="user", role="writer")
    print(f"Created: {spreadsheet.url}")
    print(f"Shared with: {USER_EMAIL}")
    return spreadsheet.url

if __name__ == "__main__":
    url = create_spreadsheet()
    print(f"\nSpreadsheet URL:\n{url}")
    print(f"\nNow update DAILY_SPREADSHEET_URL in push_daily_sheet.py with this URL.")
