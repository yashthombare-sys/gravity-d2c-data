#!/usr/bin/env python3
"""Push March 2026 ad spend data to the Amazon Daily MIS Google Sheet."""
import json, os, time
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "shiproket-mis-70c28ae6e7fb.json")
SHEET_ID_FILE = os.path.join(BASE_DIR, ".amazon_daily_sheet_id")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Load daily data with ad spend
with open(os.path.join(BASE_DIR, "amazon_daily_march_2026.json")) as f:
    daily_data = json.load(f)

# Connect to Google Sheets
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

with open(SHEET_ID_FILE) as f:
    sheet_id = f.read().strip()

spreadsheet = gc.open_by_key(sheet_id)
ws = spreadsheet.worksheet("March 2026")

# Get existing dates to find row numbers
all_dates = ws.col_values(1)
print(f"Found {len(all_dates)-1} rows in sheet\n")

updated = 0
for day in daily_data:
    date_str = day["date"]
    ad_spend = round(day["ad_spend"] * 1.18, 2)  # +18% GST
    revenue = day["revenue"]
    cogs = day["cogs"]
    fees = day["fees_total"]

    if date_str not in all_dates:
        print(f"  {date_str}: not found in sheet, skipping")
        continue

    row_idx = all_dates.index(date_str) + 1

    # Recalculate with ad spend
    total_expense = cogs + fees + ad_spend
    profit = revenue - total_expense
    profit_pct = (profit / revenue * 100) if revenue > 0 else 0
    comm_pct = (fees / revenue * 100) if revenue > 0 else 0
    mktg_pct = (ad_spend / revenue * 100) if revenue > 0 else 0

    row = [
        date_str,
        round(revenue, 2),
        round(total_expense, 2),
        round(cogs, 2),
        round(ad_spend, 2),
        round(fees, 2),
        day["orders"],
        round(profit, 2),
        round(profit_pct, 2),
        round(comm_pct, 2),
        round(mktg_pct, 2),
    ]

    ws.update(values=[row], range_name=f"A{row_idx}:K{row_idx}")
    print(f"  {date_str}: Ad Spend ₹{ad_spend:,.2f} | Profit ₹{profit:,.2f} ({profit_pct:.1f}%)")
    updated += 1
    time.sleep(0.3)

print(f"\nUpdated {updated} days with ad spend data")
print(f"Sheet: {spreadsheet.url}")
