#!/usr/bin/env python3
"""
Push Amazon ad spend to GSheet and recalculate Total Expense, Profit, Profit%, Marketing%.

Usage:
    python3 push_amazon_adspend.py 2026-03-16 15761
    # Adds 18% GST automatically, updates all dependent columns
"""
import sys, os, re
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "shiproket-mis-70c28ae6e7fb.json")
SHEET_ID = "1u7hupogAQjxyQO6uNxDk9T3ehWDis5PwBgUaYlmIGZc"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

def parse_num(s):
    return float(re.sub(r'[₹,%]', '', s.replace(',', ''))) if s else 0

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 push_amazon_adspend.py <date> <amount_before_gst>")
        print("Example: python3 push_amazon_adspend.py 2026-03-16 15761")
        sys.exit(1)

    date_str = sys.argv[1]
    raw_amount = float(sys.argv[2])
    ad_spend = round(raw_amount * 1.18, 2)  # Add 18% GST

    # Determine month tab name
    from datetime import datetime
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    month_label = dt.strftime("%B %Y")

    print(f"Date: {date_str}")
    print(f"Ad Spend: ₹{raw_amount:,.0f} + 18% GST = ₹{ad_spend:,.2f}")
    print(f"Tab: {month_label}")

    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SHEET_ID)
    ws = sheet.worksheet(month_label)

    dates = ws.col_values(1)
    if date_str not in dates:
        print(f"ERROR: {date_str} not found in sheet")
        sys.exit(1)

    row_idx = dates.index(date_str) + 1
    row = ws.row_values(row_idx)

    revenue = parse_num(row[1])
    product_expense = parse_num(row[3])
    commissions = parse_num(row[5])

    # Recalculate all dependent values
    total_expense = product_expense + ad_spend + commissions
    profit = revenue - total_expense
    profit_pct = (profit / revenue * 100) if revenue > 0 else 0
    mktg_pct = (ad_spend / revenue * 100) if revenue > 0 else 0

    # Update all columns at once
    ws.update_cell(row_idx, 3, round(total_expense, 2))    # Col C: Total Expense
    ws.update_cell(row_idx, 5, round(ad_spend, 2))         # Col E: Ad Spend
    ws.update_cell(row_idx, 8, round(profit, 2))            # Col H: Profit
    ws.update_cell(row_idx, 9, round(profit_pct, 2))        # Col I: Profit %
    ws.update_cell(row_idx, 11, round(mktg_pct, 2))         # Col K: Marketing %

    print(f"\n✅ Updated {date_str}:")
    print(f"   Ad Spend:      ₹{ad_spend:>10,.2f}")
    print(f"   Total Expense:  ₹{total_expense:>10,.2f}")
    print(f"   Profit:         ₹{profit:>10,.2f} ({profit_pct:.1f}%)")
    print(f"   Marketing %:    {mktg_pct:.1f}%")

if __name__ == "__main__":
    main()
