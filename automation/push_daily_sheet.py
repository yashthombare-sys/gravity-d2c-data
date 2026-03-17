#!/usr/bin/env python3
"""
Push daily MIS data to Google Sheets — one row per product per day.

Sheet layout:
  Date | Channel | Product | Category | Total Orders | Shipped | Delivered |
  RTO | In-Transit | Cancelled | Revenue | COGS/Unit | Total COGS |
  Logistics | Total Expense | Profit | Profit% | RTO% | Delivered% |
  Meta Ad Spend | Google Ad Spend | Amazon Ad Spend

Appends new rows each day. Does NOT overwrite previous data.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from config import BASE_DIR, COGS_MAP, CATS
from db import get_daily_rows, get_ad_spend_for_date, get_conn

# ── Google Sheets config ──────────────────────────────────
CREDS_FILE = os.path.join(BASE_DIR, "shiproket-mis-70c28ae6e7fb.json")
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
DAILY_SHEET_NAME = "Daily MIS"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "Date", "Channel", "Product", "Category",
    "Total Orders", "Shipped", "Delivered", "RTO", "In-Transit", "Cancelled",
    "Revenue", "COGS/Unit", "Total COGS", "Logistics",
    "Total Expense", "Profit", "Profit %", "RTO %", "Delivered %",
    "Meta Ad Spend", "Google Ad Spend", "Amazon Ad Spend",
]


def get_gsheet_client():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_sheet(spreadsheet, sheet_name):
    """Get existing sheet or create new one with headers."""
    try:
        ws = spreadsheet.worksheet(sheet_name)
        return ws, False
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(HEADERS))
        # Write headers
        ws.update(values=[HEADERS], range_name="A1")
        # Format header row
        ws.format("A1:V1", {
            "backgroundColor": {"red": 0.15, "green": 0.24, "blue": 0.46},
            "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1},
                          "bold": True, "fontSize": 10},
            "horizontalAlignment": "CENTER",
        })
        ws.freeze(rows=1)
        return ws, True


def build_rows(date_str, daily_data, ad_spend):
    """
    Build spreadsheet rows from daily MIS data.
    Ad spend is allocated proportionally by revenue within each channel.
    """
    rows = []

    # Calculate total revenue per channel for proportional ad allocation
    channel_revenue = {}
    for row in daily_data:
        ch = row["channel"]
        channel_revenue[ch] = channel_revenue.get(ch, 0) + row["revenue"]

    meta_total = ad_spend.get("meta", 0)
    google_total = ad_spend.get("google", 0)
    amazon_ads_total = ad_spend.get("amazon_ads", 0)

    for row in daily_data:
        channel = row["channel"]
        product = row["product"]
        revenue = row["revenue"]
        cogs_unit = COGS_MAP.get(product, row.get("cogs_unit", 0))
        delivered = row["delivered"]
        shipped = row["shipped"]
        total_orders = row["total_orders"]
        rto = row["rto"]
        freight = row["freight"]  # logistics

        total_cogs = cogs_unit * shipped
        total_expense = total_cogs + freight
        profit = revenue - total_expense

        profit_pct = (profit / revenue * 100) if revenue > 0 else 0
        rto_pct = (rto / shipped * 100) if shipped > 0 else 0
        delivered_pct = (delivered / total_orders * 100) if total_orders > 0 else 0

        # Proportional ad spend allocation
        ch_rev = channel_revenue.get(channel, 1)
        rev_share = revenue / ch_rev if ch_rev > 0 else 0

        if channel == "shiprocket":
            meta_share = round(meta_total * rev_share, 2)
            google_share = round(google_total * rev_share, 2)
            amazon_ads_share = 0
        elif channel == "amazon":
            meta_share = 0
            google_share = 0
            amazon_ads_share = round(amazon_ads_total * rev_share, 2)
        else:
            meta_share = 0
            google_share = 0
            amazon_ads_share = 0

        category_display = {
            "busyboard": "Busy Board",
            "softtoy": "Soft Toy",
            "stem": "STEM"
        }.get(CATS.get(product, ""), row.get("category", ""))

        channel_display = "D2C Shiprocket" if channel == "shiprocket" else "Amazon FBA"

        rows.append([
            date_str,
            channel_display,
            product,
            category_display,
            total_orders,
            shipped,
            delivered,
            rto,
            row["in_transit"],
            row["cancelled"],
            round(revenue, 2),
            cogs_unit,
            round(total_cogs, 2),
            round(freight, 2),
            round(total_expense, 2),
            round(profit, 2),
            round(profit_pct, 1),
            round(rto_pct, 1),
            round(delivered_pct, 1),
            meta_share,
            google_share,
            amazon_ads_share,
        ])

    return rows


def push_date(date_str):
    """Push one day's data to Google Sheets."""
    print(f"Pushing {date_str} to Google Sheets...")

    # Get data from DB
    daily_data = get_daily_rows(date_str)
    if not daily_data:
        print(f"  No data for {date_str}")
        return 0

    ad_spend = get_ad_spend_for_date(date_str)

    # Build rows
    rows = build_rows(date_str, daily_data, ad_spend)
    if not rows:
        print(f"  No rows to push")
        return 0

    # Connect to Google Sheets
    gc = get_gsheet_client()
    spreadsheet = gc.open_by_url(SPREADSHEET_URL)
    ws, created = get_or_create_sheet(spreadsheet, DAILY_SHEET_NAME)

    if created:
        print(f"  Created new sheet: '{DAILY_SHEET_NAME}'")

    # Check if date already exists (avoid duplicates)
    existing = ws.col_values(1)  # Column A = dates
    if date_str in existing:
        print(f"  {date_str} already in sheet — skipping (delete rows manually to re-push)")
        return 0

    # Find next empty row
    next_row = len(existing) + 1

    # Append rows
    cell_range = f"A{next_row}:V{next_row + len(rows) - 1}"
    ws.update(values=rows, range_name=cell_range)

    # Format currency columns (K=Revenue, M=COGS, N=Logistics, O=Expense, P=Profit, T-V=Ad Spend)
    currency_cols = "K:K,M:M,N:N,O:O,P:P,T:T,U:U,V:V"
    for col_letter in ["K", "M", "N", "O", "P", "T", "U", "V"]:
        try:
            ws.format(f"{col_letter}{next_row}:{col_letter}{next_row + len(rows) - 1}", {
                "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"}
            })
        except Exception:
            pass

    # Format percentage columns (Q=Profit%, R=RTO%, S=Delivered%)
    for col_letter in ["Q", "R", "S"]:
        try:
            ws.format(f"{col_letter}{next_row}:{col_letter}{next_row + len(rows) - 1}", {
                "numberFormat": {"type": "NUMBER", "pattern": "0.0\"%\""}
            })
        except Exception:
            pass

    print(f"  ✓ {len(rows)} rows pushed to '{DAILY_SHEET_NAME}' (rows {next_row}-{next_row + len(rows) - 1})")
    return len(rows)


def push_range(date_from, date_to):
    """Push multiple days."""
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    current = start
    total = 0

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        total += push_date(date_str)
        current += timedelta(days=1)

    print(f"\nTotal: {total} rows pushed")
    return total


def main():
    args = sys.argv[1:]

    if not args:
        # Default: push yesterday
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        push_date(yesterday)
    elif len(args) == 1:
        push_date(args[0])
    elif len(args) == 2:
        push_range(args[0], args[1])
    else:
        print("Usage:")
        print("  python3 push_daily_sheet.py                    # Push yesterday")
        print("  python3 push_daily_sheet.py 2026-03-10         # Push specific date")
        print("  python3 push_daily_sheet.py 2026-03-01 2026-03-10  # Push date range")


if __name__ == "__main__":
    main()
