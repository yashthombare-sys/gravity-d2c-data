#!/usr/bin/env python3
"""
Compare revenue totals: Local JSON files vs Google Sheets
FY 24-25 (Apr 2024 – Mar 2025) for D2C, Amazon, and Flipkart.
Read-only — does not modify anything.
"""

import json
import os
import re
import time
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"

# FY 24-25 months
MONTHS = [
    ("apr", "2024", "April 2024 MIS"),
    ("may", "2024", "May 2024 MIS"),
    ("jun", "2024", "June 2024 MIS"),
    ("jul", "2024", "July 2024 MIS"),
    ("aug", "2024", "August 2024 MIS"),
    ("sep", "2024", "September 2024 MIS"),
    ("oct", "2024", "October 2024 MIS"),
    ("nov", "2024", "November 2024 MIS"),
    ("dec", "2024", "December 2024 MIS"),
    ("jan", "2025", "January 2025 MIS"),
    ("feb", "2025", "February 2025 MIS"),
    ("mar", "2025", "March 2025 MIS"),
]


def parse_currency(val):
    """Parse '₹1,234,567' or '₹12,345.67' to float. Returns 0 if empty/unparseable."""
    if not val:
        return 0.0
    cleaned = re.sub(r'[₹,\s]', '', val)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def load_local_revenue(filepath):
    """Sum 'revenue' field across all products in a JSON file."""
    if not os.path.exists(filepath):
        return None
    with open(filepath) as f:
        data = json.load(f)
    total = 0.0
    for product, info in data.items():
        total += info.get("revenue", 0)
    return round(total, 2)


def extract_gsheet_sections(all_rows):
    """
    Parse a Google Sheet tab into sections: D2C, Amazon, Flipkart.
    Returns dict with grand total revenue for each section found.
    """
    sections = {}
    current_section = "D2C"  # First section is always D2C

    for i, row in enumerate(all_rows):
        cell_a = row[0].strip() if row[0] else ""
        cell_b = row[1].strip() if len(row) > 1 and row[1] else ""

        # Detect section headers
        if "AMAZON MIS" in cell_a.upper():
            current_section = "Amazon"
            continue
        elif "FLIPKART MIS" in cell_a.upper():
            current_section = "Flipkart"
            continue
        elif "FIRSTCRY MIS" in cell_a.upper():
            current_section = "FirstCry"
            continue
        elif "BLINKIT MIS" in cell_a.upper():
            current_section = "Blinkit"
            continue
        elif "INSTAMART MIS" in cell_a.upper():
            current_section = "Instamart"
            continue

        # Look for GRAND TOTAL row in current section
        if "GRAND TOTAL" in cell_a.upper():
            revenue = parse_currency(cell_b)
            sections[current_section] = revenue

    return sections


def main():
    # Connect to Google Sheets
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
              'https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    results = []

    for abbr, year, tab_name in MONTHS:
        # --- Local JSON revenues ---
        d2c_file = os.path.join(BASE, f"{abbr}_{year}_mis_data.json")
        amz_file = os.path.join(BASE, f"amazon_{abbr}_{year}_mis_data.json")
        fk_file = os.path.join(BASE, f"flipkart_{abbr}_{year}_mis_data.json")

        local_d2c = load_local_revenue(d2c_file)
        local_amz = load_local_revenue(amz_file)
        local_fk = load_local_revenue(fk_file)

        # --- Google Sheets revenues ---
        print(f"Reading sheet: {tab_name}...", flush=True)
        ws = sh.worksheet(tab_name)
        all_rows = ws.get_all_values()
        gs_sections = extract_gsheet_sections(all_rows)

        gs_d2c = gs_sections.get("D2C")
        gs_amz = gs_sections.get("Amazon")
        gs_fk = gs_sections.get("Flipkart")

        # Store results
        month_label = f"{abbr.capitalize()} {year}"
        results.append((month_label, "D2C", local_d2c, gs_d2c))
        if local_amz is not None or gs_amz is not None:
            results.append((month_label, "Amazon", local_amz, gs_amz))
        if local_fk is not None or gs_fk is not None:
            results.append((month_label, "Flipkart", local_fk, gs_fk))

        time.sleep(2)

    # Print comparison table
    print("\n" + "=" * 100)
    print(f"{'Month':<12} {'Channel':<10} {'Local JSON Revenue':>20} {'Google Sheets Revenue':>22} {'Difference':>15} {'Match?':>8}")
    print("=" * 100)

    d2c_local_total = 0.0
    d2c_gs_total = 0.0
    amz_local_total = 0.0
    amz_gs_total = 0.0
    fk_local_total = 0.0
    fk_gs_total = 0.0

    for month, channel, local_rev, gs_rev in results:
        local_str = f"₹{local_rev:,.2f}" if local_rev is not None else "N/A"
        gs_str = f"₹{gs_rev:,.2f}" if gs_rev is not None else "N/A"

        if local_rev is not None and gs_rev is not None:
            diff = local_rev - gs_rev
            diff_str = f"₹{diff:,.2f}" if diff != 0 else "₹0.00"
            match = "YES" if abs(diff) < 1 else "NO"
        else:
            diff_str = "—"
            match = "—"

        # Accumulate totals
        if channel == "D2C":
            if local_rev is not None: d2c_local_total += local_rev
            if gs_rev is not None: d2c_gs_total += gs_rev
        elif channel == "Amazon":
            if local_rev is not None: amz_local_total += local_rev
            if gs_rev is not None: amz_gs_total += gs_rev
        elif channel == "Flipkart":
            if local_rev is not None: fk_local_total += local_rev
            if gs_rev is not None: fk_gs_total += gs_rev

        print(f"{month:<12} {channel:<10} {local_str:>20} {gs_str:>22} {diff_str:>15} {match:>8}")

    # Print totals
    print("-" * 100)
    d2c_diff = d2c_local_total - d2c_gs_total
    print(f"{'FY TOTAL':<12} {'D2C':<10} {'₹{:,.2f}'.format(d2c_local_total):>20} {'₹{:,.2f}'.format(d2c_gs_total):>22} {'₹{:,.2f}'.format(d2c_diff):>15} {'YES' if abs(d2c_diff) < 1 else 'NO':>8}")

    if amz_local_total > 0 or amz_gs_total > 0:
        amz_diff = amz_local_total - amz_gs_total
        print(f"{'FY TOTAL':<12} {'Amazon':<10} {'₹{:,.2f}'.format(amz_local_total):>20} {'₹{:,.2f}'.format(amz_gs_total):>22} {'₹{:,.2f}'.format(amz_diff):>15} {'YES' if abs(amz_diff) < 1 else 'NO':>8}")

    if fk_local_total > 0 or fk_gs_total > 0:
        fk_diff = fk_local_total - fk_gs_total
        print(f"{'FY TOTAL':<12} {'Flipkart':<10} {'₹{:,.2f}'.format(fk_local_total):>20} {'₹{:,.2f}'.format(fk_gs_total):>22} {'₹{:,.2f}'.format(fk_diff):>15} {'YES' if abs(fk_diff) < 1 else 'NO':>8}")

    print("=" * 100)


if __name__ == "__main__":
    main()
