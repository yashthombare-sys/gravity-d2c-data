#!/usr/bin/env python3
"""
Push Blinkit MIS (Apr 2025 – Jan 2026) to Google Sheets.
Appends BELOW all existing sections in each month's tab.
NEVER modifies Shiprocket, Amazon, Flipkart, FirstCry, or Instamart sections.

Blinkit is PO-based (no commission, no Amazon-style fees).
Columns: Products(A), Revenue(B), Orders(C), COGS(D), COGS/Unit(E),
         Ads(F), Logistics(G), Profit(H), Profit %(I)
"""

import json, time, os
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
REF_DATA_FILE = os.path.join(BASE, "blinkit_ref_data.json")

NUM_COLS = 9  # A through I
SECTION_MARKER = "BLINKIT MIS"

BLINKIT_HEADERS = [
    "Products", "Revenue", "Orders", "COGS", "COGS/Unit",
    "Ads", "Logistics", "Profit", "Profit %",
]

PRODUCT_ORDER = ["V1", "V2", "V4", "V6"]

MONTHS = {
    "Nov 2024": "November 2024 MIS",
    "Dec 2024": "December 2024 MIS",
    "Jan 2025": "January 2025 MIS",
    "Feb 2025": "February 2025 MIS",
    "Mar 2025": "March 2025 MIS",
    "Apr 2025": "April 2025 MIS",
    "May 2025": "May 2025 MIS",
    "Jun 2025": "June 2025 MIS",
    "Jul 2025": "July 2025 MIS",
    "Aug 2025": "August 2025 MIS",
    "Sep 2025": "September 2025 MIS",
    "Oct 2025": "October 2025 MIS",
    "Nov 2025": "November 2025 MIS",
    "Dec 2025": "December 2025 MIS",
    "Jan 2026": "January 2026 MIS",
}

# FY 24-25 individual JSON files (different format from ref_data)
FY24_25_FILES = {
    "Nov 2024": "blinkit_nov_2024_mis_data.json",
    "Dec 2024": "blinkit_dec_2024_mis_data.json",
    "Jan 2025": "blinkit_jan_2025_mis_data.json",
    "Feb 2025": "blinkit_feb_2025_mis_data.json",
    "Mar 2025": "blinkit_mar_2025_mis_data.json",
}

COGS_MAP = {"V1": 225, "V2": 275, "V4": 170, "V6": 275}


def load_ref_data():
    with open(REF_DATA_FILE) as f:
        ref = json.load(f)
    # Also load FY 24-25 individual JSON files and convert to ref_data format
    for month_key, filename in FY24_25_FILES.items():
        fpath = os.path.join(BASE, filename)
        if not os.path.exists(fpath):
            continue
        with open(fpath) as f:
            raw = json.load(f)
        products = []
        total_rev = 0
        total_ads = 0
        for pname, pdata in raw.items():
            if pname not in PRODUCT_ORDER:
                continue
            rev = pdata.get("revenue", 0)
            orders = pdata.get("total_orders", pdata.get("delivered", 0))
            cogs_unit = COGS_MAP.get(pname, 0)
            product_exp = cogs_unit * orders
            logistics = pdata.get("freight", 0)
            ads = pdata.get("ad_spend", 0)
            total_rev += rev
            total_ads += ads
            products.append({
                "product": pname,
                "total_orders": orders,
                "total_revenue": rev,
                "product_exp": product_exp,
                "logistics": logistics,
            })
        ref[month_key] = {
            "products": products,
            "summary": {"total_revenue": total_rev, "ad_spent_total": total_ads},
        }
    return ref


def find_section_bounds(all_values, marker):
    """Find start and end of a section by its marker.
    Returns (start_row_1indexed, end_row_1indexed) or (None, None).
    End = the 'Blinkit Total' row or last non-empty row in section.
    """
    start = None
    for i, row in enumerate(all_values):
        cell = (row[0] if row else "").strip()
        if cell == marker:
            start = i + 1  # 1-indexed
        if start is not None and cell == "Blinkit Total":
            return start, i + 1
    if start is not None:
        # No total found — scan to end of section
        end = start
        for i in range(start - 1, len(all_values)):
            if any(cell.strip() for cell in all_values[i]):
                end = i + 1
            else:
                break
        return start, end
    return None, None


def find_last_content_row(all_values):
    """Find the absolute last row with any data (1-indexed)."""
    for i in range(len(all_values) - 1, -1, -1):
        if any(cell.strip() for cell in all_values[i]):
            return i + 1
    return 0


def build_blinkit_section(month_data, start_row):
    rows = []
    fmt = {"title_row": None, "header_row": None, "total_row": None}

    r = start_row
    products = month_data["products"]
    summary = month_data["summary"]
    ad_spent_total = summary["ad_spent_total"]
    total_revenue = summary["total_revenue"]

    product_map = {p["product"]: p for p in products}
    sorted_products = [product_map[name] for name in PRODUCT_ORDER if name in product_map]

    # Blank separator row
    rows.append([""] * NUM_COLS)
    r += 1

    # Title row
    rows.append([SECTION_MARKER] + [""] * (NUM_COLS - 1))
    fmt["title_row"] = r
    r += 1

    # Header row
    rows.append(BLINKIT_HEADERS)
    fmt["header_row"] = r
    r += 1

    # Product rows
    first_product_row = r
    for p in sorted_products:
        revenue = p["total_revenue"]
        orders = p["total_orders"]
        cogs = p["product_exp"]
        logistics = p["logistics"]

        revenue_share = revenue / total_revenue if total_revenue > 0 else 0
        ads = round(ad_spent_total * revenue_share, 2)

        profit = revenue - cogs - ads - logistics
        cogs_unit = round(cogs / orders, 2) if orders > 0 else 0

        rows.append([
            p["product"],
            round(revenue, 2),
            int(orders),
            round(cogs, 2),
            round(cogs_unit, 2),
            round(ads, 2),
            round(logistics, 2),
            f"=B{r}-D{r}-F{r}-G{r}",
            f'=IF(B{r}=0,"",H{r}/B{r})',
        ])
        r += 1

    last_product_row = r - 1

    # Blinkit Total row
    gt = r
    total_row = ["Blinkit Total"]
    for col_idx in range(1, NUM_COLS):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "E":
            total_row.append("")
        elif col_letter == "H":
            total_row.append(f"=B{gt}-D{gt}-F{gt}-G{gt}")
        elif col_letter == "I":
            total_row.append(f'=IF(B{gt}=0,"",H{gt}/B{gt})')
        else:
            total_row.append(f"=SUM({col_letter}{first_product_row}:{col_letter}{last_product_row})")
    rows.append(total_row)
    fmt["total_row"] = gt

    return rows, fmt


def push_month(sh, ref_data, month_key, ws_title):
    print(f"\n  Pushing Blinkit: {ws_title}...")

    if month_key not in ref_data:
        print(f"    No Blinkit data for {month_key} — skipping")
        return

    month_data = ref_data[month_key]

    try:
        ws = sh.worksheet(ws_title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"    Worksheet '{ws_title}' not found — skipping")
        return

    time.sleep(2)
    all_values = ws.get_all_values()
    time.sleep(2)

    # Check if Blinkit section already exists
    bk_start, bk_end = find_section_bounds(all_values, SECTION_MARKER)
    # Also check old marker "BLINKIT" for backward compat
    if bk_start is None:
        bk_start, bk_end = find_section_bounds(all_values, "BLINKIT")

    if bk_start is not None:
        # Clear ONLY the Blinkit section (including 1 row above for separator)
        clear_from = max(1, bk_start - 1)
        clear_to = bk_end + 1
        try:
            ws.batch_clear([f"A{clear_from}:I{clear_to}"])
            print(f"    Cleared old Blinkit data (rows {clear_from}-{clear_to})")
        except Exception as e:
            print(f"    Warning clearing old data: {e}")
        time.sleep(2)

        # Re-read
        all_values = ws.get_all_values()
        time.sleep(2)

    # Find absolute last content row and append after it
    last_row = find_last_content_row(all_values)
    start_row = last_row + 1

    blinkit_rows, fmt = build_blinkit_section(month_data, start_row)
    needed_rows = start_row + len(blinkit_rows) + 5
    if ws.row_count < needed_rows:
        ws.resize(rows=needed_rows)
        time.sleep(2)

    ws.update(
        range_name=f"A{start_row}",
        values=blinkit_rows,
        value_input_option="USER_ENTERED",
    )
    print(f"    Written {len(blinkit_rows)} rows starting at row {start_row}")
    time.sleep(2)

    # ── Formatting ──
    tr = fmt["title_row"]
    ws.format(f"A{tr}:I{tr}", {
        "backgroundColor": {"red": 0.557, "green": 0.267, "blue": 0.678},
        "textFormat": {"bold": True, "fontSize": 13,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(2)

    hr = fmt["header_row"]
    ws.format(f"A{hr}:I{hr}", {
        "backgroundColor": {"red": 0.365, "green": 0.137, "blue": 0.506},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(2)

    gt = fmt["total_row"]
    ws.format(f"A{gt}:I{gt}", {
        "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })
    time.sleep(2)

    for col in ["B", "D", "F", "G", "H"]:
        ws.format(f"{col}{start_row}:{col}{gt}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })
    time.sleep(2)

    ws.format(f"I{start_row}:I{gt}", {
        "numberFormat": {"type": "PERCENT", "pattern": "0.0%"},
    })
    time.sleep(1)

    print(f"    Done — {len(month_data['products'])} products pushed")


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    ref_data = load_ref_data()

    print("\nPushing Blinkit MIS to Google Sheets\n")
    print(f"Available months in ref data: {list(ref_data.keys())}")
    print(f"Months to push: {list(MONTHS.keys())}")

    for month_key, ws_title in MONTHS.items():
        push_month(sh, ref_data, month_key, ws_title)
        time.sleep(15)

    print(f"\nAll done! Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
