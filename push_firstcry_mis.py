#!/usr/bin/env python3
"""
Push FirstCry MIS (Apr 2025 – Feb 2026) to Google Sheets.
Appends BELOW existing Flipkart data in each month's tab.
NEVER modifies Shiprocket, Amazon, or Flipkart sections.

Columns: Products(A), Revenue(B), Orders(C), Delivered(D),
         COGS(E), COGS/Unit(F), Ad Spend(G), Profit(H)
"""

import json, time, os
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")

NUM_COLS = 15  # A through O (same sheet width)

CATEGORIES = [
    {
        "name": "BUSY BOARD CATEGORY",
        "color": {"red": 0.933, "green": 0.522, "blue": 0.133},
        "products": [
            "V1", "V2", "V3", "V4", "V5", "V6", "V7 Police Cruiser", "V8",
            "V9", "V10",
            "V1- P of 2", "V4- P of 2", "V4- P of 3",
            "V1-V2 Combo", "V1-V4 Combo", "V1-V6 Combo",
            "V2-V4 Combo", "V6-V2 Combo", "V4-V6 Combo",
            "V9-V10 Combo", "V2-V9 Combo",
            "Busy Book Blue", "Busy Book Pink",
        ],
    },
    {
        "name": "SOFT TOY CATEGORY",
        "color": {"red": 0.678, "green": 0.847, "blue": 0.902},
        "products": ["Ganesha", "Krishna", "Hanuman"],
    },
    {
        "name": "STEM CATEGORY",
        "color": {"red": 0.576, "green": 0.769, "blue": 0.490},
        "products": ["Car", "Tank", "JCB", "Drawing Board", "Color Matching Game"],
    },
]

FC_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "Returned",
    "COGS", "COGS/Unit", "", "", "",
    "", "", "", "Ad Spend", "Profit",
]

MONTHS = {
    "Jan 2025": ("January 2025 MIS", "firstcry_jan_2025_mis_data.json"),
    "Apr 2025": ("April 2025 MIS", "firstcry_apr_2025_mis_data.json"),
    "May 2025": ("May 2025 MIS", "firstcry_may_2025_mis_data.json"),
    "Jun 2025": ("June 2025 MIS", "firstcry_jun_2025_mis_data.json"),
    "Jul 2025": ("July 2025 MIS", "firstcry_jul_2025_mis_data.json"),
    "Aug 2025": ("August 2025 MIS", "firstcry_aug_2025_mis_data.json"),
    "Sep 2025": ("September 2025 MIS", "firstcry_sep_2025_mis_data.json"),
    "Oct 2025": ("October 2025 MIS", "firstcry_oct_2025_mis_data.json"),
    "Nov 2025": ("November 2025 MIS", "firstcry_nov_2025_mis_data.json"),
    "Dec 2025": ("December 2025 MIS", "firstcry_dec_2025_mis_data.json"),
    "Jan 2026": ("January 2026 MIS", "firstcry_jan_2026_mis_data.json"),
    "Feb 2026": ("February 2026 MIS", "firstcry_feb_2026_mis_data.json"),
}


COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170, "V6": 275, "V9": 778, "V10": 1009,
    "Busy Book Blue": 300, "Busy Book Pink": 300, "Human Book": 300,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862, "JCB": 862, "Drawing Board": 250,
}


def make_product_row(product, data, r):
    """Build one FirstCry product row. Uses same A-O columns as other sections."""
    cogs_unit = data.get("cogs_unit", COGS_MAP.get(product, 0))
    return [
        product,                                        # A: Products
        round(data["revenue"], 2),                      # B: Revenue
        data["total_orders"],                           # C: Orders
        data.get("delivered", data.get("shipped", data["total_orders"])),  # D: Delivered
        data.get("returned", data.get("rto", 0)),       # E: Returned
        f"=G{r}*D{r}",                                 # F: COGS = COGS/Unit × Delivered
        cogs_unit,                                      # G: COGS/Unit
        "", "", "", "", "", "",                          # H-M: empty (no fee breakdown for FirstCry)
        round(data.get("ad_spend", 0), 2),              # N: Ad Spend
        f"=B{r}-F{r}-N{r}",                             # O: Profit = Revenue - COGS - Ad Spend
    ]


def make_subtotal_row(label, first, last, r):
    """Build a subtotal row."""
    row = [label]
    for col_idx in range(1, NUM_COLS):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "G":    # COGS/Unit — no subtotal
            row.append("")
        elif col_letter in ("H", "I", "J", "K", "L", "M"):  # unused fee cols
            row.append("")
        elif col_letter == "O":  # Profit
            row.append(f"=B{r}-F{r}-N{r}")
        else:
            row.append(f"=SUM({col_letter}{first}:{col_letter}{last})")
    return row


def build_firstcry_section(product_data, start_row):
    """Build FirstCry section rows starting at start_row."""
    rows = []
    fmt = {"title_row": None, "header_row": None, "category_headers": [],
           "subtotal_rows": [], "grand_total_row": None}

    r = start_row

    # Title row
    rows.append(["FIRSTCRY MIS"] + [""] * (NUM_COLS - 1))
    fmt["title_row"] = r
    r += 1

    # Header row
    rows.append(FC_HEADERS)
    fmt["header_row"] = r
    r += 1

    subtotal_refs = []

    for category in CATEGORIES:
        rows.append([category["name"]] + [""] * (NUM_COLS - 1))
        fmt["category_headers"].append((r, category["color"]))
        r += 1

        first_product_row = r
        products_in_cat = 0

        for product in category["products"]:
            data = product_data.get(product)
            if not data or data["total_orders"] == 0:
                continue
            rows.append(make_product_row(product, data, r))
            products_in_cat += 1
            r += 1

        if products_in_cat > 0:
            last_product_row = r - 1
            rows.append(make_subtotal_row(
                f"{category['name']} — Subtotal", first_product_row, last_product_row, r
            ))
            fmt["subtotal_rows"].append((r, category["color"]))
            subtotal_refs.append(r)
            r += 1
        else:
            rows.append(["(no FirstCry orders)"] + [""] * (NUM_COLS - 1))
            r += 1

        # Spacer
        rows.append([""] * NUM_COLS)
        r += 1

    # Grand Total
    gt = r
    grand_total = ["GRAND TOTAL"]
    for col_idx in range(1, NUM_COLS):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "G":
            grand_total.append("")
        elif col_letter in ("H", "I", "J", "K", "L", "M"):
            grand_total.append("")
        elif col_letter == "O":
            grand_total.append(f"=B{gt}-F{gt}-N{gt}")
        elif col_letter in ("B", "C", "D", "E", "F", "N"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            grand_total.append(f"={refs}" if refs else "")
        else:
            grand_total.append("")
    rows.append(grand_total)
    fmt["grand_total_row"] = gt

    return rows, fmt


def find_last_grand_total(all_values):
    """Find the last GRAND TOTAL row (after Flipkart or Amazon section)."""
    last = 0
    for i, row in enumerate(all_values):
        cell = (row[0] if row else "").strip()
        if cell == "GRAND TOTAL":
            last = i + 1  # 1-indexed
    return last


def push_month(sh, ws_title, data_file):
    """Push FirstCry MIS below Flipkart in existing worksheet."""
    print(f"\n  Pushing FirstCry: {ws_title}...")

    filepath = os.path.join(BASE, data_file)
    if not os.path.exists(filepath):
        print(f"    Data file '{data_file}' not found — skipping")
        return

    with open(filepath) as f:
        product_data = json.load(f)

    try:
        ws = sh.worksheet(ws_title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"    Worksheet '{ws_title}' not found — skipping")
        return

    all_values = ws.get_all_values()
    time.sleep(1)

    # Find the last GRAND TOTAL row (should be Flipkart's)
    last_row = find_last_grand_total(all_values)
    if last_row == 0:
        last_row = len(all_values)
    print(f"    Last GRAND TOTAL at row {last_row}")

    # Ensure enough rows
    needed_rows = last_row + 50
    if ws.row_count < needed_rows:
        ws.resize(rows=needed_rows)
    time.sleep(1)

    # Start FirstCry section 3 rows after last content
    start_row = last_row + 3
    fc_rows, fmt = build_firstcry_section(product_data, start_row)

    # Clear old FirstCry data (if any)
    total_rows = len(all_values)
    clear_from = last_row + 1
    for i, row in enumerate(all_values):
        cell = (row[0] if row else "").strip()
        if cell == "FIRSTCRY MIS":
            clear_from = i + 1  # 1-indexed
            # Adjust start_row to overwrite from FirstCry title
            start_row = clear_from
            fc_rows, fmt = build_firstcry_section(product_data, start_row)
            break

    try:
        ws.batch_clear([f"A{clear_from}:O{max(total_rows + 5, start_row + len(fc_rows) + 5)}"])
    except Exception:
        pass
    time.sleep(1)

    # Write FirstCry data
    ws.update(
        range_name=f"A{start_row}",
        values=fc_rows,
        value_input_option="USER_ENTERED",
    )
    print(f"    Written {len(fc_rows)} rows starting at row {start_row}")
    time.sleep(2)

    # ── Formatting ──

    # Title row — dark teal/green (distinct from Flipkart purple & Amazon blue)
    tr = fmt["title_row"]
    ws.format(f"A{tr}:O{tr}", {
        "backgroundColor": {"red": 0.0, "green": 0.545, "blue": 0.545},
        "textFormat": {"bold": True, "fontSize": 13,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(1)

    # Header row — darker teal
    hr = fmt["header_row"]
    ws.format(f"A{hr}:O{hr}", {
        "backgroundColor": {"red": 0.0, "green": 0.392, "blue": 0.392},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(1)

    # Category headers
    for row_num, color in fmt["category_headers"]:
        ws.format(f"A{row_num}:O{row_num}", {
            "backgroundColor": color,
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        time.sleep(1)

    # Subtotal rows
    for row_num, color in fmt["subtotal_rows"]:
        light = {k: min(1, v * 0.6 + 0.4) for k, v in color.items()}
        ws.format(f"A{row_num}:O{row_num}", {
            "backgroundColor": light, "textFormat": {"bold": True},
        })
        time.sleep(1)

    # Grand Total — dark gray
    gt = fmt["grand_total_row"]
    ws.format(f"A{gt}:O{gt}", {
        "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })
    time.sleep(1)

    # Currency format (B, F, N, O)
    for col in ["B", "F", "N", "O"]:
        ws.format(f"{col}{start_row}:{col}{gt}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })
    time.sleep(1)

    products = sum(
        1 for row in fc_rows
        if row[0] and row[0] not in ("Products", "GRAND TOTAL", "FIRSTCRY MIS", "")
        and "Subtotal" not in str(row[0])
        and "CATEGORY" not in str(row[0])
        and "no FirstCry" not in str(row[0])
    )
    print(f"    {products} products pushed")


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    print("\nPushing FirstCry MIS to Google Sheets\n")

    for month_key, (ws_title, data_file) in MONTHS.items():
        push_month(sh, ws_title, data_file)
        time.sleep(15)

    print(f"\nAll done! Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
