#!/usr/bin/env python3
"""
Push Instamart MIS (Apr 2025 – Jan 2026) to Google Sheets.
Appends BELOW all existing sections in each month's tab.
NEVER modifies Shiprocket, Amazon, Flipkart, FirstCry, or Blinkit sections.

Columns: Products(A), Revenue(B), Orders(C), Delivered(D), Returned(E),
         COGS(F), COGS/Unit(G), H-M empty, Ad Spend(N), Profit(O)
"""

import json, time, os
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")

NUM_COLS = 15  # A through O
SECTION_MARKER = "INSTAMART MIS"

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

IM_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "Returned",
    "COGS", "COGS/Unit", "", "", "",
    "", "", "", "Ad Spend", "Profit",
]

MONTHS = {
    "Feb 2025": ("February 2025 MIS", "instamart_feb_2025_mis_data.json"),
    "Mar 2025": ("March 2025 MIS", "instamart_mar_2025_mis_data.json"),
    "Apr 2025": ("April 2025 MIS", "instamart_apr_2025_mis_data.json"),
    "May 2025": ("May 2025 MIS", "instamart_may_2025_mis_data.json"),
    "Jun 2025": ("June 2025 MIS", "instamart_jun_2025_mis_data.json"),
    "Jul 2025": ("July 2025 MIS", "instamart_jul_2025_mis_data.json"),
    "Aug 2025": ("August 2025 MIS", "instamart_aug_2025_mis_data.json"),
    "Sep 2025": ("September 2025 MIS", "instamart_sep_2025_mis_data.json"),
    "Oct 2025": ("October 2025 MIS", "instamart_oct_2025_mis_data.json"),
    "Nov 2025": ("November 2025 MIS", "instamart_nov_2025_mis_data.json"),
    "Dec 2025": ("December 2025 MIS", "instamart_dec_2025_mis_data.json"),
    "Jan 2026": ("January 2026 MIS", "instamart_jan_2026_mis_data.json"),
}


COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170, "V6": 275, "V9": 778, "V10": 1009,
    "Busy Book Blue": 300, "Busy Book Pink": 300, "Human Book": 300,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862, "JCB": 862, "Drawing Board": 250,
}


def make_product_row(product, data, r):
    cogs_unit = data.get("cogs_unit", COGS_MAP.get(product, 0))
    return [
        product,                                        # A
        round(data["revenue"], 2),                      # B
        data["total_orders"],                           # C
        data.get("delivered", data.get("shipped", data["total_orders"])),  # D
        data.get("returned", data.get("rto", 0)),       # E
        f"=G{r}*D{r}",                                 # F: COGS
        cogs_unit,                                      # G: COGS/Unit
        "", "", "", "", "", "",                          # H-M empty
        round(data.get("ad_spend", 0), 2),              # N: Ad Spend
        f"=B{r}-F{r}-N{r}",                             # O: Profit
    ]


def make_subtotal_row(label, first, last, r):
    row = [label]
    for col_idx in range(1, NUM_COLS):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "G":
            row.append("")
        elif col_letter in ("H", "I", "J", "K", "L", "M"):
            row.append("")
        elif col_letter == "O":
            row.append(f"=B{r}-F{r}-N{r}")
        else:
            row.append(f"=SUM({col_letter}{first}:{col_letter}{last})")
    return row


def find_section_bounds(all_values, marker):
    """Find start and end of a section by its marker.
    Returns (start_row_1indexed, end_row_1indexed) or (None, None).
    End = the GRAND TOTAL row within this section.
    """
    start = None
    for i, row in enumerate(all_values):
        cell = (row[0] if row else "").strip()
        if cell == marker:
            start = i + 1  # 1-indexed
        if start is not None and i > (start - 1) and cell == "GRAND TOTAL":
            return start, i + 1
    if start is not None:
        # No GRAND TOTAL found — scan to end of contiguous data
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


def build_instamart_section(product_data, start_row):
    rows = []
    fmt = {"title_row": None, "header_row": None, "category_headers": [],
           "subtotal_rows": [], "grand_total_row": None}

    r = start_row

    # Blank separator row
    rows.append([""] * NUM_COLS)
    r += 1

    rows.append([SECTION_MARKER] + [""] * (NUM_COLS - 1))
    fmt["title_row"] = r
    r += 1

    rows.append(IM_HEADERS)
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
            rows.append(["(no Instamart orders)"] + [""] * (NUM_COLS - 1))
            r += 1

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


def push_month(sh, ws_title, data_file):
    print(f"\n  Pushing Instamart: {ws_title}...")

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

    time.sleep(2)
    all_values = ws.get_all_values()
    time.sleep(2)

    # Check if Instamart section already exists
    im_start, im_end = find_section_bounds(all_values, SECTION_MARKER)

    if im_start is not None:
        # Clear ONLY the Instamart section (including 1 row above for separator)
        clear_from = max(1, im_start - 1)
        clear_to = im_end + 1
        try:
            ws.batch_clear([f"A{clear_from}:O{clear_to}"])
            print(f"    Cleared old Instamart data (rows {clear_from}-{clear_to})")
        except Exception as e:
            print(f"    Warning clearing old data: {e}")
        time.sleep(2)

        # Re-read
        all_values = ws.get_all_values()
        time.sleep(2)

    # Find absolute last content row and append after it
    last_row = find_last_content_row(all_values)
    start_row = last_row + 1

    im_rows, fmt = build_instamart_section(product_data, start_row)

    needed_rows = start_row + len(im_rows) + 5
    if ws.row_count < needed_rows:
        ws.resize(rows=needed_rows)
        time.sleep(2)

    ws.update(
        range_name=f"A{start_row}",
        values=im_rows,
        value_input_option="USER_ENTERED",
    )
    print(f"    Written {len(im_rows)} rows starting at row {start_row}")
    time.sleep(2)

    # ── Formatting ──

    tr = fmt["title_row"]
    ws.format(f"A{tr}:O{tr}", {
        "backgroundColor": {"red": 0.957, "green": 0.506, "blue": 0.055},
        "textFormat": {"bold": True, "fontSize": 13,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(1)

    hr = fmt["header_row"]
    ws.format(f"A{hr}:O{hr}", {
        "backgroundColor": {"red": 0.800, "green": 0.380, "blue": 0.000},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(1)

    for row_num, color in fmt["category_headers"]:
        ws.format(f"A{row_num}:O{row_num}", {
            "backgroundColor": color,
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        time.sleep(1)

    for row_num, color in fmt["subtotal_rows"]:
        light = {k: min(1, v * 0.6 + 0.4) for k, v in color.items()}
        ws.format(f"A{row_num}:O{row_num}", {
            "backgroundColor": light, "textFormat": {"bold": True},
        })
        time.sleep(1)

    gt = fmt["grand_total_row"]
    ws.format(f"A{gt}:O{gt}", {
        "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })
    time.sleep(1)

    for col in ["B", "F", "N", "O"]:
        ws.format(f"{col}{start_row}:{col}{gt}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })
    time.sleep(1)

    products = sum(
        1 for row in im_rows
        if row[0] and row[0] not in ("Products", "GRAND TOTAL", SECTION_MARKER, "")
        and "Subtotal" not in str(row[0])
        and "CATEGORY" not in str(row[0])
        and "no Instamart" not in str(row[0])
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

    print("\nPushing Instamart MIS to Google Sheets\n")

    for month_key, (ws_title, data_file) in MONTHS.items():
        push_month(sh, ws_title, data_file)
        time.sleep(15)

    print(f"\nAll done! Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
