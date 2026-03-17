#!/usr/bin/env python3
"""
Push Amazon MIS (Oct 2025 – Feb 2026) to Google Sheets.
Appends BELOW existing Shiprocket data in each month's tab.
NEVER modifies Shiprocket section.

Columns: Products(A), Revenue(B), Orders(C), Delivered(D), COGS(E), COGS/Unit(F),
         Commission(G), FBA Fees(H), Closing Fee(I), Promos(J), Refund Amt(K),
         Total Amazon Fees(L), Ad Spend(M), Profit(N), Profit %(O)
"""

import json, time, os
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")

NUM_COLS = 15  # A through O

CATEGORIES = [
    {
        "name": "BUSY BOARD CATEGORY",
        "color": {"red": 0.933, "green": 0.522, "blue": 0.133},
        "products": [
            "V1", "V2", "V3", "V4", "V6", "V9", "V10",
            "V1- P of 2", "V1- P of 3", "V2- P of 2", "V4- P of 2",
            "V6-V1 Combo", "V6-V2 Combo",
            "V1-V2 Combo", "V1-V4 Combo", "V2-V4 Combo",
            "Busy Book Blue", "Busy Book Pink", "Human Book",
            "V5", "V7 Police Cruiser", "V8",
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
        "products": ["Car", "Tank", "JCB", "Drawing Board"],
    },
]

AMAZON_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "COGS", "COGS/Unit",
    "Commission", "FBA Fees", "Closing Fee", "Promos", "Refund Amt",
    "Total Amazon Fees", "Ad Spend", "Profit", "Profit %",
]

MONTHS = {
    "Apr 2024": ("April 2024 MIS", "amazon_apr_2024_mis_data.json"),
    "May 2024": ("May 2024 MIS", "amazon_may_2024_mis_data.json"),
    "Jun 2024": ("June 2024 MIS", "amazon_jun_2024_mis_data.json"),
    "Jul 2024": ("July 2024 MIS", "amazon_jul_2024_mis_data.json"),
    "Aug 2024": ("August 2024 MIS", "amazon_aug_2024_mis_data.json"),
    "Sep 2024": ("September 2024 MIS", "amazon_sep_2024_mis_data.json"),
    "Oct 2024": ("October 2024 MIS", "amazon_oct_2024_mis_data.json"),
    "Nov 2024": ("November 2024 MIS", "amazon_nov_2024_mis_data.json"),
    "Dec 2024": ("December 2024 MIS", "amazon_dec_2024_mis_data.json"),
    "Jan 2025": ("January 2025 MIS", "amazon_jan_2025_mis_data.json"),
    "Feb 2025": ("February 2025 MIS", "amazon_feb_2025_mis_data.json"),
    "Mar 2025": ("March 2025 MIS", "amazon_mar_2025_mis_data.json"),
    "Apr 2025": ("April 2025 MIS", "amazon_apr_2025_mis_data.json"),
    "May 2025": ("May 2025 MIS", "amazon_may_2025_mis_data.json"),
    "Jun 2025": ("June 2025 MIS", "amazon_jun_2025_mis_data.json"),
    "Jul 2025": ("July 2025 MIS", "amazon_jul_2025_mis_data.json"),
    "Aug 2025": ("August 2025 MIS", "amazon_aug_2025_mis_data.json"),
    "Sep 2025": ("September 2025 MIS", "amazon_sep_2025_mis_data.json"),
    "Oct 2025": ("October 2025 MIS", "amazon_oct_mis_data.json"),
    "Nov 2025": ("November 2025 MIS", "amazon_nov_mis_data.json"),
    "Dec 2025": ("December 2025 MIS", "amazon_dec_mis_data.json"),
    "Jan 2026": ("January 2026 MIS", "amazon_jan_mis_data.json"),
    "Feb 2026": ("February 2026 MIS", "amazon_feb_mis_data.json"),
}


COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170, "V6": 275, "V9": 778, "V10": 1009,
    "V1- P of 2": 531, "V1- P of 3": 756, "V2- P of 2": 649, "V4- P of 2": 401, "V4- P of 3": 368,
    "V6- P of 2": 649, "V9- P of 2": 1664,
    "V6-V1 Combo": 608, "V6-V2 Combo": 612, "V1-V2 Combo": 524, "V1-V4 Combo": 404,
    "V2-V4 Combo": 488, "V9-V2 Combo": 488, "V9-V3 Combo": 1440, "V9-V10 Combo": 1787,
    "Busy Book Blue": 300, "Busy Book Pink": 300, "Human Book": 300,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862, "JCB": 862, "Drawing Board": 250,
    "V5": 170, "V7 Police Cruiser": 170, "V8": 170,
}


def make_product_row(product, data, r):
    """Build one Amazon product row at sheet row r. Columns A-O."""
    cogs_unit = data.get("cogs_unit", COGS_MAP.get(product, 0))
    return [
        product,                                    # A: Products
        round(data["revenue"], 2),                  # B: Revenue
        data["total_orders"],                       # C: Orders
        data.get("delivered", data.get("shipped", data["total_orders"])),  # D: Delivered
        f"=F{r}*D{r}",                             # E: COGS = COGS/Unit × Delivered
        cogs_unit,                                  # F: COGS/Unit
        round(data.get("commission", 0), 2),        # G: Commission
        round(data.get("fba_fees", 0), 2),          # H: FBA Fees
        round(data.get("closing_fee", 0), 2),       # I: Closing Fee
        round(data.get("promos", 0), 2),            # J: Promos
        round(data.get("refund_amt", 0), 2),        # K: Refund Amt
        f"=SUM(G{r}:J{r})",                        # L: Total Amazon Fees (excl refund)
        round(data.get("ad_spend", 0), 2),          # M: Ad Spend
        f"=B{r}-E{r}-L{r}-M{r}",                   # N: Profit = NetRev - COGS - Fees - Ads
        f'=IF(B{r}=0,"",N{r}/B{r})',               # O: Profit %
    ]


def make_subtotal_row(label, first, last, r):
    """Build a subtotal row."""
    row = [label]
    for col_idx in range(1, NUM_COLS):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "F":    # COGS/Unit — no subtotal
            row.append("")
        elif col_letter == "L":  # Total Amazon Fees = sum of G:J (excl refund)
            row.append(f"=SUM(G{r}:J{r})")
        elif col_letter == "N":  # Profit
            row.append(f"=B{r}-E{r}-L{r}-M{r}")
        elif col_letter == "O":  # Profit %
            row.append(f'=IF(B{r}=0,"",N{r}/B{r})')
        else:
            row.append(f"=SUM({col_letter}{first}:{col_letter}{last})")
    return row


def build_amazon_section(product_data, start_row):
    """Build Amazon section rows starting at start_row."""
    rows = []
    fmt = {"title_row": None, "header_row": None, "category_headers": [],
           "subtotal_rows": [], "grand_total_row": None}

    r = start_row

    # Title row
    rows.append(["AMAZON MIS"] + [""] * (NUM_COLS - 1))
    fmt["title_row"] = r
    r += 1

    # Header row
    rows.append(AMAZON_HEADERS)
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
            rows.append(["(no Amazon orders)"] + [""] * (NUM_COLS - 1))
            r += 1

        # Spacer
        rows.append([""] * NUM_COLS)
        r += 1

    # Grand Total
    gt = r
    grand_total = ["GRAND TOTAL"]
    for col_idx in range(1, NUM_COLS):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "F":    # COGS/Unit — skip
            grand_total.append("")
        elif col_letter == "L":  # Total Amazon Fees (excl refund)
            grand_total.append(f"=SUM(G{gt}:J{gt})")
        elif col_letter == "N":  # Profit
            grand_total.append(f"=B{gt}-E{gt}-L{gt}-M{gt}")
        elif col_letter == "O":  # Profit %
            grand_total.append(f'=IF(B{gt}=0,"",N{gt}/B{gt})')
        elif col_letter in ("B", "C", "D", "E", "G", "H", "I", "J", "K", "M"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            grand_total.append(f"={refs}" if refs else "")
        else:
            grand_total.append("")
    rows.append(grand_total)
    fmt["grand_total_row"] = gt

    return rows, fmt


def push_month(sh, ws_title, data_file):
    """Push Amazon MIS below Shiprocket in existing worksheet."""
    print(f"\n  Pushing Amazon: {ws_title}...")

    with open(os.path.join(BASE, data_file)) as f:
        product_data = json.load(f)

    try:
        ws = sh.worksheet(ws_title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"    ⚠️  Worksheet '{ws_title}' not found — skipping")
        return

    # Find Shiprocket GRAND TOTAL (first one, or D2C GRAND TOTAL)
    all_values = ws.get_all_values()
    shiprocket_last_row = 0
    for i, row in enumerate(all_values):
        cell = (row[0] if row else "").strip()
        if cell in ("GRAND TOTAL", "D2C GRAND TOTAL"):
            shiprocket_last_row = i + 1  # 1-indexed
            break
    if shiprocket_last_row == 0:
        shiprocket_last_row = len(all_values)
    print(f"    Shiprocket ends at row {shiprocket_last_row}")

    # Ensure enough rows/cols
    needed_rows = shiprocket_last_row + 45
    if ws.row_count < needed_rows:
        ws.resize(rows=needed_rows)
    if ws.col_count < NUM_COLS:
        ws.resize(cols=NUM_COLS)
    time.sleep(1)

    # Start Amazon section 2 rows after Shiprocket
    start_row = shiprocket_last_row + 3
    amazon_rows, fmt = build_amazon_section(product_data, start_row)

    # Clear old Amazon data
    total_rows = len(all_values)
    clear_from = shiprocket_last_row + 1
    if total_rows >= clear_from:
        try:
            ws.batch_clear([f"A{clear_from}:O{max(total_rows + 5, start_row + len(amazon_rows) + 5)}"])
        except Exception:
            pass
    time.sleep(1)

    # Write Amazon data
    ws.update(
        range_name=f"A{start_row}",
        values=amazon_rows,
        value_input_option="USER_ENTERED",
    )
    print(f"    Written {len(amazon_rows)} rows starting at row {start_row}")
    time.sleep(2)

    # ── Formatting ──

    # Title row — dark teal
    tr = fmt["title_row"]
    ws.format(f"A{tr}:O{tr}", {
        "backgroundColor": {"red": 0.0, "green": 0.3, "blue": 0.5},
        "textFormat": {"bold": True, "fontSize": 13,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(1)

    # Header row — dark blue
    hr = fmt["header_row"]
    ws.format(f"A{hr}:O{hr}", {
        "backgroundColor": {"red": 0.157, "green": 0.255, "blue": 0.459},
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

    # Currency format (B, E, G, H, I, J, K, L, M, N)
    for col in ["B", "E", "G", "H", "I", "J", "K", "L", "M", "N"]:
        ws.format(f"{col}{start_row}:{col}{gt}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })
    time.sleep(1)

    # Percentage format (O)
    ws.format(f"O{start_row}:O{gt}", {
        "numberFormat": {"type": "PERCENT", "pattern": "0.0%"},
    })
    time.sleep(0.5)

    products = sum(
        1 for row in amazon_rows
        if row[0] and row[0] not in ("Products", "GRAND TOTAL", "AMAZON MIS", "")
        and "Subtotal" not in str(row[0])
        and "CATEGORY" not in str(row[0])
        and "no Amazon" not in str(row[0])
    )
    print(f"    ✓ {products} products pushed")


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    print("\n🔄 Pushing Amazon MIS to Google Sheets\n")

    for month_key, (ws_title, data_file) in MONTHS.items():
        push_month(sh, ws_title, data_file)
        time.sleep(15)

    print(f"\n✅ All done! Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
