#!/usr/bin/env python3
"""Push Jan & Feb MIS to Google Sheets — separated by category category."""

import json
import time
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

# ── Brand Categories ──────────────────────────────────────────────
CATEGORYS = [
    {
        "name": "BUSY BOARD CATEGORY",
        "color": {"red": 0.933, "green": 0.522, "blue": 0.133},  # Orange
        "products": [
            "V1", "V2", "V3", "V4", "V6", "V9", "V10",
            "V1- P of 2", "V2- P of 2", "V4- P of 2", "V4- P of 3",
            "V6- P of 2", "V9 P of 2",
            "V6-V1 Combo", "V6-V2 Combo",
            "V1-V2 Combo", "V1-V4 Combo", "V2-V4 Combo",
            "V9-V2 Combo", "V9-V3 Combo", "V9-V10 Combo",
            "Busy Book Blue", "Busy Book Pink", "Human Book",
            "V5", "V7 Police Cruiser", "V8",
        ],
    },
    {
        "name": "SOFT TOY CATEGORY",
        "color": {"red": 0.678, "green": 0.847, "blue": 0.902},  # Light blue
        "products": ["Ganesha", "Krishna", "Hanuman"],
    },
    {
        "name": "STEM CATEGORY",
        "color": {"red": 0.576, "green": 0.769, "blue": 0.490},  # Green
        "products": ["Car", "Tank", "JCB"],
    },
]

COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170,
    "V1- P of 2": 531, "V2- P of 2": 649, "V4- P of 2": 401, "V4- P of 3": 368,
    "V6": 275, "V6- P of 2": 649, "V9": 778, "V9 P of 2": 1664, "V10": 1009,
    "Busy Book Pink": 300, "Busy Book Blue": 300, "Human Book": 300,
    "V9-V3 Combo": 1440, "V9-V10 Combo": 1787,
    "V1-V4 Combo": 404, "V6-V2 Combo": 612, "V1-V2 Combo": 524,
    "V2-V4 Combo": 488, "V9-V2 Combo": 488, "V6-V1 Combo": 608,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862,
    "V5": 225, "V7 Police Cruiser": 600, "V8": 700,
}

HEADERS = [
    "Products", "Total Delivered Revenue", "Total Expense", "Total P/L",
    "Profit %", "P/pcs", "Total Orders", "Shipped", "Total COGS",
    "Delivered", "Shipping Charges", "RTO", "In-Transit",
    "RTO%", "Shipped%", "Delivered%", "Cancellation%", "COGS/Unit",
]


def make_product_row(product, data, r):
    """Build one product data row at sheet row r."""
    revenue = round(data["revenue"], 2)
    freight = round(data["freight"], 2)
    return [
        product,
        revenue,
        f"=I{r}+K{r}",
        f"=B{r}-C{r}",
        f'=IF(B{r}=0,"",D{r}/B{r})',
        f'=IF(J{r}=0,"",D{r}/J{r})',
        data["total_orders"],
        data["shipped"],
        f"=R{r}*J{r}",
        data["delivered"],
        freight,
        data["rto"],
        data["in_transit"],
        f'=IF(H{r}=0,"",L{r}/H{r})',
        f'=IF(G{r}=0,"",H{r}/G{r})',
        f'=IF(G{r}=0,"",J{r}/G{r})',
        f'=IF(G{r}=0,"",(G{r}-H{r})/G{r})',
        COGS_MAP.get(product, 0),
    ]


def make_subtotal_row(label, first, last, r):
    """Build a subtotal row summing from first to last, placed at sheet row r."""
    return [
        label,
        f"=SUM(B{first}:B{last})",
        f"=I{r}+K{r}",
        f"=B{r}-C{r}",
        f'=IF(B{r}=0,"",D{r}/B{r})',
        f'=IF(J{r}=0,"",D{r}/J{r})',
        f"=SUM(G{first}:G{last})",
        f"=SUM(H{first}:H{last})",
        f"=SUM(I{first}:I{last})",
        f"=SUM(J{first}:J{last})",
        f"=SUM(K{first}:K{last})",
        f"=SUM(L{first}:L{last})",
        f"=SUM(M{first}:M{last})",
        f'=IF(H{r}=0,"",L{r}/H{r})',
        f'=IF(G{r}=0,"",H{r}/G{r})',
        f'=IF(G{r}=0,"",J{r}/G{r})',
        f'=IF(G{r}=0,"",(G{r}-H{r})/G{r})',
        "",
    ]


def build_sheet_data(product_data):
    """Returns (all_rows, formatting_info) with category-separated layout."""
    all_rows = [HEADERS]
    fmt = {
        "category_headers": [],   # (row_num, color)
        "subtotal_rows": [],   # (row_num, color)
        "grand_total_row": None,
    }

    row_num = 2  # sheet row (1-indexed, row 1 = header)
    subtotal_refs = []  # list of subtotal row numbers for grand total

    for category in CATEGORYS:
        # ── Brand header row ──
        category_header = [category["name"]] + [""] * 17
        all_rows.append(category_header)
        fmt["category_headers"].append((row_num, category["color"]))
        row_num += 1

        first_product_row = row_num
        products_in_category = 0

        for product in category["products"]:
            data = product_data.get(product)
            if not data or data["total_orders"] == 0:
                continue
            all_rows.append(make_product_row(product, data, row_num))
            products_in_category += 1
            row_num += 1

        # ── Subtotal row for this category ──
        if products_in_category > 0:
            last_product_row = row_num - 1
            subtotal_label = f"{category['name']} — Subtotal"
            all_rows.append(make_subtotal_row(subtotal_label, first_product_row, last_product_row, row_num))
            fmt["subtotal_rows"].append((row_num, category["color"]))
            subtotal_refs.append(row_num)
            row_num += 1
        else:
            # No products — add an empty note row
            all_rows.append(["(no orders this month)"] + [""] * 17)
            row_num += 1

        # ── Empty spacer row ──
        all_rows.append([""] * 18)
        row_num += 1

    # ── Grand Total row (sum of subtotals) ──
    t = row_num
    grand_total = ["GRAND TOTAL"]
    for col_idx in range(1, 18):
        col_letter = chr(ord("A") + col_idx)
        if col_letter in ("C",):
            grand_total.append(f"=I{t}+K{t}")
        elif col_letter in ("D",):
            grand_total.append(f"=B{t}-C{t}")
        elif col_letter in ("E",):
            grand_total.append(f'=IF(B{t}=0,"",D{t}/B{t})')
        elif col_letter in ("F",):
            grand_total.append(f'=IF(J{t}=0,"",D{t}/J{t})')
        elif col_letter in ("N",):
            grand_total.append(f'=IF(H{t}=0,"",L{t}/H{t})')
        elif col_letter in ("O",):
            grand_total.append(f'=IF(G{t}=0,"",H{t}/G{t})')
        elif col_letter in ("P",):
            grand_total.append(f'=IF(G{t}=0,"",J{t}/G{t})')
        elif col_letter in ("Q",):
            grand_total.append(f'=IF(G{t}=0,"",(G{t}-H{t})/G{t})')
        elif col_letter == "R":
            grand_total.append("")
        elif col_letter in ("B", "G", "H", "I", "J", "K", "L", "M"):
            # Sum from subtotal rows
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            grand_total.append(f"={refs}")
        else:
            grand_total.append("")

    all_rows.append(grand_total)
    fmt["grand_total_row"] = t

    return all_rows, fmt


def push_month(sh, ws_title, data_file):
    """Push one month's MIS to a worksheet."""
    print(f"\n{'='*60}")
    print(f"  Pushing: {ws_title}")
    print(f"{'='*60}")

    try:
        ws = sh.worksheet(ws_title)
        ws.clear()
        print(f"  Cleared existing '{ws_title}' worksheet")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=ws_title, rows=60, cols=18)
        print(f"  Created new '{ws_title}' worksheet")

    with open(data_file) as f:
        product_data = json.load(f)

    all_rows, fmt = build_sheet_data(product_data)

    # Push all data
    ws.update(range_name="A1", values=all_rows, value_input_option="USER_ENTERED")
    print(f"  Written {len(all_rows)} rows")

    # ── Formatting ──

    # Header row — dark blue + white text
    ws.format("A1:R1", {
        "backgroundColor": {"red": 0.157, "green": 0.255, "blue": 0.459},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(3)  # rate limit

    # Brand header rows — colored + bold + white text
    for row_num, color in fmt["category_headers"]:
        ws.format(f"A{row_num}:R{row_num}", {
            "backgroundColor": color,
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        time.sleep(2)

    # Subtotal rows — lighter tint + bold
    for row_num, color in fmt["subtotal_rows"]:
        # Lighten the color for subtotal
        light = {k: min(1, v * 0.6 + 0.4) for k, v in color.items()}
        ws.format(f"A{row_num}:R{row_num}", {
            "backgroundColor": light,
            "textFormat": {"bold": True},
        })
        time.sleep(2)

    # Grand Total row — dark gray + white text
    gt = fmt["grand_total_row"]
    ws.format(f"A{gt}:R{gt}", {
        "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })
    time.sleep(0.5)

    # Currency format (B, C, D, F, I, K)
    last_row = gt
    for col in ["B", "C", "D", "F", "I", "K"]:
        ws.format(f"{col}2:{col}{last_row}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })
    time.sleep(1)

    # Percentage format (E, N, O, P, Q)
    for col in ["E", "N", "O", "P", "Q"]:
        ws.format(f"{col}2:{col}{last_row}", {
            "numberFormat": {"type": "PERCENT", "pattern": "0.0%"},
        })
    time.sleep(0.5)

    # Freeze header + first column
    ws.freeze(rows=1, cols=1)

    # Count products
    product_count = sum(
        1 for row in all_rows
        if row[0] and row[0] not in ("Products", "GRAND TOTAL", "")
        and "Subtotal" not in str(row[0])
        and "CATEGORY" not in str(row[0])
        and "no orders" not in str(row[0])
    )
    print(f"  Done! {product_count} products across 3 categorys + Grand Total")


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    # Push all 11 months (Apr 2025 – Feb 2026)
    months = [
        ("April 2025 MIS", f"{BASE_DIR}/apr_2025_mis_data.json"),
        ("May 2025 MIS", f"{BASE_DIR}/may_2025_mis_data.json"),
        ("June 2025 MIS", f"{BASE_DIR}/jun_2025_mis_data.json"),
        ("July 2025 MIS", f"{BASE_DIR}/jul_2025_mis_data.json"),
        ("August 2025 MIS", f"{BASE_DIR}/aug_2025_mis_data.json"),
        ("September 2025 MIS", f"{BASE_DIR}/sep_2025_mis_data.json"),
        ("October 2025 MIS", f"{BASE_DIR}/oct_mis_data.json"),
        ("November 2025 MIS", f"{BASE_DIR}/nov_mis_data.json"),
        ("December 2025 MIS", f"{BASE_DIR}/dec_mis_data.json"),
        ("January 2026 MIS", f"{BASE_DIR}/jan_mis_data.json"),
        ("February 2026 MIS", f"{BASE_DIR}/feb_mis_data.json"),
    ]

    for ws_title, data_file in months:
        push_month(sh, ws_title, data_file)
        time.sleep(15)  # longer pause to avoid rate limits

    print(f"\n✅ All done! Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
