#!/usr/bin/env python3
"""Push January 2026 MIS data to Google Sheets."""

import json
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

# Product display order (same as MIS Excel)
PRODUCT_ORDER = [
    "V1", "V2", "V3", "V4", "V6", "V9", "V10",
    "V1- P of 2", "V2- P of 2", "V4- P of 2", "V4- P of 3",
    "V6- P of 2", "V9 P of 2",
    "V6-V1 Combo", "V6-V2 Combo",
    "V1-V2 Combo", "V1-V4 Combo", "V2-V4 Combo",
    "V9-V2 Combo", "V9-V3 Combo", "V9-V10 Combo",
    "Busy Book Blue", "Busy Book Pink", "Human Book",
    "Ganesha", "Krishna", "Hanuman",
    "Car", "Tank", "JCB",
]

# COGS/Unit from Testing MIS
COGS_MAP = {
    "V1": 225,
    "V2": 275,
    "V3": 662,
    "V4": 170,
    "V4- P of 3": 368,
    "V6": 275,
    "V9": 778,
    "V9 P of 2": 1556,
    "V10": 1009,
    "Busy Book Pink": 300,
    "Busy Book Blue": 300,
    "Human Book": 300,
    "V9-V3 Combo": 1440,
    "V9-V10 Combo": 1787,
    "V1-V4 Combo": 404,
    "V6-V2 Combo": 612,
    "V1-V2 Combo": 524,
    "V2-V4 Combo": 488,
    "V9-V2 Combo": 488,
    "V6-V1 Combo": 608,
    "Ganesha": 290,
    "Krishna": 290,
    "Hanuman": 290,
    "Car": 540,
    "Tank": 862,
}

HEADERS = [
    "Products",                  # A
    "Total Delivered Revenue",   # B
    "Total Expense",             # C
    "Total P/L",                 # D
    "Profit %",                  # E
    "P/pcs",                     # F
    "Total Orders",              # G
    "Shipped",                   # H
    "Total COGS",                # I
    "Delivered",                 # J
    "Shipping Charges",          # K
    "RTO",                       # L
    "In-Transit",                # M
    "RTO%",                      # N
    "Shipped%",                  # O
    "Delivered%",                # P
    "Cancellation%",             # Q
    "COGS/Unit",                 # R
]


def main():
    # Auth
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)

    # Open sheet
    sh = gc.open_by_url(SHEET_URL)

    # Create or get worksheet
    ws_title = "January 2026 MIS"
    try:
        ws = sh.worksheet(ws_title)
        ws.clear()
        print(f"Cleared existing '{ws_title}' worksheet")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=ws_title, rows=40, cols=18)
        print(f"Created new '{ws_title}' worksheet")

    # Load MIS data
    with open(f"{BASE_DIR}/jan_mis_data.json") as f:
        product_data = json.load(f)

    # Build all rows
    all_rows = [HEADERS]

    data_row_start = 2  # Row 2 in sheet (1-indexed, after header)
    row_num = data_row_start
    products_written = []

    for product in PRODUCT_ORDER:
        data = product_data.get(product)
        if not data or data["total_orders"] == 0:
            continue

        r = row_num
        revenue = round(data["revenue"], 2)
        freight = round(data["freight"], 2)

        row = [
            product,                    # A: Product
            revenue,                    # B: Revenue
            f"=I{r}+K{r}",            # C: Expense = COGS + Shipping
            f"=B{r}-C{r}",            # D: P/L
            f'=IF(B{r}=0,"",D{r}/B{r})',  # E: Profit%
            f'=IF(J{r}=0,"",D{r}/J{r})',  # F: P/pcs
            data["total_orders"],       # G: Total Orders
            data["shipped"],            # H: Shipped
            f"=R{r}*H{r}",            # I: COGS = COGS/Unit * Shipped
            data["delivered"],          # J: Delivered
            freight,                    # K: Shipping Charges
            data["rto"],               # L: RTO
            data["in_transit"],        # M: In-Transit
            f'=IF(H{r}=0,"",L{r}/H{r})',  # N: RTO%
            f'=IF(G{r}=0,"",H{r}/G{r})',  # O: Shipped%
            f'=IF(G{r}=0,"",J{r}/G{r})',  # P: Delivered%
            f'=IF(G{r}=0,"",(G{r}-H{r})/G{r})',  # Q: Cancel%
            COGS_MAP.get(product, 0),   # R: COGS/Unit
        ]
        all_rows.append(row)
        products_written.append(product)
        row_num += 1

    # TOTAL row
    t = row_num
    last = row_num - 1
    total_row = [
        "TOTAL",
        f"=SUM(B2:B{last})",          # B
        f"=I{t}+K{t}",                # C
        f"=B{t}-C{t}",                # D
        f'=IF(B{t}=0,"",D{t}/B{t})',  # E
        f'=IF(J{t}=0,"",D{t}/J{t})',  # F
        f"=SUM(G2:G{last})",          # G
        f"=SUM(H2:H{last})",          # H
        f"=SUM(I2:I{last})",          # I
        f"=SUM(J2:J{last})",          # J
        f"=SUM(K2:K{last})",          # K
        f"=SUM(L2:L{last})",          # L
        f"=SUM(M2:M{last})",          # M
        f'=IF(H{t}=0,"",L{t}/H{t})', # N
        f'=IF(G{t}=0,"",H{t}/G{t})', # O
        f'=IF(G{t}=0,"",J{t}/G{t})', # P
        f'=IF(G{t}=0,"",(G{t}-H{t})/G{t})',  # Q
        "",                            # R
    ]
    all_rows.append(total_row)

    # Push all data at once using USER_ENTERED so formulas work
    ws.update(range_name="A1", values=all_rows, value_input_option="USER_ENTERED")

    # Format header row (bold + blue background)
    ws.format("A1:R1", {
        "backgroundColor": {"red": 0.267, "green": 0.447, "blue": 0.769},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })

    # Format TOTAL row (bold + light blue)
    total_range = f"A{t}:R{t}"
    ws.format(total_range, {
        "backgroundColor": {"red": 0.839, "green": 0.894, "blue": 0.941},
        "textFormat": {"bold": True},
    })

    # Format currency columns (B, C, D, F, I, K)
    for col in ["B", "C", "D", "F", "I", "K"]:
        ws.format(f"{col}2:{col}{t}", {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})

    # Format percentage columns (E, N, O, P, Q)
    for col in ["E", "N", "O", "P", "Q"]:
        ws.format(f"{col}2:{col}{t}", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})

    # Freeze header
    ws.freeze(rows=1, cols=1)

    print(f"\nDone! Pushed {len(products_written)} products + TOTAL row to Google Sheets.")
    print(f"Sheet: {SHEET_URL}")
    print(f"\nProducts: {', '.join(products_written)}")

    # Summary
    total_rev = sum(product_data.get(p, {}).get("revenue", 0) for p in products_written)
    total_del = sum(product_data.get(p, {}).get("delivered", 0) for p in products_written)
    total_ord = sum(product_data.get(p, {}).get("total_orders", 0) for p in products_written)
    print(f"\nTotal Orders: {total_ord:,} | Delivered: {total_del:,} | Revenue: ₹{total_rev:,.0f}")
    print("\nNOTE: COGS/Unit column (R) is set to 0 — fill manually in the sheet.")


if __name__ == "__main__":
    main()
