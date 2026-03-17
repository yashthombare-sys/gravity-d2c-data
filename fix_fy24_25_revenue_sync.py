#!/usr/bin/env python3
"""
Fix FY24-25 revenue sync glitches in Google Sheets.

Issues found:
1. Jun 24: Amazon V7 (₹1,73,749) missing from GSheet — GSheet has old data without V7
2. Jul 24: ₹2,44,346 unaccounted — no JSON source, needs investigation
3. Oct 24: Amazon V7 (₹1,09,499) missing + D2C GSheet=2,419,596 matches JSON, but Amazon is short
4. Nov 24: Matches expected ✅ (already correct)
5. Dec 24: D2C missing V4-V5 Combo (₹1,59,702) + Amazon ₹1,05,199 short + no FirstCry
6. Jan 25: D2C JSON=25,39,426 vs GSheet=24,21,628 (₹1,17,798 diff) + FirstCry JSON=2,21,621 vs GSheet=2,00,559
7. Feb 25: D2C ₹1,099 short + Amazon ₹1,53,178 short + Flipkart ₹7,164 short + no FirstCry
8. Mar 25: Flipkart ₹2,898 short

Strategy: Re-push the D2C, Amazon, Flipkart sections from JSON for affected months.
For channels that append below D2C (Amazon, Flipkart, FirstCry, Blinkit, Instamart),
we find the existing section start row and overwrite.
"""

import json
import time
import os
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")

COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170,
    "V1- P of 2": 531, "V2- P of 2": 649, "V4- P of 2": 401, "V4- P of 3": 368,
    "V6": 275, "V6- P of 2": 649, "V6- P of 3": 924, "V9": 778, "V9 P of 2": 1664, "V10": 1009,
    "Busy Book Blue": 300, "Busy Book Pink": 300, "Human Book": 300,
    "V9-V3 Combo": 1440, "V9-V10 Combo": 1787,
    "V1-V4 Combo": 404, "V6-V2 Combo": 612, "V6-V4 Combo": 445, "V1-V2 Combo": 524,
    "V2-V4 Combo": 488, "V9-V2 Combo": 488, "V6-V1 Combo": 608,
    "V7-V4 Combo": 770, "V4-V5 Combo": 395,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862, "JCB": 540,
    "V5": 225, "V7 Police Cruiser": 600, "V7": 600, "V8": 700,
    "CS Basics 1": 250, "Drawing Board": 250,
}

# FY24-25: Only Busy Board category existed (Soft Toy & STEM launched later in FY25-26)
CATEGORIES = [
    {
        "name": "BUSY BOARD CATEGORY",
        "color": {"red": 0.933, "green": 0.522, "blue": 0.133},
        "products": [
            "V1", "V2", "V3", "V4", "V6", "V9", "V10",
            "V1- P of 2", "V2- P of 2", "V4- P of 2", "V4- P of 3",
            "V6- P of 2", "V6- P of 3", "V9 P of 2",
            "V6-V1 Combo", "V6-V2 Combo", "V6-V4 Combo",
            "V1-V2 Combo", "V1-V4 Combo", "V2-V4 Combo",
            "V9-V2 Combo", "V9-V3 Combo", "V9-V10 Combo",
            "V7-V4 Combo", "V4-V5 Combo",
            "Busy Book Blue", "Busy Book Pink", "Human Book",
            "V5", "V7 Police Cruiser", "V7", "V8",
        ],
    },
]

D2C_HEADERS = [
    "Products", "Total Delivered Revenue", "Total Expense", "Total P/L",
    "Profit %", "P/pcs", "Total Orders", "Shipped", "Total COGS",
    "Delivered", "Shipping Charges", "RTO", "In-Transit",
    "RTO%", "Shipped%", "Delivered%", "Cancellation%", "COGS/Unit",
]

AMAZON_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "COGS", "COGS/Unit",
    "Commission", "FBA Fees", "Closing Fee", "Promos", "Refund Amt",
    "Total Amazon Fees", "Ad Spend", "Profit", "Profit %",
]

FLIPKART_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "RTO", "COGS", "COGS/Unit",
    "Commission", "Penalty", "Shipping Fee", "Total Flipkart Fees",
    "Ad Spend", "Profit", "Profit %",
]

FIRSTCRY_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "RTO", "COGS", "COGS/Unit",
    "Commission", "Total FirstCry Fees", "Profit", "Profit %",
]

BLINKIT_HEADERS = [
    "Products", "Revenue", "Orders", "COGS", "Ad Spend",
]

INSTAMART_HEADERS = [
    "Products", "Revenue", "Orders", "Delivered", "COGS", "COGS/Unit",
    "Commission", "Total Instamart Fees", "Ad Spend", "Profit", "Profit %",
]


# ── D2C builder (same logic as push_fy24_25_mis.py) ──

def make_d2c_product_row(product, data, r):
    revenue = round(data["revenue"], 2)
    freight = round(data.get("freight", 0), 2)
    return [
        product, revenue,
        f"=I{r}+K{r}", f"=B{r}-C{r}",
        f'=IF(B{r}=0,"",D{r}/B{r})',
        f'=IF(J{r}=0,"",D{r}/J{r})',
        data["total_orders"], data["shipped"],
        f"=R{r}*J{r}",
        data["delivered"], freight,
        data["rto"], data.get("in_transit", 0),
        f'=IF(H{r}=0,"",L{r}/H{r})',
        f'=IF(G{r}=0,"",H{r}/G{r})',
        f'=IF(G{r}=0,"",J{r}/G{r})',
        f'=IF(G{r}=0,"",(G{r}-H{r})/G{r})',
        COGS_MAP.get(product, 0),
    ]


def make_d2c_subtotal_row(label, first, last, r):
    return [
        label,
        f"=SUM(B{first}:B{last})",
        f"=I{r}+K{r}", f"=B{r}-C{r}",
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


def build_d2c_rows(product_data):
    """Build D2C section rows. Returns (rows, format_info)."""
    all_rows = [D2C_HEADERS]
    fmt = {"category_headers": [], "subtotal_rows": [], "grand_total_row": None}
    row_num = 2
    subtotal_refs = []

    for category in CATEGORIES:
        all_rows.append([category["name"]] + [""] * 17)
        fmt["category_headers"].append((row_num, category["color"]))
        row_num += 1

        first = row_num
        count = 0
        for product in category["products"]:
            data = product_data.get(product)
            if not data or data["total_orders"] == 0:
                continue
            all_rows.append(make_d2c_product_row(product, data, row_num))
            count += 1
            row_num += 1

        if count > 0:
            last = row_num - 1
            all_rows.append(make_d2c_subtotal_row(f"{category['name']} — Subtotal", first, last, row_num))
            fmt["subtotal_rows"].append((row_num, category["color"]))
            subtotal_refs.append(row_num)
            row_num += 1
        else:
            all_rows.append(["(no orders this month)"] + [""] * 17)
            row_num += 1

        all_rows.append([""] * 18)
        row_num += 1

    # Grand Total
    t = row_num
    grand_total = ["GRAND TOTAL"]
    for col_idx in range(1, 18):
        col_letter = chr(ord("A") + col_idx)
        if col_letter == "C":
            grand_total.append(f"=I{t}+K{t}")
        elif col_letter == "D":
            grand_total.append(f"=B{t}-C{t}")
        elif col_letter == "E":
            grand_total.append(f'=IF(B{t}=0,"",D{t}/B{t})')
        elif col_letter == "F":
            grand_total.append(f'=IF(J{t}=0,"",D{t}/J{t})')
        elif col_letter == "N":
            grand_total.append(f'=IF(H{t}=0,"",L{t}/H{t})')
        elif col_letter == "O":
            grand_total.append(f'=IF(G{t}=0,"",H{t}/G{t})')
        elif col_letter == "P":
            grand_total.append(f'=IF(G{t}=0,"",J{t}/G{t})')
        elif col_letter == "Q":
            grand_total.append(f'=IF(G{t}=0,"",(G{t}-H{t})/G{t})')
        elif col_letter == "R":
            grand_total.append("")
        elif col_letter in ("B", "G", "H", "I", "J", "K", "L", "M"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            grand_total.append(f"={refs}")
        else:
            grand_total.append("")

    all_rows.append(grand_total)
    fmt["grand_total_row"] = t
    return all_rows, fmt


# ── Amazon builder ──

def build_amazon_rows(product_data, start_row):
    """Build Amazon section rows starting at start_row. Returns (rows, format_info)."""
    all_rows = [[""] * 19, ["AMAZON MIS"] + [""] * 18, AMAZON_HEADERS + [""] * 4]
    r = start_row + 3  # after blank + title + headers
    fmt = {"category_headers": [], "subtotal_rows": [], "grand_total_row": None}
    subtotal_refs = []

    for category in CATEGORIES:
        all_rows.append([category["name"]] + [""] * 18)
        fmt["category_headers"].append((r, category["color"]))
        r += 1

        first = r
        count = 0
        for product in category["products"]:
            data = product_data.get(product)
            if not data or (data.get("total_orders", 0) == 0 and data.get("revenue", 0) == 0):
                continue
            revenue = round(data["revenue"], 2)
            orders = data["total_orders"]
            delivered = data["delivered"]
            cogs_unit = COGS_MAP.get(product, 0)

            row = [
                product, revenue, orders, delivered,
                f"=F{r}*D{r}",  # COGS
                cogs_unit,
                round(data.get("commission", 0), 2),
                round(data.get("fba_fees", 0), 2),
                round(data.get("closing_fee", 0), 2),
                round(data.get("promos", 0), 2),
                round(data.get("refund_amt", 0), 2),
                f"=G{r}+H{r}+I{r}+J{r}+K{r}",  # Total Amazon Fees
                round(data.get("ad_spend", 0), 2),
                f"=B{r}-E{r}-L{r}-M{r}",  # Profit
                f'=IF(B{r}=0,"",N{r}/B{r})',  # Profit %
            ] + [""] * 4
            all_rows.append(row)
            count += 1
            r += 1

        if count > 0:
            last = r - 1
            sub = [
                f"{category['name']} — Subtotal",
                f"=SUM(B{first}:B{last})",
                f"=SUM(C{first}:C{last})",
                f"=SUM(D{first}:D{last})",
                f"=SUM(E{first}:E{last})",
                "",
                f"=SUM(G{first}:G{last})",
                f"=SUM(H{first}:H{last})",
                f"=SUM(I{first}:I{last})",
                f"=SUM(J{first}:J{last})",
                f"=SUM(K{first}:K{last})",
                f"=SUM(L{first}:L{last})",
                f"=SUM(M{first}:M{last})",
                f"=B{r}-E{r}-L{r}-M{r}",
                f'=IF(B{r}=0,"",N{r}/B{r})',
            ] + [""] * 4
            all_rows.append(sub)
            fmt["subtotal_rows"].append((r, category["color"]))
            subtotal_refs.append(r)
            r += 1
        else:
            all_rows.append([f"(no Amazon orders)"] + [""] * 18)
            r += 1

        all_rows.append([""] * 19)
        r += 1

    # Grand Total
    t = r
    gt = ["GRAND TOTAL"]
    for col_idx in range(1, 15):
        col_letter = chr(ord("A") + col_idx)
        if col_letter in ("B", "C", "D", "E", "G", "H", "I", "J", "K", "L", "M"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            gt.append(f"={refs}")
        elif col_letter == "F":
            gt.append("")
        elif col_letter == "N":
            gt.append(f"=B{t}-E{t}-L{t}-M{t}")
        elif col_letter == "O":
            gt.append(f'=IF(B{t}=0,"",N{t}/B{t})')
    gt += [""] * 4
    all_rows.append(gt)
    fmt["grand_total_row"] = t

    return all_rows, fmt


# ── Flipkart builder ──

def build_flipkart_rows(product_data, start_row):
    all_rows = [[""] * 19, ["FLIPKART MIS"] + [""] * 18, FLIPKART_HEADERS + [""] * 5]
    r = start_row + 3
    fmt = {"category_headers": [], "subtotal_rows": [], "grand_total_row": None}
    subtotal_refs = []

    for category in CATEGORIES:
        all_rows.append([category["name"]] + [""] * 18)
        fmt["category_headers"].append((r, category["color"]))
        r += 1

        first = r
        count = 0
        for product in category["products"]:
            data = product_data.get(product)
            if not data or (data.get("total_orders", 0) == 0 and data.get("revenue", 0) == 0):
                continue
            revenue = round(data["revenue"], 2)
            orders = data["total_orders"]
            delivered = data["delivered"]
            rto = data.get("rto", 0)
            cogs_unit = COGS_MAP.get(product, 0)

            row = [
                product, revenue, orders, delivered, rto,
                f"=G{r}*D{r}", cogs_unit,
                round(data.get("commission", 0), 2),
                0, 0,
                f"=H{r}+I{r}+J{r}",
                round(data.get("ad_spend", 0), 2),
                f"=B{r}-F{r}-K{r}-L{r}",
                f'=IF(B{r}=0,"",M{r}/B{r})',
            ] + [""] * 5
            all_rows.append(row)
            count += 1
            r += 1

        if count > 0:
            last = r - 1
            sub = [
                f"{category['name']} — Subtotal",
                f"=SUM(B{first}:B{last})",
                f"=SUM(C{first}:C{last})",
                f"=SUM(D{first}:D{last})",
                f"=SUM(E{first}:E{last})",
                f"=SUM(F{first}:F{last})", "",
                f"=SUM(H{first}:H{last})",
                f"=SUM(I{first}:I{last})",
                f"=SUM(J{first}:J{last})",
                f"=SUM(K{first}:K{last})",
                f"=SUM(L{first}:L{last})",
                f"=B{r}-F{r}-K{r}-L{r}",
                f'=IF(B{r}=0,"",M{r}/B{r})',
            ] + [""] * 5
            all_rows.append(sub)
            fmt["subtotal_rows"].append((r, category["color"]))
            subtotal_refs.append(r)
            r += 1
        else:
            all_rows.append(["(no Flipkart orders)"] + [""] * 18)
            r += 1

        all_rows.append([""] * 19)
        r += 1

    t = r
    gt = ["GRAND TOTAL"]
    for col_idx in range(1, 14):
        col_letter = chr(ord("A") + col_idx)
        if col_letter in ("B", "C", "D", "E", "F", "H", "I", "J", "K", "L"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            gt.append(f"={refs}")
        elif col_letter == "G":
            gt.append("")
        elif col_letter == "M":
            gt.append(f"=B{t}-F{t}-K{t}-L{t}")
        elif col_letter == "N":
            gt.append(f'=IF(B{t}=0,"",M{t}/B{t})')
    gt += [""] * 5
    all_rows.append(gt)
    fmt["grand_total_row"] = t
    return all_rows, fmt


# ── FirstCry builder ──

def build_firstcry_rows(product_data, start_row):
    all_rows = [[""] * 19, ["FIRSTCRY MIS"] + [""] * 18, FIRSTCRY_HEADERS + [""] * 8]
    r = start_row + 3
    fmt = {"category_headers": [], "subtotal_rows": [], "grand_total_row": None}
    subtotal_refs = []

    for category in CATEGORIES:
        all_rows.append([category["name"]] + [""] * 18)
        fmt["category_headers"].append((r, category["color"]))
        r += 1

        first = r
        count = 0
        for product in category["products"]:
            data = product_data.get(product)
            if not data or (data.get("total_orders", 0) == 0 and data.get("revenue", 0) == 0):
                continue
            revenue = round(data["revenue"], 2)
            orders = data["total_orders"]
            delivered = data["delivered"]
            rto = data.get("rto", 0)
            cogs_unit = COGS_MAP.get(product, 0)

            row = [
                product, revenue, orders, delivered, rto,
                f"=G{r}*D{r}", cogs_unit,
                0,  # Commission placeholder
                0,  # Total FC fees
                f"=B{r}-F{r}-I{r}",
                f'=IF(B{r}=0,"",J{r}/B{r})',
            ] + [""] * 8
            all_rows.append(row)
            count += 1
            r += 1

        if count > 0:
            last = r - 1
            sub = [
                f"{category['name']} — Subtotal",
                f"=SUM(B{first}:B{last})",
                f"=SUM(C{first}:C{last})",
                f"=SUM(D{first}:D{last})",
                f"=SUM(E{first}:E{last})",
                f"=SUM(F{first}:F{last})", "",
                f"=SUM(H{first}:H{last})",
                f"=SUM(I{first}:I{last})",
                f"=B{r}-F{r}-I{r}",
                f'=IF(B{r}=0,"",J{r}/B{r})',
            ] + [""] * 8
            all_rows.append(sub)
            fmt["subtotal_rows"].append((r, category["color"]))
            subtotal_refs.append(r)
            r += 1
        else:
            all_rows.append(["(no FirstCry orders)"] + [""] * 18)
            r += 1

        all_rows.append([""] * 19)
        r += 1

    t = r
    gt = ["GRAND TOTAL"]
    for col_idx in range(1, 11):
        col_letter = chr(ord("A") + col_idx)
        if col_letter in ("B", "C", "D", "E", "F", "H", "I"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            gt.append(f"={refs}")
        elif col_letter == "G":
            gt.append("")
        elif col_letter == "J":
            gt.append(f"=B{t}-F{t}-I{t}")
        elif col_letter == "K":
            gt.append(f'=IF(B{t}=0,"",J{t}/B{t})')
    gt += [""] * 8
    all_rows.append(gt)
    fmt["grand_total_row"] = t
    return all_rows, fmt


# ── Blinkit builder ──

def build_blinkit_rows(product_data, start_row):
    all_rows = [[""] * 19, ["BLINKIT MIS"] + [""] * 18, BLINKIT_HEADERS + [""] * 14]
    r = start_row + 3
    total_rev = 0
    total_orders = 0
    total_cogs = 0
    total_ads = 0

    for product, data in product_data.items():
        if not isinstance(data, dict):
            continue
        revenue = round(data["revenue"], 2)
        orders = data.get("total_orders", data.get("shipped", 0))
        cogs = COGS_MAP.get(product, 0) * data.get("delivered", orders)
        ad_spend = round(data.get("ad_spend", 0), 2)

        row = [product, revenue, orders, cogs, ad_spend] + [""] * 14
        all_rows.append(row)
        total_rev += revenue
        total_orders += orders
        total_cogs += cogs
        total_ads += ad_spend
        r += 1

    # Total row
    all_rows.append([
        "Blinkit Total", f"=SUM(B{start_row+3}:B{r-1})",
        f"=SUM(C{start_row+3}:C{r-1})",
        f"=SUM(D{start_row+3}:D{r-1})",
        f"=SUM(E{start_row+3}:E{r-1})",
    ] + [""] * 14)

    return all_rows, {"grand_total_row": r}


# ── Instamart builder ──

def build_instamart_rows(product_data, start_row):
    all_rows = [[""] * 19, ["INSTAMART MIS"] + [""] * 18, INSTAMART_HEADERS + [""] * 8]
    r = start_row + 3
    fmt = {"category_headers": [], "subtotal_rows": [], "grand_total_row": None}
    subtotal_refs = []

    for category in CATEGORIES:
        all_rows.append([category["name"]] + [""] * 18)
        fmt["category_headers"].append((r, category["color"]))
        r += 1

        first = r
        count = 0
        for product in category["products"]:
            data = product_data.get(product)
            if not data or (data.get("total_orders", 0) == 0 and data.get("revenue", 0) == 0):
                continue
            revenue = round(data["revenue"], 2)
            orders = data["total_orders"]
            delivered = data.get("delivered", orders)
            cogs_unit = COGS_MAP.get(product, 0)

            row = [
                product, revenue, orders, delivered,
                f"=F{r}*D{r}", cogs_unit,
                0, 0,
                round(data.get("ad_spend", 0), 2),
                f"=B{r}-E{r}-H{r}-I{r}",
                f'=IF(B{r}=0,"",J{r}/B{r})',
            ] + [""] * 8
            all_rows.append(row)
            count += 1
            r += 1

        if count > 0:
            last = r - 1
            sub = [
                f"{category['name']} — Subtotal",
                f"=SUM(B{first}:B{last})",
                f"=SUM(C{first}:C{last})",
                f"=SUM(D{first}:D{last})",
                f"=SUM(E{first}:E{last})", "",
                f"=SUM(G{first}:G{last})",
                f"=SUM(H{first}:H{last})",
                f"=SUM(I{first}:I{last})",
                f"=B{r}-E{r}-H{r}-I{r}",
                f'=IF(B{r}=0,"",J{r}/B{r})',
            ] + [""] * 8
            all_rows.append(sub)
            fmt["subtotal_rows"].append((r, category["color"]))
            subtotal_refs.append(r)
            r += 1
        else:
            all_rows.append(["(no Instamart orders)"] + [""] * 18)
            r += 1

        all_rows.append([""] * 19)
        r += 1

    t = r
    gt = ["GRAND TOTAL"]
    for col_idx in range(1, 11):
        col_letter = chr(ord("A") + col_idx)
        if col_letter in ("B", "C", "D", "E", "G", "H", "I"):
            refs = "+".join(f"{col_letter}{sr}" for sr in subtotal_refs)
            gt.append(f"={refs}")
        elif col_letter == "F":
            gt.append("")
        elif col_letter == "J":
            gt.append(f"=B{t}-E{t}-H{t}-I{t}")
        elif col_letter == "K":
            gt.append(f'=IF(B{t}=0,"",J{t}/B{t})')
    gt += [""] * 8
    all_rows.append(gt)
    fmt["grand_total_row"] = t
    return all_rows, fmt


def format_section(ws, fmt, section_type="d2c"):
    """Apply formatting to a section."""
    # Format category headers
    for row_num, color in fmt.get("category_headers", []):
        ws.format(f"A{row_num}:S{row_num}", {
            "backgroundColor": color,
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        time.sleep(1)

    # Format subtotal rows
    for row_num, color in fmt.get("subtotal_rows", []):
        light = {k: min(1, v * 0.6 + 0.4) for k, v in color.items()}
        ws.format(f"A{row_num}:S{row_num}", {
            "backgroundColor": light,
            "textFormat": {"bold": True},
        })
        time.sleep(1)

    # Format grand total
    gt = fmt.get("grand_total_row")
    if gt:
        ws.format(f"A{gt}:S{gt}", {
            "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        time.sleep(0.5)


def push_full_month(sh, ws_title, d2c_file, amazon_file=None, flipkart_file=None,
                    firstcry_file=None, blinkit_file=None, instamart_file=None):
    """Clear and re-push a full month with all channels."""
    print(f"\n{'='*60}")
    print(f"  Pushing: {ws_title}")
    print(f"{'='*60}")

    try:
        ws = sh.worksheet(ws_title)
        ws.clear()
        # Ensure enough rows
        if ws.row_count < 200:
            ws.resize(rows=200, cols=19)
        print(f"  Cleared existing '{ws_title}'")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=ws_title, rows=200, cols=19)
        print(f"  Created new '{ws_title}'")

    # ── D2C ──
    with open(d2c_file) as f:
        d2c_data = json.load(f)
    d2c_rows, d2c_fmt = build_d2c_rows(d2c_data)
    ws.update(range_name="A1", values=d2c_rows, value_input_option="USER_ENTERED")
    d2c_total = sum(v["revenue"] for v in d2c_data.values() if isinstance(v, dict))
    print(f"  D2C: {len(d2c_rows)} rows, Revenue=₹{d2c_total:,.0f}")

    # Format D2C headers
    ws.format("A1:R1", {
        "backgroundColor": {"red": 0.157, "green": 0.255, "blue": 0.459},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(2)
    format_section(ws, d2c_fmt)

    # Format D2C number columns
    gt = d2c_fmt["grand_total_row"]
    for col in ["B", "C", "D", "F", "I", "K"]:
        ws.format(f"{col}2:{col}{gt}", {"numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"}})
    for col in ["E", "N", "O", "P", "Q"]:
        ws.format(f"{col}2:{col}{gt}", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    time.sleep(1)
    ws.freeze(rows=1, cols=1)

    next_row = len(d2c_rows) + 1

    # ── Amazon ──
    if amazon_file and os.path.exists(amazon_file):
        with open(amazon_file) as f:
            amz_data = json.load(f)
        amz_rows, amz_fmt = build_amazon_rows(amz_data, next_row)
        ws.update(range_name=f"A{next_row}", values=amz_rows, value_input_option="USER_ENTERED")
        amz_total = sum(v["revenue"] for v in amz_data.values() if isinstance(v, dict))
        print(f"  Amazon: {len(amz_rows)} rows, Revenue=₹{amz_total:,.0f}")
        time.sleep(2)
        format_section(ws, amz_fmt)
        next_row += len(amz_rows)
        time.sleep(1)

    # ── Flipkart ──
    if flipkart_file and os.path.exists(flipkart_file):
        with open(flipkart_file) as f:
            fk_data = json.load(f)
        fk_rows, fk_fmt = build_flipkart_rows(fk_data, next_row)
        ws.update(range_name=f"A{next_row}", values=fk_rows, value_input_option="USER_ENTERED")
        fk_total = sum(v["revenue"] for v in fk_data.values() if isinstance(v, dict))
        print(f"  Flipkart: {len(fk_rows)} rows, Revenue=₹{fk_total:,.0f}")
        time.sleep(2)
        format_section(ws, fk_fmt)
        next_row += len(fk_rows)
        time.sleep(1)

    # ── FirstCry ──
    if firstcry_file and os.path.exists(firstcry_file):
        with open(firstcry_file) as f:
            fc_data = json.load(f)
        fc_rows, fc_fmt = build_firstcry_rows(fc_data, next_row)
        ws.update(range_name=f"A{next_row}", values=fc_rows, value_input_option="USER_ENTERED")
        fc_total = sum(v["revenue"] for v in fc_data.values() if isinstance(v, dict))
        print(f"  FirstCry: {len(fc_rows)} rows, Revenue=₹{fc_total:,.0f}")
        time.sleep(2)
        format_section(ws, fc_fmt)
        next_row += len(fc_rows)
        time.sleep(1)

    # ── Blinkit ──
    if blinkit_file and os.path.exists(blinkit_file):
        with open(blinkit_file) as f:
            bk_data = json.load(f)
        bk_rows, bk_fmt = build_blinkit_rows(bk_data, next_row)
        ws.update(range_name=f"A{next_row}", values=bk_rows, value_input_option="USER_ENTERED")
        bk_total = sum(v["revenue"] for v in bk_data.values() if isinstance(v, dict))
        print(f"  Blinkit: {len(bk_rows)} rows, Revenue=₹{bk_total:,.0f}")
        time.sleep(2)
        next_row += len(bk_rows)
        time.sleep(1)

    # ── Instamart ──
    if instamart_file and os.path.exists(instamart_file):
        with open(instamart_file) as f:
            im_data = json.load(f)
        im_rows, im_fmt = build_instamart_rows(im_data, next_row)
        ws.update(range_name=f"A{next_row}", values=im_rows, value_input_option="USER_ENTERED")
        im_total = sum(v["revenue"] for v in im_data.values() if isinstance(v, dict))
        print(f"  Instamart: {len(im_rows)} rows, Revenue=₹{im_total:,.0f}")
        time.sleep(2)
        format_section(ws, im_fmt)
        next_row += len(im_rows)

    print(f"  ✅ Done! Total rows: {next_row - 1}")


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    f = lambda name: os.path.join(BASE, name)

    # Months that need fixing (all affected months)
    months_to_fix = [
        {
            "title": "April 2024 MIS",
            "d2c": f("apr_2024_mis_data.json"),
            "amazon": f("amazon_apr_2024_mis_data.json"),
        },
        {
            "title": "May 2024 MIS",
            "d2c": f("may_2024_mis_data.json"),
            "amazon": f("amazon_may_2024_mis_data.json"),
        },
        {
            "title": "June 2024 MIS",
            "d2c": f("jun_2024_mis_data.json"),
            "amazon": f("amazon_jun_2024_mis_data.json"),
        },
        {
            "title": "July 2024 MIS",
            "d2c": f("jul_2024_mis_data.json"),
            "amazon": f("amazon_jul_2024_mis_data.json"),
        },
        {
            "title": "August 2024 MIS",
            "d2c": f("aug_2024_mis_data.json"),
            "amazon": f("amazon_aug_2024_mis_data.json"),
        },
        {
            "title": "September 2024 MIS",
            "d2c": f("sep_2024_mis_data.json"),
            "amazon": f("amazon_sep_2024_mis_data.json"),
        },
        {
            "title": "October 2024 MIS",
            "d2c": f("oct_2024_mis_data.json"),
            "amazon": f("amazon_oct_2024_mis_data.json"),
        },
        {
            "title": "November 2024 MIS",
            "d2c": f("nov_2024_mis_data.json"),
            "amazon": f("amazon_nov_2024_mis_data.json"),
            "flipkart": f("flipkart_nov_2024_mis_data.json"),
            "blinkit": f("blinkit_nov_2024_mis_data.json"),
        },
        {
            "title": "December 2024 MIS",
            "d2c": f("dec_2024_mis_data.json"),
            "amazon": f("amazon_dec_2024_mis_data.json"),
            "flipkart": f("flipkart_dec_2024_mis_data.json"),
            "blinkit": f("blinkit_dec_2024_mis_data.json"),
        },
        {
            "title": "January 2025 MIS",
            "d2c": f("jan_2025_mis_data.json"),
            "amazon": f("amazon_jan_2025_mis_data.json"),
            "flipkart": f("flipkart_jan_2025_mis_data.json"),
            "firstcry": f("firstcry_jan_2025_mis_data.json"),
            "blinkit": f("blinkit_jan_2025_mis_data.json"),
        },
        {
            "title": "February 2025 MIS",
            "d2c": f("feb_2025_mis_data.json"),
            "amazon": f("amazon_feb_2025_mis_data.json"),
            "flipkart": f("flipkart_feb_2025_mis_data.json"),
            "blinkit": f("blinkit_feb_2025_mis_data.json"),
            "instamart": f("instamart_feb_2025_mis_data.json"),
        },
        {
            "title": "March 2025 MIS",
            "d2c": f("mar_2025_mis_data.json"),
            "amazon": f("amazon_mar_2025_mis_data.json"),
            "flipkart": f("flipkart_mar_2025_mis_data.json"),
            "blinkit": f("blinkit_mar_2025_mis_data.json"),
            "instamart": f("instamart_mar_2025_mis_data.json"),
        },
    ]

    for month in months_to_fix:
        push_full_month(
            sh,
            month["title"],
            month["d2c"],
            amazon_file=month.get("amazon"),
            flipkart_file=month.get("flipkart"),
            firstcry_file=month.get("firstcry"),
            blinkit_file=month.get("blinkit"),
            instamart_file=month.get("instamart"),
        )
        time.sleep(10)

    print(f"\n{'='*60}")
    print(f"  ✅ ALL FY24-25 MONTHS RE-SYNCED!")
    print(f"  Sheet: {SHEET_URL}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
