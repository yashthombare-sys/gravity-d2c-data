#!/usr/bin/env python3
"""Re-process Oct & Nov MIS with freight from oct-nov.csv, then push to Google Sheets."""

import csv
import json
import re
import os
import time
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

DELIVERED_STATUSES = {"DELIVERED"}
RTO_STATUSES = {"RTO DELIVERED", "RTO IN TRANSIT", "RTO INITIATED", "RTO OFD",
                "REACHED BACK AT SELLER CITY", "REACHED BACK AT_SELLER_CITY"}
CANCELLED_STATUSES = {"CANCELED", "CANCELLATION REQUESTED"}
SKIP_STATUSES = {"SELF FULFILED", "QC FAILED", "RETURN DELIVERED",
                 "RETURN IN TRANSIT", "RETURN PENDING", "RETURN CANCELLED"}
SPARE_PARTS_KEYWORDS = [
    "RC TANK MOTOR", "RC CAR PCB", "Charging cable", "Documents",
    "charging cable", "spare", "SPARE", "pcb", "motor", "document"
]

PRODUCT_PATTERNS = [
    (r"V9.*V10|V10.*V9|V9\+V10|V9-V10|V9 \+ V10|V9 V10 Combo", "V9-V10 Combo"),
    (r"V9.*V3|V3.*V9|V9\+V3|V9-V3|V9 \+ V3|V9 V3 Combo", "V9-V3 Combo"),
    (r"V9.*V2|V2.*V9|V9\+V2|V9-V2|V9 \+ V2|V9 V2 Combo", "V9-V2 Combo"),
    (r"V6.*V1|V1.*V6|V6\+V1|V6-V1|V6 \+ V1|V6 V1 Combo", "V6-V1 Combo"),
    (r"V6.*V2|V2.*V6|V6\+V2|V6-V2|V6 \+ V2|V6 V2 Combo", "V6-V2 Combo"),
    (r"V1.*V4|V4.*V1|V1\+V4|V1-V4|V1 \+ V4|V1 V4 Combo", "V1-V4 Combo"),
    (r"V1.*V2|V2.*V1|V1\+V2|V1-V2|V1 \+ V2|V1 V2 Combo", "V1-V2 Combo"),
    (r"V2.*V4|V4.*V2|V2\+V4|V2-V4|V2 \+ V4|V2 V4 Combo", "V2-V4 Combo"),
    (r"V9.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V9 P of 2"),
    (r"V6.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V6- P of 2"),
    (r"V4.*(?:Pack of 3|P of 3|pack of 3|3\s*pack)", "V4- P of 3"),
    (r"V4.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V4- P of 2"),
    (r"V2.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V2- P of 2"),
    (r"V1.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V1- P of 2"),
    (r"V10(?!\d)", "V10"),
    (r"V1(?!\d)", "V1"),
    (r"V2(?!\d)", "V2"),
    (r"V3(?!\d)", "V3"),
    (r"V4(?!\d)", "V4"),
    (r"V6(?!\d)", "V6"),
    (r"V9(?!\d)", "V9"),
    (r"(?i)busy\s*book.*(?:blue|boy)", "Busy Book Blue"),
    (r"(?i)busy\s*book.*(?:pink|girl)", "Busy Book Pink"),
    (r"(?i)human\s*(?:body\s*)?(?:busy\s*)?book", "Human Book"),
    (r"(?i)(?:clap\s*cuddle|clapcuddle).*(?:ganesh|ganesha)", "Ganesha"),
    (r"(?i)(?:clap\s*cuddle|clapcuddle).*(?:krishna)", "Krishna"),
    (r"(?i)(?:clap\s*cuddle|clapcuddle).*(?:hanuman)", "Hanuman"),
    (r"(?i)ganesh", "Ganesha"),
    (r"(?i)krishna", "Krishna"),
    (r"(?i)hanuman", "Hanuman"),
    (r"(?i)(?:sooper\s*brains?\s*)?(?:rc\s*)?(?:army\s*)?tank", "Tank"),
    (r"(?i)(?:sooper\s*brains?\s*)?(?:rc\s*)?(?:racer|car)", "Car"),
    (r"(?i)(?:sooper\s*brains?\s*)?jcb", "JCB"),
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
}

CATEGORIES = [
    {
        "name": "BUSY BOARD CATEGORY",
        "color": {"red": 0.933, "green": 0.522, "blue": 0.133},
        "products": [
            "V1", "V2", "V3", "V4", "V6", "V9", "V10",
            "V1- P of 2", "V2- P of 2", "V4- P of 2", "V4- P of 3",
            "V6- P of 2", "V9 P of 2",
            "V6-V1 Combo", "V6-V2 Combo",
            "V1-V2 Combo", "V1-V4 Combo", "V2-V4 Combo",
            "V9-V2 Combo", "V9-V3 Combo", "V9-V10 Combo",
            "Busy Book Blue", "Busy Book Pink", "Human Book",
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
        "products": ["Car", "Tank", "JCB"],
    },
]

SHEET_HEADERS = [
    "Products", "Total Delivered Revenue", "Total Expense", "Total P/L",
    "Profit %", "P/pcs", "Total Orders", "Shipped", "Total COGS",
    "Delivered", "Shipping Charges", "RTO", "In-Transit",
    "RTO%", "Shipped%", "Delivered%", "Cancellation%", "COGS/Unit",
]


def classify_product(name):
    if not name: return None
    for pattern, cat in PRODUCT_PATTERNS:
        if re.search(pattern, name): return cat
    return None

def classify_status(status):
    if not status: return "unknown"
    s = status.upper().strip()
    if s in DELIVERED_STATUSES: return "delivered"
    if s in RTO_STATUSES: return "rto"
    if s in CANCELLED_STATUSES: return "cancelled"
    if s in SKIP_STATUSES or s.startswith("RETURN"): return "skip"
    return "in_transit"

def is_spare_part(name):
    if not name: return True
    nl = name.lower()
    return any(kw.lower() in nl for kw in SPARE_PARTS_KEYWORDS)


def extract_freight_by_month(csv_file):
    """Extract freight from CSV, split by month, deduplicated by Order ID."""
    oct_freight = {}
    nov_freight = {}
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            oid = row.get('Order ID', '').strip()
            val = row.get('Freight Total Amount', '').strip()
            created = row.get('Shiprocket Created At', '').strip()
            if not oid or not val:
                continue
            try:
                fv = float(val)
            except ValueError:
                continue

            # Split by month based on date
            month = created[5:7] if len(created) >= 7 else ""
            if month == "10":
                if oid not in oct_freight:
                    oct_freight[oid] = fv
            elif month == "11":
                if oid not in nov_freight:
                    nov_freight[oid] = fv
    return oct_freight, nov_freight


def process_orders(orders, freight_map):
    product_data = defaultdict(lambda: {
        "total_orders": 0, "shipped": 0, "delivered": 0,
        "rto": 0, "in_transit": 0, "cancelled": 0,
        "revenue": 0.0, "freight": 0.0,
    })
    seen = set()
    unmapped = defaultdict(int)

    for order in orders:
        order_id = str(order.get("id", order.get("order_id", "")))
        channel_order_id = str(order.get("channel_order_id", order_id))
        is_reverse = order.get("is_reverse", False)
        if is_reverse or str(is_reverse).lower() == "yes" or str(is_reverse) == "1":
            continue
        status = classify_status(order.get("status", order.get("status_code", "")))
        if status == "skip":
            continue

        products = order.get("products", order.get("order_items", []))
        if not products:
            pname = order.get("product_name", "")
            if pname:
                products = [{"name": pname,
                             "selling_price": order.get("product_price", 0),
                             "discount": order.get("discount", 0),
                             "quantity": order.get("product_quantity", 1)}]

        order_line_values = []
        order_products_info = []

        for prod in products:
            pname = prod.get("name", prod.get("product_name", ""))
            price = float(prod.get("price", prod.get("selling_price", prod.get("product_price", 0))) or 0)
            discount = float(prod.get("discount", 0) or 0)
            qty = int(prod.get("quantity", prod.get("product_quantity", 1)) or 1)

            if is_spare_part(pname) or qty > 10:
                continue
            cat = classify_product(pname)
            if not cat:
                unmapped[pname] += 1
                continue
            dedup_key = (order_id, cat)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            line_value = (price - discount) * qty
            order_line_values.append(line_value)
            order_products_info.append((cat, line_value))

        total_val = sum(order_line_values) if order_line_values else 0
        order_freight = freight_map.get(channel_order_id, freight_map.get(order_id, 0))

        for cat, line_value in order_products_info:
            pd = product_data[cat]
            pd["total_orders"] += 1
            if status == "delivered":
                pd["delivered"] += 1; pd["shipped"] += 1; pd["revenue"] += line_value
            elif status == "rto":
                pd["rto"] += 1; pd["shipped"] += 1
            elif status == "in_transit":
                pd["in_transit"] += 1; pd["shipped"] += 1
            elif status == "cancelled":
                pd["cancelled"] += 1
            if total_val > 0 and order_freight > 0:
                pd["freight"] += (line_value / total_val) * order_freight

    if unmapped:
        print(f"  Unmapped (top 10):")
        for name, count in sorted(unmapped.items(), key=lambda x: -x[1])[:10]:
            print(f"    {name}: {count}")
    return dict(product_data)


def make_product_row(product, data, r):
    return [
        product, round(data["revenue"], 2), f"=I{r}+K{r}", f"=B{r}-C{r}",
        f'=IF(B{r}=0,"",D{r}/B{r})', f'=IF(J{r}=0,"",D{r}/J{r})',
        data["total_orders"], data["shipped"], f"=R{r}*H{r}",
        data["delivered"], round(data["freight"], 2), data["rto"], data["in_transit"],
        f'=IF(H{r}=0,"",L{r}/H{r})', f'=IF(G{r}=0,"",H{r}/G{r})',
        f'=IF(G{r}=0,"",J{r}/G{r})', f'=IF(G{r}=0,"",(G{r}-H{r})/G{r})',
        COGS_MAP.get(product, 0),
    ]

def make_subtotal_row(label, first, last, r):
    return [
        label, f"=SUM(B{first}:B{last})", f"=I{r}+K{r}", f"=B{r}-C{r}",
        f'=IF(B{r}=0,"",D{r}/B{r})', f'=IF(J{r}=0,"",D{r}/J{r})',
        f"=SUM(G{first}:G{last})", f"=SUM(H{first}:H{last})",
        f"=SUM(I{first}:I{last})", f"=SUM(J{first}:J{last})",
        f"=SUM(K{first}:K{last})", f"=SUM(L{first}:L{last})",
        f"=SUM(M{first}:M{last})",
        f'=IF(H{r}=0,"",L{r}/H{r})', f'=IF(G{r}=0,"",H{r}/G{r})',
        f'=IF(G{r}=0,"",J{r}/G{r})', f'=IF(G{r}=0,"",(G{r}-H{r})/G{r})', "",
    ]


def push_to_gsheet(sh, ws_title, product_data):
    print(f"\n  Pushing: {ws_title}")
    try:
        ws = sh.worksheet(ws_title)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=ws_title, rows=60, cols=18)

    all_rows = [SHEET_HEADERS]
    cat_headers, sub_rows = [], []
    row_num = 2
    subtotal_refs = []

    for cat in CATEGORIES:
        all_rows.append([cat["name"]] + [""] * 17)
        cat_headers.append((row_num, cat["color"]))
        row_num += 1
        first = row_num
        count = 0
        for p in cat["products"]:
            d = product_data.get(p)
            if not d or d["total_orders"] == 0: continue
            all_rows.append(make_product_row(p, d, row_num))
            count += 1; row_num += 1
        if count > 0:
            last = row_num - 1
            all_rows.append(make_subtotal_row(f"{cat['name']} — Subtotal", first, last, row_num))
            sub_rows.append((row_num, cat["color"]))
            subtotal_refs.append(row_num)
            row_num += 1
        else:
            all_rows.append(["(no orders this month)"] + [""] * 17)
            row_num += 1
        all_rows.append([""] * 18); row_num += 1

    t = row_num
    gt = ["GRAND TOTAL"]
    for ci in range(1, 18):
        cl = chr(ord("A") + ci)
        if cl == "C": gt.append(f"=I{t}+K{t}")
        elif cl == "D": gt.append(f"=B{t}-C{t}")
        elif cl == "E": gt.append(f'=IF(B{t}=0,"",D{t}/B{t})')
        elif cl == "F": gt.append(f'=IF(J{t}=0,"",D{t}/J{t})')
        elif cl == "N": gt.append(f'=IF(H{t}=0,"",L{t}/H{t})')
        elif cl == "O": gt.append(f'=IF(G{t}=0,"",H{t}/G{t})')
        elif cl == "P": gt.append(f'=IF(G{t}=0,"",J{t}/G{t})')
        elif cl == "Q": gt.append(f'=IF(G{t}=0,"",(G{t}-H{t})/G{t})')
        elif cl == "R": gt.append("")
        elif cl in ("B","G","H","I","J","K","L","M"):
            gt.append("=" + "+".join(f"{cl}{sr}" for sr in subtotal_refs))
        else: gt.append("")
    all_rows.append(gt)

    ws.update(range_name="A1", values=all_rows, value_input_option="USER_ENTERED")

    ws.format("A1:R1", {
        "backgroundColor": {"red": 0.157, "green": 0.255, "blue": 0.459},
        "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(1)
    for rn, color in cat_headers:
        ws.format(f"A{rn}:R{rn}", {
            "backgroundColor": color,
            "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        }); time.sleep(0.5)
    for rn, color in sub_rows:
        light = {k: min(1, v * 0.6 + 0.4) for k, v in color.items()}
        ws.format(f"A{rn}:R{rn}", {"backgroundColor": light, "textFormat": {"bold": True}})
        time.sleep(0.5)
    ws.format(f"A{t}:R{t}", {
        "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
        "textFormat": {"bold": True, "fontSize": 11, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    }); time.sleep(0.5)
    for col in ["B", "C", "D", "F", "I", "K"]:
        ws.format(f"{col}2:{col}{t}", {"numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"}})
    time.sleep(1)
    for col in ["E", "N", "O", "P", "Q"]:
        ws.format(f"{col}2:{col}{t}", {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}})
    time.sleep(0.5)
    ws.freeze(rows=1, cols=1)

    total_rev = sum(d.get("revenue", 0) for d in product_data.values())
    total_freight = sum(d.get("freight", 0) for d in product_data.values())
    total_del = sum(d.get("delivered", 0) for d in product_data.values())
    total_ord = sum(d.get("total_orders", 0) for d in product_data.values())
    print(f"  Orders: {total_ord:,} | Delivered: {total_del:,} | Revenue: ₹{total_rev:,.0f} | Freight: ₹{total_freight:,.0f}")


def main():
    print("=" * 60)
    print("  OCT & NOV 2025 — FREIGHT REFRESH + PUSH")
    print("=" * 60)

    # Extract freight from oct-nov.csv split by month
    print("\nExtracting freight from oct-nov.csv...")
    oct_freight, nov_freight = extract_freight_by_month(f"{BASE_DIR}/oct-nov.csv")
    oct_nz = sum(1 for v in oct_freight.values() if v > 0)
    nov_nz = sum(1 for v in nov_freight.values() if v > 0)
    print(f"  Oct: {len(oct_freight)} unique orders ({oct_nz} with freight)")
    print(f"  Nov: {len(nov_freight)} unique orders ({nov_nz} with freight)")

    # Process both months
    months = [
        ("October 2025", "oct", oct_freight),
        ("November 2025", "nov", nov_freight),
    ]

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for label, prefix, freight in months:
        raw_path = os.path.join(BASE_DIR, f"{prefix}_orders_raw.json")
        if not os.path.exists(raw_path):
            print(f"\n  {raw_path} not found — skipping {label}")
            continue

        with open(raw_path) as f:
            orders = json.load(f)
        print(f"\n  Processing {label} ({len(orders)} orders)...")

        product_data = process_orders(orders, freight)

        mis_path = os.path.join(BASE_DIR, f"{prefix}_mis_data.json")
        with open(mis_path, "w") as f:
            json.dump(product_data, f, indent=2)

        push_to_gsheet(sh, f"{label} MIS", product_data)
        time.sleep(2)

    print(f"\n{'='*60}")
    print(f"  Done! {SHEET_URL}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
