#!/usr/bin/env python3
"""
Single script to regenerate MIS from CSV and sync to both Dashboard + Google Sheets.
Usage: python3 sync_all.py
"""

import csv, json, re, os, time
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials

# ── Config ────────────────────────────────────────────────────────────────────
BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
CSV_FILE = "secure_458644_reports_1773212436069998380-7e7245b0c703eb758dbcbff6c85d4412-.csv"
CSV_PATH = os.path.join(BASE, CSV_FILE)
DASHBOARD_PATH = os.path.join(BASE, "dashboard.html")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")

# ── Status Classification ─────────────────────────────────────────────────────
DELIVERED_STATUSES = {"DELIVERED"}
RTO_STATUSES = {"RTO DELIVERED", "RTO IN TRANSIT", "RTO INITIATED", "RTO OFD",
                "REACHED BACK AT SELLER CITY", "REACHED BACK AT_SELLER_CITY"}
CANCELLED_STATUSES = {"CANCELED", "CANCELLATION REQUESTED"}
LOST_STATUSES = {"LOST", "DESTROYED", "MISROUTED", "UNTRACEABLE"}
IN_TRANSIT_STATUSES = {"IN TRANSIT", "IN TRANSIT-EN-ROUTE", "OUT FOR DELIVERY",
                       "PICKED UP", "OUT FOR PICKUP", "NEW ORDER",
                       "REACHED DESTINATION HUB", "UNDELIVERED-1ST ATTEMPT",
                       "UNDELIVERED-2ND ATTEMPT", "UNDELIVERED-3RD ATTEMPT"}
SKIP_STATUSES = {"SELF FULFILED", "QC FAILED", "RETURN DELIVERED",
                 "RETURN IN TRANSIT", "RETURN PENDING", "RETURN CANCELLED"}

# ── Product Classification ────────────────────────────────────────────────────
PRODUCT_PATTERNS = [
    (r"(?i)V9[\s\-]*V10.*Combo|Beat Maker.*Jungle Piano.*Combo.*V9.*V10", "V9-V10 Combo"),
    (r"(?i)V9[\s\-]*V3.*Combo|Beat Maker.*Galaxy Explorer.*Combo.*V9.*V3", "V9-V3 Combo"),
    (r"(?i)V9[\s\-]*V2.*Combo|Beat Maker.*Tinker.*Combo", "V9-V2 Combo"),
    (r"(?i)V6[\s\-]*V4|Toggle Play.*Mini Switch", "V6-V1 Combo"),
    (r"(?i)V6[\s\-]*V1.*Combo|Toggle Play.*Spark Switch", "V6-V1 Combo"),
    (r"(?i)V6[\s\-]*V2.*Combo|Toggle Play.*Tinker Pad", "V6-V2 Combo"),
    (r"(?i)V1[\s\-]*V2.*Combo|Spark Switch.*Tinker Pad", "V1-V2 Combo"),
    (r"(?i)V1[\s\-]*V4.*Combo|Spark Switch.*Mini Switch", "V1-V4 Combo"),
    (r"(?i)V2[\s\-]*V4.*Combo|Tinker Pad.*Mini Switch", "V2-V4 Combo"),
    (r"(?i)\bV9\b.*(?:pack|P)\s*(?:of\s*)?2", "V9 P of 2"),
    (r"(?i)\bV6\b.*(?:pack|P)\s*(?:of\s*)?2", "V6- P of 2"),
    (r"(?i)\bV4\b.*(?:pack|P)\s*(?:of\s*)?3", "V4- P of 3"),
    (r"(?i)\bV4\b.*(?:pack|P)\s*(?:of\s*)?2", "V4- P of 2"),
    (r"(?i)\bV2\b.*(?:pack|P)\s*(?:of\s*)?2", "V2- P of 2"),
    (r"(?i)\bV1\b.*(?:pack|P)\s*(?:of\s*)?2", "V1- P of 2"),
    (r"(?i)\bV1C\b", "V1"),
    (r"(?i)\bV10\b", "V10"), (r"(?i)\bV9\b", "V9"), (r"(?i)\bV6\b", "V6"),
    (r"(?i)\bV4\b", "V4"), (r"(?i)\bV3\b", "V3"), (r"(?i)\bV2\b", "V2"), (r"(?i)\bV1\b", "V1"),
    (r"(?i)busy\s*book.*pink", "Busy Book Pink"), (r"(?i)busy\s*book.*blue", "Busy Book Blue"),
    (r"(?i)human\s*(body\s*)?busy\s*book|human\s*book", "Human Book"),
    (r"(?i)activity\s*busy\s*book|BLUE BUSYBOOK", "Busy Book Blue"),
    (r"(?i)ganesh", "Ganesha"), (r"(?i)krishna", "Krishna"), (r"(?i)hanuman", "Hanuman"),
    (r"(?i)\btank\b", "Tank"), (r"(?i)\bcar\b|racer\s*diy", "Car"), (r"(?i)\bjcb\b", "JCB"),
]

SPARE_KW = [
    "RC TANK MOTOR", "RC CAR PCB", "charging cable", "documents", "spare", "pcb",
    "motor", "document", "e-book", "ebook", "free e", "parcel", "sample", "gift",
    "Solar System", "Police Cruiser", "V7", "V8", "Portable Switches", "Busyboard Beatmaker",
]

MONTH_MAP = {
    (2025, 10): "Oct 2025", (2025, 11): "Nov 2025", (2025, 12): "Dec 2025",
    (2026, 1): "Jan 2026", (2026, 2): "Feb 2026",
}
MONTH_PREFIXES = {
    "Oct 2025": "oct", "Nov 2025": "nov", "Dec 2025": "dec",
    "Jan 2026": "jan", "Feb 2026": "feb",
}
MONTH_SHEET_NAMES = {
    "Oct 2025": "October 2025 MIS", "Nov 2025": "November 2025 MIS",
    "Dec 2025": "December 2025 MIS", "Jan 2026": "January 2026 MIS",
    "Feb 2026": "February 2026 MIS",
}

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

HEADERS = [
    "Products", "Total Delivered Revenue", "Total Expense", "Total P/L",
    "Profit %", "P/pcs", "Total Orders", "Shipped", "Total COGS",
    "Delivered", "Shipping Charges", "RTO", "In-Transit",
    "RTO%", "Shipped%", "Delivered%", "Cancellation%", "COGS/Unit",
]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Process CSV → JSON files
# ══════════════════════════════════════════════════════════════════════════════

def classify_status(s):
    if not s:
        return "skip"
    s = s.upper().strip()
    if s in DELIVERED_STATUSES: return "delivered"
    if s in RTO_STATUSES: return "rto"
    if s in CANCELLED_STATUSES: return "cancelled"
    if s in SKIP_STATUSES or s.startswith("RETURN"): return "skip"
    if s in LOST_STATUSES: return "lost"
    if s in IN_TRANSIT_STATUSES: return "in_transit"
    return "skip"


def classify_product(name):
    if not name:
        return None
    for pat, cat in PRODUCT_PATTERNS:
        if re.search(pat, name):
            return cat
    return None


def is_spare(name):
    if not name:
        return True
    nl = name.lower()
    return any(kw.lower() in nl for kw in SPARE_KW)


def process_csv():
    print("=" * 60)
    print("  STEP 1: Processing CSV → JSON")
    print("=" * 60)

    monthly = {
        m: defaultdict(lambda: {
            "total_orders": 0, "shipped": 0, "delivered": 0, "rto": 0,
            "in_transit": 0, "cancelled": 0, "lost": 0,
            "revenue": 0.0, "freight": 0.0,
        })
        for m in MONTH_MAP.values()
    }
    seen = set()

    with open(CSV_PATH, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            is_rev = (row.get("Is Reverse") or "").strip()
            if is_rev.lower() in ("yes", "1", "true"):
                continue
            channel = (row.get("Channel") or "").strip()
            if channel == "CUSTOM":
                continue
            created = row.get("Channel Created At") or ""
            if len(created) < 7:
                continue
            try:
                parts = created.split("-")
                year, month = int(parts[0]), int(parts[1])
            except Exception:
                continue
            m = MONTH_MAP.get((year, month))
            if not m:
                continue
            status = classify_status(row.get("Status", ""))
            if status == "skip":
                continue
            oid = (row.get("Order ID") or "").strip()
            pname = (row.get("Product Name") or "").strip()
            if is_spare(pname):
                continue
            try:
                qty = int(float(row.get("Product Quantity") or 1))
            except Exception:
                qty = 1
            if qty > 10:
                continue
            cat = classify_product(pname)
            if not cat:
                continue
            dedup = (oid, cat)
            if dedup in seen:
                continue
            seen.add(dedup)

            try:
                price = float(row.get("Product Price") or 0)
            except Exception:
                price = 0
            if price == 0:  # Skip exchange/replacement orders (₹0 price)
                continue
            try:
                discount = float(row.get("Discount Value") or 0)
            except Exception:
                discount = 0
            try:
                freight = float(row.get("Freight Total Amount") or 0)
            except Exception:
                freight = 0

            line_value = (price - discount) * qty
            pd = monthly[m][cat]
            pd["total_orders"] += 1

            if status == "delivered":
                pd["delivered"] += 1
                pd["shipped"] += 1
                pd["revenue"] += line_value
            elif status == "rto":
                pd["rto"] += 1
                pd["shipped"] += 1
            elif status == "in_transit":
                pd["in_transit"] += 1
                pd["shipped"] += 1
            elif status == "lost":
                pd["lost"] += 1
                pd["shipped"] += 1
            elif status == "cancelled":
                pd["cancelled"] += 1

            if status != "cancelled" and freight > 0:
                pd["freight"] += freight

    DATA = {}
    for m in MONTH_MAP.values():
        d = {k: dict(v) for k, v in monthly[m].items() if v["total_orders"] > 0}
        prefix = MONTH_PREFIXES[m]
        path = os.path.join(BASE, f"{prefix}_mis_data.json")
        with open(path, "w") as f:
            json.dump(d, f, indent=2)
        DATA[m] = d
        t_orders = sum(v["total_orders"] for v in d.values())
        t_del = sum(v["delivered"] for v in d.values())
        t_rev = sum(v["revenue"] for v in d.values())
        print(f"  {m}: {t_orders} orders, {t_del} delivered, ₹{t_rev/100000:.2f}L")

    # Save inline data for dashboard
    with open(os.path.join(BASE, "dashboard_inline_data.json"), "w") as f:
        json.dump(DATA, f, separators=(",", ":"))

    print("  JSON files saved ✓")
    return DATA


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: Update Dashboard HTML
# ══════════════════════════════════════════════════════════════════════════════

def update_dashboard():
    print("\n" + "=" * 60)
    print("  STEP 2: Updating Dashboard")
    print("=" * 60)

    with open(os.path.join(BASE, "dashboard_inline_data.json")) as f:
        new_data = f.read().strip()
    with open(DASHBOARD_PATH) as f:
        html = f.read()

    new_html = re.sub(
        r"DATA=\{.*?\};\nrender\(\);",
        f"DATA={new_data};\nrender();",
        html,
        flags=re.DOTALL,
    )

    with open(DASHBOARD_PATH, "w") as f:
        f.write(new_html)

    print("  dashboard.html updated ✓")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: Push to Google Sheets
# ══════════════════════════════════════════════════════════════════════════════

def make_product_row(product, data, r):
    revenue = round(data["revenue"], 2)
    freight = round(data["freight"], 2)
    return [
        product, revenue, f"=I{r}+K{r}", f"=B{r}-C{r}",
        f'=IF(B{r}=0,"",D{r}/B{r})', f'=IF(J{r}=0,"",D{r}/J{r})',
        data["total_orders"], data["shipped"], f"=R{r}*J{r}",
        data["delivered"], freight, data["rto"], data["in_transit"],
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


def build_sheet_data(product_data):
    all_rows = [HEADERS]
    fmt = {"category_headers": [], "subtotal_rows": [], "grand_total_row": None}
    row_num = 2
    subtotal_refs = []

    for category in CATEGORIES:
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

        if products_in_category > 0:
            last_product_row = row_num - 1
            all_rows.append(make_subtotal_row(
                f"{category['name']} — Subtotal", first_product_row, last_product_row, row_num
            ))
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


def push_month(sh, ws_title, data_file):
    print(f"  Pushing: {ws_title}...")
    try:
        ws = sh.worksheet(ws_title)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=ws_title, rows=60, cols=18)

    with open(data_file) as f:
        product_data = json.load(f)

    all_rows, fmt = build_sheet_data(product_data)
    ws.update(range_name="A1", values=all_rows, value_input_option="USER_ENTERED")

    # Formatting
    ws.format("A1:R1", {
        "backgroundColor": {"red": 0.157, "green": 0.255, "blue": 0.459},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "horizontalAlignment": "CENTER",
    })
    time.sleep(3)

    for row_num, color in fmt["category_headers"]:
        ws.format(f"A{row_num}:R{row_num}", {
            "backgroundColor": color,
            "textFormat": {"bold": True, "fontSize": 11,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        time.sleep(2)

    for row_num, color in fmt["subtotal_rows"]:
        light = {k: min(1, v * 0.6 + 0.4) for k, v in color.items()}
        ws.format(f"A{row_num}:R{row_num}", {
            "backgroundColor": light, "textFormat": {"bold": True},
        })
        time.sleep(2)

    gt = fmt["grand_total_row"]
    ws.format(f"A{gt}:R{gt}", {
        "backgroundColor": {"red": 0.20, "green": 0.20, "blue": 0.20},
        "textFormat": {"bold": True, "fontSize": 11,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })
    time.sleep(0.5)

    last_row = gt
    for col in ["B", "C", "D", "F", "I", "K"]:
        ws.format(f"{col}2:{col}{last_row}", {
            "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0"},
        })
    time.sleep(1)

    for col in ["E", "N", "O", "P", "Q"]:
        ws.format(f"{col}2:{col}{last_row}", {
            "numberFormat": {"type": "PERCENT", "pattern": "0.0%"},
        })
    time.sleep(0.5)

    ws.freeze(rows=1, cols=1)
    products = sum(
        1 for row in all_rows
        if row[0] and row[0] not in ("Products", "GRAND TOTAL", "")
        and "Subtotal" not in str(row[0])
        and "CATEGORY" not in str(row[0])
        and "no orders" not in str(row[0])
    )
    print(f"    → {products} products, {len(all_rows)} rows ✓")


def push_to_gsheets():
    print("\n" + "=" * 60)
    print("  STEP 3: Pushing to Google Sheets")
    print("=" * 60)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for m in MONTH_MAP.values():
        prefix = MONTH_PREFIXES[m]
        ws_title = MONTH_SHEET_NAMES[m]
        data_file = os.path.join(BASE, f"{prefix}_mis_data.json")
        push_month(sh, ws_title, data_file)
        time.sleep(15)

    print(f"\n  Google Sheets updated ✓")
    print(f"  {SHEET_URL}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n🔄 ClapStore MIS — Full Sync\n")

    # Step 1: CSV → JSON
    process_csv()

    # Step 2: JSON → Dashboard
    update_dashboard()

    # Step 3: JSON → Google Sheets
    push_to_gsheets()

    print("\n" + "=" * 60)
    print("  ✅ ALL SYNCED — Dashboard + Google Sheets match!")
    print("=" * 60)
