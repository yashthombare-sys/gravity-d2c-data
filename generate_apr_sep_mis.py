#!/usr/bin/env python3
"""
Generate MIS data for April–September 2025
- D2C: from Shiprocket CSV export
- Amazon: from order TSVs + settlement TSVs + ad spend invoice
Outputs: per-month JSON files (same format as existing Oct–Feb)
"""

import csv, json, os, re
from collections import defaultdict
from datetime import datetime

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
NEW_DATA = os.path.join(BASE, "BOTH SHIProket & AMAZON DATA")

# ── Month definitions ─────────────────────────────────────────────────────────
MONTHS = [
    ("Apr 2025", 2025, 4),
    ("May 2025", 2025, 5),
    ("Jun 2025", 2025, 6),
    ("Jul 2025", 2025, 7),
    ("Aug 2025", 2025, 8),
    ("Sep 2025", 2025, 9),
]

MONTH_NUM_MAP = {(y, m): label for label, y, m in MONTHS}

# ── Product mapping (D2C) ─────────────────────────────────────────────────────
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
    (r"(?i)police\s*cruiser.*V7|V7.*police\s*cruiser|montessori\s*police\s*cruiser\s*V7", "V7 Police Cruiser"),
    (r"(?i)rhyme\s*house.*V8|busy\s*board.*V8|V8\s*rechargeable|V8(?!\d)", "V8"),
    (r"(?i)buzz\s*lite.*V5|busy\s*board.*V5|V5(?!\d)", "V5"),
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
    (r"(?i)drawing\s*board", "Drawing Board"),
]

SPARE_PARTS_KEYWORDS = [
    "RC TANK MOTOR", "RC CAR PCB", "Charging cable", "Documents",
    "charging cable", "spare", "SPARE", "pcb", "motor", "document"
]

# D2C statuses
DELIVERED_STATUSES = {"DELIVERED"}
RTO_STATUSES = {"RTO DELIVERED", "RTO IN TRANSIT", "RTO INITIATED", "RTO OFD",
                "REACHED BACK AT SELLER CITY", "REACHED BACK AT_SELLER_CITY",
                "RTO ACKNOWLEDGED"}
CANCELLED_STATUSES = {"CANCELED", "CANCELLATION REQUESTED"}
SKIP_STATUSES = {"SELF FULFILED", "QC FAILED", "RETURN DELIVERED",
                 "RETURN IN TRANSIT", "RETURN PENDING", "RETURN CANCELLED"}
LOST_STATUSES = {"LOST", "DAMAGED", "DESTROYED"}

# Amazon SKU mapping
SKU_MAP = {
    "PortablebusyboardV1.5": "V1", "PortablebusyboardV01.5": "V1", "NEW_V1": "V1",
    "PortableBusyBoardV2": "V2", "PortablebusyboardV2": "V2", "NEW_V2": "V2",
    "QK-GUA5-RIKR": "V2",
    "bb_v03": "V3", "bb_v03 SF": "V3", "PortablebusyboardV03new": "V3",
    "PortablebusyboardV7": "V4", "PortableBusyboardV7": "V4", "PortablebusyboardV7 SF": "V4",
    "PortablebusyboardV6": "V6", "PortablebusyboardV6 SF": "V6",
    "PortablebusyboardV5": "V5", "PortablebusyboardV05 SF": "V5",
    "PortablebusyboardV07": "V7 Police Cruiser",
    "PortableBusyBoard_V09": "V9", "new_PortableBusyBoard V9": "V9",
    "PortablebusyboardV3new": "V3",
    "bb_v10": "V10", "bb_v10_SF": "V10", "bb_v14_SF": "V10",
    "busybook_blue": "Busy Book Blue", "Busybook01": "Busy Book Blue", "Busybookblue": "Busy Book Blue",
    "busybook_pink": "Busy Book Pink", "Busybookpink": "Busy Book Pink",
    "Humanbody01": "Human Book",
    "V1pack2": "V1- P of 2", "V1pack3": "V1- P of 3",
    "V4pack2": "V4- P of 2", "V4pack3": "V4- P of 3",
    "new_ComboV2_V4": "V2-V4 Combo", "new_ComboV2_V6": "V6-V2 Combo",
    "ComboV1_V6": "V6-V1 Combo", "ComboV1_V2": "V1-V2 Combo",
    "comboV1_V4": "V1-V4 Combo", "ComboV1_V4": "V1-V4 Combo",
    "Combo (V9/V10)": "V9-V10 Combo",
    "Ganesha_02": "Ganesha",
    "DIY_Tank01": "Tank",
    "CS Basics 1": "Drawing Board",
}

COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170,
    "V1- P of 2": 531, "V1- P of 3": 531, "V2- P of 2": 649,
    "V4- P of 2": 401, "V4- P of 3": 368,
    "V6": 275, "V6- P of 2": 649, "V9": 778, "V9 P of 2": 1664, "V10": 1009,
    "Busy Book Pink": 300, "Busy Book Blue": 300, "Human Book": 300,
    "V9-V3 Combo": 1440, "V9-V10 Combo": 1787,
    "V1-V4 Combo": 404, "V6-V2 Combo": 612, "V1-V2 Combo": 524,
    "V2-V4 Combo": 488, "V9-V2 Combo": 488, "V6-V1 Combo": 608,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862, "Drawing Board": 250, "JCB": 540,
    "V5": 225, "V7 Police Cruiser": 600, "V8": 700,
}


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
    return any(kw.lower() in nl for kw in SPARE_PARTS_KEYWORDS)


def classify_status(status):
    if not status:
        return "unknown"
    s = status.upper().strip()
    if s in DELIVERED_STATUSES:
        return "delivered"
    if s in RTO_STATUSES:
        return "rto"
    if s in CANCELLED_STATUSES:
        return "cancelled"
    if s in SKIP_STATUSES or s.startswith("RETURN"):
        return "skip"
    if s in LOST_STATUSES:
        return "lost"
    return "in_transit"


# ══════════════════════════════════════════════════════════════════════════════
# D2C: Process Shiprocket CSV
# ══════════════════════════════════════════════════════════════════════════════

def process_d2c():
    print("=" * 60)
    print("  D2C: Processing Shiprocket CSV (Apr–Sep 2025)")
    print("=" * 60)

    csv_path = os.path.join(NEW_DATA,
        "secure_458644_reports_1773308258832104245-41e46b5691a7adfb5cb70b2c043585fb-.csv")

    data = {label: defaultdict(lambda: {
        "total_orders": 0, "shipped": 0, "delivered": 0,
        "rto": 0, "in_transit": 0, "cancelled": 0, "lost": 0,
        "revenue": 0.0, "freight": 0.0,
    }) for label, _, _ in MONTHS}

    unmapped = defaultdict(int)
    seen = set()
    spare_count = 0

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_id = (row.get("Order ID") or "").strip()
            created_at = (row.get("Shiprocket Created At") or "").strip()
            status_raw = (row.get("Status") or "").strip()
            product_name = (row.get("Product Name") or "").strip()
            is_reverse = (row.get("Is Reverse") or "").strip()

            # Skip reverse orders
            if is_reverse and is_reverse.lower() in ("yes", "1", "true"):
                continue

            # Skip custom/RE/CUS/COL order IDs (per MIS flow rules)
            if order_id.startswith(("RE-", "CUS-", "COL-", "CUSTOM")):
                continue

            # Parse month
            if not created_at or len(created_at) < 7:
                continue
            try:
                dt = datetime.strptime(created_at[:10], "%Y-%m-%d")
                month_key = MONTH_NUM_MAP.get((dt.year, dt.month))
            except ValueError:
                continue
            if not month_key:
                continue

            status = classify_status(status_raw)
            if status == "skip":
                continue

            if is_spare(product_name):
                spare_count += 1
                continue

            product = classify_product(product_name)
            if not product:
                unmapped[product_name] += 1
                continue

            # Parse quantities and prices
            try:
                qty = int(float(row.get("Product Quantity") or 1))
            except (ValueError, TypeError):
                qty = 1
            if qty > 10:
                continue

            try:
                price = float(row.get("Product Price") or 0)
            except (ValueError, TypeError):
                price = 0
            try:
                discount = float(row.get("Discount Value") or 0)
            except (ValueError, TypeError):
                discount = 0
            try:
                freight = float(row.get("Freight Total Amount") or 0)
            except (ValueError, TypeError):
                freight = 0

            # Dedup by (order_id, product)
            dedup_key = (order_id, product)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            pd = data[month_key][product]
            pd["total_orders"] += 1

            if status == "delivered":
                pd["delivered"] += 1
                pd["shipped"] += 1
                pd["revenue"] += (price - discount) * qty
                pd["freight"] += freight
            elif status == "rto":
                pd["rto"] += 1
                pd["shipped"] += 1
                pd["freight"] += freight
            elif status == "in_transit":
                pd["in_transit"] += 1
                pd["shipped"] += 1
                pd["freight"] += freight
            elif status == "lost":
                pd["lost"] += 1
                pd["shipped"] += 1
                pd["freight"] += freight
            elif status == "cancelled":
                pd["cancelled"] += 1
                # No freight for cancelled

    if unmapped:
        print(f"\n  Unmapped products (top 15):")
        for name, cnt in sorted(unmapped.items(), key=lambda x: -x[1])[:15]:
            print(f"    {name}: {cnt}")
    print(f"  Spare parts skipped: {spare_count}")

    # Convert defaultdicts to regular dicts and save
    for label, _, _ in MONTHS:
        month_data = {k: dict(v) for k, v in data[label].items() if v["total_orders"] > 0}
        prefix = label.lower().replace(" ", "_")
        path = os.path.join(BASE, f"{prefix}_mis_data.json")
        with open(path, "w") as f:
            json.dump(month_data, f, indent=2)

        t_orders = sum(v["total_orders"] for v in month_data.values())
        t_del = sum(v["delivered"] for v in month_data.values())
        t_rev = sum(v["revenue"] for v in month_data.values())
        t_rto = sum(v["rto"] for v in month_data.values())
        t_ship = sum(v["shipped"] for v in month_data.values())
        t_freight = sum(v["freight"] for v in month_data.values())
        rto_pct = (t_rto / t_ship * 100) if t_ship > 0 else 0
        del_pct = (t_del / t_orders * 100) if t_orders > 0 else 0

        print(f"\n  {label}:")
        print(f"    Orders: {t_orders:,} | Shipped: {t_ship:,} | Delivered: {t_del:,} | RTO: {t_rto:,}")
        print(f"    Revenue: ₹{t_rev/100000:.2f}L | Freight: ₹{t_freight/100000:.2f}L")
        print(f"    RTO%: {rto_pct:.1f}% | Delivered%: {del_pct:.1f}%")
        print(f"    Saved: {path}")

    return data


# ══════════════════════════════════════════════════════════════════════════════
# Amazon: Process Orders
# ══════════════════════════════════════════════════════════════════════════════

def process_amazon_orders():
    print("\n" + "=" * 60)
    print("  Amazon: Processing Order Files")
    print("=" * 60)

    data = {label: defaultdict(lambda: {
        "total_orders": 0, "delivered": 0, "cancelled": 0,
        "revenue": 0.0,
    }) for label, _, _ in MONTHS}

    unmapped_skus = defaultdict(int)

    # Order files: 333726-333731 (Apr-Sep monthly)
    order_files = [f for f in os.listdir(NEW_DATA) if f.endswith(".txt")]

    for fn in sorted(order_files):
        filepath = os.path.join(NEW_DATA, fn)
        try:
            with open(filepath, encoding="utf-8-sig") as f:
                first_line = f.readline()
                if "amazon-order-id" not in first_line:
                    continue  # Not an order file
                f.seek(0)
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    purchase_date = (row.get("purchase-date") or "").strip()
                    if not purchase_date or len(purchase_date) < 7:
                        continue
                    try:
                        parts = purchase_date[:10].split("-")
                        month_key = MONTH_NUM_MAP.get((int(parts[0]), int(parts[1])))
                    except Exception:
                        continue
                    if not month_key:
                        continue

                    sku = (row.get("sku") or "").strip()
                    product = SKU_MAP.get(sku)
                    if not product:
                        if sku:
                            unmapped_skus[sku] += 1
                        continue

                    status = (row.get("item-status") or row.get("order-status") or "").strip()
                    try:
                        item_price = float(row.get("item-price") or 0)
                    except Exception:
                        item_price = 0

                    pd = data[month_key][product]
                    pd["total_orders"] += 1

                    if status == "Cancelled":
                        pd["cancelled"] += 1
                    elif status in ("Shipped", "Shipped - Delivered to Buyer"):
                        pd["delivered"] += 1
                        pd["revenue"] += item_price
        except Exception as e:
            continue

    if unmapped_skus:
        print(f"\n  Unmapped SKUs ({len(unmapped_skus)} unique):")
        for sku, cnt in sorted(unmapped_skus.items(), key=lambda x: -x[1])[:10]:
            print(f"    {sku}: {cnt}")

    for label, _, _ in MONTHS:
        t_orders = sum(v["total_orders"] for v in data[label].values())
        t_del = sum(v["delivered"] for v in data[label].values())
        t_rev = sum(v["revenue"] for v in data[label].values())
        print(f"  {label}: {t_orders} orders, {t_del} delivered, ₹{t_rev/100000:.2f}L revenue")

    return data


# ══════════════════════════════════════════════════════════════════════════════
# Amazon: Process Settlements
# ══════════════════════════════════════════════════════════════════════════════

def process_amazon_settlements():
    print("\n" + "=" * 60)
    print("  Amazon: Processing Settlement Files")
    print("=" * 60)

    fees = {label: defaultdict(lambda: {
        "commission": 0.0, "fba_fees": 0.0, "closing_fee": 0.0,
        "promos": 0.0, "refund_amt": 0.0,
    }) for label, _, _ in MONTHS}

    settlement_files = [f for f in os.listdir(NEW_DATA) if f.endswith(".txt")]

    for fn in sorted(settlement_files):
        filepath = os.path.join(NEW_DATA, fn)
        try:
            with open(filepath, encoding="utf-8-sig") as f:
                first_line = f.readline()
                if "settlement-id" not in first_line:
                    continue  # Not a settlement file
                f.seek(0)
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    posted = row.get("posted-date", "")
                    if not posted or len(posted) < 10:
                        continue
                    try:
                        parts = posted.strip().split(".")
                        day, month, year_rest = int(parts[0]), int(parts[1]), parts[2][:4]
                        month_key = MONTH_NUM_MAP.get((int(year_rest), month))
                    except Exception:
                        continue
                    if not month_key:
                        continue

                    sku = (row.get("sku") or "").strip()
                    product = SKU_MAP.get(sku)
                    if not product:
                        continue

                    tx_type = (row.get("transaction-type") or "").strip()
                    desc = (row.get("amount-description") or "").strip()
                    try:
                        amount = float(row.get("amount") or 0)
                    except Exception:
                        continue

                    pd = fees[month_key][product]

                    if tx_type == "Order":
                        if "Commission" in desc:
                            pd["commission"] += amount
                        elif "FBA" in desc:
                            pd["fba_fees"] += amount
                        elif "closing fee" in desc.lower():
                            pd["closing_fee"] += amount
                        elif desc == "Promo rebates":
                            pd["promos"] += amount
                    elif tx_type == "Refund":
                        if desc == "Principal":
                            pd["refund_amt"] += amount
                        elif "Commission" in desc:
                            pd["commission"] += amount
                        elif "FBA" in desc:
                            pd["fba_fees"] += amount
                        elif "closing fee" in desc.lower():
                            pd["closing_fee"] += amount
        except Exception:
            continue

    for label, _, _ in MONTHS:
        t_comm = sum(abs(v["commission"]) for v in fees[label].values())
        t_fba = sum(abs(v["fba_fees"]) for v in fees[label].values())
        t_close = sum(abs(v["closing_fee"]) for v in fees[label].values())
        total = t_comm + t_fba + t_close
        print(f"  {label}: Comm ₹{t_comm/100000:.2f}L, FBA ₹{t_fba/100000:.2f}L, "
              f"Close ₹{t_close/100000:.2f}L → Total ₹{total/100000:.2f}L")

    return fees


# ══════════════════════════════════════════════════════════════════════════════
# Amazon: Process Ad Spend from invoice CSV
# ══════════════════════════════════════════════════════════════════════════════

def process_amazon_adspend(orders):
    print("\n" + "=" * 60)
    print("  Amazon: Processing Ad Spend (invoice)")
    print("=" * 60)

    monthly_totals = {label: 0.0 for label, _, _ in MONTHS}
    mon_name_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12,
    }

    invoice_path = os.path.join(BASE, "ADS", "statement 20250312 to 20260312 (1).csv")
    with open(invoice_path, encoding="utf-8-sig") as f:
        first_line = f.readline()
        if "Country" not in first_line:
            reader = csv.DictReader(f)
        else:
            f.seek(0)
            reader = csv.DictReader(f)

        for row in reader:
            date_str = row.get("Invoice issue Date", "")
            m_match = re.match(r"(\d+)\s+(\w+),\s*(\d+)", date_str)
            if not m_match:
                continue
            day, mon_name, year = m_match.groups()
            mon_num = mon_name_map.get(mon_name, 0)
            month_key = MONTH_NUM_MAP.get((int(year), mon_num))
            if not month_key:
                continue

            amt_str = (row.get("Amount paid (not converted)") or "")
            amt_str = amt_str.replace("₹", "").replace("\u20b9", "").replace(",", "").replace('"', "").strip()
            if not amt_str:
                billed = (row.get("Amount billed (not converted)") or "")
                billed = billed.replace("₹", "").replace("\u20b9", "").replace(",", "").replace('"', "").strip()
                tax = (row.get("Tax amount billed (not converted)") or "")
                tax = tax.replace("₹", "").replace("\u20b9", "").replace(",", "").replace('"', "").strip()
                try:
                    amt = float(billed or 0) + float(tax or 0)
                except Exception:
                    continue
            else:
                try:
                    amt = float(amt_str)
                except Exception:
                    continue

            monthly_totals[month_key] += amt

    print("  Monthly ad spend (inc GST):")
    for label, _, _ in MONTHS:
        print(f"    {label}: ₹{monthly_totals[label]:>10,.0f} ({monthly_totals[label]/100000:.2f}L)")

    # Distribute by revenue proportion (no per-SKU SP report for these months)
    adspend_per_product = {label: {} for label, _, _ in MONTHS}
    for label, _, _ in MONTHS:
        total_ad = monthly_totals[label]
        if total_ad == 0:
            continue
        month_orders = orders.get(label, {})
        total_rev = sum(v["revenue"] for v in month_orders.values() if isinstance(v, dict))
        if total_rev == 0:
            continue
        for product, v in month_orders.items():
            if isinstance(v, dict) and v["revenue"] > 0:
                share = v["revenue"] / total_rev
                adspend_per_product[label][product] = round(total_ad * share, 2)

    return adspend_per_product, monthly_totals


# ══════════════════════════════════════════════════════════════════════════════
# Amazon: Build MIS JSON
# ══════════════════════════════════════════════════════════════════════════════

def build_amazon_mis(orders, fees, adspend_per_product, monthly_totals):
    print("\n" + "=" * 60)
    print("  Amazon: Building MIS")
    print("=" * 60)

    prefix_map = {
        "Apr 2025": "amazon_apr_2025",
        "May 2025": "amazon_may_2025",
        "Jun 2025": "amazon_jun_2025",
        "Jul 2025": "amazon_jul_2025",
        "Aug 2025": "amazon_aug_2025",
        "Sep 2025": "amazon_sep_2025",
    }

    for label, _, _ in MONTHS:
        month_data = {}
        all_products = set(orders[label].keys()) | set(fees[label].keys())

        for product in sorted(all_products):
            o = orders[label].get(product, {
                "total_orders": 0, "delivered": 0, "cancelled": 0, "revenue": 0.0,
            })
            f = fees[label].get(product, {
                "commission": 0.0, "fba_fees": 0.0, "closing_fee": 0.0,
                "promos": 0.0, "refund_amt": 0.0,
            })

            delivered = o["delivered"]
            cogs_unit = COGS_MAP.get(product, 0)
            cogs = cogs_unit * delivered
            revenue = round(o["revenue"], 2)

            commission = round(abs(f["commission"]), 2)
            fba_fees = round(abs(f["fba_fees"]), 2)
            closing_fee = round(abs(f["closing_fee"]), 2)
            promos = round(abs(f["promos"]), 2)
            refund_amt = round(abs(f["refund_amt"]), 2)
            total_amazon_fees = round(commission + fba_fees + closing_fee + promos, 2)
            net_revenue = round(revenue - refund_amt, 2)

            ad_spend = round(adspend_per_product.get(label, {}).get(product, 0), 2)
            profit = round(net_revenue - cogs - total_amazon_fees - ad_spend, 2)
            profit_pct = round(profit / net_revenue, 4) if net_revenue > 0 else 0

            if o["total_orders"] == 0 and revenue == 0 and total_amazon_fees == 0:
                continue

            month_data[product] = {
                "revenue": net_revenue,
                "total_orders": o["total_orders"],
                "delivered": delivered,
                "cancelled": o.get("cancelled", 0),
                "cogs": round(cogs, 2),
                "cogs_unit": cogs_unit,
                "commission": commission,
                "fba_fees": fba_fees,
                "closing_fee": closing_fee,
                "promos": promos,
                "refund_amt": refund_amt,
                "total_amazon_fees": total_amazon_fees,
                "ad_spend": ad_spend,
                "profit": profit,
                "profit_pct": profit_pct,
            }

        path = os.path.join(BASE, f"{prefix_map[label]}_mis_data.json")
        with open(path, "w") as f_out:
            json.dump(month_data, f_out, indent=2)

        t_orders = sum(v["total_orders"] for v in month_data.values())
        t_del = sum(v["delivered"] for v in month_data.values())
        t_rev = sum(v["revenue"] for v in month_data.values())
        t_cogs = sum(v["cogs"] for v in month_data.values())
        t_fees = sum(v["total_amazon_fees"] for v in month_data.values())
        t_ads = sum(v["ad_spend"] for v in month_data.values())
        t_profit = sum(v["profit"] for v in month_data.values())

        print(f"\n  {label}:")
        print(f"    Orders: {t_orders} | Delivered: {t_del}")
        print(f"    Revenue: ₹{t_rev/100000:.2f}L | COGS: ₹{t_cogs/100000:.2f}L")
        print(f"    Amazon Fees: ₹{t_fees/100000:.2f}L | Ads: ₹{t_ads/100000:.2f}L")
        print(f"    Profit: ₹{t_profit/100000:.2f}L ({t_profit/t_rev*100:.1f}%)" if t_rev > 0 else "    Profit: ₹0")
        print(f"    Saved: {path}")

    # Save monthly ad spend
    with open(os.path.join(BASE, "amazon_adspend_apr_sep.json"), "w") as f:
        json.dump(monthly_totals, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  MIS Generator — April to September 2025")
    print("=" * 60)

    # D2C
    d2c_data = process_d2c()

    # Amazon
    amz_orders = process_amazon_orders()
    amz_fees = process_amazon_settlements()
    amz_adspend, amz_monthly = process_amazon_adspend(amz_orders)
    build_amazon_mis(amz_orders, amz_fees, amz_adspend, amz_monthly)

    print("\n" + "=" * 60)
    print("  ALL DONE — Apr–Sep 2025 MIS JSONs generated")
    print("=" * 60)
