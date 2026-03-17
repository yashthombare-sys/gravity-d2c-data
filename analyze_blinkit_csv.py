#!/usr/bin/env python3
"""Blinkit Sales CSV — Monthly Summary Analysis"""

import csv
from collections import defaultdict
from datetime import datetime

CSV_PATH = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data/Blinkit/sales_csv-1913268.csv"

# Product mapping (Blinkit name keyword → internal SKU)
PRODUCT_MAP = {
    "Toggle Play": "V1",
    "Mini Switch": "V4",
    "Spark Switch": "V2",
    "Tinker Pad": "V6",
}

def map_product(item_name):
    for keyword, sku in PRODUCT_MAP.items():
        if keyword in item_name:
            return sku
    return None  # unmapped

def main():
    # {month_str: {product: {qty, mrp_value}}}
    monthly = defaultdict(lambda: defaultdict(lambda: {"qty": 0, "mrp_value": 0.0}))
    unmapped_items = set()
    all_dates = []
    row_count = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            item_name = row["item_name"].strip()
            date_str = row["date"].strip()
            qty = float(row["qty_sold"])
            mrp = float(row["mrp"])

            dt = datetime.strptime(date_str, "%Y-%m-%d")
            all_dates.append(dt)
            month_key = dt.strftime("%Y-%m")

            sku = map_product(item_name)
            if sku is None:
                unmapped_items.add(item_name)
                product_label = item_name
            else:
                product_label = sku

            monthly[month_key][product_label]["qty"] += qty
            monthly[month_key][product_label]["mrp_value"] += mrp  # mrp column already = qty × unit_mrp

    # Date range
    min_date = min(all_dates)
    max_date = max(all_dates)
    print("=" * 80)
    print("BLINKIT SALES — MONTHLY SUMMARY")
    print("=" * 80)
    print(f"Data rows    : {row_count:,}")
    print(f"Date range   : {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
    print(f"Months       : {len(monthly)}")
    print()

    # Check if mrp is per-unit or total (verify with first row logic)
    # From the CSV: qty=2, mrp=3998 and qty=1, mrp=1999 → mrp = qty × unit_price (1999)
    # So mrp column IS already qty × unit_mrp. Good.

    grand_total_qty = 0
    grand_total_mrp = 0.0

    for month_key in sorted(monthly.keys()):
        products = monthly[month_key]
        month_qty = 0
        month_mrp = 0.0

        print("-" * 80)
        print(f"  {month_key}")
        print(f"  {'Product':<20} {'Qty Sold':>10} {'MRP Value (₹)':>15}")
        print(f"  {'-'*20} {'-'*10} {'-'*15}")

        for product in sorted(products.keys()):
            q = products[product]["qty"]
            v = products[product]["mrp_value"]
            month_qty += q
            month_mrp += v
            print(f"  {product:<20} {q:>10.0f} {v:>15,.0f}")

        print(f"  {'TOTAL':<20} {month_qty:>10.0f} {month_mrp:>15,.0f}")
        grand_total_qty += month_qty
        grand_total_mrp += month_mrp

    print()
    print("=" * 80)
    print(f"  {'GRAND TOTAL':<20} {grand_total_qty:>10.0f} {grand_total_mrp:>15,.0f}")
    print("=" * 80)

    # Product-level grand totals
    print()
    print("PRODUCT-LEVEL GRAND TOTALS (all months)")
    print(f"  {'Product':<20} {'Qty Sold':>10} {'MRP Value (₹)':>15}")
    print(f"  {'-'*20} {'-'*10} {'-'*15}")
    product_totals = defaultdict(lambda: {"qty": 0, "mrp_value": 0.0})
    for month_key in sorted(monthly.keys()):
        for product, vals in monthly[month_key].items():
            product_totals[product]["qty"] += vals["qty"]
            product_totals[product]["mrp_value"] += vals["mrp_value"]
    for product in sorted(product_totals.keys()):
        q = product_totals[product]["qty"]
        v = product_totals[product]["mrp_value"]
        print(f"  {product:<20} {q:>10.0f} {v:>15,.0f}")

    # Unmapped products
    print()
    if unmapped_items:
        print("UNMAPPED PRODUCTS (not in product mapping):")
        for item in sorted(unmapped_items):
            print(f"  - {item}")
    else:
        print("All products mapped successfully. No missing products.")

    # City breakdown (bonus)
    print()
    print("CITY BREAKDOWN (all months)")
    city_totals = defaultdict(lambda: {"qty": 0, "mrp_value": 0.0})
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row["city_name"].strip()
            qty = float(row["qty_sold"])
            mrp = float(row["mrp"])
            city_totals[city]["qty"] += qty
            city_totals[city]["mrp_value"] += mrp
    print(f"  {'City':<20} {'Qty Sold':>10} {'MRP Value (₹)':>15}")
    print(f"  {'-'*20} {'-'*10} {'-'*15}")
    for city in sorted(city_totals.keys(), key=lambda c: -city_totals[c]["mrp_value"]):
        q = city_totals[city]["qty"]
        v = city_totals[city]["mrp_value"]
        print(f"  {city:<20} {q:>10.0f} {v:>15,.0f}")

if __name__ == "__main__":
    main()
