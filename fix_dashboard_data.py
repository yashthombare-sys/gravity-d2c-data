#!/usr/bin/env python3
"""Fix dashboard.html D2C DATA by reading directly from MIS JSON files (not Google Sheets)."""

import json, re, os

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
DASHBOARD = os.path.join(BASE, "dashboard.html")

MONTHS = {
    "Oct 2025": "oct_mis_data.json",
    "Nov 2025": "nov_mis_data.json",
    "Dec 2025": "dec_mis_data.json",
    "Jan 2026": "jan_mis_data.json",
    "Feb 2026": "feb_mis_data.json",
}


def to_js_product(p, d):
    """Convert one product dict to JS object string."""
    return (
        f'"{p}":{{total_orders:{d["total_orders"]},'
        f'shipped:{d["shipped"]},'
        f'delivered:{d["delivered"]},'
        f'rto:{d["rto"]},'
        f'in_transit:{d.get("in_transit", 0)},'
        f'cancelled:{d.get("cancelled", d["total_orders"] - d["shipped"])},'
        f'lost:{d.get("lost", 0)},'
        f'revenue:{round(d["revenue"], 2)},'
        f'freight:{round(d["freight"], 2)}}}'
    )


def main():
    all_months = {}
    for month_key, fname in MONTHS.items():
        fpath = os.path.join(BASE, fname)
        with open(fpath) as f:
            data = json.load(f)
        products = []
        for p, d in data.items():
            if d["total_orders"] == 0 and d["revenue"] == 0:
                continue
            # Compute cancelled if not present
            if "cancelled" not in d:
                d["cancelled"] = max(d["total_orders"] - d["shipped"], 0)
            if "lost" not in d:
                d["lost"] = 0
            products.append(to_js_product(p, d))
        all_months[month_key] = "{" + ",".join(products) + "}"
        print(f"  {month_key}: {len(products)} products")

    # Build the DATA= line
    data_js = "DATA={" + ",".join(f'"{m}":{all_months[m]}' for m in MONTHS) + "};"

    # Read dashboard
    with open(DASHBOARD) as f:
        html = f.read()

    # Replace the DATA= line
    pattern = r'DATA=\{.*?\};'
    new_html = re.sub(pattern, data_js, html, count=1, flags=re.DOTALL)

    if new_html == html:
        print("\n⚠️  Could not find DATA= line to replace!")
        return

    with open(DASHBOARD, "w") as f:
        f.write(new_html)

    print(f"\n✅ D2C DATA fixed in dashboard.html from JSON files")


if __name__ == "__main__":
    main()
