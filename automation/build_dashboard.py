"""
Build dashboard.html from SQLite database.
Reads monthly MIS data → injects as inline JS into the HTML template.
"""
import json, re, os
from db import get_monthly_mis, get_monthly_ad_spend
from config import DASHBOARD_PATH, COGS_MAP, CATS

def build_data_js(d2c_data, amz_data, d2c_ad_spend, amz_ad_spend, months):
    """Build the inline JS data strings for the dashboard."""

    # Build DATA object (D2C Shiprocket)
    data_obj = {}
    for m in months:
        data_obj[m] = d2c_data.get(m, {})

    # Build AMZ_DATA object
    amz_obj = {}
    for m in months:
        amz_obj[m] = amz_data.get(m, {})

    # Build ad spend maps
    d2c_spent = {}
    amz_spent = {}
    for m in months:
        d2c_total = 0
        amz_total = 0
        if m in d2c_ad_spend:
            for platform, amount in d2c_ad_spend[m].items():
                if platform in ("meta", "google", "manual"):
                    d2c_total += amount
        if m in amz_ad_spend:
            for platform, amount in amz_ad_spend[m].items():
                if platform == "amazon_ads":
                    amz_total += amount
        d2c_spent[m] = round(d2c_total)
        amz_spent[m] = round(amz_total)

    return data_obj, amz_obj, d2c_spent, amz_spent


def format_product_data(product_dict):
    """Format a product dict as compact JS object."""
    return (f"{{total_orders:{product_dict['total_orders']},"
            f"shipped:{product_dict['shipped']},"
            f"delivered:{product_dict['delivered']},"
            f"rto:{product_dict.get('rto', 0)},"
            f"in_transit:{product_dict.get('in_transit', 0)},"
            f"cancelled:{product_dict.get('cancelled', 0)},"
            f"lost:{product_dict.get('lost', 0)},"
            f"revenue:{product_dict['revenue']},"
            f"freight:{product_dict['freight']}}}")


def build_data_string(monthly_data, months):
    """Build compact JS object string for DATA or AMZ_DATA."""
    parts = []
    for m in months:
        products = monthly_data.get(m, {})
        prod_parts = []
        for pname, pdata in sorted(products.items()):
            prod_parts.append(f'"{pname}":{format_product_data(pdata)}')
        parts.append(f'"{m}":{{{",".join(prod_parts)}}}')
    return "{" + ",".join(parts) + "}"


def inject_into_dashboard(d2c_data, amz_data, d2c_spent, amz_spent, months):
    """
    Read dashboard.html, replace the DATA=... and AMZ_DATA lines with fresh data.
    """
    if not os.path.exists(DASHBOARD_PATH):
        print(f"Dashboard not found at {DASHBOARD_PATH}")
        return False

    with open(DASHBOARD_PATH, "r") as f:
        html = f.read()

    # Build new data strings
    data_str = build_data_string(d2c_data, months)
    amz_str = build_data_string(amz_data, months)

    # Build AMZ_AD_MAP
    amz_ad_parts = [f'"{m}":{amz_spent.get(m, 0)}' for m in months]
    amz_ad_str = "{" + ",".join(amz_ad_parts) + "}"

    # Build MONTHS array
    months_str = "[" + ",".join(f'"{m}"' for m in months) + "]"

    # Replace DATA=... (matches from DATA={ to the next };)
    html = re.sub(
        r'DATA\s*=\s*\{[^;]*\};',
        f'DATA={data_str};',
        html,
        count=1
    )

    # Replace AMZ_AD_MAP
    html = re.sub(
        r'const AMZ_AD_MAP\s*=\s*\{[^}]*\};',
        f'const AMZ_AD_MAP={amz_ad_str};',
        html,
        count=1
    )

    # Replace MONTHS array
    html = re.sub(
        r'const MONTHS\s*=\s*\[[^\]]*\];',
        f'const MONTHS={months_str};',
        html,
        count=1
    )

    # Replace AMZ_DATA month entries (Oct-Jan inline data)
    for m in months:
        if m not in amz_data or not amz_data[m]:
            continue
        month_data_str = build_data_string({m: amz_data[m]}, [m])
        # Remove outer braces to get just the month entry
        inner = month_data_str[1:-1]  # "Oct 2025":{...}
        pattern = rf'AMZ_DATA\["{re.escape(m)}"\]\s*=\s*\{{[^;]*\}};'
        replacement = f'AMZ_DATA["{m}"]={{{",".join(f"{chr(34)}{p}{chr(34)}:{format_product_data(d)}" for p, d in sorted(amz_data[m].items()))}}};'
        html = re.sub(pattern, replacement, html, count=1)

    with open(DASHBOARD_PATH, "w") as f:
        f.write(html)

    print(f"  Dashboard updated: {DASHBOARD_PATH}")
    return True


def rebuild_dashboard():
    """Full rebuild: read DB → inject into dashboard.html."""
    print("Building dashboard from database...")

    # Get data from DB
    d2c_data = get_monthly_mis("shiprocket")
    amz_data = get_monthly_mis("amazon")
    ad_spend = get_monthly_ad_spend()

    # Determine months (sorted chronologically)
    all_months = set()
    all_months.update(d2c_data.keys())
    all_months.update(amz_data.keys())

    # Sort months chronologically
    month_order = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                   "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
    def month_sort_key(m):
        parts = m.split()
        return (int(parts[1]), month_order.get(parts[0], 0))

    months = sorted(all_months, key=month_sort_key)

    if not months:
        print("  No data in database yet.")
        return False

    # Split ad spend into D2C vs Amazon
    d2c_ad = {}
    amz_ad = {}
    for m, platforms in ad_spend.items():
        d2c_ad[m] = {k: v for k, v in platforms.items() if k in ("meta", "google", "manual")}
        amz_ad[m] = {k: v for k, v in platforms.items() if k == "amazon_ads"}

    d2c_spent = {}
    amz_spent = {}
    for m in months:
        d2c_spent[m] = sum(d2c_ad.get(m, {}).values())
        amz_spent[m] = sum(amz_ad.get(m, {}).values())

    success = inject_into_dashboard(d2c_data, amz_data, d2c_spent, amz_spent, months)

    if success:
        total_d2c_rev = sum(sum(p["revenue"] for p in prods.values()) for prods in d2c_data.values())
        total_amz_rev = sum(sum(p["revenue"] for p in prods.values()) for prods in amz_data.values())
        print(f"  Months: {', '.join(months)}")
        print(f"  D2C Revenue: ₹{total_d2c_rev:,.0f}")
        print(f"  Amazon Revenue: ₹{total_amz_rev:,.0f}")

    return success


if __name__ == "__main__":
    rebuild_dashboard()
