#!/usr/bin/env python3
"""
Fetch Shiprocket orders for the current month, build daily fulfillment/TAT data,
inject into mtd_daily_data.json and dashboard.html.

Usage: python3 shiprocket_sync.py
"""

import os, sys, json, re, requests
from datetime import datetime, timedelta, date
from collections import defaultdict

# Add automation/ to path for imports
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "automation"))

from shiprocket_fetch import fetch_orders
from config import classify_status, load_env

API_BASE = "https://apiv2.shiprocket.in/v1/external"
OUTPUT_FILE = os.path.join(BASE, "mtd_daily_data.json")
DASHBOARD = os.path.join(BASE, "dashboard.html")


def get_credentials():
    """Get Shiprocket email/password from env vars (GitHub Actions) or .env file."""
    email = os.environ.get("SHIPROCKET_EMAIL", "")
    password = os.environ.get("SHIPROCKET_PASSWORD", "")
    if email and password:
        return email, password
    # Fallback: read from .env
    env = load_env()
    email = env.get("SHIPROCKET_EMAIL", "")
    password = env.get("SHIPROCKET_PASSWORD", "")
    return email, password


def login_and_get_token(email, password):
    """Authenticate with Shiprocket and get a fresh bearer token."""
    url = f"{API_BASE}/auth/login"
    resp = requests.post(url, json={"email": email, "password": password}, timeout=30)
    if resp.status_code != 200:
        raise PermissionError(f"Shiprocket login failed ({resp.status_code}): {resp.text[:200]}")
    data = resp.json()
    token = data.get("token", "")
    if not token:
        raise PermissionError(f"No token in login response: {list(data.keys())}")
    return token


def build_headers(token):
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }


def fetch_all_orders(headers, month_start, today):
    """Fetch orders from 14 days before month start through today."""
    lookback = month_start - timedelta(days=14)
    date_from = lookback.strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")

    print(f"  Fetching orders {date_from} to {date_to}...")
    orders = fetch_orders(date_from, date_to, headers=headers)
    print(f"  {len(orders)} orders fetched")
    return orders


def parse_order_date(order, field="created_at"):
    """Extract and parse a date field from an order."""
    val = order.get(field, "")
    if not val or val == "None":
        return None
    s = str(val).strip()
    # Try multiple formats
    for fmt in (
        "%Y-%m-%d",              # 2026-03-31
        "%Y-%m-%d %H:%M:%S",    # 2026-03-31 15:55:00
        "%d %b %Y, %I:%M %p",   # 31 Mar 2026, 03:55 PM
        "%d %b %Y",             # 31 Mar 2026
        "%d-%b-%Y",             # 31-Mar-2026
        "%d/%m/%Y",             # 31/03/2026
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # Last resort: try first 10 chars as YYYY-MM-DD
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def parse_date_str(val):
    """Parse a date string in various Shiprocket formats."""
    if not val or str(val).strip() in ("", "None"):
        return None
    s = str(val).strip()
    for fmt in (
        "%Y-%m-%d", "%Y-%m-%d %H:%M:%S",
        "%d %b %Y, %I:%M %p", "%d %b %Y",
        "%d-%b-%Y", "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def get_delivered_date(order):
    """Try to get delivery date from shipments or top-level fields."""
    # Try shipments array
    shipments = order.get("shipments", [])
    if shipments and isinstance(shipments, list):
        for s in shipments:
            dd = s.get("delivered_date") or s.get("delivery_date")
            result = parse_date_str(dd)
            if result:
                return result

    # Try top-level delivered_date
    dd = order.get("delivered_date") or order.get("delivery_date")
    return parse_date_str(dd)


def get_order_value(order):
    """Get total order value."""
    # Try total field first
    total = order.get("total", 0)
    if total:
        try:
            return float(total)
        except (ValueError, TypeError):
            pass

    # Sum line items
    products = order.get("products", order.get("order_items", []))
    if not products:
        return float(order.get("sub_total", order.get("product_price", 0)) or 0)

    value = 0
    for prod in products:
        price = float(prod.get("price", prod.get("selling_price", 0)) or 0)
        discount = float(prod.get("discount", 0) or 0)
        qty = int(prod.get("quantity", prod.get("product_quantity", 1)) or 1)
        value += (price - discount) * qty
    return value


def build_daily_data(orders, month_start, today):
    """
    Aggregate orders into daily fulfillment data.
    Only includes days within the current month.
    """
    daily = defaultdict(lambda: {
        "new_orders": 0, "new_value": 0.0,
        "shipped": 0, "shipped_value": 0.0,
        "delivered": 0, "delivered_value": 0.0,
        "in_transit": 0, "rto": 0, "cancelled": 0,
        "tat_total_days": 0.0, "tat_count": 0,
    })

    month_str = month_start.strftime("%Y-%m")
    skipped = {"reverse": 0, "spare": 0, "no_date": 0}

    for order in orders:
        # Skip reverse orders
        is_reverse = order.get("is_reverse", 0)
        if is_reverse or str(is_reverse).lower() in ("yes", "1", "true"):
            skipped["reverse"] += 1
            continue

        created = parse_order_date(order, "created_at")
        if not created:
            skipped["no_date"] += 1
            continue

        # Only process orders created this month (for new_orders count)
        created_str = created.strftime("%Y-%m-%d")
        created_month = created.strftime("%Y-%m")

        status_raw = order.get("status", order.get("status_code", ""))
        status = classify_status(status_raw)
        if status in ("skip", "unknown"):
            continue

        order_value = get_order_value(order)
        delivered_date = get_delivered_date(order)

        # Count as "new order" on its creation date (this month only)
        if created_month == month_str:
            day = daily[created_str]
            day["new_orders"] += 1
            day["new_value"] += order_value

            # Shipped = any order that has moved beyond "new" (delivered, in_transit, rto)
            if status in ("delivered", "in_transit", "rto"):
                day["shipped"] += 1
                day["shipped_value"] += order_value

            if status == "delivered":
                day["delivered"] += 1
                day["delivered_value"] += order_value
            elif status == "in_transit":
                day["in_transit"] += 1
            elif status == "rto":
                day["rto"] += 1
            elif status == "cancelled":
                day["cancelled"] += 1

        # TAT: if delivered this month, compute TAT regardless of creation month
        if status == "delivered" and delivered_date:
            delivered_str = delivered_date.strftime("%Y-%m-%d")
            delivered_month = delivered_date.strftime("%Y-%m")
            if delivered_month == month_str:
                tat_days = (delivered_date - created).days
                if 0 <= tat_days <= 30:  # sanity check
                    # Attribute TAT to the delivery date
                    day = daily[delivered_str]
                    day["tat_total_days"] += tat_days
                    day["tat_count"] += 1

    # Build final output with avg_tat_days
    result = {}
    for dt_str in sorted(daily.keys()):
        d = daily[dt_str]
        avg_tat = round(d["tat_total_days"] / d["tat_count"], 1) if d["tat_count"] > 0 else 0
        result[dt_str] = {
            "new_orders": d["new_orders"],
            "new_value": round(d["new_value"], 2),
            "shipped": d["shipped"],
            "shipped_value": round(d["shipped_value"], 2),
            "delivered": d["delivered"],
            "delivered_value": round(d["delivered_value"], 2),
            "in_transit": d["in_transit"],
            "rto": d["rto"],
            "cancelled": d["cancelled"],
            "avg_tat_days": avg_tat,
        }

    print(f"  Skipped: {skipped}")
    return result


def inject_into_dashboard(data):
    """Update MTD_DATA in dashboard.html to include shiprocket key."""
    if not os.path.exists(DASHBOARD):
        print("  dashboard.html not found, skipping injection")
        return

    with open(DASHBOARD, "r") as f:
        html = f.read()

    marker_start = "// ── MTD_DATA_START ──"
    marker_end = "// ── MTD_DATA_END ──"

    if marker_start not in html:
        print("  MTD_DATA markers not found in dashboard.html")
        return

    # Extract existing MTD_DATA
    pattern = re.escape(marker_start) + r"\n(.*?)\n" + re.escape(marker_end)
    match = re.search(pattern, html, re.DOTALL)
    if not match:
        print("  Could not extract MTD_DATA from dashboard.html")
        return

    js_line = match.group(1).strip()
    # Parse the JSON from "const MTD_DATA={...};"
    json_match = re.search(r'const MTD_DATA\s*=\s*(\{.*\})\s*;', js_line, re.DOTALL)
    if not json_match:
        print("  Could not parse MTD_DATA JSON")
        return

    try:
        mtd_data = json.loads(json_match.group(1))
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        return

    # Add shiprocket data
    mtd_data["shiprocket"] = data

    # Write back
    new_js = f"const MTD_DATA={json.dumps(mtd_data)};"
    replacement = f"{marker_start}\n{new_js}\n{marker_end}"
    html = re.sub(pattern, replacement, html, flags=re.DOTALL)

    with open(DASHBOARD, "w") as f:
        f.write(html)
    print("  Updated MTD_DATA with shiprocket data in dashboard.html")


def main():
    print("\nShiprocket Fulfillment Sync\n")

    email, password = get_credentials()
    if not email or not password:
        print("ERROR: SHIPROCKET_EMAIL and SHIPROCKET_PASSWORD not found in env or .env")
        sys.exit(1)

    print("  Logging in to Shiprocket...")
    token = login_and_get_token(email, password)
    print("  Login successful — token obtained")

    headers = build_headers(token)
    today = date.today()
    month_start = today.replace(day=1)

    # Fetch orders
    orders = fetch_all_orders(headers, month_start, today)
    if not orders:
        print("  No orders found — writing empty data")
        daily_data = {}
    else:
        daily_data = build_daily_data(orders, month_start, today)

    # Summary
    total_new = sum(d["new_orders"] for d in daily_data.values())
    total_delivered = sum(d["delivered"] for d in daily_data.values())
    total_value = sum(d["new_value"] for d in daily_data.values())
    tat_days_sum = sum(d["avg_tat_days"] * (1 if d["avg_tat_days"] > 0 else 0) for d in daily_data.values())
    tat_day_count = sum(1 for d in daily_data.values() if d["avg_tat_days"] > 0)
    month_avg_tat = round(tat_days_sum / tat_day_count, 1) if tat_day_count > 0 else 0

    print(f"\n  Summary ({month_start.strftime('%b %Y')}):")
    print(f"    Days with data: {len(daily_data)}")
    print(f"    New orders: {total_new} | Value: {total_value:,.0f}")
    print(f"    Delivered: {total_delivered}")
    print(f"    Avg TAT: {month_avg_tat} days")

    # Update mtd_daily_data.json
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r") as f:
            mtd_json = json.load(f)
    else:
        mtd_json = {}

    mtd_json["shiprocket"] = daily_data

    with open(OUTPUT_FILE, "w") as f:
        json.dump(mtd_json, f, indent=2)
    print(f"\n  Saved shiprocket data to {OUTPUT_FILE}")

    # Inject into dashboard.html
    inject_into_dashboard(daily_data)

    print("\nDone.\n")


if __name__ == "__main__":
    main()
