#!/usr/bin/env python3
"""
Fetch Shiprocket orders for the current month, build daily fulfillment/TAT data,
inject into mtd_daily_data.json and dashboard.html.

Usage: python3 shiprocket_sync.py
"""

import os, sys, json, re, requests, argparse, shutil
from datetime import datetime, timedelta, date
from collections import defaultdict
from calendar import monthrange

import gspread
from google.oauth2.service_account import Credentials

# Force unbuffered output (for GitHub Actions log visibility)
sys.stdout.reconfigure(line_buffering=True)

# Add automation/ to path for imports
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "automation"))

from config import classify_status, classify_product, is_spare_part, CATS, load_env

API_BASE = "https://apiv2.shiprocket.in/v1/external"
OUTPUT_FILE = os.path.join(BASE, "mtd_daily_data.json")
DASHBOARD = os.path.join(BASE, "dashboard.html")

# Google Sheets config
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
D2C_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LfLi67xq8P1bxEAmJysQuci7vOOuqxEoYxTrYEab0yA/"
FULFILLMENT_TAB = "Fulfillment"
GSHEET_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

FULFILLMENT_HEADERS = [
    "Date", "New Orders", "New Value", "Pending", "Pending Value",
    "Shipped", "Shipped Value", "Delivered", "Delivered Value",
    "In Transit", "RTO", "Cancelled", "Avg TAT Days",
    "Avg Ship Days", "Avg Deliver Days", "Products JSON",
]


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
    """Fetch orders for current month with progress logging."""
    date_from = month_start.strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")

    print(f"  Fetching orders {date_from} to {date_to}...", flush=True)

    all_orders = []
    page = 1
    per_page = 200

    while True:
        url = f"{API_BASE}/orders?per_page={per_page}&page={page}&from={date_from}&to={date_to}"
        try:
            resp = requests.get(url, headers=headers, timeout=20)
        except requests.exceptions.RequestException as e:
            print(f"  Network error on page {page}: {e}", flush=True)
            break

        if resp.status_code == 401:
            raise PermissionError("Shiprocket token expired/invalid.")
        if resp.status_code == 503:
            print(f"  503 on page {page}, retrying...", flush=True)
            import time; time.sleep(3)
            continue
        if resp.status_code != 200:
            print(f"  API error {resp.status_code} on page {page}", flush=True)
            break

        data = resp.json()

        # Handle nested response structure
        if isinstance(data, dict) and "data" in data:
            orders = data["data"]
            if isinstance(orders, dict):
                orders = orders.get("data", orders.get("orders", []))
            if not isinstance(orders, list):
                orders = []
        elif isinstance(data, list):
            orders = data
        else:
            orders = []

        if not orders:
            break

        all_orders.extend(orders)
        print(f"  Page {page}: +{len(orders)} orders ({len(all_orders)} total)", flush=True)

        # Pagination check
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            last_page = data["data"].get("last_page", 0)
            if last_page and page >= last_page:
                break

        if len(orders) < per_page:
            break

        page += 1

    print(f"  {len(all_orders)} orders fetched", flush=True)
    return all_orders


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


def get_shipped_date(order):
    """Try to get shipped/pickup date from shipments or top-level fields."""
    # Try shipments array
    shipments = order.get("shipments", [])
    if shipments and isinstance(shipments, list):
        for s in shipments:
            for field in ("pickup_date", "shipped_date", "pickup_scheduled_date", "created_at"):
                result = parse_date_str(s.get(field))
                if result:
                    return result

    # Try top-level fields
    for field in ("pickup_date", "shipped_date", "pickup_scheduled_date"):
        result = parse_date_str(order.get(field))
        if result:
            return result

    return None


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


def _empty_bucket():
    return {
        "new_orders": 0, "new_value": 0.0,
        "pending": 0, "pending_value": 0.0,
        "shipped": 0, "shipped_value": 0.0,
        "delivered": 0, "delivered_value": 0.0,
        "in_transit": 0, "rto": 0, "cancelled": 0,
        "tat_total_days": 0.0, "tat_count": 0,
        "ship_tat_total": 0.0, "ship_tat_count": 0,
        "deliver_tat_total": 0.0, "deliver_tat_count": 0,
    }


def _add_to_bucket(bucket, status, order_value):
    bucket["new_orders"] += 1
    bucket["new_value"] += order_value
    if status == "pending":
        # Not yet shipped — awaiting pickup
        bucket["pending"] += 1
        bucket["pending_value"] += order_value
    elif status in ("delivered", "in_transit", "rto"):
        bucket["shipped"] += 1
        bucket["shipped_value"] += order_value
        if status == "delivered":
            bucket["delivered"] += 1
            bucket["delivered_value"] += order_value
        elif status == "in_transit":
            bucket["in_transit"] += 1
        elif status == "rto":
            bucket["rto"] += 1
    elif status == "cancelled":
        bucket["cancelled"] += 1


def _finalize_bucket(d):
    avg_tat = round(d["tat_total_days"] / d["tat_count"], 1) if d["tat_count"] > 0 else 0
    avg_ship = round(d["ship_tat_total"] / d["ship_tat_count"], 1) if d["ship_tat_count"] > 0 else 0
    avg_deliver = round(d["deliver_tat_total"] / d["deliver_tat_count"], 1) if d["deliver_tat_count"] > 0 else 0
    return {
        "new_orders": d["new_orders"],
        "new_value": round(d["new_value"], 2),
        "pending": d["pending"],
        "pending_value": round(d["pending_value"], 2),
        "shipped": d["shipped"],
        "shipped_value": round(d["shipped_value"], 2),
        "delivered": d["delivered"],
        "delivered_value": round(d["delivered_value"], 2),
        "in_transit": d["in_transit"],
        "rto": d["rto"],
        "cancelled": d["cancelled"],
        "avg_tat_days": avg_tat,
        "avg_ship_days": avg_ship,
        "avg_deliver_days": avg_deliver,
    }


def _get_order_product(order):
    """Identify the primary product in an order using classify_product()."""
    products = order.get("products", order.get("order_items", []))
    if products and isinstance(products, list):
        for p in products:
            name = p.get("name", p.get("product_name", p.get("sku", "")))
            classified = classify_product(name)
            if classified and not is_spare_part(name):
                return classified
    # Try channel_order_id or product_name at top level
    for field in ("product_name", "channel_order_id", "customer_name"):
        val = order.get(field, "")
        if val:
            classified = classify_product(str(val))
            if classified:
                return classified
    return "Other"


def build_daily_data(orders, month_start, today):
    """
    Aggregate orders into daily fulfillment data with per-product breakdown.
    Only includes days within the current month.
    """
    daily = defaultdict(lambda: {**_empty_bucket(), "products": defaultdict(_empty_bucket)})

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

        created_str = created.strftime("%Y-%m-%d")
        created_month = created.strftime("%Y-%m")

        status_raw = order.get("status", order.get("status_code", ""))
        status = classify_status(status_raw)
        if status in ("skip", "unknown"):
            continue

        order_value = get_order_value(order)
        delivered_date = get_delivered_date(order)
        shipped_date = get_shipped_date(order)
        product = _get_order_product(order)

        # Count as "new order" on its creation date (this month only)
        if created_month == month_str:
            day = daily[created_str]
            _add_to_bucket(day, status, order_value)
            _add_to_bucket(day["products"][product], status, order_value)

        # Ship TAT (Order→Shipped): for shipped orders, accumulate on creation date
        if shipped_date and status in ("delivered", "in_transit", "rto"):
            ship_days = (shipped_date - created).days
            if 0 <= ship_days <= 15 and created_month == month_str:
                day = daily[created_str]
                day["ship_tat_total"] += ship_days
                day["ship_tat_count"] += 1
                day["products"][product]["ship_tat_total"] += ship_days
                day["products"][product]["ship_tat_count"] += 1

        # TAT: if delivered this month, compute TAT regardless of creation month
        if status == "delivered" and delivered_date:
            delivered_str = delivered_date.strftime("%Y-%m-%d")
            delivered_month = delivered_date.strftime("%Y-%m")
            if delivered_month == month_str:
                tat_days = (delivered_date - created).days
                if 0 <= tat_days <= 30:
                    day = daily[delivered_str]
                    day["tat_total_days"] += tat_days
                    day["tat_count"] += 1
                    day["products"][product]["tat_total_days"] += tat_days
                    day["products"][product]["tat_count"] += 1

                # Deliver TAT (Shipped→Delivered): accumulate on delivered date
                if shipped_date:
                    del_days = (delivered_date - shipped_date).days
                    if 0 <= del_days <= 30:
                        day["deliver_tat_total"] += del_days
                        day["deliver_tat_count"] += 1
                        day["products"][product]["deliver_tat_total"] += del_days
                        day["products"][product]["deliver_tat_count"] += 1

    # Build final output
    result = {}
    for dt_str in sorted(daily.keys()):
        d = daily[dt_str]
        entry = _finalize_bucket(d)
        # Per-product breakdown
        entry["products"] = {}
        for pname, pbucket in d["products"].items():
            entry["products"][pname] = _finalize_bucket(pbucket)
        result[dt_str] = entry

    print(f"  Skipped: {skipped}", flush=True)
    return result


def _day_to_row(date_str, day_data):
    """Convert a day's finalized data dict to a flat row for Google Sheets."""
    return [
        date_str,
        day_data["new_orders"],
        round(day_data["new_value"], 2),
        day_data.get("pending", 0),
        round(day_data.get("pending_value", 0), 2),
        day_data["shipped"],
        round(day_data.get("shipped_value", 0), 2),
        day_data["delivered"],
        round(day_data.get("delivered_value", 0), 2),
        day_data.get("in_transit", 0),
        day_data.get("rto", 0),
        day_data.get("cancelled", 0),
        day_data.get("avg_tat_days", 0),
        day_data.get("avg_ship_days", 0),
        day_data.get("avg_deliver_days", 0),
        json.dumps(day_data.get("products", {})),
    ]


def write_to_fulfillment_sheet(all_daily_data):
    """Write fulfillment data to the Fulfillment tab in Google Sheets."""
    print("\n  Writing to Google Sheet (Fulfillment tab)...")
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=GSHEET_SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(D2C_SHEET_URL)

    # Get or create Fulfillment tab
    try:
        ws = sh.worksheet(FULFILLMENT_TAB)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=FULFILLMENT_TAB, rows=100, cols=len(FULFILLMENT_HEADERS))
        print(f"  Created new '{FULFILLMENT_TAB}' tab")

    # Read existing data to preserve dates not in current fetch
    existing = ws.get_all_values()
    existing_dates = {}
    if len(existing) > 1:
        for i, row in enumerate(existing[1:], start=2):  # 1-indexed, skip header
            if row and row[0]:
                existing_dates[row[0]] = i

    # Build rows from API data
    sorted_dates = sorted(all_daily_data.keys())
    rows_to_write = [FULFILLMENT_HEADERS]
    # Keep existing dates not covered by API
    for row in existing[1:]:
        if row and row[0] and row[0] not in all_daily_data:
            rows_to_write.append(row)
    # Add API data
    for d in sorted_dates:
        rows_to_write.append(_day_to_row(d, all_daily_data[d]))
    # Sort all rows by date (skip header)
    rows_to_write[1:] = sorted(rows_to_write[1:], key=lambda r: r[0])

    # Clear and rewrite
    ws.clear()
    ws.update(range_name='A1', values=rows_to_write)
    print(f"  Written {len(rows_to_write)-1} days to Fulfillment tab")


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


def _print_summary(daily_data, label):
    total_new = sum(d["new_orders"] for d in daily_data.values())
    total_delivered = sum(d["delivered"] for d in daily_data.values())
    total_value = sum(d["new_value"] for d in daily_data.values())
    tat_days_sum = sum(d["avg_tat_days"] for d in daily_data.values() if d["avg_tat_days"] > 0)
    tat_day_count = sum(1 for d in daily_data.values() if d["avg_tat_days"] > 0)
    month_avg_tat = round(tat_days_sum / tat_day_count, 1) if tat_day_count > 0 else 0
    print(f"\n  Summary ({label}):")
    print(f"    Days with data: {len(daily_data)}")
    print(f"    New orders: {total_new} | Value: {total_value:,.0f}")
    print(f"    Delivered: {total_delivered}")
    print(f"    Avg TAT: {month_avg_tat} days")


def main():
    parser = argparse.ArgumentParser(description="Shiprocket fulfillment sync")
    parser.add_argument("--historical", type=int, default=0, metavar="N",
                        help="Fetch last N months of historical data (e.g. --historical 6)")
    args = parser.parse_args()

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

    # Build list of months to fetch
    months_to_fetch = []
    if args.historical > 0:
        # Fetch last N months + current month
        for i in range(args.historical, -1, -1):
            # Go back i months from today
            y, m = today.year, today.month - i
            while m <= 0:
                m += 12
                y -= 1
            months_to_fetch.append(date(y, m, 1))
        print(f"  Historical mode: fetching {len(months_to_fetch)} months")
    else:
        months_to_fetch.append(today.replace(day=1))

    # Fetch and process each month — API data only
    all_daily_data = {}

    for month_start in months_to_fetch:
        # End date: last day of month, or today if current month
        _, last_day = monthrange(month_start.year, month_start.month)
        month_end = date(month_start.year, month_start.month, last_day)
        if month_end > today:
            month_end = today

        month_label = month_start.strftime("%b %Y")
        print(f"\n  ── {month_label} ──")

        orders = fetch_all_orders(headers, month_start, month_end)
        if not orders:
            print(f"  No orders for {month_label}")
            continue

        month_data = build_daily_data(orders, month_start, month_end)
        _print_summary(month_data, month_label)

        # Merge into all_daily_data (overwrite days in this month)
        all_daily_data.update(month_data)

    # Write to Google Sheet
    write_to_fulfillment_sheet(all_daily_data)

    total_days = len(all_daily_data)
    months_covered = sorted(set(k[:7] for k in all_daily_data.keys()))
    print(f"\n  {total_days} days ({len(months_covered)} months) written to Google Sheet")

    # Write directly to mtd_daily_data.json (bypasses Google Sheets round-trip in sync_mtd.py)
    if all_daily_data and os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                output = json.load(f)
            existing_sr = output.get("shiprocket", {})
            sr_before = len(existing_sr)
            existing_sr.update(all_daily_data)
            output["shiprocket"] = dict(sorted(existing_sr.items()))
            output["shiprocket_synced"] = datetime.now().strftime("%Y-%m-%dT%H:%M")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(output, f, indent=2)
            print(f"  mtd_daily_data.json: shiprocket {sr_before} → {len(output['shiprocket'])} dates")
            inject_into_dashboard(output["shiprocket"])
        except Exception as e:
            print(f"  Warning: could not write to mtd_daily_data.json: {e}")

    print("\nDone.\n")


if __name__ == "__main__":
    main()
