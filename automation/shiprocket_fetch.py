"""
Fetch Shiprocket orders for a given date range and process into MIS format.
"""
import requests, json, os, sys
from collections import defaultdict
from config import (load_env, classify_status, classify_product,
                    is_spare_part, COGS_MAP)

API_BASE = "https://apiv2.shiprocket.in/v1/external"

def get_headers():
    env = load_env()
    token = env.get("SHIPROCKET_API_TOKEN", "")
    if not token:
        raise ValueError("SHIPROCKET_API_TOKEN not found in .env")
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

def fetch_orders(date_from, date_to, headers=None):
    """
    Fetch all orders between date_from and date_to (YYYY-MM-DD).
    Returns list of order dicts.
    """
    if headers is None:
        headers = get_headers()

    all_orders = []
    page = 1
    per_page = 200

    while True:
        url = f"{API_BASE}/orders?per_page={per_page}&page={page}&from={date_from}&to={date_to}"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"  Network error on page {page}: {e}")
            break

        if resp.status_code == 401:
            raise PermissionError("Shiprocket token expired. Please refresh it.")
        if resp.status_code == 503:
            print(f"  503 on page {page}, retrying...")
            import time
            time.sleep(5)
            continue
        if resp.status_code != 200:
            print(f"  API error {resp.status_code} on page {page}")
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

        # Pagination check
        meta = data.get("meta", {}) if isinstance(data, dict) else {}
        pagination = meta.get("pagination", {})
        total_pages = pagination.get("total_pages", 0)

        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            last_page = data["data"].get("last_page", 0)
            if last_page and page >= last_page:
                break

        if total_pages and page >= total_pages:
            break
        if len(orders) < per_page:
            break

        page += 1

    return all_orders

def process_orders(orders):
    """
    Process raw Shiprocket orders into per-product MIS dict.
    Returns: { "V1": {total_orders, shipped, delivered, rto, ..., revenue, freight}, ... }
    """
    products = defaultdict(lambda: {
        "total_orders": 0, "shipped": 0, "delivered": 0, "rto": 0,
        "in_transit": 0, "cancelled": 0, "lost": 0, "revenue": 0, "freight": 0
    })
    seen = set()
    skipped = {"spare": 0, "unmapped": 0, "reverse": 0, "qty": 0}

    for order in orders:
        order_id = str(order.get("id", order.get("order_id", "")))
        channel_order_id = str(order.get("channel_order_id", ""))
        status_raw = order.get("status", order.get("status_code", ""))
        is_reverse = order.get("is_reverse", 0)

        # Skip reverse orders
        if is_reverse or str(is_reverse).lower() in ("yes", "1", "true"):
            skipped["reverse"] += 1
            continue

        # Skip non-CUSTOM channels? No — include all D2C
        channel = order.get("channel_name", order.get("channel", ""))
        if channel and channel.upper() == "CUSTOM":
            continue

        status = classify_status(status_raw)
        if status == "skip" or status == "unknown":
            continue

        # Get products from order
        order_products = order.get("products", order.get("order_items", []))
        if not order_products:
            pname = order.get("product_name", "")
            if pname:
                order_products = [{
                    "name": pname,
                    "price": order.get("product_price", order.get("price", 0)),
                    "discount": order.get("discount", 0),
                    "quantity": order.get("product_quantity", order.get("quantity", 1)),
                }]

        # Get freight for this order
        order_freight = float(order.get("freight_charges",
                        order.get("shipping_charges",
                        order.get("freight", 0))) or 0)

        # Calculate total order value for proportional freight allocation
        line_values = []
        valid_products = []
        for prod in order_products:
            pname = prod.get("name", prod.get("product_name", ""))
            if is_spare_part(pname):
                skipped["spare"] += 1
                continue

            price = float(prod.get("price", prod.get("selling_price",
                         prod.get("product_price", 0))) or 0)
            discount = float(prod.get("discount", 0) or 0)
            qty = int(prod.get("quantity", prod.get("product_quantity", 1)) or 1)

            if qty > 10:
                skipped["qty"] += 1
                continue

            category = classify_product(pname)
            if not category:
                skipped["unmapped"] += 1
                continue

            line_value = (price - discount) * qty
            line_values.append(line_value)
            valid_products.append((category, line_value, qty))

        total_order_value = sum(line_values)

        for category, line_value, qty in valid_products:
            dedup_key = (order_id, category)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            pd = products[category]
            pd["total_orders"] += 1

            # Freight allocation (proportional)
            if total_order_value > 0 and order_freight > 0:
                freight_share = (line_value / total_order_value) * order_freight
            else:
                freight_share = 0

            if status == "delivered":
                pd["delivered"] += 1
                pd["shipped"] += 1
                pd["revenue"] += line_value
                pd["freight"] += freight_share
            elif status == "rto":
                pd["rto"] += 1
                pd["shipped"] += 1
                pd["freight"] += freight_share
            elif status == "cancelled":
                pd["cancelled"] += 1
            elif status == "in_transit":
                pd["in_transit"] += 1
                pd["shipped"] += 1
                pd["freight"] += freight_share

    return dict(products), skipped

def fetch_and_process(date_from, date_to):
    """
    Full pipeline: fetch orders for date range → process → return MIS dict.
    """
    print(f"  Shiprocket: Fetching orders {date_from} to {date_to}...")
    orders = fetch_orders(date_from, date_to)
    print(f"  Shiprocket: {len(orders)} orders fetched")

    if not orders:
        return {}, {"spare": 0, "unmapped": 0, "reverse": 0, "qty": 0}

    products, skipped = process_orders(orders)
    total_revenue = sum(p["revenue"] for p in products.values())
    total_delivered = sum(p["delivered"] for p in products.values())
    print(f"  Shiprocket: {len(products)} products, {total_delivered} delivered, ₹{total_revenue:,.0f} revenue")
    return products, skipped


if __name__ == "__main__":
    # Test: fetch yesterday's orders
    from datetime import datetime, timedelta
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Testing Shiprocket fetch for {yesterday}")
    products, skipped = fetch_and_process(yesterday, yesterday)
    print(f"\nProducts: {len(products)}")
    for p, d in sorted(products.items(), key=lambda x: -x[1]["revenue"]):
        print(f"  {p}: {d['delivered']} delivered, ₹{d['revenue']:,.0f}")
    print(f"\nSkipped: {skipped}")
