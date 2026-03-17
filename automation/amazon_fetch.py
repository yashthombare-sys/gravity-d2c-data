"""
Fetch Amazon orders via SP-API for a given date range and process into MIS format.
Uses Orders API + Finances API for fee breakdown.
"""
import requests, json, time, os
from collections import defaultdict
from datetime import datetime, timedelta
from config import load_env, AMAZON_SKU_MAP, COGS_MAP

# Amazon SP-API endpoints (India marketplace)
MARKETPLACE_ID = "A21TJRUUN4KGV"  # Amazon.in
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com"  # India is in EU region


def get_access_token():
    """Exchange refresh token for access token."""
    env = load_env()
    client_id = env.get("AMAZON_CLIENT_ID", "")
    client_secret = env.get("AMAZON_CLIENT_SECRET", "")
    refresh_token = env.get("AMAZON_REFRESH_TOKEN", "")

    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Amazon SP-API credentials missing in .env")

    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }, timeout=15)

    if resp.status_code != 200:
        raise PermissionError(f"Amazon token refresh failed: {resp.status_code} {resp.text}")

    return resp.json()["access_token"]


def fetch_orders(date_from, date_to, access_token):
    """
    Fetch Amazon orders for date range using Orders API.
    date_from, date_to: YYYY-MM-DD strings.
    Returns list of order dicts with items.
    """
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }

    # Convert to ISO 8601 timestamps
    created_after = f"{date_from}T00:00:00Z"
    created_before = f"{date_to}T23:59:59Z"

    all_orders = []
    next_token = None

    while True:
        if next_token:
            url = f"{SP_API_BASE}/orders/v0/orders?NextToken={requests.utils.quote(next_token)}&MarketplaceIds={MARKETPLACE_ID}"
        else:
            url = (f"{SP_API_BASE}/orders/v0/orders"
                   f"?MarketplaceIds={MARKETPLACE_ID}"
                   f"&CreatedAfter={created_after}"
                   f"&CreatedBefore={created_before}"
                   f"&FulfillmentChannels=AFN"  # Amazon Fulfilled only
                   f"&MaxResultsPerPage=100")

        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"    Network error fetching orders: {e}")
            break

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("x-amz-rate-limit-reset", 2))
            print(f"    Rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            continue

        if resp.status_code != 200:
            print(f"    Orders API error {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json()
        orders = data.get("payload", {}).get("Orders", [])
        all_orders.extend(orders)

        next_token = data.get("payload", {}).get("NextToken")
        if not next_token:
            break

        time.sleep(0.5)  # Rate limit courtesy

    # Fetch order items for each order
    enriched = []
    for order in all_orders:
        order_id = order.get("AmazonOrderId", "")
        status = order.get("OrderStatus", "")

        # Get order items
        items_url = f"{SP_API_BASE}/orders/v0/orders/{order_id}/orderItems"
        try:
            resp = requests.get(items_url, headers=headers, timeout=15)
            if resp.status_code == 429:
                time.sleep(2)
                resp = requests.get(items_url, headers=headers, timeout=15)
            if resp.status_code == 200:
                items = resp.json().get("payload", {}).get("OrderItems", [])
                order["items"] = items
            else:
                order["items"] = []
        except requests.exceptions.RequestException:
            order["items"] = []

        enriched.append(order)
        time.sleep(0.2)  # Rate limit

    return enriched


def fetch_financial_events(date_from, date_to, access_token):
    """
    Fetch financial events (fees, refunds) for date range.
    Returns dict: { order_id: { product: {commission, fba, closing, refund, ...} } }
    """
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
    }

    posted_after = f"{date_from}T00:00:00Z"
    posted_before = f"{date_to}T23:59:59Z"

    fees_by_order = defaultdict(lambda: defaultdict(lambda: {
        "commission": 0, "fba_fee": 0, "closing_fee": 0,
        "shipping_fee": 0, "refund_amount": 0, "promo": 0, "tcs": 0
    }))

    next_token = None
    page = 0

    while True:
        if next_token:
            url = f"{SP_API_BASE}/finances/v0/financialEvents?NextToken={requests.utils.quote(next_token)}"
        else:
            url = (f"{SP_API_BASE}/finances/v0/financialEvents"
                   f"?PostedAfter={posted_after}"
                   f"&PostedBefore={posted_before}"
                   f"&MaxResultsPerPage=100")

        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"    Finance API error: {e}")
            break

        if resp.status_code == 429:
            time.sleep(3)
            continue

        if resp.status_code != 200:
            print(f"    Finance API {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json()
        events = data.get("payload", {}).get("FinancialEvents", {})

        # Process shipment events (fees on orders)
        for event in events.get("ShipmentEventList", []):
            for item in event.get("ShipmentItemList", []):
                sku = item.get("SellerSKU", "")
                product = AMAZON_SKU_MAP.get(sku, sku)
                order_id = event.get("AmazonOrderId", "")

                fd = fees_by_order[order_id][product]

                for fee in item.get("ItemFeeList", []):
                    fee_type = fee.get("FeeType", "")
                    amount = float(fee.get("FeeAmount", {}).get("CurrencyAmount", 0))
                    if "Commission" in fee_type:
                        fd["commission"] += abs(amount)
                    elif "FBA" in fee_type or "Fulfilment" in fee_type:
                        fd["fba_fee"] += abs(amount)
                    elif "ClosingFee" in fee_type or "closing" in fee_type.lower():
                        fd["closing_fee"] += abs(amount)
                    elif "Shipping" in fee_type:
                        fd["shipping_fee"] += abs(amount)

                for promo in item.get("PromotionList", []):
                    amount = float(promo.get("PromotionAmount", {}).get("CurrencyAmount", 0))
                    fd["promo"] += abs(amount)

        # Process refund events
        for event in events.get("RefundEventList", []):
            for item in event.get("ShipmentItemAdjustmentList", []):
                sku = item.get("SellerSKU", "")
                product = AMAZON_SKU_MAP.get(sku, sku)
                order_id = event.get("AmazonOrderId", "")

                for charge in item.get("ItemChargeAdjustmentList", []):
                    amount = float(charge.get("ChargeAmount", {}).get("CurrencyAmount", 0))
                    fees_by_order[order_id][product]["refund_amount"] += abs(amount)

        next_token = data.get("payload", {}).get("NextToken")
        if not next_token:
            break

        page += 1
        time.sleep(0.5)

    return dict(fees_by_order)


def process_orders(orders, fees_by_order=None):
    """
    Process Amazon orders into per-product MIS dict.
    Returns: { "V1": {total_orders, shipped, delivered, cancelled, revenue, freight}, ... }
    """
    products = defaultdict(lambda: {
        "total_orders": 0, "shipped": 0, "delivered": 0, "rto": 0,
        "in_transit": 0, "cancelled": 0, "lost": 0, "revenue": 0, "freight": 0
    })

    for order in orders:
        order_id = order.get("AmazonOrderId", "")
        status = order.get("OrderStatus", "")

        for item in order.get("items", []):
            sku = item.get("SellerSKU", "")
            product = AMAZON_SKU_MAP.get(sku)

            # Try product name if SKU not mapped
            if not product:
                from config import classify_product
                product = classify_product(item.get("Title", ""))
            if not product:
                continue

            qty = int(item.get("QuantityOrdered", 1))
            price_info = item.get("ItemPrice", {})
            price = float(price_info.get("Amount", 0)) if price_info else 0
            promo_info = item.get("PromotionDiscount", {})
            promo = float(promo_info.get("Amount", 0)) if promo_info else 0

            pd = products[product]
            pd["total_orders"] += qty

            if status in ("Shipped", "Unshipped", "PartiallyShipped"):
                pd["shipped"] += qty
                pd["delivered"] += qty  # FBA = shipped ≈ delivered
                pd["revenue"] += price - promo
            elif status == "Canceled" or status == "Cancelled":
                pd["cancelled"] += qty

            # Add fees from financial events
            if fees_by_order and order_id in fees_by_order:
                fee_data = fees_by_order[order_id].get(product, {})
                total_fees = (fee_data.get("commission", 0) +
                             fee_data.get("fba_fee", 0) +
                             fee_data.get("closing_fee", 0) +
                             fee_data.get("shipping_fee", 0) +
                             fee_data.get("refund_amount", 0))
                pd["freight"] += total_fees

    return dict(products)


def fetch_and_process(date_from, date_to):
    """
    Full pipeline: authenticate → fetch orders + fees → process → return MIS dict.
    """
    print(f"  Amazon: Authenticating...")
    try:
        access_token = get_access_token()
    except Exception as e:
        print(f"  Amazon: Auth failed — {e}")
        return {}, {}

    print(f"  Amazon: Fetching orders {date_from} to {date_to}...")
    orders = fetch_orders(date_from, date_to, access_token)
    print(f"  Amazon: {len(orders)} orders fetched")

    print(f"  Amazon: Fetching financial events...")
    fees = fetch_financial_events(date_from, date_to, access_token)
    print(f"  Amazon: {len(fees)} order fee records")

    if not orders:
        return {}, fees

    products = process_orders(orders, fees)
    total_revenue = sum(p["revenue"] for p in products.values())
    total_delivered = sum(p["delivered"] for p in products.values())
    print(f"  Amazon: {len(products)} products, {total_delivered} delivered, ₹{total_revenue:,.0f} revenue")
    return products, fees


if __name__ == "__main__":
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Testing Amazon fetch for {yesterday}")
    products, _ = fetch_and_process(yesterday, yesterday)
    print(f"\nProducts: {len(products)}")
    for p, d in sorted(products.items(), key=lambda x: -x[1]["revenue"]):
        print(f"  {p}: {d['delivered']} delivered, ₹{d['revenue']:,.0f}")
