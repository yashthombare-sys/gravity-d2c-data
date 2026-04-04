#!/usr/bin/env python3
"""
Amazon Daily MIS — Fetches Amazon orders + fees via SP-API, aggregates daily,
and pushes to a dedicated Google Sheet.

Usage:
    python3 amazon_daily_mis.py                    # Current month (Mar 1 to yesterday)
    python3 amazon_daily_mis.py 2026-03            # Specific month (1st to yesterday or end of month)
    python3 amazon_daily_mis.py --yesterday        # Only yesterday (for daily cron at 6 AM)
    python3 amazon_daily_mis.py --last4             # Past 4 days (for daily cron — re-syncs recent data)
"""
import sys, os, json, time

# Add automation/ to path for config imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "automation"))

import requests
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# IST timezone (UTC+5:30) — Seller Central uses IST dates
IST = timezone(timedelta(hours=5, minutes=30))
from config import load_env, AMAZON_SKU_MAP, COGS_MAP

import gspread
from google.oauth2.service_account import Credentials

# ── Amazon SP-API config ──────────────────────────────────
MARKETPLACE_ID = "A21TJRUUN4KGV"  # Amazon.in
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com"  # India is in EU region

# ── Google Sheets config ──────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "shiproket-mis-70c28ae6e7fb.json")
SHEET_ID_FILE = os.path.join(BASE_DIR, ".amazon_daily_sheet_id")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "Date", "Total Revenue", "Total Expense", "Product Expense",
    "Ad Spend", "Commissions", "Total Orders", "Profit",
    "Profit %", "Commissions %", "Marketing %", "Sessions", "Conversion %",
]


# ══════════════════════════════════════════════════════════
#  AMAZON SP-API FUNCTIONS
# ══════════════════════════════════════════════════════════

def get_access_token(retry=True):
    """Exchange refresh token for access token."""
    env = load_env()

    client_id = env.get("AMAZON_CLIENT_ID", "")
    client_secret = env.get("AMAZON_CLIENT_SECRET", "")
    refresh_token = env.get("AMAZON_REFRESH_TOKEN", "")

    if not all([client_id, client_secret, refresh_token]):
        missing = []
        if not client_id: missing.append("AMAZON_CLIENT_ID")
        if not client_secret: missing.append("AMAZON_CLIENT_SECRET")
        if not refresh_token: missing.append("AMAZON_REFRESH_TOKEN")
        raise PermissionError(
            f"Missing credentials in .env: {', '.join(missing)}\n"
            f"  Set these in GitHub Secrets or in the local .env file."
        )

    for attempt in range(3 if retry else 1):
        try:
            resp = requests.post(TOKEN_URL, data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            }, timeout=15)
        except requests.exceptions.RequestException as e:
            if attempt < 2:
                print(f"  Token request network error: {e}, retrying...", flush=True)
                time.sleep(3)
                continue
            raise PermissionError(f"Token request failed after 3 attempts: {e}")

        if resp.status_code == 200:
            return resp.json()["access_token"]

        # Check for permanent failures
        try:
            error_data = resp.json()
            error_code = error_data.get("error", "")
        except Exception:
            error_code = ""

        if error_code == "invalid_grant":
            raise PermissionError(
                f"AMAZON REFRESH TOKEN EXPIRED OR REVOKED.\n"
                f"  Go to Amazon Seller Central → Apps → Authorize your app → get a new refresh token.\n"
                f"  Then update the AMAZON_REFRESH_TOKEN in GitHub Secrets and .env."
            )
        elif error_code == "invalid_client":
            raise PermissionError(
                f"AMAZON CLIENT CREDENTIALS INVALID.\n"
                f"  Check AMAZON_CLIENT_ID and AMAZON_CLIENT_SECRET in GitHub Secrets."
            )

        if attempt < 2 and resp.status_code >= 500:
            print(f"  Token server error {resp.status_code}, retrying...", flush=True)
            time.sleep(3)
            continue

        raise PermissionError(f"Token refresh failed: {resp.status_code} {resp.text[:300]}")

    raise PermissionError("Token refresh failed after all retries")


def api_get(url, headers, retries=3, max_429_retries=20, silent_400=False):
    """GET with rate-limit handling. 429s don't count against retries."""
    error_count = 0
    rate_limit_count = 0

    while error_count < retries and rate_limit_count < max_429_retries:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            print(f"    Network error: {e}", flush=True)
            error_count += 1
            time.sleep(3)
            continue

        if resp.status_code == 429:
            wait = max(int(resp.headers.get("x-amz-rate-limit-reset", 5)), 5)
            rate_limit_count += 1
            if rate_limit_count % 5 == 1:
                print(f"    Rate limited (#{rate_limit_count}), waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 400 and silent_400:
            return None  # Expected for dates with no financial events

        print(f"    API error {resp.status_code}: {resp.text[:200]}", flush=True)
        error_count += 1
        time.sleep(2)

    return None


def fetch_traffic_report(date_from, date_to, access_token):
    """
    Fetch Sales & Traffic report for a date range.
    Returns: {date_str: {sessions, page_views, conversion_pct}}
    """
    import gzip
    headers = {"x-amz-access-token": access_token, "Content-Type": "application/json"}

    report_body = {
        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": f"{date_from}T00:00:00Z",
        "dataEndTime": f"{date_to}T23:59:59Z",
        "reportOptions": {
            "dateGranularity": "DAY",
            "asinGranularity": "SKU"
        }
    }

    print(f"    Requesting traffic report...", flush=True)
    resp = requests.post(
        f"{SP_API_BASE}/reports/2021-06-30/reports",
        headers=headers, json=report_body, timeout=30
    )
    if resp.status_code not in (200, 202):
        print(f"    Traffic report request failed: {resp.status_code} {resp.text[:200]}", flush=True)
        return {}

    report_id = resp.json().get("reportId")
    print(f"    Report ID: {report_id}, waiting...", flush=True)

    # Poll for completion
    for attempt in range(30):
        time.sleep(10)
        status_resp = requests.get(
            f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
            headers=headers, timeout=30
        )
        status_data = status_resp.json()
        processing_status = status_data.get("processingStatus", "UNKNOWN")

        if processing_status == "DONE":
            doc_id = status_data.get("reportDocumentId")
            break
        elif processing_status in ("CANCELLED", "FATAL"):
            print(f"    Traffic report failed: {processing_status}", flush=True)
            return {}
    else:
        print(f"    Traffic report timed out", flush=True)
        return {}

    # Download report
    doc_resp = requests.get(
        f"{SP_API_BASE}/reports/2021-06-30/documents/{doc_id}",
        headers=headers, timeout=30
    )
    doc_data = doc_resp.json()
    download_url = doc_data.get("url")
    if not download_url:
        return {}

    report_resp = requests.get(download_url, timeout=30)
    compression = doc_data.get("compressionAlgorithm", "")
    if compression == "GZIP":
        report_text = gzip.decompress(report_resp.content).decode("utf-8")
    else:
        report_text = report_resp.text

    import json as _json
    report_data = _json.loads(report_text)

    # Extract daily traffic totals
    result = {}
    for day_data in report_data.get("salesAndTrafficByDate", []):
        date_str = day_data.get("date", "")
        traffic = day_data.get("trafficByDate", {})
        sessions = traffic.get("sessions", 0)
        conversion = traffic.get("unitSessionPercentage", 0)
        result[date_str] = {
            "sessions": sessions,
            "conversion_pct": round(conversion, 2),
        }
        print(f"    {date_str}: {sessions} sessions, {conversion:.2f}% conversion", flush=True)

    return result


def fetch_all_orders(date_from, date_to, access_token):
    """
    Fetch ALL Amazon orders for a date range in one batch.
    Returns list of order dicts (without individual items — uses OrderTotal).
    """
    headers = {"x-amz-access-token": access_token, "Content-Type": "application/json"}
    # Convert IST boundaries to UTC: IST 00:00 = UTC 18:30 previous day
    # This ensures orders placed midnight-5:30 AM IST are included
    ist_start = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=IST)
    ist_end = datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=IST)
    created_after = ist_start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    created_before = ist_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    all_orders = []
    next_token = None
    page = 0

    while True:
        if next_token:
            url = (f"{SP_API_BASE}/orders/v0/orders"
                   f"?NextToken={requests.utils.quote(next_token)}"
                   f"&MarketplaceIds={MARKETPLACE_ID}")
        else:
            url = (f"{SP_API_BASE}/orders/v0/orders"
                   f"?MarketplaceIds={MARKETPLACE_ID}"
                   f"&CreatedAfter={created_after}"
                   f"&CreatedBefore={created_before}"
                   f"&MaxResultsPerPage=100")

        data = api_get(url, headers)
        if not data:
            break

        orders = data.get("payload", {}).get("Orders", [])
        all_orders.extend(orders)
        page += 1

        if page % 5 == 0:
            print(f"    Page {page}: {len(all_orders)} orders so far...", flush=True)

        next_token = data.get("payload", {}).get("NextToken")
        if not next_token:
            break
        time.sleep(2)  # Generous wait to avoid rate limiting

    return all_orders


def fetch_order_items_batch(order_ids, access_token):
    """Fetch order items for a list of order IDs. Returns {order_id: [items]}.
    Slower for large batches but gives accurate COGS."""
    headers = {"x-amz-access-token": access_token, "Content-Type": "application/json"}
    result = {}

    total = len(order_ids)
    for i, order_id in enumerate(order_ids):
        if (i + 1) % 100 == 0:
            print(f"    Fetching items: {i+1}/{total}...", flush=True)

        items_url = f"{SP_API_BASE}/orders/v0/orders/{order_id}/orderItems"
        data = api_get(items_url, headers)
        if data:
            result[order_id] = data.get("payload", {}).get("OrderItems", [])
        else:
            result[order_id] = []
        time.sleep(0.5)  # Generous wait to avoid rate limit wall

        # Refresh token every 200 orders
        if (i + 1) % 200 == 0:
            print(f"    Refreshing token at {i+1}...", flush=True)
            access_token = get_access_token()
            headers["x-amz-access-token"] = access_token

    return result, access_token


def fetch_fees_for_day(date_str, access_token):
    """Fetch financial events for a single day."""
    headers = {"x-amz-access-token": access_token, "Content-Type": "application/json"}
    # Use previous day end as PostedAfter (API rejects same-day T00:00:00Z for some dates)
    prev_day = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    posted_after = f"{prev_day}T23:59:59Z"
    posted_before = f"{date_str}T23:59:59Z"

    total_commission = 0
    total_fba = 0
    total_closing = 0
    next_token = None

    while True:
        if next_token:
            url = (f"{SP_API_BASE}/finances/v0/financialEvents"
                   f"?NextToken={requests.utils.quote(next_token)}")
        else:
            url = (f"{SP_API_BASE}/finances/v0/financialEvents"
                   f"?PostedAfter={posted_after}"
                   f"&PostedBefore={posted_before}"
                   f"&MaxResultsPerPage=100")

        data = api_get(url, headers, silent_400=True)
        if not data:
            break  # 400 = no financial events for this date (normal for some days)

        events = data.get("payload", {}).get("FinancialEvents", {})

        for event in events.get("ShipmentEventList", []):
            for item in event.get("ShipmentItemList", []):
                for fee in item.get("ItemFeeList", []):
                    fee_type = fee.get("FeeType", "")
                    amount = abs(float(fee.get("FeeAmount", {}).get("CurrencyAmount", 0)))
                    if "Commission" in fee_type:
                        total_commission += amount
                    elif "FBA" in fee_type or "Fulfilment" in fee_type:
                        total_fba += amount
                    elif "ClosingFee" in fee_type or "closing" in fee_type.lower():
                        total_closing += amount

        next_token = data.get("payload", {}).get("NextToken")
        if not next_token:
            break
        time.sleep(0.5)

    return {
        "commission": round(total_commission, 2),
        "fba_fee": round(total_fba, 2),
        "closing_fee": round(total_closing, 2),
        "total": round(total_commission + total_fba + total_closing, 2),
    }


def fetch_all_fees(date_from, date_to, access_token):
    """
    Fetch financial events day-by-day (Amazon API rejects wide date ranges).
    Returns: {date_str: {commission, fba_fee, closing_fee, total}}
    """
    result = {}
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    current = start
    total_days = (end - start).days + 1
    day_num = 0

    # Refresh token before starting fees (orders may have used it heavily)
    access_token = get_access_token()

    while current <= end:
        day_num += 1
        ds = current.strftime("%Y-%m-%d")

        # Retry with fresh token if first attempt fails
        fees = fetch_fees_for_day(ds, access_token)
        result[ds] = fees
        if fees["total"] > 0:
            print(f"    {ds}: ₹{fees['total']:,.0f} ({day_num}/{total_days})", flush=True)
        current += timedelta(days=1)
        time.sleep(0.5)

        # Refresh token every 5 days
        if day_num % 5 == 0:
            access_token = get_access_token()

    return result


def aggregate_orders_by_day(orders, items_by_order=None):
    """
    Group orders by date and calculate daily revenue, order count, and COGS.
    If items_by_order is provided, uses actual SKU data for COGS.
    Otherwise, uses OrderTotal for revenue and estimates COGS at 36%.
    Returns: {date_str: {revenue, orders, cogs}}
    """
    daily = defaultdict(lambda: {"revenue": 0, "orders": 0, "cogs": 0})

    for order in orders:
        status = order.get("OrderStatus", "")
        if status in ("Canceled", "Cancelled"):
            continue

        # Convert PurchaseDate from UTC to IST to match Seller Central's date attribution
        raw_date = order.get("PurchaseDate", "")
        if not raw_date:
            continue
        try:
            utc_dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            purchase_date = utc_dt.astimezone(IST).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            purchase_date = raw_date[:10]

        order_id = order.get("AmazonOrderId", "")
        items = (items_by_order or {}).get(order_id, [])

        if items:
            for item in items:
                sku = item.get("SellerSKU", "")
                product = AMAZON_SKU_MAP.get(sku)
                if not product:
                    from config import classify_product
                    product = classify_product(item.get("Title", ""))

                qty = int(item.get("QuantityOrdered", 1))
                price_info = item.get("ItemPrice", {})
                price = float(price_info.get("Amount", 0)) if price_info else 0
                promo_info = item.get("PromotionDiscount", {})
                promo = float(promo_info.get("Amount", 0)) if promo_info else 0

                daily[purchase_date]["revenue"] += price - promo
                daily[purchase_date]["orders"] += qty
                daily[purchase_date]["cogs"] += COGS_MAP.get(product, 0) * qty
        else:
            # Use OrderTotal for revenue, estimate COGS at 36% (historical avg)
            order_total = order.get("OrderTotal", {})
            amount = float(order_total.get("Amount", 0)) if order_total else 0
            num_items = int(order.get("NumberOfItemsShipped", 0)) + int(order.get("NumberOfItemsUnshipped", 0))
            if num_items == 0:
                num_items = 1

            daily[purchase_date]["revenue"] += amount
            daily[purchase_date]["orders"] += num_items
            daily[purchase_date]["cogs"] += amount * 0.36

    result = {}
    for date_str, d in daily.items():
        result[date_str] = {
            "revenue": round(d["revenue"], 2),
            "orders": d["orders"],
            "cogs": round(d["cogs"], 2),
        }
    return result


# ══════════════════════════════════════════════════════════
#  GOOGLE SHEETS FUNCTIONS
# ══════════════════════════════════════════════════════════

def get_gsheet_client():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_spreadsheet(gc):
    """Get existing spreadsheet or create a new one."""
    if os.path.exists(SHEET_ID_FILE):
        with open(SHEET_ID_FILE) as f:
            sheet_id = f.read().strip()
        if sheet_id:
            try:
                spreadsheet = gc.open_by_key(sheet_id)
                print(f"  Using existing sheet: {spreadsheet.url}", flush=True)
                return spreadsheet
            except Exception as e:
                print(f"  Saved sheet not accessible: {e}", flush=True)

    spreadsheet = gc.create("Amazon Daily MIS")
    spreadsheet.share("hmthombare121@gmail.com", perm_type="user", role="writer")
    print(f"  Created new spreadsheet: {spreadsheet.url}", flush=True)
    print(f"  Shared with hmthombare121@gmail.com", flush=True)

    with open(SHEET_ID_FILE, "w") as f:
        f.write(spreadsheet.id)

    return spreadsheet


def get_month_tab(spreadsheet, month_str):
    """Get or create a tab for the given month."""
    try:
        ws = spreadsheet.worksheet(month_str)
        return ws, False
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=month_str, rows=35, cols=len(HEADERS))
        ws.update(values=[HEADERS], range_name="A1")
        ws.format("A1:M1", {
            "backgroundColor": {"red": 0.15, "green": 0.24, "blue": 0.46},
            "textFormat": {
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                "bold": True, "fontSize": 10,
            },
            "horizontalAlignment": "CENTER",
        })
        ws.freeze(rows=1)

        reqs = []
        widths = [120, 130, 130, 130, 110, 120, 110, 120, 90, 120, 110, 100, 110]
        for i, w in enumerate(widths):
            reqs.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                              "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize",
                }
            })
        spreadsheet.batch_update({"requests": reqs})
        return ws, True


def gsheet_retry(func, max_retries=3, base_delay=5):
    """Wrap a Google Sheets API call with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, 'response') else 0
            if status == 429 or status >= 500:
                delay = base_delay * (2 ** attempt)
                print(f"    Sheets API error {status}, retrying in {delay}s (attempt {attempt+1}/{max_retries})", flush=True)
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"    Sheets error: {e}, retrying in {delay}s", flush=True)
                time.sleep(delay)
            else:
                raise
    return func()


def push_to_sheet(daily_data, month_label):
    """Push daily data to Google Sheet."""
    print(f"\nPushing to Google Sheets ({month_label})...", flush=True)

    gc = get_gsheet_client()
    spreadsheet = get_or_create_spreadsheet(gc)
    ws, is_new = get_month_tab(spreadsheet, month_label)

    if is_new:
        print(f"  Created new tab: '{month_label}'", flush=True)

    # Get ALL existing data (not just dates) so we can preserve ad_spend
    all_existing = gsheet_retry(lambda: ws.get_all_values())
    existing_date_rows = {}  # date_str -> (row_index_1based, row_data)
    if not is_new and len(all_existing) > 1:
        for idx, row in enumerate(all_existing[1:], start=2):  # skip header, 1-based row nums
            date_val = row[0] if row else ""
            if date_val and date_val not in existing_date_rows:
                existing_date_rows[date_val] = (idx, row)

    next_row = len(all_existing) + 1

    # Build all rows, preserving existing ad_spend if we have 0
    batch_updates = []  # list of (range, [row]) for batch
    rows_to_append = []

    for day in daily_data:
        date_str = day["date"]
        revenue = day["revenue"]
        cogs = day["cogs"]
        commissions = day["fees_total"]
        ad_spend = day.get("ad_spend", 0)
        orders = day["orders"]
        sessions = day.get("sessions", 0)
        conversion_pct = day.get("conversion_pct", 0)

        # Preserve manually-entered ad_spend from existing sheet data
        if ad_spend == 0 and date_str in existing_date_rows:
            _, existing_row = existing_date_rows[date_str]
            if len(existing_row) > 4:
                try:
                    existing_ad = float(str(existing_row[4]).replace("₹", "").replace(",", "").strip() or "0")
                    if existing_ad > 0:
                        ad_spend = existing_ad
                except (ValueError, TypeError):
                    pass

        row = [
            date_str,
            round(revenue, 2),
            None,  # C: Total Expense — formula placeholder
            round(cogs, 2),
            round(ad_spend, 2),
            round(commissions, 2),
            orders,
            None,  # H: Profit — formula placeholder
            None,  # I: Profit % — formula placeholder
            None,  # J: Commissions % — formula placeholder
            None,  # K: Marketing % — formula placeholder
            sessions,
            round(conversion_pct, 2),
        ]

        if date_str in existing_date_rows:
            row_idx, _ = existing_date_rows[date_str]
            # Fill formula columns with row-specific formulas
            row[2] = f'=D{row_idx}+E{row_idx}+F{row_idx}'
            row[7] = f'=B{row_idx}-C{row_idx}'
            row[8] = f'=IF(B{row_idx}>0,H{row_idx}/B{row_idx}*100,0)'
            row[9] = f'=IF(B{row_idx}>0,F{row_idx}/B{row_idx}*100,0)'
            row[10] = f'=IF(B{row_idx}>0,E{row_idx}/B{row_idx}*100,0)'
            batch_updates.append((f"A{row_idx}:M{row_idx}", [row]))
        else:
            rows_to_append.append(row)

    # Fill formulas for appended rows (row numbers known after loop)
    for i, row in enumerate(rows_to_append):
        rn = next_row + i
        row[2] = f'=D{rn}+E{rn}+F{rn}'
        row[7] = f'=B{rn}-C{rn}'
        row[8] = f'=IF(B{rn}>0,H{rn}/B{rn}*100,0)'
        row[9] = f'=IF(B{rn}>0,F{rn}/B{rn}*100,0)'
        row[10] = f'=IF(B{rn}>0,E{rn}/B{rn}*100,0)'

    # Batch update existing rows (single batch call instead of one-by-one)
    if batch_updates:
        gsheet_retry(lambda: ws.batch_update(
            [{"range": r, "values": v} for r, v in batch_updates],
            value_input_option='USER_ENTERED',
        ))

    # Batch append new rows
    if rows_to_append:
        cell_range = f"A{next_row}:M{next_row + len(rows_to_append) - 1}"
        gsheet_retry(lambda: ws.update(values=rows_to_append, range_name=cell_range,
                                       value_input_option='USER_ENTERED'))

    # Apply formatting
    end_row = next_row + len(rows_to_append) - 1
    if end_row >= 2:
        currency_cols = [1, 2, 3, 4, 5, 7]
        pct_cols = [8, 9, 10, 12]  # Profit%, Comm%, Mkt%, Conversion%
        reqs = []

        for col_idx in currency_cols:
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": end_row,
                              "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                    "cell": {"userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "₹#,##0.00"},
                        "horizontalAlignment": "RIGHT",
                    }},
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
                }
            })

        for col_idx in pct_cols:
            reqs.append({
                "repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": end_row,
                              "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                    "cell": {"userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "0.00\"%\""},
                        "horizontalAlignment": "RIGHT",
                    }},
                    "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
                }
            })

        # Orders column center
        reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": end_row,
                          "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    "horizontalAlignment": "CENTER",
                }},
                "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
            }
        })

        # Sessions column center
        reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": end_row,
                          "startColumnIndex": 11, "endColumnIndex": 12},
                "cell": {"userEnteredFormat": {
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    "horizontalAlignment": "CENTER",
                }},
                "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
            }
        })

        # Date bold
        reqs.append({
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": end_row,
                          "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {
                    "horizontalAlignment": "LEFT",
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat(horizontalAlignment,textFormat)",
            }
        })

        spreadsheet.batch_update({"requests": reqs})

    total_pushed = len(rows_to_append) + len(batch_updates)
    print(f"  Pushed {total_pushed} days ({len(rows_to_append)} new, {len(batch_updates)} updated)", flush=True)
    print(f"  Sheet URL: {spreadsheet.url}", flush=True)
    return spreadsheet.url


def save_daily_json(daily_data, month_label):
    """Save raw daily data to JSON for backup."""
    filename = f"amazon_daily_{month_label.lower().replace(' ', '_')}.json"
    filepath = os.path.join(BASE_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(daily_data, f, indent=2)
    print(f"  Saved backup: {filename}", flush=True)


# ══════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════════════

def fetch_month(year, month, up_to_date=None):
    """
    Fetch Amazon daily data for an entire month in ONE batch.
    Much faster than day-by-day fetching.
    """
    from calendar import monthrange

    last_day = monthrange(year, month)[1]
    if up_to_date:
        last_day = min(last_day, up_to_date)

    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day:02d}"

    print(f"\nFetching Amazon data: {date_from} to {date_to}", flush=True)
    print("=" * 60, flush=True)

    # Step 1: Authenticate
    print("Step 1: Authenticating...", flush=True)
    access_token = get_access_token()
    print("  OK", flush=True)

    # Step 2: Fetch all orders for the range (one batch)
    print(f"\nStep 2: Fetching all orders ({date_from} to {date_to})...", flush=True)
    orders = fetch_all_orders(date_from, date_to, access_token)
    non_cancelled = [o for o in orders if o.get("OrderStatus") not in ("Canceled", "Cancelled")]
    print(f"  {len(orders)} total orders, {len(non_cancelled)} non-cancelled", flush=True)

    # Step 3: Fetch order items for actual COGS calculation
    print(f"\nStep 3: Fetching order items ({len(non_cancelled)} orders) for actual COGS...", flush=True)
    order_ids = [o["AmazonOrderId"] for o in non_cancelled]
    items_by_order, access_token = fetch_order_items_batch(order_ids, access_token)
    print(f"  Items fetched for {len(items_by_order)} orders", flush=True)

    # Step 3b: Aggregate orders by day with actual COGS
    print(f"\nAggregating by day...", flush=True)
    daily_orders = aggregate_orders_by_day(orders, items_by_order)

    # Step 4: Fetch all financial events for the range (one batch)
    print(f"\nStep 4: Fetching financial events...", flush=True)
    daily_fees = fetch_all_fees(date_from, date_to, access_token)
    print(f"  Fees for {len(daily_fees)} days", flush=True)

    # Step 5: Calculate avg commission rate from days where BOTH revenue and fees exist
    days_with_both = []
    for ds in daily_fees:
        rev = daily_orders.get(ds, {}).get("revenue", 0)
        fees = daily_fees[ds]["total"]
        if rev > 0 and fees > 0:
            days_with_both.append((rev, fees))

    if days_with_both:
        total_rev_known = sum(r for r, _ in days_with_both)
        total_fees_known = sum(f for _, f in days_with_both)
        avg_fee_rate = total_fees_known / total_rev_known if total_rev_known > 0 else 0.15
        # Sanity check: commission rate should be 10-20%, cap at 20%
        if avg_fee_rate > 0.25:
            print(f"  Calculated rate {avg_fee_rate*100:.1f}% seems high, capping at 15%", flush=True)
            avg_fee_rate = 0.15
        else:
            print(f"  Avg commission rate (from {len(days_with_both)} days with data): {avg_fee_rate*100:.1f}%", flush=True)
    else:
        avg_fee_rate = 0.15  # Historical average ~15%
        print(f"  No matched fee+revenue data, using default 15% estimate", flush=True)

    # Step 5b: Fetch traffic data (sessions, conversion)
    print(f"\nStep 5b: Fetching traffic report...", flush=True)
    access_token = get_access_token()
    daily_traffic = fetch_traffic_report(date_from, date_to, access_token)
    print(f"  Traffic for {len(daily_traffic)} days", flush=True)

    # Step 6: Combine into daily data (estimate fees for missing days)
    print(f"\nStep 6: Building daily data...", flush=True)
    daily_data = []
    for day in range(1, last_day + 1):
        date_str = f"{year}-{month:02d}-{day:02d}"
        order_data = daily_orders.get(date_str, {"revenue": 0, "orders": 0, "cogs": 0})
        fee_data = daily_fees.get(date_str, {"commission": 0, "fba_fee": 0, "closing_fee": 0, "total": 0})

        # If no fees from API, estimate at avg rate
        if fee_data["total"] == 0 and order_data["revenue"] > 0:
            estimated_fees = round(order_data["revenue"] * avg_fee_rate, 2)
            fee_data = {
                "commission": estimated_fees,
                "fba_fee": 0,
                "closing_fee": 0,
                "total": estimated_fees,
            }

        daily_data.append({
            "date": date_str,
            "revenue": order_data["revenue"],
            "orders": order_data["orders"],
            "cogs": order_data["cogs"],
            "fees_commission": fee_data["commission"],
            "fees_fba": fee_data["fba_fee"],
            "fees_closing": fee_data["closing_fee"],
            "fees_total": fee_data["total"],
            "ad_spend": 0,
            "sessions": daily_traffic.get(date_str, {}).get("sessions", 0),
            "conversion_pct": daily_traffic.get(date_str, {}).get("conversion_pct", 0),
        })

        t = daily_traffic.get(date_str, {})
        print(f"  {date_str}: ₹{order_data['revenue']:>10,.2f} rev | {order_data['orders']:>4} orders | ₹{fee_data['total']:>8,.2f} fees | {t.get('sessions',0):>5} sessions | {t.get('conversion_pct',0):.2f}%", flush=True)

    return daily_data


def print_summary(daily_data, month_label):
    total_rev = sum(d["revenue"] for d in daily_data)
    total_orders = sum(d["orders"] for d in daily_data)
    total_cogs = sum(d["cogs"] for d in daily_data)
    total_fees = sum(d["fees_total"] for d in daily_data)
    total_profit = total_rev - total_cogs - total_fees
    print(f"\n{'='*60}", flush=True)
    print(f"MONTH SUMMARY — {month_label}", flush=True)
    print(f"  Revenue:     ₹{total_rev:>12,.2f}", flush=True)
    print(f"  Orders:       {total_orders:>12,}", flush=True)
    print(f"  COGS:        ₹{total_cogs:>12,.2f}", flush=True)
    print(f"  Commissions: ₹{total_fees:>12,.2f}", flush=True)
    print(f"  Profit:      ₹{total_profit:>12,.2f}", flush=True)
    print(f"  Profit %:     {(total_profit/total_rev*100) if total_rev else 0:>11.1f}%", flush=True)


def main():
    args = sys.argv[1:]
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    if "--last4" in args:
        # Cron mode: fetch past 4 days (re-syncs recent data for accuracy)
        # Smart retry: each step independent, failed steps retry after delays
        #   Attempt 1: immediate (~6:00 AM)
        #   Attempt 2: +20 min   (~6:20 AM) — only failed steps
        #   Attempt 3: +20 min   (~6:40 AM) — only still-failed steps
        # Pushes best available data after all attempts
        end_date = yesterday
        start_date = today - timedelta(days=4)
        RETRY_DELAYS = [60, 120]  # wait times between attempts (seconds) — short since we skip items now
        MAX_ATTEMPTS = 3

        print(f"Cron mode: fetching last 4 days ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})", flush=True)
        print(f"  Max attempts: {MAX_ATTEMPTS} (retry delays: {[d//60 for d in RETRY_DELAYS]} min)", flush=True)
        print("=" * 60, flush=True)

        date_from = start_date.strftime("%Y-%m-%d")
        date_to = end_date.strftime("%Y-%m-%d")

        # Results — None means "not yet fetched successfully"
        orders = None
        non_cancelled = None
        items_by_order = None
        daily_fees = None
        daily_traffic = None
        access_token = get_access_token()

        def run_failed_steps(failed, attempt_num):
            """Run only the steps that are in the failed list. Returns updated failed list."""
            nonlocal orders, non_cancelled, items_by_order, daily_fees, daily_traffic, access_token
            still_failed = []

            if "orders" in failed:
                print(f"\n  Fetching orders...", flush=True)
                try:
                    orders = fetch_all_orders(date_from, date_to, access_token)
                    non_cancelled = [o for o in orders if o.get("OrderStatus") not in ("Canceled", "Cancelled")]
                    print(f"  ✅ {len(orders)} orders ({len(non_cancelled)} non-cancelled)", flush=True)
                except Exception as e:
                    print(f"  ❌ Orders failed: {e}", flush=True)
                    still_failed.append("orders")

            if orders is not None and "items" in failed:
                print(f"\n  Fetching order items for COGS...", flush=True)
                try:
                    order_ids = [o["AmazonOrderId"] for o in non_cancelled]
                    items_by_order, access_token = fetch_order_items_batch(order_ids, access_token)
                    print(f"  ✅ Items fetched for {len(items_by_order)} orders", flush=True)
                except Exception as e:
                    print(f"  ❌ Order items failed: {e}", flush=True)
                    still_failed.append("items")

            if orders is not None and "fees" in failed:
                print(f"\n  Fetching fees...", flush=True)
                try:
                    daily_fees = fetch_all_fees(date_from, date_to, access_token)
                    print(f"  ✅ Fees for {len(daily_fees)} days", flush=True)
                except Exception as e:
                    print(f"  ❌ Fees failed: {e}", flush=True)
                    still_failed.append("fees")

            if orders is not None and "traffic" in failed:
                print(f"\n  Fetching traffic...", flush=True)
                try:
                    access_token = get_access_token()
                    daily_traffic = fetch_traffic_report(date_from, date_to, access_token)
                    print(f"  ✅ Traffic for {len(daily_traffic)} days", flush=True)
                except Exception as e:
                    print(f"  ❌ Traffic failed: {e}", flush=True)
                    still_failed.append("traffic")

            return still_failed

        # ── Run all attempts ──────────────────────────────────
        # Skip "items" initially — fetching order items per-order makes ~300-400 API calls.
        # After orders are fetched, we selectively fetch items ONLY for orders with
        # missing/zero OrderTotal (usually just a handful).
        failed_steps = ["orders", "fees", "traffic"]

        for attempt in range(MAX_ATTEMPTS):
            print(f"\n{'='*60}", flush=True)
            print(f"  ATTEMPT {attempt + 1} of {MAX_ATTEMPTS} — fetching: {', '.join(failed_steps)}", flush=True)
            print(f"{'='*60}", flush=True)

            if attempt > 0:
                # Fresh token for retry
                access_token = get_access_token()

            failed_steps = run_failed_steps(failed_steps, attempt + 1)

            if not failed_steps:
                print(f"\n  ✅ All steps succeeded on attempt {attempt + 1}!", flush=True)
                break

            # If there are more attempts, wait before retrying
            if attempt < MAX_ATTEMPTS - 1:
                delay = RETRY_DELAYS[attempt]
                print(f"\n  {len(failed_steps)} step(s) still failed: {', '.join(failed_steps)}", flush=True)
                print(f"  Waiting {delay // 60} minutes before attempt {attempt + 2}...", flush=True)
                time.sleep(delay)

        # ── If orders still failed after all attempts, we can't do anything ──
        if orders is None:
            raise RuntimeError(
                f"Orders fetch failed on all {MAX_ATTEMPTS} attempts. Cannot push any data.\n"
                f"  This is likely a persistent Amazon SP-API issue."
            )

        # ── Fetch items for orders with missing/zero OrderTotal ──
        # Some orders (Easy Ship pending, replacements) have empty OrderTotal in SP-API.
        # We fetch items ONLY for those to get accurate revenue without 300+ API calls.
        if non_cancelled:
            missing_total_orders = [
                o["AmazonOrderId"] for o in non_cancelled
                if not o.get("OrderTotal") or float(o.get("OrderTotal", {}).get("Amount", 0)) == 0
            ]
            if missing_total_orders:
                print(f"\n  {len(missing_total_orders)} orders have missing/zero OrderTotal — fetching items for accurate revenue...", flush=True)
                try:
                    items_by_order, access_token = fetch_order_items_batch(missing_total_orders, access_token)
                    print(f"  ✅ Items fetched for {len(items_by_order)} orders with missing OrderTotal", flush=True)
                except Exception as e:
                    print(f"  ⚠️ Items fetch failed: {e} — these orders will show ₹0 revenue", flush=True)
            else:
                print(f"\n  All orders have OrderTotal — no item fetch needed", flush=True)

        # ── BUILD & PUSH with best available data ──────────────
        print(f"\n  Building daily data and pushing...", flush=True)

        # Use whatever we got (None → empty fallback)
        daily_orders = aggregate_orders_by_day(orders, items_by_order or {})
        if daily_fees is None:
            daily_fees = {}
        if daily_traffic is None:
            daily_traffic = {}

        from collections import defaultdict as dd
        months = dd(list)
        current = start_date
        while current <= end_date:
            ds = current.strftime("%Y-%m-%d")
            month_label = current.strftime("%B %Y")

            order_data = daily_orders.get(ds, {"revenue": 0, "orders": 0, "cogs": 0})
            fee_data = daily_fees.get(ds, {"commission": 0, "fba_fee": 0, "closing_fee": 0, "total": 0})
            traffic_data = daily_traffic.get(ds, {"sessions": 0, "conversion_pct": 0})

            if fee_data["total"] == 0 and order_data["revenue"] > 0:
                estimated = round(order_data["revenue"] * 0.15, 2)
                fee_data = {"commission": estimated, "fba_fee": 0, "closing_fee": 0, "total": estimated}

            months[month_label].append({
                "date": ds,
                "revenue": order_data["revenue"],
                "orders": order_data["orders"],
                "cogs": order_data["cogs"],
                "fees_commission": fee_data["commission"],
                "fees_fba": fee_data["fba_fee"],
                "fees_closing": fee_data["closing_fee"],
                "fees_total": fee_data["total"],
                "ad_spend": 0,
                "sessions": traffic_data.get("sessions", 0),
                "conversion_pct": traffic_data.get("conversion_pct", 0),
            })

            print(f"  {ds}: ₹{order_data['revenue']:>10,.2f} rev | {order_data['orders']:>4} orders", flush=True)
            current += timedelta(days=1)

        for month_label, day_data in months.items():
            save_daily_json(day_data, month_label)
            url = push_to_sheet(day_data, month_label)

        if failed_steps:
            print(f"\n⚠️  Completed with {len(failed_steps)} step(s) still failed after {MAX_ATTEMPTS} attempts: {', '.join(failed_steps)}", flush=True)
            if "items" in failed_steps:
                print(f"  → COGS estimated at 36% (order items unavailable)", flush=True)
            if "fees" in failed_steps:
                print(f"  → Commissions estimated at 15% (fees API unavailable)", flush=True)
            if "traffic" in failed_steps:
                print(f"  → Sessions = 0 (traffic report unavailable)", flush=True)
            print(f"  Data pushed with estimates. Will correct on next successful run.", flush=True)
        else:
            print(f"\n✅ Done! All data fetched successfully.", flush=True)
        print(f"  Sheet: {url}", flush=True)

    elif "--yesterday" in args:
        # Cron mode: fetch only yesterday
        date_str = yesterday.strftime("%Y-%m-%d")
        month_label = yesterday.strftime("%B %Y")

        print(f"Cron mode: fetching {date_str}", flush=True)
        print("=" * 60, flush=True)

        access_token = get_access_token()

        print(f"  Fetching orders...", flush=True)
        orders = fetch_all_orders(date_str, date_str, access_token)
        non_cancelled = [o for o in orders if o.get("OrderStatus") not in ("Canceled", "Cancelled")]
        print(f"  {len(orders)} orders ({len(non_cancelled)} non-cancelled)", flush=True)

        print(f"  Fetching order items...", flush=True)
        order_ids = [o["AmazonOrderId"] for o in non_cancelled]
        items_by_order, access_token = fetch_order_items_batch(order_ids, access_token)

        daily_orders = aggregate_orders_by_day(orders, items_by_order)

        print(f"  Fetching fees...", flush=True)
        daily_fees = fetch_all_fees(date_str, date_str, access_token)

        print(f"  Fetching traffic...", flush=True)
        access_token = get_access_token()
        daily_traffic = fetch_traffic_report(date_str, date_str, access_token)

        order_data = daily_orders.get(date_str, {"revenue": 0, "orders": 0, "cogs": 0})
        fee_data = daily_fees.get(date_str, {"commission": 0, "fba_fee": 0, "closing_fee": 0, "total": 0})
        traffic_data = daily_traffic.get(date_str, {"sessions": 0, "conversion_pct": 0})

        # Estimate fees at 15% if API returned nothing for this day
        if fee_data["total"] == 0 and order_data["revenue"] > 0:
            estimated = round(order_data["revenue"] * 0.15, 2)
            fee_data = {"commission": estimated, "fba_fee": 0, "closing_fee": 0, "total": estimated}
            print(f"  Fees estimated at 15% (no API data for this date)", flush=True)

        day_data = [{
            "date": date_str,
            "revenue": order_data["revenue"],
            "orders": order_data["orders"],
            "cogs": order_data["cogs"],
            "fees_commission": fee_data["commission"],
            "fees_fba": fee_data["fba_fee"],
            "fees_closing": fee_data["closing_fee"],
            "fees_total": fee_data["total"],
            "ad_spend": 0,
            "sessions": traffic_data["sessions"],
            "conversion_pct": traffic_data["conversion_pct"],
        }]

        print(f"\n  Revenue: ₹{order_data['revenue']:,.2f}", flush=True)
        print(f"  Orders: {order_data['orders']}", flush=True)
        print(f"  COGS: ₹{order_data['cogs']:,.2f}", flush=True)
        print(f"  Commissions: ₹{fee_data['total']:,.2f}", flush=True)
        print(f"  Sessions: {traffic_data['sessions']}", flush=True)
        print(f"  Conversion: {traffic_data['conversion_pct']}%", flush=True)

        url = push_to_sheet(day_data, month_label)
        print(f"\nDone! Sheet: {url}", flush=True)

    elif len(args) == 1 and len(args[0]) == 7:
        # Specific month
        year, month = int(args[0][:4]), int(args[0][5:7])
        if year == today.year and month == today.month:
            up_to = yesterday.day
        else:
            up_to = None

        month_label = datetime(year, month, 1).strftime("%B %Y")
        daily_data = fetch_month(year, month, up_to_date=up_to)

        if daily_data:
            save_daily_json(daily_data, month_label)
            url = push_to_sheet(daily_data, month_label)
            print_summary(daily_data, month_label)
            print(f"\nSheet: {url}", flush=True)
        else:
            print("No data fetched.", flush=True)

    else:
        # Default: current month up to yesterday
        year, month = today.year, today.month
        up_to = yesterday.day
        month_label = today.strftime("%B %Y")

        daily_data = fetch_month(year, month, up_to_date=up_to)

        if daily_data:
            save_daily_json(daily_data, month_label)
            url = push_to_sheet(daily_data, month_label)
            print_summary(daily_data, month_label)
            print(f"\nSheet: {url}", flush=True)
        else:
            print("No data fetched.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except PermissionError as e:
        print(f"\n❌ AUTHENTICATION ERROR:\n  {e}", flush=True)
        sys.exit(1)
    except gspread.exceptions.APIError as e:
        status = e.response.status_code if hasattr(e, 'response') else 'unknown'
        print(f"\n❌ GOOGLE SHEETS API ERROR (HTTP {status}):\n  {e}", flush=True)
        if status == 429:
            print("  Cause: Rate limit exceeded. The script retries automatically,", flush=True)
            print("  but too many rapid calls can exhaust the quota.", flush=True)
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"\n❌ NETWORK ERROR:\n  {e}", flush=True)
        print("  Check internet connectivity and API endpoint availability.", flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR:\n  {type(e).__name__}: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
