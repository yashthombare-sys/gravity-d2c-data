#!/usr/bin/env python3
"""
Sync MTD (Month-To-Date) daily data from 2 Google Sheets into mtd_daily_data.json
and inject into dashboard.html as an MTD view.

Sources:
1. D2C Daily (Shiprocket ROI section): cols R-AA
2. Amazon Daily MIS: flat table

Usage: python3 sync_mtd.py
"""

import os, json, time, warnings, re, sys, calendar
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import gspread
from google.oauth2.service_account import Credentials

warnings.filterwarnings("ignore")

BASE = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
OUTPUT_FILE = os.path.join(BASE, "mtd_daily_data.json")
DASHBOARD = os.path.join(BASE, "dashboard.html")

D2C_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LfLi67xq8P1bxEAmJysQuci7vOOuqxEoYxTrYEab0yA/"
AMZ_SHEET_URL = "https://docs.google.com/spreadsheets/d/1u7hupogAQjxyQO6uNxDk9T3ehWDis5PwBgUaYlmIGZc/"

# How many months of history to sync (current month + N previous months)
LOOKBACK_MONTHS = 5

# ── D2C header names to search for (case-insensitive partial match) ──
# These map logical field names to possible header strings found in the sheet.
# The script will scan the header/marker rows to find column indices dynamically.
D2C_HEADER_MAP = {
    "date":           ["date"],
    "ad_spend":       ["total ad spent", "ad spent", "ad spend", "total ad spend"],
    "sessions":       ["sessions", "session"],
    "shopify_orders": ["shopify orders", "shopify order"],
    "orders":         ["total product quantity", "total quantity", "product quantity"],
    "revenue":        ["total sales", "total revenue", "revenue", "sales"],
    "cogs":           ["total cogs", "cogs"],
    "total_expense":  ["total spends", "total spend", "total expense", "total expenses"],
    "profit":         ["profit"],
    "profit_pct":     ["profit %", "profit%"],
    "mkt_pct":        ["marketing spends %", "marketing spend %", "mkt %", "marketing %"],
    "cogs_logistics": ["cogs+logistics", "cogs + logistics", "cogs logistics"],
}

# Fallback hardcoded column indices (used only if header detection fails)
D2C_COL_FALLBACK = {
    "date": 17, "ad_spend": 4, "sessions": 5, "shopify_orders": 7,
    "orders": 19, "revenue": 20, "cogs": 21, "total_expense": 22,
    "profit": 23, "profit_pct": 24, "mkt_pct": 25, "cogs_logistics": 26,
}

# Amazon header names (searched in row 1)
AMZ_HEADER_MAP = {
    "date":            ["date"],
    "revenue":         ["total revenue", "revenue"],
    "total_expense":   ["total expense", "total expenses"],
    "product_expense": ["product expense", "product expenses"],
    "ad_spend":        ["ad spend", "ad spent"],
    "commissions":     ["commissions", "commission"],
    "orders":          ["total orders", "orders"],
    "profit":          ["profit"],
    "profit_pct":      ["profit %", "profit%"],
}

AMZ_COL_FALLBACK = {
    "date": 0, "revenue": 1, "total_expense": 2, "product_expense": 3,
    "ad_spend": 4, "commissions": 5, "orders": 6, "profit": 7, "profit_pct": 8,
}


# ── Dynamic month tab generation ──────────────────────────
def generate_month_tabs(lookback=LOOKBACK_MONTHS):
    """Auto-generate tab name candidates for the current month + N previous months."""
    today = date.today()
    tabs = {}
    for i in range(lookback + 1):
        dt = today.replace(day=1) - relativedelta(months=i)
        month_name = dt.strftime("%B")       # "March"
        month_abbr = dt.strftime("%b")       # "Mar"
        year_full = dt.strftime("%Y")        # "2026"
        year_short = dt.strftime("%y")       # "26"
        key = f"{month_abbr} {year_full}"    # "Mar 2026"

        tabs[key] = {
            "d2c": [
                month_name,                           # "March"
                f"{month_abbr}{year_short}",          # "Mar26"
                f"{month_name} {year_full}",          # "March 2026"
                f"{month_abbr} {year_full}",          # "Mar 2026"
                f"{month_abbr}{year_full}",           # "Mar2026"
                f"{month_name} {year_short}",         # "March 26"
            ],
            "amz": [
                f"{month_name} {year_full}",          # "March 2026"
                month_name,                           # "March"
                f"{month_abbr} {year_full}",          # "Mar 2026"
            ],
        }
    return tabs


# ── Header-based column detection ──────────────────────────
def find_columns_by_header(header_row, header_map, fallback_map):
    """
    Scan a header row and match column indices by name.
    Returns a dict of {field_name: column_index}.
    Falls back to hardcoded indices for any field not found.
    """
    col_map = {}
    header_lower = [str(h).strip().lower() for h in header_row]

    for field, candidates in header_map.items():
        found = False
        for candidate in candidates:
            candidate_lower = candidate.lower()
            for idx, h in enumerate(header_lower):
                if candidate_lower == h or candidate_lower in h:
                    col_map[field] = idx
                    found = True
                    break
            if found:
                break
        if not found and field in fallback_map:
            col_map[field] = fallback_map[field]
            print(f"    ⚠️  Column '{field}' not found in headers, using fallback index {fallback_map[field]}")

    return col_map


def safe_float(val, field_name=None):
    if val is None:
        return 0.0
    s = str(val).replace("₹", "").replace(",", "").replace("%", "").strip()
    if s in ("", "-", "#DIV/0!", "#REF!", "#VALUE!", "#N/A"):
        return 0.0
    # Handle Google Sheets serial date numbers that end up in numeric fields
    try:
        return float(s)
    except ValueError:
        if field_name:
            print(f"    ⚠️  Unexpected value in '{field_name}': '{val}'")
        return 0.0


def parse_date(val):
    """Parse various date formats into YYYY-MM-DD string."""
    if not val:
        return None
    s = str(val).strip()
    if not s or s in ("-", "#REF!"):
        return None

    # Handle Google Sheets serial date numbers (e.g., 46112 for 2026-03-25)
    try:
        num = float(s)
        if 30000 < num < 60000:  # plausible serial date range
            from datetime import timedelta
            epoch = datetime(1899, 12, 30)
            d = epoch + timedelta(days=int(num))
            return d.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass

    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y", "%d/%m/%Y", "%b %d, %Y",
                "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            d = datetime.strptime(s, fmt)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def find_tab(sh, tab_names):
    """Try to find a worksheet by multiple name variants."""
    for name in tab_names:
        try:
            return sh.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            continue
    return None


def api_call_with_retry(func, max_retries=3, base_delay=5):
    """Wrap a Google Sheets API call with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, 'response') else 0
            if status == 429 or status >= 500:
                delay = base_delay * (2 ** attempt)
                print(f"    ⚠️  API error {status}, retrying in {delay}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"    ⚠️  Error: {e}, retrying in {delay}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                raise
    return func()  # final attempt


def read_d2c_section(data, start_idx, col_map):
    """Read one D2C Shiprocket ROI section starting from a data row index."""
    result = {}
    min_cols = max(col_map.get("revenue", 0), col_map.get("orders", 0)) + 1

    for row in data[start_idx:]:
        if len(row) < min_cols:
            continue

        # Stop conditions: Total row or next section marker
        cell_a = str(row[0]).strip() if row[0] else ""
        date_col = col_map.get("date", 17)
        cell_r = str(row[date_col]).strip() if len(row) > date_col else ""
        cell_a_up = cell_a.upper()

        if "TOTAL" in cell_a_up or "TOTAL" in cell_r.upper():
            break
        # Only stop on section markers in column A (not data columns)
        if re.match(r'^(SOOPER|CLAPCUDDLE|CLAP\s*CUDDLE|STEM|SOFT\s*TOY)', cell_a_up):
            break

        dt = parse_date(row[date_col])
        if not dt:
            continue

        revenue = safe_float(row[col_map["revenue"]], "revenue")
        orders = int(safe_float(row[col_map["orders"]], "orders"))

        if revenue == 0 and orders == 0:
            continue

        sessions_idx = col_map.get("sessions")
        shopify_idx = col_map.get("shopify_orders")

        result[dt] = {
            "revenue": round(revenue, 2),
            "orders": orders,
            "ad_spend": round(safe_float(row[col_map["ad_spend"]], "ad_spend") if len(row) > col_map.get("ad_spend", 999) else 0, 2),
            "cogs": round(safe_float(row[col_map["cogs"]], "cogs") if len(row) > col_map.get("cogs", 999) else 0, 2),
            "total_expense": round(safe_float(row[col_map["total_expense"]], "total_expense") if len(row) > col_map.get("total_expense", 999) else 0, 2),
            "profit": round(safe_float(row[col_map["profit"]], "profit") if len(row) > col_map.get("profit", 999) else 0, 2),
            "cogs_logistics": round(safe_float(row[col_map["cogs_logistics"]], "cogs_logistics") if len(row) > col_map.get("cogs_logistics", 999) else 0, 2),
            "sessions": int(safe_float(row[sessions_idx], "sessions")) if sessions_idx and len(row) > sessions_idx else 0,
            "shopify_orders": int(safe_float(row[shopify_idx], "shopify_orders")) if shopify_idx and len(row) > shopify_idx else 0,
        }

    return result


def find_section_markers(data):
    """
    Find section start rows for Busy Board, STEM, and Soft Toy.
    Returns (bb_start, stem_start, soft_start).
    Only matches markers at the START of column A text. Stops after first match per section.
    """
    bb_start = None
    stem_start = None
    soft_start = None

    for i, row in enumerate(data):
        cell_a = str(row[0]).strip().upper() if row[0] else ""
        if not cell_a:
            continue

        # Busy Board section: look for "BUSY BOARD" or the first ROI header
        if bb_start is None and re.match(r'^(BUSY\s*BOARD|MONTESSORI)', cell_a):
            bb_start = i + 2  # marker row, header row, then data

        # STEM section
        if stem_start is None and re.match(r'^(SOOPER|STEM)', cell_a):
            stem_start = i + 2

        # Soft Toy section
        if soft_start is None and re.match(r'^(CLAPCUDDLE|CLAP\s*CUDDLE|SOFT\s*TOY)', cell_a):
            soft_start = i + 2

    # Default: if no busy board marker, data starts at row 4 (index 3)
    if bb_start is None:
        bb_start = 3

    # Validate ordering
    if stem_start is not None and soft_start is not None and stem_start > soft_start:
        print(f"    ⚠️  Section order issue: STEM (row {stem_start}) after Soft Toy (row {soft_start})")

    return bb_start, stem_start, soft_start


def find_d2c_header_row(data, section_start):
    """Find the header row just before data starts in a D2C section."""
    # The header row is typically 1 row before data start (section_start - 1)
    header_idx = max(0, section_start - 1)
    if header_idx < len(data):
        return data[header_idx]
    return []


def read_d2c_daily(sh, month_key, month_tabs):
    """Read D2C Shiprocket ROI for all 3 categories (Busy Board, STEM, Soft Toy)."""
    tabs = month_tabs.get(month_key, {}).get("d2c", [])
    ws = find_tab(sh, tabs)
    if not ws:
        return {}, {}, {}

    data = api_call_with_retry(lambda: ws.get_all_values())

    bb_start, stem_start, soft_start = find_section_markers(data)

    # Detect columns from the header row of the first section
    header_row = find_d2c_header_row(data, bb_start)
    col_map = find_columns_by_header(header_row, D2C_HEADER_MAP, D2C_COL_FALLBACK)

    # Read each category
    bb_data = read_d2c_section(data, bb_start, col_map)

    # For STEM/Soft, re-detect headers in case their section has different column layout
    if stem_start:
        stem_header = find_d2c_header_row(data, stem_start)
        stem_cols = find_columns_by_header(stem_header, D2C_HEADER_MAP, D2C_COL_FALLBACK) if any(stem_header) else col_map
        stem_data = read_d2c_section(data, stem_start, stem_cols)
    else:
        stem_data = {}

    if soft_start:
        soft_header = find_d2c_header_row(data, soft_start)
        soft_cols = find_columns_by_header(soft_header, D2C_HEADER_MAP, D2C_COL_FALLBACK) if any(soft_header) else col_map
        soft_data = read_d2c_section(data, soft_start, soft_cols)
    else:
        soft_data = {}

    return bb_data, stem_data, soft_data


def read_amazon_daily(sh, month_key, month_tabs):
    """Read Amazon daily MIS for a month."""
    tabs = month_tabs.get(month_key, {}).get("amz", [])
    ws = find_tab(sh, tabs)
    if not ws:
        return {}

    data = api_call_with_retry(lambda: ws.get_all_values())
    if not data:
        return {}

    # Detect columns from header row
    col_map = find_columns_by_header(data[0], AMZ_HEADER_MAP, AMZ_COL_FALLBACK)

    result = {}
    for row in data[1:]:
        if len(row) <= col_map.get("orders", 6):
            continue

        dt = parse_date(row[col_map["date"]])
        if not dt:
            continue

        revenue = safe_float(row[col_map["revenue"]], "revenue")
        orders = int(safe_float(row[col_map["orders"]], "orders"))

        if revenue == 0 and orders == 0:
            continue

        result[dt] = {
            "revenue": round(revenue, 2),
            "orders": orders,
            "ad_spend": round(safe_float(row[col_map["ad_spend"]], "ad_spend") if len(row) > col_map.get("ad_spend", 999) else 0, 2),
            "commissions": round(safe_float(row[col_map["commissions"]], "commissions") if len(row) > col_map.get("commissions", 999) else 0, 2),
            "product_expense": round(safe_float(row[col_map["product_expense"]], "product_expense") if len(row) > col_map.get("product_expense", 999) else 0, 2),
            "total_expense": round(safe_float(row[col_map["total_expense"]], "total_expense") if len(row) > col_map.get("total_expense", 999) else 0, 2),
            "profit": round(safe_float(row[col_map["profit"]], "profit") if len(row) > col_map.get("profit", 999) else 0, 2),
        }

    return result


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)

    print("\n🔄 Syncing MTD daily data\n")

    # Generate month tabs dynamically
    month_tabs = generate_month_tabs()
    print(f"  Syncing {len(month_tabs)} months: {', '.join(month_tabs.keys())}\n")

    # Open both sheets with retry
    d2c_sh = api_call_with_retry(lambda: gc.open_by_url(D2C_SHEET_URL))
    amz_sh = api_call_with_retry(lambda: gc.open_by_url(AMZ_SHEET_URL))

    all_bb = {}    # Busy Board (Montessori Labs)
    all_stem = {}  # STEM (Sooperbrains)
    all_soft = {}  # Soft Toy (Clapcuddles)
    all_amz = {}
    errors = []

    for month_key in month_tabs:
        print(f"  {month_key}:")

        try:
            bb, stem, soft = read_d2c_daily(d2c_sh, month_key, month_tabs)
            if bb:
                all_bb.update(bb)
                print(f"    Busy Board: {len(bb)} days, Revenue=₹{sum(v['revenue'] for v in bb.values()):,.0f}")
            if stem:
                all_stem.update(stem)
                print(f"    STEM: {len(stem)} days, Revenue=₹{sum(v['revenue'] for v in stem.values()):,.0f}")
            if soft:
                all_soft.update(soft)
                print(f"    Soft Toy: {len(soft)} days, Revenue=₹{sum(v['revenue'] for v in soft.values()):,.0f}")
            if not bb and not stem and not soft:
                print(f"    D2C: no tab found (tried: {month_tabs[month_key]['d2c']})")
        except Exception as e:
            msg = f"D2C {month_key}: {e}"
            errors.append(msg)
            print(f"    ❌ D2C error: {e}")
        time.sleep(1)

        try:
            amz = read_amazon_daily(amz_sh, month_key, month_tabs)
            if amz:
                all_amz.update(amz)
                total_rev = sum(v["revenue"] for v in amz.values())
                print(f"    Amazon: {len(amz)} days, Revenue=₹{total_rev:,.0f}")
            else:
                print(f"    Amazon: no tab found (tried: {month_tabs[month_key]['amz']})")
        except Exception as e:
            msg = f"Amazon {month_key}: {e}"
            errors.append(msg)
            print(f"    ❌ Amazon error: {e}")
        time.sleep(1)

    # Build combined D2C (sum of all 3 categories per date)
    all_dates = sorted(set(list(all_bb.keys()) + list(all_stem.keys()) + list(all_soft.keys())))
    all_d2c = {}
    for dt in all_dates:
        bb = all_bb.get(dt, {})
        st = all_stem.get(dt, {})
        sf = all_soft.get(dt, {})
        all_d2c[dt] = {
            "revenue": round(bb.get("revenue", 0) + st.get("revenue", 0) + sf.get("revenue", 0), 2),
            "orders": int(bb.get("orders", 0) + st.get("orders", 0) + sf.get("orders", 0)),
            "ad_spend": round(bb.get("ad_spend", 0) + st.get("ad_spend", 0) + sf.get("ad_spend", 0), 2),
            "cogs": round(bb.get("cogs", 0) + st.get("cogs", 0) + sf.get("cogs", 0), 2),
            "total_expense": round(bb.get("total_expense", 0) + st.get("total_expense", 0) + sf.get("total_expense", 0), 2),
            "profit": round(bb.get("profit", 0) + st.get("profit", 0) + sf.get("profit", 0), 2),
            "sessions": int(bb.get("sessions", 0) + st.get("sessions", 0) + sf.get("sessions", 0)),
            "shopify_orders": int(bb.get("shopify_orders", 0) + st.get("shopify_orders", 0) + sf.get("shopify_orders", 0)),
        }

    # Save JSON — preserve existing keys (e.g. shiprocket data from shiprocket_sync.py)
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r") as f:
            output = json.load(f)
    else:
        output = {}

    output.update({
        "d2c": dict(sorted(all_d2c.items())),
        "d2c_busyboard": dict(sorted(all_bb.items())),
        "d2c_stem": dict(sorted(all_stem.items())),
        "d2c_softtoy": dict(sorted(all_soft.items())),
        "amazon": dict(sorted(all_amz.items())),
        "last_synced": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    })

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n📁 Saved: {OUTPUT_FILE}")

    # Summary
    d2c_total = sum(v["revenue"] for v in all_d2c.values())
    bb_total = sum(v["revenue"] for v in all_bb.values())
    stem_total = sum(v["revenue"] for v in all_stem.values())
    soft_total = sum(v["revenue"] for v in all_soft.values())
    amz_total = sum(v["revenue"] for v in all_amz.values())
    print(f"\n📊 Summary:")
    print(f"   D2C Total: {len(all_d2c)} days | ₹{d2c_total/100000:,.2f}L")
    print(f"     Busy Board: ₹{bb_total/100000:,.2f}L")
    print(f"     STEM:       ₹{stem_total/100000:,.2f}L")
    print(f"     Soft Toy:   ₹{soft_total/100000:,.2f}L")
    print(f"   Amazon: {len(all_amz)} days | ₹{amz_total/100000:,.2f}L")
    print(f"   Grand Total: ₹{(d2c_total+amz_total)/100000:,.2f}L")

    # Report errors
    if errors:
        print(f"\n⚠️  {len(errors)} error(s) during sync:")
        for err in errors:
            print(f"   ❌ {err}")
        # Still inject whatever data we got — partial data is better than none
        print("   (Injecting partial data into dashboard)")

    # Inject into dashboard.html
    inject_into_dashboard(output)

    # Don't exit(1) on partial failures — the dashboard and JSON have already been
    # updated with whatever data we got. Exiting with error prevents the commit step
    # from saving the partial data, which is worse than having partial data.
    if errors:
        print("   Partial data saved — dashboard updated with available data")


def inject_into_dashboard(data):
    """Inject MTD data into dashboard.html between markers."""
    if not os.path.exists(DASHBOARD):
        print("   ⚠️  dashboard.html not found, skipping injection")
        return

    with open(DASHBOARD, "r") as f:
        html = f.read()

    # Build JS data
    js_data = f"const MTD_DATA={json.dumps(data)};"

    marker_start = "// ── MTD_DATA_START ──"
    marker_end = "// ── MTD_DATA_END ──"

    if marker_start in html:
        pattern = re.escape(marker_start) + r".*?" + re.escape(marker_end)
        replacement = f"{marker_start}\n{js_data}\n{marker_end}"
        html = re.sub(pattern, replacement, html, flags=re.DOTALL)
        print("   ✅ Updated MTD data in dashboard.html")
    else:
        # Insert before the SYNC_DATA_START marker
        insert_point = html.find("// ── SYNC_DATA_START ──")
        if insert_point > 0:
            injection = f"{marker_start}\n{js_data}\n{marker_end}\n\n"
            html = html[:insert_point] + injection + html[insert_point:]
            print("   ✅ Injected MTD data into dashboard.html")
        else:
            print("   ⚠️  Could not find injection point in dashboard.html")
            return

    with open(DASHBOARD, "w") as f:
        f.write(html)


if __name__ == "__main__":
    main()
