#!/usr/bin/env python3
"""
Sync MTD (Month-To-Date) daily data from 2 Google Sheets into mtd_daily_data.json
and inject into dashboard.html as an MTD view.

Sources:
1. D2C Daily (Shiprocket ROI section): cols R-AA
2. Amazon Daily MIS: flat table

Usage: python3 sync_mtd.py
"""

import os, json, time, warnings, re
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

warnings.filterwarnings("ignore")

BASE = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
OUTPUT_FILE = os.path.join(BASE, "mtd_daily_data.json")
DASHBOARD = os.path.join(BASE, "dashboard.html")

D2C_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LfLi67xq8P1bxEAmJysQuci7vOOuqxEoYxTrYEab0yA/"
AMZ_SHEET_URL = "https://docs.google.com/spreadsheets/d/1u7hupogAQjxyQO6uNxDk9T3ehWDis5PwBgUaYlmIGZc/"

# D2C Shiprocket ROI columns (0-indexed)
D2C_COL = {
    "date": 17,       # R
    "ad_spend": 4,    # E - Total ad Spent (with GST, from Shopify ROI section)
    "sessions": 5,    # F - Sessions
    "shopify_orders": 7, # H - Shopify Orders
    "orders": 19,     # T - Total Product Quantity
    "revenue": 20,    # U - Total Sales
    "cogs": 21,       # V - Total Cogs
    "total_expense": 22,  # W - Total Spends
    "profit": 23,     # X - Profit
    "profit_pct": 24, # Y - Profit %
    "mkt_pct": 25,    # Z - Marketing spends %
    "cogs_logistics": 26, # AA - COGS+Logistics
}

# Amazon columns (0-indexed)
AMZ_COL = {
    "date": 0,
    "revenue": 1,
    "total_expense": 2,
    "product_expense": 3,
    "ad_spend": 4,
    "commissions": 5,
    "orders": 6,
    "profit": 7,
    "profit_pct": 8,
}

# Tab name variants per month
MONTH_TABS = {
    "Mar 2026": {
        "d2c": ["March", "Mar26", "March 2026"],
        "amz": ["March 2026"],
    },
    "Feb 2026": {
        "d2c": ["Feb26", "February", "February 2026"],
        "amz": ["February 2026"],
    },
    "Jan 2026": {
        "d2c": ["Jan26", "January", "January 2026"],
        "amz": ["January 2026"],
    },
    "Dec 2025": {
        "d2c": ["Dec25", "December", "December 2025"],
        "amz": ["December 2025"],
    },
    "Nov 2025": {
        "d2c": ["Nov25", "November", "November 2025"],
        "amz": ["November 2025"],
    },
    "Oct 2025": {
        "d2c": ["Oct25", "October", "October 2025"],
        "amz": ["October 2025"],
    },
}


def safe_float(val):
    if val is None:
        return 0.0
    s = str(val).replace("₹", "").replace(",", "").replace("%", "").strip()
    if s in ("", "-", "#DIV/0!", "#REF!", "#VALUE!", "#N/A"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_date(val):
    """Parse various date formats into YYYY-MM-DD string."""
    if not val:
        return None
    s = str(val).strip()
    if not s or s in ("-", "#REF!"):
        return None

    # Try YYYY-MM-DD
    try:
        d = datetime.strptime(s, "%Y-%m-%d")
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Try D-Mon-YYYY (e.g., 1-Mar-2026)
    try:
        d = datetime.strptime(s, "%d-%b-%Y")
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Try D-Mon-YY
    try:
        d = datetime.strptime(s, "%d-%b-%y")
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Try DD/MM/YYYY
    try:
        d = datetime.strptime(s, "%d/%m/%Y")
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Try Mon D, YYYY
    try:
        d = datetime.strptime(s, "%b %d, %Y")
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    return None


def find_tab(sh, tab_names):
    """Try to find a worksheet by multiple name variants."""
    for name in tab_names:
        try:
            return sh.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            continue
    return None


def read_d2c_section(data, start_idx):
    """Read one D2C Shiprocket ROI section starting from a data row index."""
    result = {}
    for row in data[start_idx:]:
        if len(row) <= D2C_COL["revenue"]:
            continue

        # Stop conditions: Total row, section marker, or header row
        cell_a = str(row[0]).strip() if row[0] else ""
        cell_r = str(row[D2C_COL["date"]]).strip() if len(row) > D2C_COL["date"] else ""
        cell_a_up = cell_a.upper()
        cell_r_up = cell_r.upper()

        if "TOTAL" in cell_a_up or "TOTAL" in cell_r_up:
            break
        if "START" in cell_a_up or "SOOPER" in cell_a_up or "CLAPCUDDLE" in cell_a_up:
            break

        dt = parse_date(row[D2C_COL["date"]])
        if not dt:
            continue

        revenue = safe_float(row[D2C_COL["revenue"]])
        orders = int(safe_float(row[D2C_COL["orders"]]))

        if revenue == 0 and orders == 0:
            continue

        sessions = int(safe_float(row[D2C_COL["sessions"]])) if len(row) > D2C_COL["sessions"] else 0
        shopify_orders = int(safe_float(row[D2C_COL["shopify_orders"]])) if len(row) > D2C_COL["shopify_orders"] else 0

        result[dt] = {
            "revenue": round(revenue, 2),
            "orders": orders,
            "ad_spend": round(safe_float(row[D2C_COL["ad_spend"]]), 2),
            "cogs": round(safe_float(row[D2C_COL["cogs"]]), 2),
            "total_expense": round(safe_float(row[D2C_COL["total_expense"]]), 2),
            "profit": round(safe_float(row[D2C_COL["profit"]]), 2),
            "cogs_logistics": round(safe_float(row[D2C_COL["cogs_logistics"]]) if len(row) > D2C_COL["cogs_logistics"] else 0, 2),
            "sessions": sessions,
            "shopify_orders": shopify_orders,
        }

    return result


def read_d2c_daily(sh, month_key):
    """Read D2C Shiprocket ROI for all 3 categories (Busy Board, STEM, Soft Toy)."""
    tabs = MONTH_TABS.get(month_key, {}).get("d2c", [])
    ws = find_tab(sh, tabs)
    if not ws:
        return {}, {}, {}

    data = ws.get_all_values()

    # Find section start rows by looking for markers
    # Busy Board: starts at row 4 (index 3) — default first section
    # STEM (Sooperbrains): row with "SOOPER_BRAINS_START" or header row after it
    # Soft Toy (Clapcuddles): row with "CLAPCUDDLES_START" or header row after it
    bb_start = 3  # default: row 4 (index 3)
    stem_start = None
    soft_start = None

    for i, row in enumerate(data):
        cell_a = str(row[0]).strip().upper() if row[0] else ""
        if "SOOPER" in cell_a or "STEM" in cell_a:
            # Data starts 2 rows after marker (marker, header, data)
            stem_start = i + 2
        if "CLAPCUDDLE" in cell_a or "SOFT" in cell_a:
            soft_start = i + 2

    # Read each category
    bb_data = read_d2c_section(data, bb_start)
    stem_data = read_d2c_section(data, stem_start) if stem_start else {}
    soft_data = read_d2c_section(data, soft_start) if soft_start else {}

    return bb_data, stem_data, soft_data


def read_amazon_daily(sh, month_key):
    """Read Amazon daily MIS for a month."""
    tabs = MONTH_TABS.get(month_key, {}).get("amz", [])
    ws = find_tab(sh, tabs)
    if not ws:
        return {}

    data = ws.get_all_values()
    result = {}

    # Row 1 is headers, data starts row 2 (index 1)
    for row in data[1:]:
        if len(row) <= AMZ_COL["orders"]:
            continue

        dt = parse_date(row[AMZ_COL["date"]])
        if not dt:
            continue

        revenue = safe_float(row[AMZ_COL["revenue"]])
        orders = int(safe_float(row[AMZ_COL["orders"]]))

        if revenue == 0 and orders == 0:
            continue

        result[dt] = {
            "revenue": round(revenue, 2),
            "orders": orders,
            "ad_spend": round(safe_float(row[AMZ_COL["ad_spend"]]), 2),
            "commissions": round(safe_float(row[AMZ_COL["commissions"]]), 2),
            "product_expense": round(safe_float(row[AMZ_COL["product_expense"]]), 2),
            "total_expense": round(safe_float(row[AMZ_COL["total_expense"]]), 2),
            "profit": round(safe_float(row[AMZ_COL["profit"]]), 2),
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

    # Open both sheets
    d2c_sh = gc.open_by_url(D2C_SHEET_URL)
    amz_sh = gc.open_by_url(AMZ_SHEET_URL)

    all_bb = {}    # Busy Board (Montessori Labs)
    all_stem = {}  # STEM (Sooperbrains)
    all_soft = {}  # Soft Toy (Clapcuddles)
    all_amz = {}

    for month_key in MONTH_TABS:
        print(f"  {month_key}:")

        bb, stem, soft = read_d2c_daily(d2c_sh, month_key)
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
            print(f"    D2C: no tab found")
        time.sleep(1)

        amz = read_amazon_daily(amz_sh, month_key)
        if amz:
            all_amz.update(amz)
            total_rev = sum(v["revenue"] for v in amz.values())
            print(f"    Amazon: {len(amz)} days, Revenue=₹{total_rev:,.0f}")
        else:
            print(f"    Amazon: no tab found")
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

    # Save JSON
    output = {
        "d2c": dict(sorted(all_d2c.items())),
        "d2c_busyboard": dict(sorted(all_bb.items())),
        "d2c_stem": dict(sorted(all_stem.items())),
        "d2c_softtoy": dict(sorted(all_soft.items())),
        "amazon": dict(sorted(all_amz.items())),
        "last_synced": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }

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

    # Inject into dashboard.html
    inject_into_dashboard(output)


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
