#!/usr/bin/env python3
"""
Sync daily data into dashboard.html from two Google Sheets:
1. D2C Daily — Shopify expense sheet (Shiprocket columns R-AA)
2. Amazon Daily MIS — daily orders + fees

Usage:  python3 sync_daily_dashboard.py
"""

import os, re, json, time
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
DASHBOARD = os.path.join(BASE, "daily_dashboard.html")

D2C_SHEET_URL = "https://docs.google.com/spreadsheets/d/1LfLi67xq8P1bxEAmJysQuci7vOOuqxEoYxTrYEab0yA/"
AMZ_SHEET_URL = "https://docs.google.com/spreadsheets/d/1u7hupogAQjxyQO6uNxDk9T3ehWDis5PwBgUaYlmIGZc/"

# Tab name variants to try for D2C expense sheet
D2C_MONTHS = {
    "Oct 2025": ["October", "Oct25", "October 2025"],
    "Nov 2025": ["November", "Nov25", "November 2025"],
    "Dec 2025": ["December", "Dec25", "December 2025"],
    "Jan 2026": ["January", "Jan26", "January 2026"],
    "Feb 2026": ["February", "Feb26", "February 2026"],
    "Mar 2026": ["March", "Mar26", "March 2026"],
}

# Tab name variants for Amazon daily MIS
AMZ_MONTHS = {
    "Oct 2025": ["October 2025"],
    "Nov 2025": ["November 2025"],
    "Dec 2025": ["December 2025"],
    "Jan 2026": ["January 2026"],
    "Feb 2026": ["February 2026"],
    "Mar 2026": ["March 2026"],
}

# D2C Shiprocket columns (0-indexed: R=17, S=18, T=19, U=20, V=21, W=22, X=23, Y=24, Z=25, AA=26)
D2C_COL = {
    "date": 17,
    "ads": 18,
    "orders": 19,
    "revenue": 20,
    "cogs": 21,
    "total_spends": 22,
    "profit": 23,
    "profit_pct": 24,
    "mkt_pct": 25,
    "cogs_log_pct": 26,
}

# Amazon daily columns (0-indexed)
AMZ_COL = {
    "date": 0,
    "revenue": 1,
    "total_expense": 2,
    "cogs": 3,
    "ads": 4,
    "commissions": 5,
    "orders": 6,
    "profit": 7,
    "profit_pct": 8,
    "comm_pct": 9,
    "mkt_pct": 10,
}


def safe_float(val):
    if val is None:
        return 0.0
    s = str(val).replace("₹", "").replace(",", "").replace("%", "").strip()
    if s == "" or s == "-" or s == "—":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def safe_int(val):
    return int(safe_float(val))


def parse_date(val):
    """Extract day number from a date cell. Returns string like '1', '15', or None."""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() == "date" or s.lower() == "total":
        return None
    # Try parsing as just a number (day)
    try:
        d = int(float(s))
        if 1 <= d <= 31:
            return str(d)
    except (ValueError, TypeError):
        pass
    import re as _re
    # YYYY-MM-DD format (e.g., "2026-03-01")
    m = _re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return str(int(m.group(3)))
    # D-Mon-YYYY format (e.g., "1-Mar-2026")
    m = _re.match(r"(\d{1,2})-[A-Za-z]+-\d{2,4}", s)
    if m:
        return str(int(m.group(1)))
    # DD/MM/YYYY or D/M/YYYY
    m = _re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", s)
    if m:
        return str(int(m.group(1)))
    return None


def find_worksheet(sh, tab_names):
    """Try multiple tab name variants, return first found worksheet or None."""
    for name in tab_names:
        try:
            ws = sh.worksheet(name)
            return ws
        except gspread.exceptions.WorksheetNotFound:
            continue
    return None


def read_d2c_daily(sh):
    """Read daily D2C data from the expense sheet."""
    result = {}
    for month_key, tab_names in D2C_MONTHS.items():
        ws = find_worksheet(sh, tab_names)
        if not ws:
            print(f"  D2C {month_key}: no tab found ({tab_names})")
            result[month_key] = []
            continue

        all_values = ws.get_all_values()
        time.sleep(2)

        rows = []
        category = "busyboard"  # default: section 1

        for row in all_values:
            # Check for category markers
            cell0 = str(row[0]).strip() if row else ""
            if "SOOPER_BRAINS_START" in cell0 or "Sooper Brains" in cell0:
                category = "softtoy"
                continue
            if "CLAPCUDDLES_START" in cell0 or "ClapCuddles" in cell0:
                category = "stem"
                continue

            # Check if row has valid date in column R
            if len(row) <= D2C_COL["date"]:
                continue
            date_val = parse_date(row[D2C_COL["date"]])
            if not date_val:
                continue

            orders = safe_int(row[D2C_COL["orders"]]) if len(row) > D2C_COL["orders"] else 0
            revenue = safe_float(row[D2C_COL["revenue"]]) if len(row) > D2C_COL["revenue"] else 0
            cogs = safe_float(row[D2C_COL["cogs"]]) if len(row) > D2C_COL["cogs"] else 0
            ads = safe_float(row[D2C_COL["ads"]]) if len(row) > D2C_COL["ads"] else 0
            profit = safe_float(row[D2C_COL["profit"]]) if len(row) > D2C_COL["profit"] else 0
            profit_pct = safe_float(row[D2C_COL["profit_pct"]]) if len(row) > D2C_COL["profit_pct"] else 0
            mkt_pct = safe_float(row[D2C_COL["mkt_pct"]]) if len(row) > D2C_COL["mkt_pct"] else 0
            cogs_log_pct = safe_float(row[D2C_COL["cogs_log_pct"]]) if len(row) > D2C_COL["cogs_log_pct"] else 0

            if orders == 0 and revenue == 0:
                continue

            rows.append({
                "date": date_val,
                "orders": orders,
                "revenue": round(revenue, 2),
                "cogs": round(cogs, 2),
                "ads": round(ads, 2),
                "profit": round(profit, 2),
                "profit_pct": round(profit_pct, 1),
                "mkt_pct": round(mkt_pct, 1),
                "cogs_log_pct": round(cogs_log_pct, 1),
                "category": category,
            })

        result[month_key] = rows
        print(f"  D2C {month_key}: {len(rows)} daily entries ({ws.title})")

    return result


def read_amz_daily(sh):
    """Read daily Amazon data from the Amazon Daily MIS sheet."""
    result = {}
    for month_key, tab_names in AMZ_MONTHS.items():
        ws = find_worksheet(sh, tab_names)
        if not ws:
            print(f"  AMZ {month_key}: no tab found ({tab_names})")
            result[month_key] = []
            continue

        all_values = ws.get_all_values()
        time.sleep(2)

        rows = []
        for row in all_values:
            if len(row) < 7:
                continue
            date_val = parse_date(row[AMZ_COL["date"]])
            if not date_val:
                continue

            revenue = safe_float(row[AMZ_COL["revenue"]])
            orders = safe_int(row[AMZ_COL["orders"]])
            if orders == 0 and revenue == 0:
                continue

            cogs = safe_float(row[AMZ_COL["cogs"]])
            ads = safe_float(row[AMZ_COL["ads"]])
            commissions = safe_float(row[AMZ_COL["commissions"]])
            profit = safe_float(row[AMZ_COL["profit"]])
            profit_pct = safe_float(row[AMZ_COL["profit_pct"]])
            comm_pct = safe_float(row[AMZ_COL["comm_pct"]]) if len(row) > AMZ_COL["comm_pct"] else 0
            mkt_pct = safe_float(row[AMZ_COL["mkt_pct"]]) if len(row) > AMZ_COL["mkt_pct"] else 0

            rows.append({
                "date": date_val,
                "orders": orders,
                "revenue": round(revenue, 2),
                "cogs": round(cogs, 2),
                "ads": round(ads, 2),
                "commissions": round(commissions, 2),
                "profit": round(profit, 2),
                "profit_pct": round(profit_pct, 1),
                "comm_pct": round(comm_pct, 1),
                "mkt_pct": round(mkt_pct, 1),
            })

        result[month_key] = rows
        print(f"  AMZ {month_key}: {len(rows)} daily entries ({ws.title})")

    return result


def to_js_daily_d2c(data):
    """Convert D2C daily data to JS object string."""
    parts = []
    for month_key in D2C_MONTHS:
        rows = data.get(month_key, [])
        if not rows:
            parts.append(f'"{month_key}":[]')
            continue
        row_strs = []
        for r in rows:
            row_strs.append(
                f'{{date:"{r["date"]}",orders:{r["orders"]},revenue:{r["revenue"]},'
                f'cogs:{r["cogs"]},ads:{r["ads"]},profit:{r["profit"]},'
                f'profit_pct:{r["profit_pct"]},mkt_pct:{r["mkt_pct"]},'
                f'cogs_log_pct:{r["cogs_log_pct"]},category:"{r["category"]}"}}'
            )
        parts.append(f'"{month_key}":[{",".join(row_strs)}]')
    return "{" + ",".join(parts) + "}"


def to_js_daily_amz(data):
    """Convert Amazon daily data to JS object string."""
    parts = []
    for month_key in AMZ_MONTHS:
        rows = data.get(month_key, [])
        if not rows:
            parts.append(f'"{month_key}":[]')
            continue
        row_strs = []
        for r in rows:
            row_strs.append(
                f'{{date:"{r["date"]}",orders:{r["orders"]},revenue:{r["revenue"]},'
                f'cogs:{r["cogs"]},ads:{r["ads"]},commissions:{r["commissions"]},'
                f'profit:{r["profit"]},profit_pct:{r["profit_pct"]},'
                f'comm_pct:{r["comm_pct"]},mkt_pct:{r["mkt_pct"]}}}'
            )
        parts.append(f'"{month_key}":[{",".join(row_strs)}]')
    return "{" + ",".join(parts) + "}"


def update_dashboard(d2c_data, amz_data):
    """Replace inline daily data in dashboard.html."""
    with open(DASHBOARD, "r") as f:
        html = f.read()

    d2c_js = to_js_daily_d2c(d2c_data)
    amz_js = to_js_daily_amz(amz_data)

    replacement = (
        "// ── DAILY_DATA_START ──\n"
        f"const DAILY_D2C={d2c_js};\n"
        f"const DAILY_AMZ={amz_js};\n"
        "// ── DAILY_DATA_END ──"
    )

    pattern = r'// ── DAILY_DATA_START ──.*?// ── DAILY_DATA_END ──'
    match = re.search(pattern, html, flags=re.DOTALL)
    if not match:
        print("\n⚠️  Could not find DAILY_DATA markers in dashboard.html!")
        return False

    new_html = html[:match.start()] + replacement + html[match.end():]
    with open(DASHBOARD, "w") as f:
        f.write(new_html)
    return True


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)

    print("\n🔄 Syncing daily data into dashboard\n")

    # Read D2C daily
    print("📋 Reading D2C expense sheet...")
    d2c_sh = gc.open_by_url(D2C_SHEET_URL)
    d2c_data = read_d2c_daily(d2c_sh)

    # Read Amazon daily
    print("\n📋 Reading Amazon Daily MIS...")
    amz_sh = gc.open_by_url(AMZ_SHEET_URL)
    amz_data = read_amz_daily(amz_sh)

    # Update dashboard
    print("\n📝 Updating dashboard.html...")
    if update_dashboard(d2c_data, amz_data):
        d2c_total = sum(len(v) for v in d2c_data.values())
        amz_total = sum(len(v) for v in amz_data.values())
        print(f"\n✅ Daily data synced! {d2c_total} D2C + {amz_total} Amazon daily entries")
        print("   Open dashboard.html in browser to see Daily view")
    else:
        print("\n❌ Failed to update dashboard")


if __name__ == "__main__":
    main()
