#!/usr/bin/env python3
"""
Sync dashboard.html with data from Google Sheets.

Reads Shiprocket (D2C), Amazon, Flipkart, FirstCry, Blinkit, Instamart, and Cred sections
from each month's tab, then rewrites the inline data in dashboard.html.

Usage:  python3 sync_dashboard.py
"""

import os, re, json, time
import gspread
from google.oauth2.service_account import Credentials

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = os.path.join(BASE, "shiproket-mis-70c28ae6e7fb.json")
DASHBOARD = os.path.join(BASE, "dashboard.html")

# FY 2025-26 months — actively synced from Google Sheet
MONTHS = {
    "Apr 2025": "April 2025 MIS",
    "May 2025": "May 2025 MIS",
    "Jun 2025": "June 2025 MIS",
    "Jul 2025": "July 2025 MIS",
    "Aug 2025": "August 2025 MIS",
    "Sep 2025": "September 2025 MIS",
    "Oct 2025": "October 2025 MIS",
    "Nov 2025": "November 2025 MIS",
    "Dec 2025": "December 2025 MIS",
    "Jan 2026": "January 2026 MIS",
    "Feb 2026": "February 2026 MIS",
    "Mar 2026": "March 2026 MIS",
}

# FY 2024-25 months — frozen in dashboard, no longer fetched
FY24_25_MONTHS = [
    "Apr 2024", "May 2024", "Jun 2024", "Jul 2024",
    "Aug 2024", "Sep 2024", "Oct 2024", "Nov 2024",
    "Dec 2024", "Jan 2025", "Feb 2025", "Mar 2025",
]

# Full list for dashboard output (both FYs)
ALL_MONTHS = FY24_25_MONTHS + list(MONTHS.keys())

# ── Column indices for Shiprocket section (0-based) ──
# A=Products, B=Revenue, C=TotalExpense, D=P/L, E=Profit%, F=P/pcs,
# G=TotalOrders, H=Shipped, I=COGS, J=Delivered, K=ShippingCharges,
# L=RTO, M=In-Transit, N=RTO%, O=Shipped%, P=Delivered%, Q=Cancel%, R=COGS/Unit
SR_COL = {
    "product": 0, "revenue": 1, "orders": 6, "shipped": 7,
    "delivered": 9, "rto": 11, "in_transit": 12, "freight": 10,
}

# ── Column indices for Amazon section (0-based) ──
# A=Products, B=Revenue, C=Orders, D=Delivered, E=COGS, F=COGS/Unit,
# G=Commission, H=FBA Fees, I=Closing Fee, J=Promos, K=Refund Amt,
# L=Total Amazon Fees, M=Ad Spend, N=Profit, O=Profit%
AMZ_COL = {
    "product": 0, "revenue": 1, "orders": 2, "delivered": 3,
    "cogs": 4, "cogs_unit": 5, "commission": 6,
    "fba_fees": 7, "closing_fee": 8, "promos": 9, "refund_amt": 10,
    "amazon_fees": 11, "ad_spend": 12, "profit": 13, "profit_pct": 14,
}

SKIP_LABELS = {
    "", "Products", "GRAND TOTAL", "AMAZON MIS", "FLIPKART MIS", "FIRSTCRY MIS",
    "INSTAMART MIS", "BLINKIT", "BLINKIT MIS", "CRED MIS",
    "BUSY BOARD CATEGORY", "SOFT TOY CATEGORY", "STEM CATEGORY",
}

# ── Column indices for Cred section (0-based) ──
# A=Products, B=Revenue, C=TotalExpense, D=Total P/L, E=Profit%, F=P/pcs,
# G=Product Expense, H=Delivered, I=COGS/Unit
CRED_COL = {
    "product": 0, "revenue": 1, "expense": 2, "delivered": 7,
}

# ── Column indices for Flipkart section (0-based) ──
# A=Products, B=Revenue, C=Orders, D=Delivered, E=Returned,
# F=COGS, G=COGS/Unit, H=Commission, I=Fixed Fee, J=Shipping Fee,
# K=Rev Shipping, L=Refund Amt, M=Total FK Fees, N=Ad Spend, O=Profit
FK_COL = {
    "product": 0, "revenue": 1, "orders": 2, "delivered": 3, "returned": 4,
    "cogs": 5, "cogs_unit": 6, "commission": 7, "fixed_fee": 8,
    "shipping_fee": 9, "reverse_shipping": 10, "refund_amt": 11,
    "fk_fees": 12, "ad_spend": 13, "profit": 14,
}


# ── Column indices for FirstCry section (0-based) ──
# A=Products, B=Revenue, C=Orders, D=Delivered, E=Returned,
# F=COGS, G=COGS/Unit, H-M=empty, N=Ad Spend, O=Profit
FC_COL = {
    "product": 0, "revenue": 1, "orders": 2, "delivered": 3, "returned": 4,
    "cogs": 5, "cogs_unit": 6, "ad_spend": 13, "profit": 14,
}

# ── Column indices for Blinkit section (0-based) ──
# A=Products, B=Revenue, C=Orders, D=COGS, E=COGS/Unit, F=Ads, G=Logistics, H=Profit, I=Profit%
BK_COL = {
    "product": 0, "revenue": 1, "orders": 2, "cogs": 3, "cogs_unit": 4,
    "ads": 5, "logistics": 6, "profit": 7, "profit_pct": 8,
}


# ── Column indices for Instamart section (0-based) ──
# Same layout as FirstCry: A=Products, B=Revenue, C=Orders, D=Delivered, E=Returned,
# F=COGS, G=COGS/Unit, H-M=empty, N=Ad Spend, O=Profit
IM_COL = {
    "product": 0, "revenue": 1, "orders": 2, "delivered": 3, "returned": 4,
    "cogs": 5, "cogs_unit": 6, "ad_spend": 13, "profit": 14,
}


def safe_float(val):
    """Parse a cell value to float, handling ₹, commas, formulas, blanks."""
    if val is None:
        return 0.0
    s = str(val).replace("₹", "").replace(",", "").strip()
    if s == "" or s == "-":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def safe_int(val):
    return int(safe_float(val))


def read_shiprocket_section(rows):
    """Parse Shiprocket rows into dashboard DATA format."""
    data = {}
    for row in rows:
        product = str(row[SR_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product:
            continue
        rev = safe_float(row[SR_COL["revenue"]])
        orders = safe_int(row[SR_COL["orders"]])
        if orders == 0 and rev == 0:
            continue
        shipped = safe_int(row[SR_COL["shipped"]]) if len(row) > SR_COL["shipped"] else 0
        delivered = safe_int(row[SR_COL["delivered"]]) if len(row) > SR_COL["delivered"] else 0
        rto = safe_int(row[SR_COL["rto"]]) if len(row) > SR_COL["rto"] else 0
        in_transit = safe_int(row[SR_COL["in_transit"]]) if len(row) > SR_COL["in_transit"] else 0
        freight = safe_float(row[SR_COL["freight"]]) if len(row) > SR_COL["freight"] else 0.0
        cancelled = orders - shipped  # no explicit column
        data[product] = {
            "total_orders": orders,
            "shipped": shipped,
            "delivered": delivered,
            "rto": rto,
            "in_transit": in_transit,
            "cancelled": max(cancelled, 0),
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": round(freight, 2),
        }
    return data


def read_amazon_section(rows):
    """Parse Amazon rows into dashboard AMZ_DATA format."""
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 13:
            continue
        product = str(row[AMZ_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Amazon" in product:
            continue
        rev = safe_float(row[AMZ_COL["revenue"]])
        orders = safe_int(row[AMZ_COL["orders"]])
        if orders == 0 and rev == 0:
            continue
        ad = safe_float(row[AMZ_COL["ad_spend"]])
        total_ad_spend += ad
        data[product] = {
            "total_orders": orders,
            "shipped": safe_int(row[AMZ_COL["delivered"]]),  # FBA: delivered = shipped
            "delivered": safe_int(row[AMZ_COL["delivered"]]),
            "rto": 0,
            "in_transit": 0,
            "cancelled": 0,
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,  # Amazon has no freight — fees are in commission/fba/closing
            "ad_spend": round(ad, 2),
            "commission": round(safe_float(row[AMZ_COL["commission"]]) if len(row) > AMZ_COL["commission"] else 0, 2),
            "fba_fees": round(safe_float(row[AMZ_COL["fba_fees"]]) if len(row) > AMZ_COL["fba_fees"] else 0, 2),
            "closing_fee": round(safe_float(row[AMZ_COL["closing_fee"]]) if len(row) > AMZ_COL["closing_fee"] else 0, 2),
            "promos": round(safe_float(row[AMZ_COL["promos"]]) if len(row) > AMZ_COL["promos"] else 0, 2),
            "refund_amt": round(safe_float(row[AMZ_COL["refund_amt"]]) if len(row) > AMZ_COL["refund_amt"] else 0, 2),
        }
    return data, round(total_ad_spend)


def read_flipkart_section(rows):
    """Parse Flipkart rows into dashboard FK_DATA format."""
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 13:
            continue
        product = str(row[FK_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Flipkart" in product:
            continue
        rev = safe_float(row[FK_COL["revenue"]])
        orders = safe_int(row[FK_COL["orders"]])
        if orders == 0 and rev == 0:
            continue
        ad = safe_float(row[FK_COL["ad_spend"]])
        total_ad_spend += ad
        delivered = safe_int(row[FK_COL["delivered"]])
        returned = safe_int(row[FK_COL["returned"]])
        data[product] = {
            "total_orders": orders,
            "shipped": delivered + returned,  # shipped = delivered + returned
            "delivered": delivered,
            "rto": returned,
            "in_transit": 0,
            "cancelled": max(orders - delivered - returned, 0),
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,  # Flipkart fees are in commission/fixed/shipping, not freight
            "ad_spend": round(ad, 2),
            "commission": round(safe_float(row[FK_COL["commission"]]) if len(row) > FK_COL["commission"] else 0, 2),
            "fixed_fee": round(safe_float(row[FK_COL["fixed_fee"]]) if len(row) > FK_COL["fixed_fee"] else 0, 2),
            "shipping_fee": round(safe_float(row[FK_COL["shipping_fee"]]) if len(row) > FK_COL["shipping_fee"] else 0, 2),
            "reverse_shipping": round(safe_float(row[FK_COL["reverse_shipping"]]) if len(row) > FK_COL["reverse_shipping"] else 0, 2),
            "refund_amt": round(safe_float(row[FK_COL["refund_amt"]]) if len(row) > FK_COL["refund_amt"] else 0, 2),
        }
    return data, round(total_ad_spend)


def read_firstcry_section(rows):
    """Parse FirstCry rows into dashboard FC_DATA format."""
    data = {}
    for row in rows:
        if len(row) < 8:
            continue
        product = str(row[FC_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no FirstCry" in product:
            continue
        rev = safe_float(row[FC_COL["revenue"]])
        orders = safe_int(row[FC_COL["orders"]])
        if orders == 0 and rev == 0:
            continue
        delivered = safe_int(row[FC_COL["delivered"]])
        returned = safe_int(row[FC_COL["returned"]]) if len(row) > FC_COL["returned"] else 0
        ad = safe_float(row[FC_COL["ad_spend"]]) if len(row) > FC_COL["ad_spend"] else 0
        data[product] = {
            "total_orders": orders,
            "shipped": delivered,  # FirstCry: delivered = shipped (they handle logistics)
            "delivered": delivered,
            "rto": returned,
            "in_transit": 0,
            "cancelled": 0,
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,  # No freight for FirstCry (they handle shipping)
            "ad_spend": round(ad, 2),
        }
    return data


def read_blinkit_section(rows):
    """Parse Blinkit rows into dashboard BK_DATA format."""
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 7:
            continue
        product = str(row[BK_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "Blinkit Total" in product:
            continue
        rev = safe_float(row[BK_COL["revenue"]])
        orders = safe_int(row[BK_COL["orders"]])
        if orders == 0 and rev == 0:
            continue
        ads = safe_float(row[BK_COL["ads"]])
        logistics = safe_float(row[BK_COL["logistics"]])
        total_ad_spend += ads
        data[product] = {
            "total_orders": orders,
            "shipped": orders,      # PO model: all orders are shipped/delivered
            "delivered": orders,
            "rto": 0,
            "in_transit": 0,
            "cancelled": 0,
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": round(logistics, 2),
            "ad_spend": round(ads, 2),
        }
    return data, round(total_ad_spend)


def read_instamart_section(rows):
    """Parse Instamart rows into dashboard IM_DATA format."""
    data = {}
    total_ad_spend = 0.0

    # Detect if column E is "Returned" or "COGS" by checking the header row
    has_returned_col = False
    for row in rows:
        h = str(row[0]).strip()
        if h == "Products":
            col_e = str(row[4]).strip() if len(row) > 4 else ""
            has_returned_col = (col_e == "Returned")
            break

    for row in rows:
        if len(row) < 8:
            continue
        product = str(row[IM_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Instamart" in product:
            continue
        rev = safe_float(row[IM_COL["revenue"]])
        orders = safe_int(row[IM_COL["orders"]])
        if orders == 0 and rev == 0:
            continue
        delivered = safe_int(row[IM_COL["delivered"]])
        returned = safe_int(row[IM_COL["returned"]]) if has_returned_col and len(row) > IM_COL["returned"] else 0
        ad = safe_float(row[IM_COL["ad_spend"]]) if has_returned_col and len(row) > IM_COL["ad_spend"] else 0
        # When no Returned column, ad_spend is at index 8 (shifted left by 1)
        if not has_returned_col and len(row) > 8:
            ad = safe_float(row[8])  # Ad Spend is col I when no Returned column
        total_ad_spend += ad
        data[product] = {
            "total_orders": orders,
            "shipped": delivered,
            "delivered": delivered,
            "rto": returned,
            "in_transit": 0,
            "cancelled": 0,
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,
            "ad_spend": round(ad, 2),
        }
    return data, round(total_ad_spend)


def read_cred_section(rows):
    """Parse Cred rows into dashboard CRED_DATA format."""
    data = {}
    for row in rows:
        if len(row) < 8:
            continue
        product = str(row[CRED_COL["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Cred" in product:
            continue
        rev = safe_float(row[CRED_COL["revenue"]])
        delivered = safe_int(row[CRED_COL["delivered"]])
        if delivered == 0 and rev == 0:
            continue
        expense = safe_float(row[CRED_COL["expense"]])
        data[product] = {
            "total_orders": delivered,
            "shipped": delivered,
            "delivered": delivered,
            "rto": 0,
            "in_transit": 0,
            "cancelled": 0,
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,
        }
    return data


def fetch_all_months(sh):
    """Read all months from Google Sheets, return D2C, Amazon, Flipkart, FirstCry, Blinkit, Instamart, Cred data and ad maps."""
    d2c_data = {}
    amz_data = {}
    amz_ad_map = {}
    fk_data = {}
    fk_ad_map = {}
    fc_data = {}
    bk_data = {}
    bk_ad_map = {}
    im_data = {}
    im_ad_map = {}
    cred_data = {}

    for month_key, ws_title in MONTHS.items():
        print(f"  Reading {ws_title}...")
        try:
            ws = sh.worksheet(ws_title)
        except gspread.exceptions.WorksheetNotFound:
            print(f"    Not found — skipping")
            d2c_data[month_key] = {}
            amz_data[month_key] = {}
            amz_ad_map[month_key] = 0
            fk_data[month_key] = {}
            fk_ad_map[month_key] = 0
            fc_data[month_key] = {}
            bk_data[month_key] = {}
            bk_ad_map[month_key] = 0
            im_data[month_key] = {}
            im_ad_map[month_key] = 0
            cred_data[month_key] = {}
            continue

        all_values = ws.get_all_values()
        time.sleep(2)  # rate limit

        # Find section markers — track all GRAND TOTAL rows
        sr_grand_total = None
        amz_start = None
        amz_grand_total = None
        fk_start = None
        fk_grand_total = None
        fc_start = None
        fc_grand_total = None
        bk_start = None
        bk_total = None
        im_start = None
        im_grand_total = None
        cred_start = None
        cred_grand_total = None

        for i, row in enumerate(all_values):
            cell = str(row[0]).strip() if row else ""
            if cell in ("GRAND TOTAL", "D2C GRAND TOTAL") and sr_grand_total is None:
                sr_grand_total = i
            if cell == "AMAZON MIS" and amz_start is None:
                amz_start = i
            if cell == "GRAND TOTAL" and amz_start is not None and i > amz_start and amz_grand_total is None:
                amz_grand_total = i
            if cell == "FLIPKART MIS" and fk_start is None:
                fk_start = i
            if cell == "GRAND TOTAL" and fk_start is not None and i > fk_start and fk_grand_total is None:
                fk_grand_total = i
            if cell == "FIRSTCRY MIS" and fc_start is None:
                fc_start = i
            if cell == "GRAND TOTAL" and fc_start is not None and i > fc_start and fc_grand_total is None:
                fc_grand_total = i
            if cell in ("BLINKIT", "BLINKIT MIS") and bk_start is None:
                bk_start = i
            if cell == "Blinkit Total" and bk_start is not None and bk_total is None:
                bk_total = i
            if cell == "INSTAMART MIS" and im_start is None:
                im_start = i
            if cell == "GRAND TOTAL" and im_start is not None and i > im_start and im_grand_total is None:
                im_grand_total = i
            if cell == "CRED MIS" and cred_start is None:
                cred_start = i
            if cell == "GRAND TOTAL" and cred_start is not None and i > cred_start and cred_grand_total is None:
                cred_grand_total = i

        # Parse Shiprocket section (rows 2 to GRAND TOTAL, skip header row 1)
        if sr_grand_total:
            sr_rows = all_values[2:sr_grand_total]
            d2c_data[month_key] = read_shiprocket_section(sr_rows)
            print(f"    D2C: {len(d2c_data[month_key])} products")
        else:
            d2c_data[month_key] = {}
            print(f"    D2C: no GRAND TOTAL found")

        # Parse Amazon section
        if amz_start is not None:
            end = amz_grand_total if amz_grand_total else len(all_values)
            amz_rows = all_values[amz_start + 2 : end]
            amz_data[month_key], amz_ad_map[month_key] = read_amazon_section(amz_rows)
            print(f"    Amazon: {len(amz_data[month_key])} products, ad spend: ₹{amz_ad_map[month_key]:,}")
        else:
            amz_data[month_key] = {}
            amz_ad_map[month_key] = 0
            print(f"    Amazon: no section found")

        # Parse Flipkart section
        if fk_start is not None:
            end = fk_grand_total if fk_grand_total else len(all_values)
            fk_rows = all_values[fk_start + 2 : end]
            fk_data[month_key], fk_ad_map[month_key] = read_flipkart_section(fk_rows)
            print(f"    Flipkart: {len(fk_data[month_key])} products, ad spend: ₹{fk_ad_map[month_key]:,}")
        else:
            fk_data[month_key] = {}
            fk_ad_map[month_key] = 0
            print(f"    Flipkart: no section found")

        # Parse FirstCry section
        if fc_start is not None:
            end = fc_grand_total if fc_grand_total else len(all_values)
            fc_rows = all_values[fc_start + 2 : end]
            fc_data[month_key] = read_firstcry_section(fc_rows)
            print(f"    FirstCry: {len(fc_data[month_key])} products")
        else:
            fc_data[month_key] = {}
            print(f"    FirstCry: no section found")

        # Parse Blinkit section
        if bk_start is not None:
            end = bk_total if bk_total else len(all_values)
            bk_rows = all_values[bk_start + 2 : end]
            bk_data[month_key], bk_ad_map[month_key] = read_blinkit_section(bk_rows)
            print(f"    Blinkit: {len(bk_data[month_key])} products, ad spend: ₹{bk_ad_map[month_key]:,}")
        else:
            bk_data[month_key] = {}
            bk_ad_map[month_key] = 0
            print(f"    Blinkit: no section found")

        # Parse Instamart section
        if im_start is not None:
            end = im_grand_total if im_grand_total else len(all_values)
            im_rows = all_values[im_start + 2 : end]
            im_data[month_key], im_ad_map[month_key] = read_instamart_section(im_rows)
            print(f"    Instamart: {len(im_data[month_key])} products, ad spend: ₹{im_ad_map[month_key]:,}")
        else:
            im_data[month_key] = {}
            im_ad_map[month_key] = 0
            print(f"    Instamart: no section found")

        # Parse Cred section
        if cred_start is not None:
            end = cred_grand_total if cred_grand_total else len(all_values)
            cred_rows = all_values[cred_start + 2 : end]
            cred_data[month_key] = read_cred_section(cred_rows)
            print(f"    Cred: {len(cred_data[month_key])} products")
        else:
            cred_data[month_key] = {}

    return d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data


def to_js_obj(data, include_ad_spend=False):
    """Convert a month's product dict to compact JS object string."""
    parts = []
    for p, d in data.items():
        fields = (
            f"total_orders:{d['total_orders']},"
            f"shipped:{d['shipped']},"
            f"delivered:{d['delivered']},"
            f"rto:{d['rto']},"
            f"in_transit:{d.get('in_transit', 0)},"
            f"cancelled:{d.get('cancelled', 0)},"
            f"lost:{d.get('lost', 0)},"
            f"revenue:{d['revenue']},"
            f"freight:{d['freight']}"
        )
        if include_ad_spend and "ad_spend" in d:
            fields += f",ad_spend:{d['ad_spend']}"
            fields += f",commission:{d.get('commission',0)}"
            fields += f",fba_fees:{d.get('fba_fees',0)}"
            fields += f",closing_fee:{d.get('closing_fee',0)}"
            fields += f",promos:{d.get('promos',0)}"
            fields += f",refund_amt:{d.get('refund_amt',0)}"
        parts.append(f'"{p}":{{{fields}}}')
    return "{" + ",".join(parts) + "}"


def update_dashboard(d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data):
    """Rewrite the inline data section in dashboard.html."""
    with open(DASHBOARD, "r") as f:
        html = f.read()

    # Use ALL_MONTHS for output
    months = ALL_MONTHS

    # Also update the MONTHS array in the HTML
    months_js = "const MONTHS=" + json.dumps(months) + ";"
    html = re.sub(r'const MONTHS=\[.*?\];', months_js, html)

    # ── Preserve FY24-25 frozen data from existing HTML ──
    sync_match = re.search(r'// ── SYNC_DATA_START ──(.*?)// ── SYNC_DATA_END ──', html, re.DOTALL)
    existing_sync = sync_match.group(1) if sync_match else ""

    def extract_frozen(var_pattern, frozen_months, existing_text):
        """Extract FY24-25 month values from existing SYNC_DATA JS."""
        result = {}
        match = re.search(var_pattern + r'\{([^}]*)\}', existing_text)
        if match:
            for m in frozen_months:
                val_match = re.search(rf'"{re.escape(m)}":(\{{[^}}]*\}}|[^,}}]+)', match.group(1))
                if val_match:
                    result[m] = val_match.group(1)
        return result

    # Extract frozen FY24-25 D2C data
    frozen_d2c = extract_frozen(r'DATA=\{', FY24_25_MONTHS, existing_sync)
    frozen_amz_ad = extract_frozen(r'AMZ_AD_MAP=\{', FY24_25_MONTHS, existing_sync)
    frozen_fk_ad = extract_frozen(r'FK_AD_MAP=\{', FY24_25_MONTHS, existing_sync)
    frozen_bk_ad = extract_frozen(r'BK_AD_MAP=\{', FY24_25_MONTHS, existing_sync)
    frozen_im_ad = extract_frozen(r'IM_AD_MAP=\{', FY24_25_MONTHS, existing_sync)

    # Extract frozen channel data (AMZ_DATA, FK_DATA, etc.)
    def extract_frozen_channel(var_name, frozen_months, existing_text):
        """Extract per-month assignment lines for frozen months."""
        result = {}
        for m in frozen_months:
            pat = re.escape(f'{var_name}["{m}"]=') + r'(\{[^;]*\});'
            val_match = re.search(pat, existing_text)
            if val_match:
                result[m] = val_match.group(1)
        return result

    frozen_amz_data = extract_frozen_channel('AMZ_DATA', FY24_25_MONTHS, existing_sync)
    frozen_fk_data = extract_frozen_channel('FK_DATA', FY24_25_MONTHS, existing_sync)
    frozen_fc_data = extract_frozen_channel('FC_DATA', FY24_25_MONTHS, existing_sync)
    frozen_bk_data = extract_frozen_channel('BK_DATA', FY24_25_MONTHS, existing_sync)
    frozen_im_data = extract_frozen_channel('IM_DATA', FY24_25_MONTHS, existing_sync)
    frozen_cred_data = extract_frozen_channel('CRED_DATA', FY24_25_MONTHS, existing_sync)

    # Build DATA= line (D2C)
    d2c_obj = {}
    for m in months:
        if m in frozen_d2c:
            d2c_obj[m] = frozen_d2c[m]  # raw JS from existing HTML
        else:
            d2c_obj[m] = to_js_obj(d2c_data.get(m, {}))
    data_js = "DATA={" + ",".join(f'"{m}":{d2c_obj[m]}' for m in months) + "};"

    # Helper: get ad map value (frozen or fresh)
    def ad_val(ad_map_fresh, frozen_ad, m):
        if m in frozen_ad:
            return frozen_ad[m]
        return str(ad_map_fresh.get(m, 0))

    # Build AMZ_AD_MAP line
    ad_map_js = "const AMZ_AD_MAP={" + ",".join(
        f'"{m}":{ad_val(amz_ad_map, frozen_amz_ad, m)}' for m in months
    ) + "};"

    # Build AMZ_DATA lines
    amz_lines = []
    amz_init = ",".join(f'"{m}":{{}}' for m in months)
    amz_lines.append(f'const AMZ_DATA={{{amz_init}}};')
    for m in months:
        if m in frozen_amz_data:
            amz_lines.append(f'AMZ_DATA["{m}"]={frozen_amz_data[m]};')
        elif amz_data.get(m):
            amz_lines.append(f'AMZ_DATA["{m}"]={to_js_obj(amz_data[m], include_ad_spend=True)};')

    # Build FK_AD_MAP line
    fk_ad_map_js = "const FK_AD_MAP={" + ",".join(
        f'"{m}":{ad_val(fk_ad_map, frozen_fk_ad, m)}' for m in months
    ) + "};"

    # Build FK_DATA lines
    fk_lines = []
    fk_init = ",".join(f'"{m}":{{}}' for m in months)
    fk_lines.append(f'const FK_DATA={{{fk_init}}};')
    for m in months:
        if m in frozen_fk_data:
            fk_lines.append(f'FK_DATA["{m}"]={frozen_fk_data[m]};')
        elif fk_data.get(m):
            fk_lines.append(f'FK_DATA["{m}"]={to_js_obj(fk_data[m], include_ad_spend=True)};')

    # Build FC_DATA lines (FirstCry)
    fc_lines = []
    fc_init = ",".join(f'"{m}":{{}}' for m in months)
    fc_lines.append(f'const FC_DATA={{{fc_init}}};')
    for m in months:
        if m in frozen_fc_data:
            fc_lines.append(f'FC_DATA["{m}"]={frozen_fc_data[m]};')
        elif fc_data.get(m):
            fc_lines.append(f'FC_DATA["{m}"]={to_js_obj(fc_data[m])};')

    # Build BK_AD_MAP line
    bk_ad_map_js = "const BK_AD_MAP={" + ",".join(
        f'"{m}":{ad_val(bk_ad_map, frozen_bk_ad, m)}' for m in months
    ) + "};"

    # Build BK_DATA lines (Blinkit)
    bk_lines = []
    bk_init = ",".join(f'"{m}":{{}}' for m in months)
    bk_lines.append(f'const BK_DATA={{{bk_init}}};')
    for m in months:
        if m in frozen_bk_data:
            bk_lines.append(f'BK_DATA["{m}"]={frozen_bk_data[m]};')
        elif bk_data.get(m):
            bk_lines.append(f'BK_DATA["{m}"]={to_js_obj(bk_data[m], include_ad_spend=True)};')

    # Build IM_AD_MAP line
    im_ad_map_js = "const IM_AD_MAP={" + ",".join(
        f'"{m}":{ad_val(im_ad_map, frozen_im_ad, m)}' for m in months
    ) + "};"

    # Build IM_DATA lines (Instamart)
    im_lines = []
    im_init = ",".join(f'"{m}":{{}}' for m in months)
    im_lines.append(f'const IM_DATA={{{im_init}}};')
    for m in months:
        if m in frozen_im_data:
            im_lines.append(f'IM_DATA["{m}"]={frozen_im_data[m]};')
        elif im_data.get(m):
            im_lines.append(f'IM_DATA["{m}"]={to_js_obj(im_data[m], include_ad_spend=True)};')

    # Build CRED_DATA lines
    cred_lines = []
    cred_init = ",".join(f'"{m}":{{}}' for m in months)
    cred_lines.append(f'const CRED_DATA={{{cred_init}}};')
    for m in months:
        if m in frozen_cred_data:
            cred_lines.append(f'CRED_DATA["{m}"]={frozen_cred_data[m]};')
        elif cred_data.get(m):
            cred_lines.append(f'CRED_DATA["{m}"]={to_js_obj(cred_data[m])};')

    # Replace inline data section between explicit markers
    pattern = r'// ── SYNC_DATA_START ──.*?// ── SYNC_DATA_END ──'
    replacement = (
        "// ── SYNC_DATA_START ──\n"
        f"{ad_map_js}\n"
        f"{fk_ad_map_js}\n"
        f"{bk_ad_map_js}\n"
        f"{im_ad_map_js}\n\n"
        f"{data_js}\n"
        "// ── Build Amazon DATA in same format as D2C ──────────────\n"
        + "\n".join(amz_lines) + "\n"
        "// ── Build Flipkart DATA in same format as D2C ──────────────\n"
        + "\n".join(fk_lines) + "\n"
        "// ── Build FirstCry DATA in same format as D2C ──────────────\n"
        + "\n".join(fc_lines) + "\n"
        "// ── Build Blinkit DATA in same format as D2C ──────────────\n"
        + "\n".join(bk_lines) + "\n"
        "// ── Build Instamart DATA in same format as D2C ──────────────\n"
        + "\n".join(im_lines) + "\n"
        "// ── Build Cred DATA in same format as D2C ──────────────\n"
        + "\n".join(cred_lines) + "\n"
        "// ── SYNC_DATA_END ──"
    )

    match = re.search(pattern, html, flags=re.DOTALL)
    if not match:
        print("\n⚠️  Could not find inline data section to replace!")
        return False
    new_html = html[:match.start()] + replacement + html[match.end():]

    # ── SAFETY CHECK 1: Verify no data was lost ──
    # Count total D2C revenue in new data vs existing file
    import subprocess
    old_rev_match = re.findall(r'revenue:(\d+(?:\.\d+)?)', html[match.start():match.end()])
    new_rev_match = re.findall(r'revenue:(\d+(?:\.\d+)?)', replacement)
    old_total = sum(float(r) for r in old_rev_match) if old_rev_match else 0
    new_total = sum(float(r) for r in new_rev_match) if new_rev_match else 0
    if old_total > 0 and new_total < old_total * 0.5:
        print(f"\n❌ SAFETY ABORT: New data has significantly less revenue (₹{new_total:,.0f}) than existing (₹{old_total:,.0f})")
        print("   This likely means data was lost. Skipping write to protect dashboard.")
        return False

    # ── SAFETY CHECK 2: Validate JS syntax before writing ──
    # Extract the script block and check with node
    script_start = new_html.find('<script>', new_html.find('<body'))
    script_end = new_html.find('</script>', script_start)
    if script_start > 0 and script_end > 0:
        js_code = new_html[script_start + 8:script_end]
        tmp_js = os.path.join(BASE, '.tmp_syntax_check.js')
        try:
            with open(tmp_js, 'w') as f:
                f.write(js_code)
            result = subprocess.run(['node', '--check', tmp_js], capture_output=True, text=True)
            if result.returncode != 0:
                print(f"\n❌ SAFETY ABORT: JS syntax error detected in generated dashboard!")
                print(f"   {result.stderr.strip()}")
                print("   Skipping write to protect dashboard.")
                return False
        finally:
            if os.path.exists(tmp_js):
                os.remove(tmp_js)

    with open(DASHBOARD, "w") as f:
        f.write(new_html)

    return True


def verify_password():
    """Require password before allowing monthly MIS changes."""
    import getpass, hashlib
    PASS_HASH = "a0f3285b07c26c0dcd2191447f391170d06035e8d57e31a048ba87074f3a9a15"
    pw = getpass.getpass("\n🔒 Enter password to unlock Monthly MIS changes: ")
    if hashlib.sha256(pw.encode()).hexdigest() != PASS_HASH:
        print("❌ Wrong password. Monthly MIS update blocked.")
        return False
    print("✅ Password accepted. Proceeding with sync...\n")
    return True


def main():
    if not verify_password():
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    print("\n🔄 Syncing dashboard from Google Sheets\n")

    d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data = fetch_all_months(sh)

    print("\n📝 Updating dashboard.html...")
    if update_dashboard(d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data):
        total_d2c = sum(len(v) for v in d2c_data.values())
        total_amz = sum(len(v) for v in amz_data.values())
        total_fk = sum(len(v) for v in fk_data.values())
        total_fc = sum(len(v) for v in fc_data.values())
        total_bk = sum(len(v) for v in bk_data.values())
        total_im = sum(len(v) for v in im_data.values())
        total_cred = sum(len(v) for v in cred_data.values())
        print(f"\n✅ Dashboard synced! {total_d2c} D2C + {total_amz} Amazon + {total_fk} Flipkart + {total_fc} FirstCry + {total_bk} Blinkit + {total_im} Instamart + {total_cred} Cred products across {len(MONTHS)} months")
        print(f"   Open dashboard.html in browser to see updated data")
    else:
        print("\n❌ Failed to update dashboard")


if __name__ == "__main__":
    main()
