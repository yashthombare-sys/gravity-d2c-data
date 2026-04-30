#!/usr/bin/env python3
"""
Sync dashboard.html with data from Google Sheets.

Reads Shiprocket (D2C), Amazon, Flipkart, FirstCry, Blinkit, Instamart, and Cred sections
from each month's tab, then rewrites the inline data in dashboard.html.

Usage:  python3 sync_dashboard.py
"""

import os, re, json, time, shutil, subprocess
import gspread
from google.oauth2.service_account import Credentials

BASE = os.path.dirname(os.path.abspath(__file__))
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
FY24_25_SHEET_KEY = "1-ICA_vT55I_Mu9ZVcWxKKFui3F7gil7sPNeCiZ-21s0"
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
    "Apr 2026": "April 2026 MIS",
}

# FY 2024-25 months — Google Sheet tabs no longer exist; data read from local backup JSON
FY24_25_MONTHS_MAP = {
    "Apr 2024": "April 2024 MIS",
    "May 2024": "May 2024 MIS",
    "Jun 2024": "June 2024 MIS",
    "Jul 2024": "July 2024 MIS",
    "Aug 2024": "August 2024 MIS",
    "Sep 2024": "September 2024 MIS",
    "Oct 2024": "October 2024 MIS",
    "Nov 2024": "November 2024 MIS",
    "Dec 2024": "December 2024 MIS",
    "Jan 2025": "January 2025 MIS",
    "Feb 2025": "February 2025 MIS",
    "Mar 2025": "March 2025 MIS",
}
FY24_25_MONTHS = []  # empty — FY24-25 data injected from JSON, not from frozen HTML
FY24_25_JSON = os.path.join(BASE, "fy2024_25_data.json")

# Full list for dashboard output (both FYs)
ALL_MONTHS = list(FY24_25_MONTHS_MAP.keys()) + list(MONTHS.keys())

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

# ── Header aliases for dynamic column detection ──────────────────────────────
# Converts section header rows → column indices. First matching alias wins.
# Falls back to the *_COL hardcoded dicts above if no header match is found.
SR_HEADER_MAP = {
    "revenue":    ["revenue", "net revenue", "total revenue"],
    "orders":     ["total orders", "new orders", "orders", "total product orders"],
    "shipped":    ["shipped", "total shipped", "dispatched"],
    "delivered":  ["delivered", "total delivered", "net delivered"],
    "rto":        ["rto", "total rto", "returned", "returns"],
    "in_transit": ["in transit", "in-transit", "in_transit", "transit"],
    "freight":    ["shipping charges", "freight", "logistics", "shipping cost", "logistic", "delivery charges"],
}
AMZ_HEADER_MAP = {
    "revenue":     ["revenue", "net revenue", "total revenue", "sales"],
    "orders":      ["orders", "total orders", "gross orders", "ordered units"],
    "delivered":   ["delivered", "units delivered", "total delivered", "shipped"],
    "cogs":        ["cogs", "total cogs", "product cost", "cost of goods"],
    "cogs_unit":   ["cogs/unit", "cogs per unit", "unit cogs", "cost per unit"],
    "commission":  ["commission", "commissions", "referral fee", "marketplace commission"],
    "fba_fees":    ["fba fees", "fba fee", "fulfillment fees", "fulfillment fee", "amazon fulfillment"],
    "closing_fee": ["closing fee", "closing fees", "variable closing"],
    "promos":      ["promos", "promotions", "promotion", "coupon"],
    "refund_amt":  ["refund", "refunds", "refund amount", "refund amt", "return amount"],
    "amazon_fees": ["total amazon fees", "amazon fees", "total fees", "platform fees"],
    "ad_spend":    ["ad spend", "ad spent", "advertising", "total ad spend", "sponsored"],
    "profit":      ["profit", "net profit", "p/l", "profit/loss"],
    "profit_pct":  ["profit %", "profit%", "p/l %", "margin %", "net margin"],
}
FK_HEADER_MAP = {
    "revenue":          ["revenue", "net revenue", "total revenue", "sales"],
    "orders":           ["orders", "total orders", "gross orders"],
    "delivered":        ["delivered", "total delivered", "net delivered"],
    "returned":         ["returned", "returns", "total returned", "rto"],
    "cogs":             ["cogs", "total cogs", "product cost"],
    "cogs_unit":        ["cogs/unit", "cogs per unit", "unit cogs"],
    "commission":       ["commission", "commissions", "tech fee", "marketplace commission"],
    "fixed_fee":        ["fixed fee", "fixed fees", "collection fee", "pickup fee"],
    "shipping_fee":     ["shipping fee", "forward shipping", "forward freight", "forward logistic"],
    "reverse_shipping": ["reverse shipping", "return shipping", "reverse logistic"],
    "refund_amt":       ["refund", "refunds", "refund amount", "refund amt"],
    "fk_fees":          ["total flipkart fees", "total fk fees", "platform fees"],
    "ad_spend":         ["ad spend", "ad spent", "advertising", "marketing spend"],
    "profit":           ["profit", "net profit", "p/l"],
}
FC_HEADER_MAP = {
    "revenue":   ["revenue", "net revenue", "total revenue", "sales"],
    "orders":    ["orders", "total orders", "gross orders"],
    "delivered": ["delivered", "total delivered", "net delivered"],
    "returned":  ["returned", "returns", "total returned"],
    "cogs":      ["cogs", "total cogs", "product cost"],
    "cogs_unit": ["cogs/unit", "cogs per unit"],
    "ad_spend":  ["ad spend", "ad spent", "advertising", "marketing"],
    "profit":    ["profit", "net profit", "p/l"],
}
BK_HEADER_MAP = {
    "revenue":    ["revenue", "net revenue", "sales"],
    "orders":     ["orders", "total orders"],
    "cogs":       ["cogs", "product cost"],
    "cogs_unit":  ["cogs/unit", "cogs per unit"],
    "ads":        ["ads", "ad spend", "advertising", "blended ads", "marketing"],
    "logistics":  ["logistics", "delivery", "freight", "logistic cost"],
    "profit":     ["profit", "net profit", "p/l"],
    "profit_pct": ["profit %", "profit%", "margin", "margin %"],
}
IM_HEADER_MAP = {
    "revenue":   ["revenue", "net revenue", "sales"],
    "orders":    ["orders", "total orders"],
    "delivered": ["delivered", "total delivered"],
    "returned":  ["returned", "returns", "total returned"],
    "cogs":      ["cogs", "product cost"],
    "cogs_unit": ["cogs/unit", "cogs per unit"],
    "ad_spend":  ["ad spend", "ad spent", "advertising", "marketing"],
    "profit":    ["profit", "net profit", "p/l"],
}
CRED_HEADER_MAP = {
    "revenue":   ["revenue", "net revenue", "sales"],
    "expense":   ["total expense", "expense", "total expenses", "total spend"],
    "delivered": ["delivered", "total delivered"],
}


def find_cols(header_row, header_map, fallback):
    """Detect column indices from a section's header row using alias matching.
    Falls back to hardcoded fallback dict for any field whose header isn't found."""
    col_map = {"product": 0}  # product name always in col A
    header_lower = [str(h).strip().lower() for h in header_row]
    for field, candidates in header_map.items():
        found = False
        for candidate in candidates:
            cl = candidate.lower()
            for idx, h in enumerate(header_lower):
                if cl == h or cl in h:
                    col_map[field] = idx
                    found = True
                    break
            if found:
                break
        if not found:
            col_map[field] = fallback.get(field, 0)
    return col_map


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


def read_shiprocket_section(rows, col_map=None):
    """Parse Shiprocket rows into dashboard DATA format."""
    if col_map is None:
        col_map = SR_COL
    data = {}
    for row in rows:
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product:
            continue
        rev = safe_float(row[col_map["revenue"]])
        orders = safe_int(row[col_map["orders"]])
        if orders == 0 and rev == 0:
            continue
        shipped = safe_int(row[col_map["shipped"]]) if len(row) > col_map["shipped"] else 0
        delivered = safe_int(row[col_map["delivered"]]) if len(row) > col_map["delivered"] else 0
        rto = safe_int(row[col_map["rto"]]) if len(row) > col_map["rto"] else 0
        in_transit = safe_int(row[col_map["in_transit"]]) if len(row) > col_map["in_transit"] else 0
        freight = safe_float(row[col_map["freight"]]) if len(row) > col_map["freight"] else 0.0
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


def read_amazon_section(rows, col_map=None):
    """Parse Amazon rows into dashboard AMZ_DATA format."""
    if col_map is None:
        col_map = AMZ_COL
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 5:
            continue
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Amazon" in product:
            continue
        rev = safe_float(row[col_map["revenue"]]) if len(row) > col_map["revenue"] else 0
        orders = safe_int(row[col_map["orders"]]) if len(row) > col_map["orders"] else 0
        if orders == 0 and rev == 0:
            continue
        ad = safe_float(row[col_map["ad_spend"]]) if len(row) > col_map["ad_spend"] else 0
        total_ad_spend += ad
        delivered_idx = col_map["delivered"]
        data[product] = {
            "total_orders": orders,
            "shipped": safe_int(row[delivered_idx]) if len(row) > delivered_idx else 0,
            "delivered": safe_int(row[delivered_idx]) if len(row) > delivered_idx else 0,
            "rto": 0,
            "in_transit": 0,
            "cancelled": 0,
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,
            "ad_spend": round(ad, 2),
            "commission": round(safe_float(row[col_map["commission"]]) if len(row) > col_map["commission"] else 0, 2),
            "fba_fees": round(safe_float(row[col_map["fba_fees"]]) if len(row) > col_map["fba_fees"] else 0, 2),
            "closing_fee": round(safe_float(row[col_map["closing_fee"]]) if len(row) > col_map["closing_fee"] else 0, 2),
            "promos": round(safe_float(row[col_map["promos"]]) if len(row) > col_map["promos"] else 0, 2),
            "refund_amt": round(safe_float(row[col_map["refund_amt"]]) if len(row) > col_map["refund_amt"] else 0, 2),
        }
    return data, round(total_ad_spend)


def read_flipkart_section(rows, col_map=None):
    """Parse Flipkart rows into dashboard FK_DATA format."""
    if col_map is None:
        col_map = FK_COL
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 5:
            continue
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Flipkart" in product:
            continue
        rev = safe_float(row[col_map["revenue"]]) if len(row) > col_map["revenue"] else 0
        orders = safe_int(row[col_map["orders"]]) if len(row) > col_map["orders"] else 0
        if orders == 0 and rev == 0:
            continue
        ad = safe_float(row[col_map["ad_spend"]]) if len(row) > col_map["ad_spend"] else 0
        total_ad_spend += ad
        delivered = safe_int(row[col_map["delivered"]]) if len(row) > col_map["delivered"] else 0
        returned = safe_int(row[col_map["returned"]]) if len(row) > col_map["returned"] else 0
        data[product] = {
            "total_orders": orders,
            "shipped": delivered + returned,
            "delivered": delivered,
            "rto": returned,
            "in_transit": 0,
            "cancelled": max(orders - delivered - returned, 0),
            "lost": 0,
            "revenue": round(rev, 2),
            "freight": 0,
            "ad_spend": round(ad, 2),
            "commission": round(safe_float(row[col_map["commission"]]) if len(row) > col_map["commission"] else 0, 2),
            "fixed_fee": round(safe_float(row[col_map["fixed_fee"]]) if len(row) > col_map["fixed_fee"] else 0, 2),
            "shipping_fee": round(safe_float(row[col_map["shipping_fee"]]) if len(row) > col_map["shipping_fee"] else 0, 2),
            "reverse_shipping": round(safe_float(row[col_map["reverse_shipping"]]) if len(row) > col_map["reverse_shipping"] else 0, 2),
            "refund_amt": round(safe_float(row[col_map["refund_amt"]]) if len(row) > col_map["refund_amt"] else 0, 2),
        }
    return data, round(total_ad_spend)


def read_firstcry_section(rows, col_map=None):
    """Parse FirstCry rows into dashboard FC_DATA format."""
    if col_map is None:
        col_map = FC_COL
    data = {}
    for row in rows:
        if len(row) < 4:
            continue
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no FirstCry" in product:
            continue
        rev = safe_float(row[col_map["revenue"]]) if len(row) > col_map["revenue"] else 0
        orders = safe_int(row[col_map["orders"]]) if len(row) > col_map["orders"] else 0
        if orders == 0 and rev == 0:
            continue
        delivered = safe_int(row[col_map["delivered"]]) if len(row) > col_map["delivered"] else 0
        returned = safe_int(row[col_map["returned"]]) if len(row) > col_map.get("returned", 999) else 0
        ad = safe_float(row[col_map["ad_spend"]]) if len(row) > col_map.get("ad_spend", 999) else 0
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
    return data


def read_blinkit_section(rows, col_map=None):
    """Parse Blinkit rows into dashboard BK_DATA format."""
    if col_map is None:
        col_map = BK_COL
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 4:
            continue
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "Blinkit Total" in product:
            continue
        rev = safe_float(row[col_map["revenue"]]) if len(row) > col_map["revenue"] else 0
        orders = safe_int(row[col_map["orders"]]) if len(row) > col_map["orders"] else 0
        if orders == 0 and rev == 0:
            continue
        ads = safe_float(row[col_map["ads"]]) if len(row) > col_map.get("ads", 999) else 0
        logistics = safe_float(row[col_map["logistics"]]) if len(row) > col_map.get("logistics", 999) else 0
        total_ad_spend += ads
        data[product] = {
            "total_orders": orders,
            "shipped": orders,
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


def read_instamart_section(rows, col_map=None):
    """Parse Instamart rows into dashboard IM_DATA format."""
    if col_map is None:
        col_map = IM_COL
    data = {}
    total_ad_spend = 0.0
    for row in rows:
        if len(row) < 4:
            continue
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Instamart" in product:
            continue
        rev = safe_float(row[col_map["revenue"]]) if len(row) > col_map["revenue"] else 0
        orders = safe_int(row[col_map["orders"]]) if len(row) > col_map["orders"] else 0
        if orders == 0 and rev == 0:
            continue
        delivered = safe_int(row[col_map["delivered"]]) if len(row) > col_map["delivered"] else 0
        returned = safe_int(row[col_map["returned"]]) if len(row) > col_map.get("returned", 999) else 0
        ad = safe_float(row[col_map["ad_spend"]]) if len(row) > col_map.get("ad_spend", 999) else 0
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


def read_cred_section(rows, col_map=None):
    """Parse Cred rows into dashboard CRED_DATA format."""
    if col_map is None:
        col_map = CRED_COL
    data = {}
    for row in rows:
        if len(row) < 3:
            continue
        product = str(row[col_map["product"]]).strip()
        if not product or product in SKIP_LABELS:
            continue
        if "Subtotal" in product or "CATEGORY" in product or "no Cred" in product:
            continue
        rev = safe_float(row[col_map["revenue"]]) if len(row) > col_map["revenue"] else 0
        delivered = safe_int(row[col_map["delivered"]]) if len(row) > col_map.get("delivered", 999) else 0
        if delivered == 0 and rev == 0:
            continue
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


def _find_sections_and_parse(all_values, month_key, d2c_data, amz_data, amz_ad_map,
                              fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map,
                              im_data, im_ad_map, cred_data):
    """Shared section-finding + parsing logic for both Google Sheets and JSON data."""
    sr_grand_total = amz_start = amz_grand_total = None
    fk_start = fk_grand_total = fc_start = fc_grand_total = None
    bk_start = bk_total = im_start = im_grand_total = cred_start = cred_grand_total = None

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

    sr_header = all_values[1] if len(all_values) > 1 else []
    sr_col_map = find_cols(sr_header, SR_HEADER_MAP, SR_COL)

    def _header(start):
        idx = start + 1 if start is not None else None
        return all_values[idx] if idx is not None and idx < len(all_values) else []

    amz_col_map  = find_cols(_header(amz_start),  AMZ_HEADER_MAP,  AMZ_COL)
    fk_col_map   = find_cols(_header(fk_start),   FK_HEADER_MAP,   FK_COL)
    fc_col_map   = find_cols(_header(fc_start),   FC_HEADER_MAP,   FC_COL)
    bk_col_map   = find_cols(_header(bk_start),   BK_HEADER_MAP,   BK_COL)
    im_col_map   = find_cols(_header(im_start),   IM_HEADER_MAP,   IM_COL)
    cred_col_map = find_cols(_header(cred_start), CRED_HEADER_MAP, CRED_COL)

    if sr_grand_total:
        d2c_data[month_key] = read_shiprocket_section(all_values[2:sr_grand_total], sr_col_map)
    else:
        d2c_data[month_key] = {}

    if amz_start is not None:
        end = amz_grand_total if amz_grand_total else len(all_values)
        amz_data[month_key], amz_ad_map[month_key] = read_amazon_section(all_values[amz_start + 2:end], amz_col_map)
    else:
        amz_data[month_key] = {}; amz_ad_map[month_key] = 0

    if fk_start is not None:
        end = fk_grand_total if fk_grand_total else len(all_values)
        fk_data[month_key], fk_ad_map[month_key] = read_flipkart_section(all_values[fk_start + 2:end], fk_col_map)
    else:
        fk_data[month_key] = {}; fk_ad_map[month_key] = 0

    if fc_start is not None:
        end = fc_grand_total if fc_grand_total else len(all_values)
        fc_data[month_key] = read_firstcry_section(all_values[fc_start + 2:end], fc_col_map)
    else:
        fc_data[month_key] = {}

    if bk_start is not None:
        end = bk_total if bk_total else len(all_values)
        bk_data[month_key], bk_ad_map[month_key] = read_blinkit_section(all_values[bk_start + 2:end], bk_col_map)
    else:
        bk_data[month_key] = {}; bk_ad_map[month_key] = 0

    if im_start is not None:
        end = im_grand_total if im_grand_total else len(all_values)
        im_data[month_key], im_ad_map[month_key] = read_instamart_section(all_values[im_start + 2:end], im_col_map)
    else:
        im_data[month_key] = {}; im_ad_map[month_key] = 0

    cred_data[month_key] = {}


def fetch_fy24_25_from_json():
    """Read FY24-25 MIS data from fy2024_25_data.json (Google Sheet tabs no longer exist)."""
    d2c_data = {}; amz_data = {}; amz_ad_map = {}
    fk_data = {};  fk_ad_map = {}; fc_data = {}
    bk_data = {};  bk_ad_map = {}; im_data = {}; im_ad_map = {}; cred_data = {}

    if not os.path.exists(FY24_25_JSON):
        print(f"  Warning: {FY24_25_JSON} not found — FY24-25 data will be empty")
        for mk in FY24_25_MONTHS_MAP:
            d2c_data[mk] = {}; amz_data[mk] = {}; amz_ad_map[mk] = 0
            fk_data[mk] = {}; fk_ad_map[mk] = 0; fc_data[mk] = {}
            bk_data[mk] = {}; bk_ad_map[mk] = 0; im_data[mk] = {}; im_ad_map[mk] = 0; cred_data[mk] = {}
        return d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data

    with open(FY24_25_JSON) as f:
        backup = json.load(f)

    for month_key, tab_name in FY24_25_MONTHS_MAP.items():
        if tab_name not in backup:
            print(f"  FY24-25 {tab_name}: missing from backup JSON")
            d2c_data[month_key] = {}; amz_data[month_key] = {}; amz_ad_map[month_key] = 0
            fk_data[month_key] = {}; fk_ad_map[month_key] = 0; fc_data[month_key] = {}
            bk_data[month_key] = {}; bk_ad_map[month_key] = 0; im_data[month_key] = {}; im_ad_map[month_key] = 0; cred_data[month_key] = {}
            continue
        all_values = backup[tab_name]
        _find_sections_and_parse(all_values, month_key, d2c_data, amz_data, amz_ad_map,
                                 fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map,
                                 im_data, im_ad_map, cred_data)
        print(f"  FY24-25 {tab_name}: D2C={len(d2c_data[month_key])} prods, AMZ={len(amz_data[month_key])} prods")

    return d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data


def fetch_dashboard_metrics(sh):
    """Read Overall Ad Spend, CM2, and Ex Tax Revenue per month from the Dashboard tab.
    Returns (overall_ad_spend, cm2_by_month, rev_ex_by_month).
    """
    try:
        ws = sh.worksheet('Dashboard')
        rows = ws.get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        print("  Dashboard tab not found — adSpent/CM2 will not be updated")
        return {}, {}, {}

    header = rows[0] if rows else []
    col_to_month = {}
    for i, cell in enumerate(header):
        cell = str(cell).strip()
        if '-' in cell and len(cell) == 6:
            mon, yr = cell.split('-')
            col_to_month[i] = f"{mon} 20{yr}"

    def _extract_row(label):
        for row in rows:
            if str(row[0]).strip() == label:
                return {mk: safe_float(row[ci]) for ci, mk in col_to_month.items() if ci < len(row)}
        return {}

    overall  = _extract_row('Overall Ad Spend (Inc Tax)')
    cm2      = _extract_row('CM2 (Contribution Margin 2)')
    rev_ex   = _extract_row('Total Net Revenue (Ex Tax)')

    print(f"  Dashboard tab: Ad Spend {len(overall)}m, CM2 {len(cm2)}m, Rev(Ex) {len(rev_ex)}m")
    return overall, cm2, rev_ex


def fetch_fy24_25_cm2(gc):
    """Read CM2 row from each FY24-25 monthly tab in the old sheet."""
    try:
        old_sh = gc.open_by_key(FY24_25_SHEET_KEY)
    except Exception as e:
        print(f"  FY24-25 old sheet not accessible: {e}")
        return {}
    result = {}
    for month_key, tab_name in FY24_25_MONTHS_MAP.items():
        try:
            ws = old_sh.worksheet(tab_name)
            rows = ws.get_all_values()
            for row in rows:
                if row and str(row[0]).strip() == 'CM2':
                    val = safe_float(re.sub(r'[₹,\s]', '', str(row[1]))) if len(row) > 1 else None
                    if val is not None:
                        result[month_key] = val
                    break
        except Exception as e:
            print(f"  FY24-25 {tab_name}: {e}")
    print(f"  FY24-25 CM2: {len(result)} months read from old sheet")
    return result


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

        # Build column maps from each section's header row (row immediately after the section title)
        # Falls back to hardcoded *_COL dicts if header names don't match aliases
        sr_header = all_values[1] if len(all_values) > 1 else []
        sr_col_map = find_cols(sr_header, SR_HEADER_MAP, SR_COL)

        def _header(start):
            idx = start + 1 if start is not None else None
            return all_values[idx] if idx is not None and idx < len(all_values) else []

        amz_col_map  = find_cols(_header(amz_start),  AMZ_HEADER_MAP,  AMZ_COL)
        fk_col_map   = find_cols(_header(fk_start),   FK_HEADER_MAP,   FK_COL)
        fc_col_map   = find_cols(_header(fc_start),   FC_HEADER_MAP,   FC_COL)
        bk_col_map   = find_cols(_header(bk_start),   BK_HEADER_MAP,   BK_COL)
        im_col_map   = find_cols(_header(im_start),   IM_HEADER_MAP,   IM_COL)
        cred_col_map = find_cols(_header(cred_start), CRED_HEADER_MAP, CRED_COL)

        # Parse Shiprocket section (rows 2 to GRAND TOTAL, skip header row 1)
        if sr_grand_total:
            sr_rows = all_values[2:sr_grand_total]
            d2c_data[month_key] = read_shiprocket_section(sr_rows, sr_col_map)
            print(f"    D2C: {len(d2c_data[month_key])} products")
        else:
            d2c_data[month_key] = {}
            print(f"    D2C: no GRAND TOTAL found")

        # Parse Amazon section
        if amz_start is not None:
            end = amz_grand_total if amz_grand_total else len(all_values)
            amz_rows = all_values[amz_start + 2 : end]
            amz_data[month_key], amz_ad_map[month_key] = read_amazon_section(amz_rows, amz_col_map)
            print(f"    Amazon: {len(amz_data[month_key])} products, ad spend: ₹{amz_ad_map[month_key]:,}")
        else:
            amz_data[month_key] = {}
            amz_ad_map[month_key] = 0
            print(f"    Amazon: no section found")

        # Parse Flipkart section
        if fk_start is not None:
            end = fk_grand_total if fk_grand_total else len(all_values)
            fk_rows = all_values[fk_start + 2 : end]
            fk_data[month_key], fk_ad_map[month_key] = read_flipkart_section(fk_rows, fk_col_map)
            print(f"    Flipkart: {len(fk_data[month_key])} products, ad spend: ₹{fk_ad_map[month_key]:,}")
        else:
            fk_data[month_key] = {}
            fk_ad_map[month_key] = 0
            print(f"    Flipkart: no section found")

        # Parse FirstCry section
        if fc_start is not None:
            end = fc_grand_total if fc_grand_total else len(all_values)
            fc_rows = all_values[fc_start + 2 : end]
            fc_data[month_key] = read_firstcry_section(fc_rows, fc_col_map)
            print(f"    FirstCry: {len(fc_data[month_key])} products")
        else:
            fc_data[month_key] = {}
            print(f"    FirstCry: no section found")

        # Parse Blinkit section
        if bk_start is not None:
            end = bk_total if bk_total else len(all_values)
            bk_rows = all_values[bk_start + 2 : end]
            bk_data[month_key], bk_ad_map[month_key] = read_blinkit_section(bk_rows, bk_col_map)
            print(f"    Blinkit: {len(bk_data[month_key])} products, ad spend: ₹{bk_ad_map[month_key]:,}")
        else:
            bk_data[month_key] = {}
            bk_ad_map[month_key] = 0
            print(f"    Blinkit: no section found")

        # Parse Instamart section
        if im_start is not None:
            end = im_grand_total if im_grand_total else len(all_values)
            im_rows = all_values[im_start + 2 : end]
            im_data[month_key], im_ad_map[month_key] = read_instamart_section(im_rows, im_col_map)
            print(f"    Instamart: {len(im_data[month_key])} products, ad spend: ₹{im_ad_map[month_key]:,}")
        else:
            im_data[month_key] = {}
            im_ad_map[month_key] = 0
            print(f"    Instamart: no section found")

        # Parse Cred section
        if cred_start is not None:
            end = cred_grand_total if cred_grand_total else len(all_values)
            cred_rows = all_values[cred_start + 2 : end]
            cred_data[month_key] = read_cred_section(cred_rows, cred_col_map)
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


def update_dashboard(d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data, d2c_ad_spend=None, cm2_data=None, rev_ex_data=None):
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
        """Extract FY24-25 month values from existing SYNC_DATA JS (flat objects only)."""
        result = {}
        match = re.search(var_pattern + r'\{([^}]*)\}', existing_text)
        if match:
            for m in frozen_months:
                val_match = re.search(rf'"{re.escape(m)}":(\{{[^}}]*\}}|[^,}}]+)', match.group(1))
                if val_match:
                    result[m] = val_match.group(1)
        return result

    def extract_frozen_d2c(existing_text, frozen_months):
        """Extract FY24-25 D2C data from DATA={...} using brace balancing (handles nested objects)."""
        result = {}
        data_match = re.search(r'DATA=(\{.*\});', existing_text)
        if not data_match:
            return result
        data_str = data_match.group(1)
        for m in frozen_months:
            key = f'"{m}":'
            start = data_str.find(key)
            if start == -1:
                continue
            val_start = start + len(key)
            depth = 0
            for i in range(val_start, len(data_str)):
                if data_str[i] == '{': depth += 1
                elif data_str[i] == '}': depth -= 1
                if depth == 0:
                    result[m] = data_str[val_start:i+1]
                    break
        return result

    # Extract frozen FY24-25 D2C data (needs brace balancing for nested product objects)
    frozen_d2c = extract_frozen_d2c(existing_sync, FY24_25_MONTHS)
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

    # Log frozen data extraction counts (helps debug silent failures in CI)
    print(f"  Frozen D2C: {len(frozen_d2c)}/12 months extracted")
    print(f"  Frozen AMZ: {len(frozen_amz_data)}/12 months")
    print(f"  Frozen FK:  {len(frozen_fk_data)}/12 months")
    print(f"  Frozen FC:  {len(frozen_fc_data)}/12 months")
    print(f"  Frozen BK:  {len(frozen_bk_data)}/12 months")
    print(f"  Frozen IM:  {len(frozen_im_data)}/12 months")
    print(f"  Frozen CRED: {len(frozen_cred_data)}/12 months")
    print(f"  Frozen ad maps — AMZ:{len(frozen_amz_ad)} FK:{len(frozen_fk_ad)} BK:{len(frozen_bk_ad)} IM:{len(frozen_im_ad)}")

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
        + f"const SHEET_CM2={json.dumps(cm2_data or {})};\n"
        + f"const SHEET_REV_EX={json.dumps(rev_ex_data or {})};\n"
        "// ── SYNC_DATA_END ──"
    )

    match = re.search(pattern, html, flags=re.DOTALL)
    if not match:
        print("\n⚠️  Could not find inline data section to replace!")
        return False
    new_html = html[:match.start()] + replacement + html[match.end():]

    # ── SAFETY CHECK 1: Verify FY24-25 frozen data preserved ──
    old_section = html[match.start():match.end()]
    for m in FY24_25_MONTHS:
        old_has = f'"{m}":{{' in old_section and old_section.count(f'"{m}":{{') > 0
        new_has = f'"{m}":{{' in replacement
        # Check the month has actual product data (not just empty {})
        old_pat = re.search(rf'"{re.escape(m)}":\{{[^}}]', old_section)
        new_pat = re.search(rf'"{re.escape(m)}":\{{[^}}]', replacement)
        if old_pat and not new_pat:
            print(f"\n❌ SAFETY ABORT: FY24-25 month '{m}' had data but would be wiped!")
            print("   Frozen data extraction likely failed. Skipping write to protect dashboard.")
            return False

    # ── SAFETY CHECK 2: Verify total revenue not significantly reduced ──
    old_rev_match = re.findall(r'revenue:(\d+(?:\.\d+)?)', old_section)
    new_rev_match = re.findall(r'revenue:(\d+(?:\.\d+)?)', replacement)
    old_total = sum(float(r) for r in old_rev_match) if old_rev_match else 0
    new_total = sum(float(r) for r in new_rev_match) if new_rev_match else 0
    if old_total > 0 and new_total < old_total * 0.85:
        print(f"\n❌ SAFETY ABORT: Revenue dropped >15% (₹{new_total:,.0f} vs ₹{old_total:,.0f})")
        print("   This likely means data was lost. Skipping write to protect dashboard.")
        return False

    # ── SAFETY CHECK 3: Validate JS syntax before writing ──
    # Extract the script block and check with node
    script_start = new_html.find('<script>', new_html.find('<body'))
    script_end = new_html.find('</script>', script_start)
    if script_start > 0 and script_end > 0:
        js_code = new_html[script_start + 8:script_end]
        tmp_js = os.path.join(BASE, '.tmp_syntax_check.js')
        try:
            with open(tmp_js, 'w') as f:
                f.write(js_code)
            try:
                result = subprocess.run(['node', '--check', tmp_js], capture_output=True, text=True)
                if result.returncode != 0:
                    print(f"\n❌ SAFETY ABORT: JS syntax error detected in generated dashboard!")
                    print(f"   {result.stderr.strip()}")
                    print("   Skipping write to protect dashboard.")
                    return False
            except FileNotFoundError:
                print("  ⚠️ node not found — skipping JS syntax check")
        finally:
            if os.path.exists(tmp_js):
                os.remove(tmp_js)

    # ── SAFETY CHECK 4: Section size regression ──
    old_len = len(old_section)
    new_len = len(replacement)
    if old_len > 1000 and new_len < old_len * 0.80:
        print(f"\n❌ SAFETY ABORT: SYNC_DATA section shrank by >20% ({new_len} vs {old_len} chars)")
        print("   This likely means data was lost. Skipping write.")
        return False

    # ── Inject fresh adSpent (D2C-only ad spend from Dashboard tab) ──
    if d2c_ad_spend:
        ad_match = re.search(r'adSpent=\{[^}]*\}', new_html)
        if ad_match:
            existing_ad = {}
            for item in re.finditer(r"'([^']+)':(\d+)", ad_match.group(0)):
                existing_ad[item.group(1)] = int(item.group(2))
            merged = {}
            for m in ALL_MONTHS:
                val = d2c_ad_spend.get(m)
                if val is not None and val >= 0:
                    merged[m] = round(val)
                elif m in existing_ad:
                    merged[m] = existing_ad[m]
            items = ','.join(f"'{m}':{merged[m]}" for m in ALL_MONTHS if m in merged)
            new_html = re.sub(r'adSpent=\{[^}]*\}', f'adSpent={{{items}}}', new_html)
            print(f"  adSpent updated for {len(merged)} months")

    # ── Create backup before overwriting ──
    if os.path.exists(DASHBOARD):
        shutil.copy2(DASHBOARD, DASHBOARD + ".bak")
        print(f"  Backup created: dashboard.html.bak")

    with open(DASHBOARD, "w") as f:
        f.write(new_html)

    return True


def verify_password():
    """Require password before allowing monthly MIS changes."""
    import getpass, hashlib
    PASS_HASH = "36a563bf4749a28e870b221d2fee544e6029196ca11b2f952763525c32b1f984"
    pw = getpass.getpass("\n🔒 Enter password to unlock Monthly MIS changes: ")
    if hashlib.sha256(pw.encode()).hexdigest() != PASS_HASH:
        print("❌ Wrong password. Monthly MIS update blocked.")
        return False
    print("✅ Password accepted. Proceeding with sync...\n")
    return True


def main():
    # Skip password check in CI (GitHub Actions sets GITHUB_ACTIONS=true)
    if not os.environ.get("GITHUB_ACTIONS"):
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

    print("\nLoading FY24-25 data from backup JSON...")
    fy24_d2c, fy24_amz, fy24_amz_ad, fy24_fk, fy24_fk_ad, fy24_fc, fy24_bk, fy24_bk_ad, fy24_im, fy24_im_ad, fy24_cred = fetch_fy24_25_from_json()
    d2c_data.update(fy24_d2c)
    amz_data.update(fy24_amz)
    amz_ad_map.update(fy24_amz_ad)
    fk_data.update(fy24_fk)
    fk_ad_map.update(fy24_fk_ad)
    fc_data.update(fy24_fc)
    bk_data.update(fy24_bk)
    bk_ad_map.update(fy24_bk_ad)
    im_data.update(fy24_im)
    im_ad_map.update(fy24_im_ad)
    cred_data.update(fy24_cred)

    print("\nFetching Dashboard tab metrics...")
    overall_ad_spend, cm2_data, rev_ex_data = fetch_dashboard_metrics(sh)

    print("\nFetching FY24-25 CM2 from old sheet...")
    fy24_25_cm2 = fetch_fy24_25_cm2(gc)
    cm2_data.update(fy24_25_cm2)  # FY25-26 from Dashboard tab, FY24-25 from old sheet

    d2c_ad_spend = {}
    for m in MONTHS:
        if m in overall_ad_spend:
            marketplace = (amz_ad_map.get(m, 0) + fk_ad_map.get(m, 0)
                           + bk_ad_map.get(m, 0) + im_ad_map.get(m, 0))
            d2c_ad_spend[m] = max(0, overall_ad_spend[m] - marketplace)

    print("\nUpdating dashboard.html...")
    if update_dashboard(d2c_data, amz_data, amz_ad_map, fk_data, fk_ad_map, fc_data, bk_data, bk_ad_map, im_data, im_ad_map, cred_data, d2c_ad_spend, cm2_data, rev_ex_data):
        total_d2c = sum(len(v) for v in d2c_data.values())
        total_amz = sum(len(v) for v in amz_data.values())
        total_fk = sum(len(v) for v in fk_data.values())
        total_fc = sum(len(v) for v in fc_data.values())
        total_bk = sum(len(v) for v in bk_data.values())
        total_im = sum(len(v) for v in im_data.values())
        total_cred = sum(len(v) for v in cred_data.values())
        print(f"\nDashboard synced! {total_d2c} D2C + {total_amz} Amazon + {total_fk} Flipkart + {total_fc} FirstCry + {total_bk} Blinkit + {total_im} Instamart + {total_cred} Cred products across {len(ALL_MONTHS)} months")
        print(f"   Open dashboard.html in browser to see updated data")
    else:
        print("\n❌ Failed to update dashboard")


if __name__ == "__main__":
    main()
