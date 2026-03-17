"""
Shared configuration: COGS, product patterns, categories, credentials loader.
"""
import os, re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "automation", "master_data.db")
DASHBOARD_PATH = os.path.join(BASE_DIR, "dashboard.html")
ENV_PATH = os.path.join(BASE_DIR, ".env")

# ── Load .env ──────────────────────────────────────────────
def load_env():
    env = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    return env

# ── COGS per unit ──────────────────────────────────────────
COGS_MAP = {
    "V1": 225, "V2": 275, "V3": 662, "V4": 170, "V6": 275, "V9": 778, "V10": 1009,
    "V1- P of 2": 531, "V1- P of 3": 797, "V2- P of 2": 649,
    "V4- P of 2": 401, "V4- P of 3": 368, "V6- P of 2": 649, "V9 P of 2": 1664,
    "V6-V1 Combo": 608, "V6-V2 Combo": 612, "V1-V2 Combo": 524,
    "V1-V4 Combo": 404, "V2-V4 Combo": 488, "V9-V2 Combo": 488,
    "V9-V3 Combo": 1440, "V9-V10 Combo": 1787,
    "Busy Book Blue": 300, "Busy Book Pink": 300, "Human Book": 300,
    "Ganesha": 290, "Krishna": 290, "Hanuman": 290,
    "Car": 540, "Tank": 862, "JCB": 862, "CS Basics 1": 250,
}

# ── Category mapping ──────────────────────────────────────
CATS = {
    "V1": "busyboard", "V2": "busyboard", "V3": "busyboard", "V4": "busyboard",
    "V6": "busyboard", "V9": "busyboard", "V10": "busyboard",
    "V1- P of 2": "busyboard", "V1- P of 3": "busyboard", "V2- P of 2": "busyboard",
    "V4- P of 2": "busyboard", "V4- P of 3": "busyboard", "V6- P of 2": "busyboard",
    "V9 P of 2": "busyboard",
    "V6-V1 Combo": "busyboard", "V6-V2 Combo": "busyboard", "V1-V2 Combo": "busyboard",
    "V1-V4 Combo": "busyboard", "V2-V4 Combo": "busyboard", "V9-V2 Combo": "busyboard",
    "V9-V3 Combo": "busyboard", "V9-V10 Combo": "busyboard",
    "Busy Book Blue": "busyboard", "Busy Book Pink": "busyboard", "Human Book": "busyboard",
    "CS Basics 1": "stem",
    "Ganesha": "softtoy", "Krishna": "softtoy", "Hanuman": "softtoy",
    "Car": "stem", "Tank": "stem", "JCB": "stem",
}

# ── Shiprocket product name patterns ──────────────────────
PRODUCT_PATTERNS = [
    # Combos first
    (r"V9.*V10|V10.*V9|V9\+V10|V9-V10|V9 \+ V10|V9 V10 Combo", "V9-V10 Combo"),
    (r"V9.*V3|V3.*V9|V9\+V3|V9-V3|V9 \+ V3|V9 V3 Combo", "V9-V3 Combo"),
    (r"V9.*V2|V2.*V9|V9\+V2|V9-V2|V9 \+ V2|V9 V2 Combo", "V9-V2 Combo"),
    (r"V6.*V1|V1.*V6|V6\+V1|V6-V1|V6 \+ V1|V6 V1 Combo", "V6-V1 Combo"),
    (r"V6.*V2|V2.*V6|V6\+V2|V6-V2|V6 \+ V2|V6 V2 Combo", "V6-V2 Combo"),
    (r"V1.*V4|V4.*V1|V1\+V4|V1-V4|V1 \+ V4|V1 V4 Combo", "V1-V4 Combo"),
    (r"V1.*V2|V2.*V1|V1\+V2|V1-V2|V1 \+ V2|V1 V2 Combo", "V1-V2 Combo"),
    (r"V2.*V4|V4.*V2|V2\+V4|V2-V4|V2 \+ V4|V2 V4 Combo", "V2-V4 Combo"),
    # Packs
    (r"V9.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V9 P of 2"),
    (r"V6.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V6- P of 2"),
    (r"V4.*(?:Pack of 3|P of 3|pack of 3|3\s*pack)", "V4- P of 3"),
    (r"V4.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V4- P of 2"),
    (r"V2.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V2- P of 2"),
    (r"V1.*(?:Pack of 3|P of 3|pack of 3|3\s*pack)", "V1- P of 3"),
    (r"V1.*(?:Pack of 2|P of 2|pack of 2|2\s*pack)", "V1- P of 2"),
    # Individual (V10 before V1)
    (r"V10(?!\d)", "V10"),
    (r"V1(?!\d)", "V1"),
    (r"V2(?!\d)", "V2"),
    (r"V3(?!\d)", "V3"),
    (r"V4(?!\d)", "V4"),
    (r"V6(?!\d)", "V6"),
    (r"V9(?!\d)", "V9"),
    # Busy books
    (r"(?i)busy\s*book.*(?:blue|boy)", "Busy Book Blue"),
    (r"(?i)busy\s*book.*(?:pink|girl)", "Busy Book Pink"),
    (r"(?i)human\s*(?:body\s*)?(?:busy\s*)?book", "Human Book"),
    (r"(?i)cs\s*basics?\s*1", "CS Basics 1"),
    # Soft toys
    (r"(?i)(?:clap\s*cuddle|clapcuddle).*(?:ganesh|ganesha)", "Ganesha"),
    (r"(?i)(?:clap\s*cuddle|clapcuddle).*(?:krishna)", "Krishna"),
    (r"(?i)(?:clap\s*cuddle|clapcuddle).*(?:hanuman)", "Hanuman"),
    (r"(?i)ganesh", "Ganesha"),
    (r"(?i)krishna", "Krishna"),
    (r"(?i)hanuman", "Hanuman"),
    # RC vehicles
    (r"(?i)(?:sooper\s*brains?\s*)?(?:rc\s*)?(?:army\s*)?tank", "Tank"),
    (r"(?i)(?:sooper\s*brains?\s*)?(?:rc\s*)?(?:racer|car)", "Car"),
    (r"(?i)(?:sooper\s*brains?\s*)?jcb", "JCB"),
]

# ── Amazon SKU → product name mapping ─────────────────────
AMAZON_SKU_MAP = {
    "PortablebusyboardV1.5": "V1", "PortableBusyBoardV2": "V2",
    "bb_v03": "V3", "PortablebusyboardV7": "V4",
    "PortablebusyboardV6": "V6", "PortableBusyBoard_V09": "V9", "bb_v10": "V10",
    "busybook_blue": "Busy Book Blue", "busybook_pink": "Busy Book Pink",
    "Humanbody01": "Human Book",
    "V1pack2": "V1- P of 2", "V2pack2": "V2- P of 2", "V4pack2": "V4- P of 2",
    "V6pack2": "V6- P of 2", "V9pack2": "V9 P of 2",
    "new_ComboV2_V4": "V2-V4 Combo", "ComboV1_V6": "V6-V1 Combo",
    "ComboV6_V2": "V6-V2 Combo", "ComboV1_V2": "V1-V2 Combo",
    "ComboV1_V4": "V1-V4 Combo", "ComboV9_V2": "V9-V2 Combo",
    "ComboV9_V3": "V9-V3 Combo", "ComboV9_V10": "V9-V10 Combo",
    "Ganesha_02": "Ganesha", "Krishna_02": "Krishna", "Hanuman_02": "Hanuman",
    "DIY_Tank01": "Tank", "DIY_Car01": "Car", "DIY_JCB01": "JCB",
    "V4pack3": "V4- P of 3", "V1pack3": "V1- P of 3",
}

# ── Shiprocket status classification ──────────────────────
DELIVERED_STATUSES = {"DELIVERED"}
RTO_STATUSES = {"RTO DELIVERED", "RTO IN TRANSIT", "RTO INITIATED", "RTO OFD",
                "REACHED BACK AT SELLER CITY", "REACHED BACK AT_SELLER_CITY"}
CANCELLED_STATUSES = {"CANCELED", "CANCELLATION REQUESTED"}
SKIP_STATUSES = {"SELF FULFILED", "QC FAILED", "RETURN DELIVERED",
                 "RETURN IN TRANSIT", "RETURN PENDING", "RETURN CANCELLED"}

SPARE_PARTS_KEYWORDS = [
    "motor", "pcb", "charging cable", "documents", "spare",
    "document", "e-book", "sample", "gift", "solar system", "portable switches"
]

def classify_status(status):
    if not status:
        return "unknown"
    s = status.upper().strip()
    if s in DELIVERED_STATUSES:
        return "delivered"
    if s in RTO_STATUSES:
        return "rto"
    if s in CANCELLED_STATUSES:
        return "cancelled"
    if s in SKIP_STATUSES or s.startswith("RETURN"):
        return "skip"
    return "in_transit"

def classify_product(product_name):
    if not product_name:
        return None
    for pattern, category in PRODUCT_PATTERNS:
        if re.search(pattern, product_name):
            return category
    return None

def is_spare_part(product_name):
    if not product_name:
        return True
    name_lower = product_name.lower()
    return any(kw in name_lower for kw in SPARE_PARTS_KEYWORDS)

def get_month_label(date_str):
    """Convert YYYY-MM-DD to 'Mon YYYY' label."""
    from datetime import datetime
    dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
    return dt.strftime("%b %Y")
