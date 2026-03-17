"""
SQLite database: schema, insert, query helpers.
Stores daily MIS data for both Shiprocket and Amazon channels.
"""
import sqlite3, os
from config import DB_PATH, COGS_MAP, CATS

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS daily_mis (
            date TEXT NOT NULL,
            channel TEXT NOT NULL,          -- 'shiprocket' or 'amazon'
            product TEXT NOT NULL,
            category TEXT,                  -- 'busyboard', 'softtoy', 'stem'
            total_orders INTEGER DEFAULT 0,
            shipped INTEGER DEFAULT 0,
            delivered INTEGER DEFAULT 0,
            rto INTEGER DEFAULT 0,
            in_transit INTEGER DEFAULT 0,
            cancelled INTEGER DEFAULT 0,
            lost INTEGER DEFAULT 0,
            revenue REAL DEFAULT 0,
            freight REAL DEFAULT 0,         -- shipping (shiprocket) or amazon fees (amazon)
            cogs_unit REAL DEFAULT 0,
            PRIMARY KEY (date, channel, product)
        );

        CREATE TABLE IF NOT EXISTS ad_spend (
            date TEXT NOT NULL,
            platform TEXT NOT NULL,         -- 'meta', 'google', 'amazon_ads', 'manual'
            amount REAL DEFAULT 0,
            PRIMARY KEY (date, platform)
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now','localtime')),
            channel TEXT,
            date_fetched TEXT,
            orders_fetched INTEGER,
            status TEXT,                    -- 'success', 'error'
            message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_daily_mis_date ON daily_mis(date);
        CREATE INDEX IF NOT EXISTS idx_daily_mis_month ON daily_mis(date, channel);
    """)
    conn.commit()
    conn.close()

def upsert_daily_mis(date, channel, product, data):
    """Insert or update a single product's daily MIS row."""
    conn = get_conn()
    conn.execute("""
        INSERT INTO daily_mis (date, channel, product, category, total_orders, shipped,
            delivered, rto, in_transit, cancelled, lost, revenue, freight, cogs_unit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, channel, product) DO UPDATE SET
            category=excluded.category, total_orders=excluded.total_orders,
            shipped=excluded.shipped, delivered=excluded.delivered, rto=excluded.rto,
            in_transit=excluded.in_transit, cancelled=excluded.cancelled, lost=excluded.lost,
            revenue=excluded.revenue, freight=excluded.freight, cogs_unit=excluded.cogs_unit
    """, (
        date, channel, product, CATS.get(product, ""),
        data.get("total_orders", 0), data.get("shipped", 0),
        data.get("delivered", 0), data.get("rto", 0),
        data.get("in_transit", 0), data.get("cancelled", 0),
        data.get("lost", 0), data.get("revenue", 0),
        data.get("freight", 0), COGS_MAP.get(product, 0)
    ))
    conn.commit()
    conn.close()

def upsert_batch(date, channel, products_dict):
    """Bulk insert/update all products for a given date+channel."""
    conn = get_conn()
    rows = []
    for product, data in products_dict.items():
        rows.append((
            date, channel, product, CATS.get(product, ""),
            data.get("total_orders", 0), data.get("shipped", 0),
            data.get("delivered", 0), data.get("rto", 0),
            data.get("in_transit", 0), data.get("cancelled", 0),
            data.get("lost", 0), round(data.get("revenue", 0), 2),
            round(data.get("freight", 0), 2), COGS_MAP.get(product, 0)
        ))
    conn.executemany("""
        INSERT INTO daily_mis (date, channel, product, category, total_orders, shipped,
            delivered, rto, in_transit, cancelled, lost, revenue, freight, cogs_unit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, channel, product) DO UPDATE SET
            category=excluded.category, total_orders=excluded.total_orders,
            shipped=excluded.shipped, delivered=excluded.delivered, rto=excluded.rto,
            in_transit=excluded.in_transit, cancelled=excluded.cancelled, lost=excluded.lost,
            revenue=excluded.revenue, freight=excluded.freight, cogs_unit=excluded.cogs_unit
    """, rows)
    conn.commit()
    conn.close()
    return len(rows)

def upsert_ad_spend(date, platform, amount):
    conn = get_conn()
    conn.execute("""
        INSERT INTO ad_spend (date, platform, amount) VALUES (?, ?, ?)
        ON CONFLICT(date, platform) DO UPDATE SET amount=excluded.amount
    """, (date, platform, amount))
    conn.commit()
    conn.close()

def log_sync(channel, date_fetched, orders_fetched, status, message=""):
    conn = get_conn()
    conn.execute("""
        INSERT INTO sync_log (channel, date_fetched, orders_fetched, status, message)
        VALUES (?, ?, ?, ?, ?)
    """, (channel, date_fetched, orders_fetched, status, message))
    conn.commit()
    conn.close()

def get_monthly_mis(channel=None):
    """
    Aggregate daily data by month → returns dict like dashboard expects.
    { "Oct 2025": { "V1": {total_orders, shipped, delivered, ...}, ... }, ... }
    """
    conn = get_conn()
    where = "WHERE channel = ?" if channel else ""
    params = (channel,) if channel else ()

    rows = conn.execute(f"""
        SELECT
            CASE cast(strftime('%m', date) as integer)
                WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
                WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
                WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
                WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
            END || ' ' || strftime('%Y', date) as month,
            product,
            SUM(total_orders) as total_orders,
            SUM(shipped) as shipped,
            SUM(delivered) as delivered,
            SUM(rto) as rto,
            SUM(in_transit) as in_transit,
            SUM(cancelled) as cancelled,
            SUM(lost) as lost,
            SUM(revenue) as revenue,
            SUM(freight) as freight,
            MAX(cogs_unit) as cogs_unit
        FROM daily_mis
        {where}
        GROUP BY month, product
        ORDER BY date, product
    """, params).fetchall()
    conn.close()

    result = {}
    for r in rows:
        month = r["month"]
        if month not in result:
            result[month] = {}
        result[month][r["product"]] = {
            "total_orders": r["total_orders"],
            "shipped": r["shipped"],
            "delivered": r["delivered"],
            "rto": r["rto"],
            "in_transit": r["in_transit"],
            "cancelled": r["cancelled"],
            "lost": r["lost"],
            "revenue": round(r["revenue"], 2),
            "freight": round(r["freight"], 2),
        }
    return result

def get_monthly_ad_spend():
    """Return ad spend aggregated by month and platform."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            CASE cast(strftime('%m', date) as integer)
                WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
                WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
                WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
                WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
            END || ' ' || strftime('%Y', date) as month,
            platform,
            SUM(amount) as total
        FROM ad_spend
        GROUP BY month, platform
    """).fetchall()
    conn.close()

    result = {}
    for r in rows:
        month = r["month"]
        if month not in result:
            result[month] = {}
        result[month][r["platform"]] = round(r["total"], 2)
    return result

def get_daily_rows(date_str=None):
    """
    Get daily MIS rows. If date_str given, returns rows for that date only.
    Returns list of dicts with all fields + ad_spend columns.
    """
    conn = get_conn()
    if date_str:
        rows = conn.execute("""
            SELECT d.date, d.channel, d.product, d.category,
                   d.total_orders, d.shipped, d.delivered, d.rto,
                   d.in_transit, d.cancelled, d.lost,
                   d.revenue, d.freight, d.cogs_unit
            FROM daily_mis d
            WHERE d.date = ?
            ORDER BY d.channel, d.product
        """, (date_str,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT d.date, d.channel, d.product, d.category,
                   d.total_orders, d.shipped, d.delivered, d.rto,
                   d.in_transit, d.cancelled, d.lost,
                   d.revenue, d.freight, d.cogs_unit
            FROM daily_mis d
            ORDER BY d.date DESC, d.channel, d.product
        """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_ad_spend_for_date(date_str):
    """Get ad spend by platform for a specific date."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT platform, amount FROM ad_spend WHERE date = ?", (date_str,)
    ).fetchall()
    conn.close()
    return {r["platform"]: r["amount"] for r in rows}

def get_date_range():
    """Get min and max dates in the database."""
    conn = get_conn()
    row = conn.execute("SELECT MIN(date) as min_date, MAX(date) as max_date FROM daily_mis").fetchone()
    conn.close()
    return row["min_date"], row["max_date"]

def get_sync_history(limit=20):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM sync_log ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# Initialize DB on import
init_db()
