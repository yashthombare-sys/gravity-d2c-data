#!/usr/bin/env python3
"""
Master daily sync script.
Fetches yesterday's data from Shiprocket + Amazon → stores in SQLite → rebuilds dashboard.

Usage:
    python3 daily_sync.py                  # Sync yesterday
    python3 daily_sync.py 2026-03-10       # Sync specific date
    python3 daily_sync.py 2026-03-01 2026-03-10  # Sync date range
    python3 daily_sync.py --backfill 2025-10-01 2026-03-11  # Backfill historical data
    python3 daily_sync.py --status         # Show sync status
"""
import sys, os

# Ensure automation/ is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
import db
from config import get_month_label

def sync_date(date_str, skip_amazon=False):
    """Sync a single date: fetch both channels → store in DB → log."""
    print(f"\n{'='*50}")
    print(f"Syncing: {date_str}")
    print(f"{'='*50}")

    # ── Shiprocket ────────────────────────────────────
    try:
        from shiprocket_fetch import fetch_and_process as sr_fetch
        products, skipped = sr_fetch(date_str, date_str)
        if products:
            count = db.upsert_batch(date_str, "shiprocket", products)
            db.log_sync("shiprocket", date_str, count, "success",
                       f"{sum(p['delivered'] for p in products.values())} delivered")
            print(f"  ✓ Shiprocket: {count} products saved")
        else:
            db.log_sync("shiprocket", date_str, 0, "success", "No orders")
            print(f"  ✓ Shiprocket: No orders for {date_str}")
    except Exception as e:
        db.log_sync("shiprocket", date_str, 0, "error", str(e))
        print(f"  ✗ Shiprocket error: {e}")

    # ── Amazon ────────────────────────────────────────
    if not skip_amazon:
        try:
            from amazon_fetch import fetch_and_process as amz_fetch
            products, fees = amz_fetch(date_str, date_str)
            if products:
                count = db.upsert_batch(date_str, "amazon", products)
                db.log_sync("amazon", date_str, count, "success",
                           f"{sum(p['delivered'] for p in products.values())} delivered")
                print(f"  ✓ Amazon: {count} products saved")
            else:
                db.log_sync("amazon", date_str, 0, "success", "No orders")
                print(f"  ✓ Amazon: No orders for {date_str}")
        except Exception as e:
            db.log_sync("amazon", date_str, 0, "error", str(e))
            print(f"  ✗ Amazon error: {e}")
    else:
        print(f"  ⊘ Amazon: skipped")

    # ── Push to Google Sheets ─────────────────────────
    try:
        from push_daily_sheet import push_date
        push_date(date_str)
    except Exception as e:
        print(f"  ✗ Google Sheets error: {e}")


def sync_range(date_from, date_to, skip_amazon=False):
    """Sync a range of dates."""
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")
    current = start

    total_days = (end - start).days + 1
    print(f"\nSyncing {total_days} days: {date_from} → {date_to}")

    day_num = 0
    while current <= end:
        day_num += 1
        date_str = current.strftime("%Y-%m-%d")
        print(f"\n[{day_num}/{total_days}] ", end="")
        sync_date(date_str, skip_amazon=skip_amazon)
        current += timedelta(days=1)

    print(f"\n{'='*50}")
    print(f"Range sync complete: {total_days} days processed")


def rebuild():
    """Rebuild dashboard from DB."""
    from build_dashboard import rebuild_dashboard
    rebuild_dashboard()


def show_status():
    """Show current sync status."""
    print("\n=== Sync Status ===")

    min_date, max_date = db.get_date_range()
    if not min_date:
        print("No data in database yet.")
        return

    print(f"Date range: {min_date} → {max_date}")

    # Monthly summary
    d2c = db.get_monthly_mis("shiprocket")
    amz = db.get_monthly_mis("amazon")

    print(f"\n{'Month':<12} {'D2C Rev':>12} {'D2C Ord':>8} {'AMZ Rev':>12} {'AMZ Ord':>8}")
    print("-" * 54)

    all_months = sorted(set(list(d2c.keys()) + list(amz.keys())))
    for m in all_months:
        d2c_rev = sum(p["revenue"] for p in d2c.get(m, {}).values())
        d2c_ord = sum(p["total_orders"] for p in d2c.get(m, {}).values())
        amz_rev = sum(p["revenue"] for p in amz.get(m, {}).values())
        amz_ord = sum(p["total_orders"] for p in amz.get(m, {}).values())
        print(f"{m:<12} ₹{d2c_rev:>10,.0f} {d2c_ord:>8,} ₹{amz_rev:>10,.0f} {amz_ord:>8,}")

    # Recent sync log
    print(f"\nRecent syncs:")
    history = db.get_sync_history(10)
    for h in history:
        print(f"  {h['timestamp']} | {h['channel']:>10} | {h['date_fetched']} | {h['status']} | {h['message']}")


def import_historical_json():
    """
    Import existing JSON MIS data into SQLite.
    This bootstraps the DB with data already generated by the old scripts.
    """
    import json
    from config import BASE_DIR

    files = {
        "shiprocket": {
            "oct_mis_data.json": "Oct 2025",
            "nov_mis_data.json": "Nov 2025",
            "dec_mis_data.json": "Dec 2025",
            "jan_mis_data.json": "Jan 2026",
            "feb_mis_data.json": "Feb 2026",
        },
        "amazon": {
            "amazon_oct_mis_data.json": "Oct 2025",
            "amazon_nov_mis_data.json": "Nov 2025",
            "amazon_dec_mis_data.json": "Dec 2025",
            "amazon_jan_mis_data.json": "Jan 2026",
            "amazon_feb_mis_data.json": "Feb 2026",
        }
    }

    # Month label → representative date (mid-month)
    month_dates = {
        "Oct 2025": "2025-10-15", "Nov 2025": "2025-11-15",
        "Dec 2025": "2025-12-15", "Jan 2026": "2026-01-15",
        "Feb 2026": "2026-02-15",
    }

    for channel, file_map in files.items():
        for filename, month in file_map.items():
            filepath = os.path.join(BASE_DIR, filename)
            if not os.path.exists(filepath):
                print(f"  Skip: {filename} (not found)")
                continue

            with open(filepath) as f:
                data = json.load(f)

            # Convert to standard format
            products = {}
            for product, pdata in data.items():
                if channel == "amazon":
                    # Amazon MIS has different field names
                    products[product] = {
                        "total_orders": pdata.get("total_orders", 0),
                        "shipped": pdata.get("shipped", 0),
                        "delivered": pdata.get("shipped", 0),  # FBA: shipped ≈ delivered
                        "rto": 0,
                        "in_transit": 0,
                        "cancelled": pdata.get("cancelled", 0),
                        "lost": 0,
                        "revenue": pdata.get("revenue", 0),
                        "freight": pdata.get("amazon_fees", 0) + pdata.get("refund_amount", 0),
                    }
                else:
                    # Shiprocket MIS - already in correct format
                    products[product] = {
                        "total_orders": pdata.get("total_orders", 0),
                        "shipped": pdata.get("shipped", 0),
                        "delivered": pdata.get("delivered", 0),
                        "rto": pdata.get("rto", 0),
                        "in_transit": pdata.get("in_transit", 0),
                        "cancelled": pdata.get("cancelled", 0),
                        "lost": pdata.get("lost", 0),
                        "revenue": pdata.get("revenue", 0),
                        "freight": pdata.get("freight", 0),
                    }

            date_str = month_dates[month]
            count = db.upsert_batch(date_str, channel, products)
            print(f"  ✓ {channel} {month}: {count} products imported from {filename}")

    # Import Amazon ad spend
    adspend_file = os.path.join(BASE_DIR, "amazon_adspend_monthly.json")
    if os.path.exists(adspend_file):
        with open(adspend_file) as f:
            adspend = json.load(f)
        for month, amount in adspend.items():
            date_str = month_dates.get(month, "")
            if date_str:
                db.upsert_ad_spend(date_str, "amazon_ads", amount)
                print(f"  ✓ Amazon ad spend {month}: ₹{amount:,.0f}")

    # Feb Amazon ad spend (hardcoded in dashboard)
    db.upsert_ad_spend("2026-02-15", "amazon_ads", 1039494)
    print(f"  ✓ Amazon ad spend Feb 2026: ₹1,039,494")

    print("\nHistorical import complete!")


def main():
    args = sys.argv[1:]

    if not args:
        # Default: sync yesterday + rebuild dashboard
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        sync_date(yesterday)
        rebuild()
        return

    if args[0] == "--status":
        show_status()
        return

    if args[0] == "--import":
        print("Importing historical JSON data into SQLite...")
        import_historical_json()
        rebuild()
        return

    if args[0] == "--rebuild":
        rebuild()
        return

    if args[0] == "--backfill":
        if len(args) >= 3:
            sync_range(args[1], args[2])
            rebuild()
        else:
            print("Usage: daily_sync.py --backfill YYYY-MM-DD YYYY-MM-DD")
        return

    if args[0] == "--shiprocket-only":
        if len(args) >= 3:
            sync_range(args[1], args[2], skip_amazon=True)
        elif len(args) >= 2:
            sync_date(args[1], skip_amazon=True)
        else:
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            sync_date(yesterday, skip_amazon=True)
        rebuild()
        return

    # Single date or date range
    if len(args) == 1:
        sync_date(args[0])
        rebuild()
    elif len(args) == 2:
        sync_range(args[0], args[1])
        rebuild()


if __name__ == "__main__":
    main()
