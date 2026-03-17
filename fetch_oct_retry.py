#!/usr/bin/env python3
"""Fetch October 2025 orders with retry on 503 errors."""

import requests
import json
import os
import time

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
API_BASE = "https://apiv2.shiprocket.in/v1/external"

with open(os.path.join(BASE_DIR, ".env")) as f:
    for line in f:
        if line.startswith("SHIPROCKET_API_TOKEN="):
            TOKEN = line.strip().split("=", 1)[1]

HEADERS_API = {"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"}


def fetch_orders_with_retry(date_from, date_to, label):
    all_orders = []
    page = 1
    per_page = 200
    max_retries = 3

    print(f"Fetching {label} ({date_from} to {date_to})...")

    while True:
        url = f"{API_BASE}/orders?per_page={per_page}&page={page}&from={date_from}&to={date_to}"
        print(f"  Page {page}...", end=" ", flush=True)

        for attempt in range(max_retries):
            resp = requests.get(url, headers=HEADERS_API)
            if resp.status_code == 503:
                wait = 10 * (attempt + 1)
                print(f"503 error, retrying in {wait}s...", end=" ", flush=True)
                time.sleep(wait)
                continue
            break

        if resp.status_code == 401:
            print("\nERROR: Token expired.")
            return None
        if resp.status_code != 200:
            print(f"\nERROR: {resp.status_code} after {max_retries} retries")
            return None

        data = resp.json()
        if isinstance(data, dict) and "data" in data:
            orders = data["data"]
            if isinstance(orders, dict):
                orders = orders.get("data", orders.get("orders", []))
            if not isinstance(orders, list):
                orders = []
        elif isinstance(data, list):
            orders = data
        else:
            orders = []

        if not orders:
            print("0 (done)")
            break

        all_orders.extend(orders)
        print(f"{len(orders)} orders (total: {len(all_orders)})")

        if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
            last_page = data["data"].get("last_page", 0)
            if last_page and page >= last_page:
                break

        if len(orders) < per_page:
            break

        page += 1
        time.sleep(0.5)  # Small delay between pages to avoid 503

    print(f"  Total: {len(all_orders)} orders")
    return all_orders


if __name__ == "__main__":
    orders = fetch_orders_with_retry("2025-10-01", "2025-10-31", "October 2025")
    if orders:
        path = os.path.join(BASE_DIR, "oct_orders_raw.json")
        with open(path, "w") as f:
            json.dump(orders, f, default=str)
        print(f"Saved {len(orders)} orders to {path}")
    else:
        print("Failed to fetch October orders.")
