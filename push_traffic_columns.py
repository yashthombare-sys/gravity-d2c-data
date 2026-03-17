#!/usr/bin/env python3
"""Add Sessions and Conversion % columns to existing Amazon Daily MIS sheet."""
import sys, os, json, time, gzip
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "automation"))

import requests
import gspread
from google.oauth2.service_account import Credentials
from config import load_env

MARKETPLACE_ID = "A21TJRUUN4KGV"
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "shiproket-mis-70c28ae6e7fb.json")
SHEET_ID_FILE = os.path.join(BASE_DIR, ".amazon_daily_sheet_id")

def get_access_token():
    env = load_env()
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": env.get("AMAZON_REFRESH_TOKEN", ""),
        "client_id": env.get("AMAZON_CLIENT_ID", ""),
        "client_secret": env.get("AMAZON_CLIENT_SECRET", ""),
    }, timeout=15)
    return resp.json()["access_token"]

# Step 1: Fetch traffic report for Mar 1-15
print("Step 1: Fetching traffic report...", flush=True)
token = get_access_token()
headers = {"x-amz-access-token": token, "Content-Type": "application/json"}

resp = requests.post(
    f"{SP_API_BASE}/reports/2021-06-30/reports",
    headers=headers,
    json={
        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": "2026-03-01T00:00:00Z",
        "dataEndTime": "2026-03-15T23:59:59Z",
        "reportOptions": {"dateGranularity": "DAY", "asinGranularity": "SKU"}
    },
    timeout=30
)
report_id = resp.json()["reportId"]
print(f"  Report ID: {report_id}, waiting...", flush=True)

# Poll
for attempt in range(30):
    time.sleep(10)
    status_resp = requests.get(
        f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
        headers=headers, timeout=30
    )
    status_data = status_resp.json()
    if status_data.get("processingStatus") == "DONE":
        doc_id = status_data["reportDocumentId"]
        print(f"  Ready!", flush=True)
        break
else:
    print("  Timed out!")
    sys.exit(1)

# Download
doc_resp = requests.get(
    f"{SP_API_BASE}/reports/2021-06-30/documents/{doc_id}",
    headers=headers, timeout=30
)
doc_data = doc_resp.json()
report_resp = requests.get(doc_data["url"], timeout=30)
if doc_data.get("compressionAlgorithm") == "GZIP":
    report_text = gzip.decompress(report_resp.content).decode("utf-8")
else:
    report_text = report_resp.text

report_data = json.loads(report_text)

# Extract daily traffic
daily_traffic = {}
for day in report_data.get("salesAndTrafficByDate", []):
    date_str = day["date"]
    t = day.get("trafficByDate", {})
    daily_traffic[date_str] = {
        "sessions": t.get("sessions", 0),
        "conversion_pct": round(t.get("unitSessionPercentage", 0), 2),
    }
    print(f"  {date_str}: {daily_traffic[date_str]['sessions']} sessions, {daily_traffic[date_str]['conversion_pct']}%", flush=True)

# Step 2: Update Google Sheet — add headers + data in columns L & M
print("\nStep 2: Updating Google Sheet...", flush=True)
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=[
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
])
gc = gspread.authorize(creds)

with open(SHEET_ID_FILE) as f:
    sheet_id = f.read().strip()

spreadsheet = gc.open_by_key(sheet_id)
ws = spreadsheet.worksheet("March 2026")

# Expand sheet to 13 columns if needed
if ws.col_count < 13:
    ws.resize(cols=13)
    time.sleep(1)

# Add headers
ws.update(values=[["Sessions", "Conversion %"]], range_name="L1:M1")

# Get dates from column A
all_dates = ws.col_values(1)

updated = 0
for i, date_val in enumerate(all_dates):
    if i == 0:  # skip header
        continue
    row_idx = i + 1
    traffic = daily_traffic.get(date_val, {"sessions": 0, "conversion_pct": 0})
    ws.update(values=[[traffic["sessions"], traffic["conversion_pct"]]], range_name=f"L{row_idx}:M{row_idx}")
    print(f"  Row {row_idx} ({date_val}): {traffic['sessions']} sessions, {traffic['conversion_pct']}%", flush=True)
    updated += 1
    time.sleep(0.3)

print(f"\nDone! Updated {updated} rows with Sessions + Conversion %")
print(f"Sheet: {spreadsheet.url}")
