#!/usr/bin/env python3
"""Test if we can fetch Sales & Traffic report (Glance Views) from Amazon SP-API."""
import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "automation"))

import requests
from config import load_env
from datetime import datetime, timedelta

MARKETPLACE_ID = "A21TJRUUN4KGV"  # Amazon.in
TOKEN_URL = "https://api.amazon.com/auth/o2/token"
SP_API_BASE = "https://sellingpartnerapi-eu.amazon.com"

def get_access_token():
    env = load_env()
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "refresh_token": env.get("AMAZON_REFRESH_TOKEN", ""),
        "client_id": env.get("AMAZON_CLIENT_ID", ""),
        "client_secret": env.get("AMAZON_CLIENT_SECRET", ""),
    }, timeout=15)
    if resp.status_code != 200:
        raise PermissionError(f"Token refresh failed: {resp.status_code} {resp.text}")
    return resp.json()["access_token"]

# Step 1: Get token
print("Step 1: Getting access token...")
token = get_access_token()
print("  OK\n")

headers = {"x-amz-access-token": token, "Content-Type": "application/json"}

# Step 2: Request Sales & Traffic report for yesterday
yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
print(f"Step 2: Requesting Sales & Traffic report for {yesterday}...")

report_body = {
    "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
    "marketplaceIds": [MARKETPLACE_ID],
    "dataStartTime": f"{yesterday}T00:00:00Z",
    "dataEndTime": f"{yesterday}T23:59:59Z",
    "reportOptions": {
        "dateGranularity": "DAY",
        "asinGranularity": "SKU"
    }
}

resp = requests.post(
    f"{SP_API_BASE}/reports/2021-06-30/reports",
    headers=headers,
    json=report_body,
    timeout=30
)

print(f"  Status: {resp.status_code}")
print(f"  Response: {resp.text[:500]}")

if resp.status_code in (200, 202):
    report_id = resp.json().get("reportId")
    print(f"\n  Report ID: {report_id}")

    # Step 3: Poll for report completion
    print("\nStep 3: Waiting for report to be ready...")
    for attempt in range(20):
        time.sleep(10)
        status_resp = requests.get(
            f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
            headers=headers,
            timeout=30
        )
        status_data = status_resp.json()
        processing_status = status_data.get("processingStatus", "UNKNOWN")
        print(f"  Attempt {attempt+1}: {processing_status}")

        if processing_status == "DONE":
            doc_id = status_data.get("reportDocumentId")
            print(f"\n  Report ready! Document ID: {doc_id}")

            # Step 4: Download report
            print("\nStep 4: Downloading report...")
            doc_resp = requests.get(
                f"{SP_API_BASE}/reports/2021-06-30/documents/{doc_id}",
                headers=headers,
                timeout=30
            )
            doc_data = doc_resp.json()
            download_url = doc_data.get("url")

            if download_url:
                compression = doc_data.get("compressionAlgorithm", "")
                print(f"  Compression: {compression or 'none'}")
                report_resp = requests.get(download_url, timeout=30)

                if compression == "GZIP":
                    import gzip
                    report_text = gzip.decompress(report_resp.content).decode("utf-8")
                else:
                    report_text = report_resp.text

                report_data = json.loads(report_text)

                # Save raw data
                with open("test_glance_views_output.json", "w") as f:
                    json.dump(report_data, f, indent=2)
                print("  Saved to test_glance_views_output.json")

                # Show summary
                traffic = report_data.get("salesAndTrafficByAsin", [])
                print(f"\n  Found {len(traffic)} ASINs with traffic data\n")
                for item in traffic[:5]:
                    asin = item.get("parentAsin") or item.get("childAsin", "?")
                    traffic_data = item.get("trafficByAsin", {})
                    sessions = traffic_data.get("sessions", 0)
                    page_views = traffic_data.get("pageViews", 0)
                    print(f"    ASIN {asin}: {page_views} page views, {sessions} sessions")

                if len(traffic) > 5:
                    print(f"    ... and {len(traffic)-5} more")
            break
        elif processing_status in ("CANCELLED", "FATAL"):
            print(f"\n  Report failed: {processing_status}")
            print(f"  Full response: {json.dumps(status_data, indent=2)}")
            break
    else:
        print("\n  Timed out waiting for report")
else:
    print(f"\n  Failed to request report. Your API may not have access to this report type.")
    print(f"  You may need to add 'sellingpartnerapi::brand_analytics' scope to your app.")
