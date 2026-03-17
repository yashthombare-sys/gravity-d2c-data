#!/usr/bin/env python3
"""Push D2C Ad Spend (inc GST) into column S of each month's Shiprocket MIS tab."""

import time
import gspread
from google.oauth2.service_account import Credentials

BASE_DIR = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"
CREDS_FILE = f"{BASE_DIR}/shiproket-mis-70c28ae6e7fb.json"

# Ad spend per category per month (from screenshot — inc GST)
AD_SPEND = {
    "October 2025 MIS": {
        "BUSY BOARD": 5156490,
        "SOFT TOY": 0,
        "STEM": 0,
    },
    "November 2025 MIS": {
        "BUSY BOARD": 4231133,
        "SOFT TOY": 0,
        "STEM": 0,
    },
    "December 2025 MIS": {
        "BUSY BOARD": 4047973,
        "SOFT TOY": 0,
        "STEM": 0,
    },
    "January 2026 MIS": {
        "BUSY BOARD": 3615161,
        "SOFT TOY": 25758.70,
        "STEM": 231242,
    },
    "February 2026 MIS": {
        "BUSY BOARD": 3234731,
        "SOFT TOY": 420751,
        "STEM": 502152,
    },
}


def main():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for ws_title, spend in AD_SPEND.items():
        print(f"\n{'='*60}")
        print(f"  {ws_title}")
        print(f"{'='*60}")

        ws = sh.worksheet(ws_title)
        time.sleep(1)

        # Ensure column S exists
        if ws.col_count < 19:
            ws.resize(cols=19)
            time.sleep(1)

        # Read column A to find subtotal and grand total rows
        col_a = ws.col_values(1)
        time.sleep(1)

        updates = []  # list of (cell, value)

        for i, val in enumerate(col_a):
            row = i + 1
            if not val:
                continue

            if "BUSY BOARD" in val and "Subtotal" in val:
                updates.append((f"S{row}", spend["BUSY BOARD"]))
                print(f"  Row {row}: {val} → ₹{spend['BUSY BOARD']:,.0f}")

            elif "SOFT TOY" in val and "Subtotal" in val:
                updates.append((f"S{row}", spend["SOFT TOY"]))
                print(f"  Row {row}: {val} → ₹{spend['SOFT TOY']:,.0f}")

            elif "STEM" in val and "Subtotal" in val:
                updates.append((f"S{row}", spend["STEM"]))
                print(f"  Row {row}: {val} → ₹{spend['STEM']:,.0f}")

            elif "GRAND TOTAL" in val.upper():
                # Sum of the subtotal S cells
                subtotal_cells = [u[0] for u in updates]
                formula = "=" + "+".join(subtotal_cells)
                updates.append((f"S{row}", formula))
                print(f"  Row {row}: GRAND TOTAL → {formula}")
                break  # Stop here — don't touch Amazon section below

        # Push all updates
        for cell, value in updates:
            ws.update(range_name=cell, values=[[value]], value_input_option="USER_ENTERED")
            time.sleep(1)

        total = spend["BUSY BOARD"] + spend["SOFT TOY"] + spend["STEM"]
        print(f"  Total ad spend: ₹{total:,.0f}")
        time.sleep(5)

    print(f"\n✅ Done! D2C ad spend pushed to all 5 months.")
    print(f"Sheet: {SHEET_URL}")


if __name__ == "__main__":
    main()
