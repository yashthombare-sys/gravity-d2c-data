"""
Cleanup FY 24-25 tabs: Remove SOFT TOY CATEGORY and STEM CATEGORY sections
from all FY 24-25 month tabs in the MIS Google Sheet.

Handles multiple sections per tab (D2C, Amazon, Flipkart, FirstCry, Blinkit).
Updates GRAND TOTAL formulas to only reference Busy Board subtotals.
"""

import gspread
from google.oauth2.service_account import Credentials
import time
import re

BASE = "/Users/yashthombare/Desktop/Gravity/Shiprocket D2C data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CREDS_FILE = f"{BASE}/shiproket-mis-70c28ae6e7fb.json"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1-aln640f4OxRmoS9R5EBvnQACp6edzxrMQDU6sgd3Lc/"

FY24_25_TABS = [
    "April 2024 MIS",
    "May 2024 MIS",
    "June 2024 MIS",
    "July 2024 MIS",
    "August 2024 MIS",
    "September 2024 MIS",
    "October 2024 MIS",
    "November 2024 MIS",
    "December 2024 MIS",
    "January 2025 MIS",
    "February 2025 MIS",
    "March 2025 MIS",
]

CATEGORIES_TO_REMOVE = ["SOFT TOY CATEGORY", "STEM CATEGORY"]


def find_rows_to_delete(all_values):
    """
    Scan all rows and identify row indices (1-based) to delete.
    These are the SOFT TOY and STEM category blocks:
      - Category header row
      - Product rows / "(no orders)" rows under it
      - Subtotal row
      - Blank spacer rows between categories (between Busy Board subtotal and next cat,
        and between categories, and after last category before GRAND TOTAL)

    Also returns GRAND TOTAL row indices and their corresponding Busy Board subtotal rows.
    """
    rows_to_delete = set()
    grand_total_info = []  # list of (grand_total_row_1based, busy_board_subtotal_row_1based)

    i = 0
    n = len(all_values)

    while i < n:
        cell0 = all_values[i][0].strip() if all_values[i] and all_values[i][0] else ""

        # Track Busy Board subtotal rows (we need these for GRAND TOTAL formula fix)
        if "BUSY BOARD CATEGORY" in cell0 and "Subtotal" in cell0:
            last_bb_subtotal = i + 1  # 1-based

            # Mark blank rows after Busy Board subtotal (spacers before Soft Toy)
            j = i + 1
            while j < n:
                next_cell = all_values[j][0].strip() if all_values[j] and all_values[j][0] else ""
                if next_cell == "":
                    rows_to_delete.add(j + 1)  # blank spacer
                    j += 1
                elif next_cell in CATEGORIES_TO_REMOVE:
                    break  # found the category to remove
                else:
                    break
            i = j
            continue

        # Check if this row starts a category to remove
        if cell0 in CATEGORIES_TO_REMOVE:
            rows_to_delete.add(i + 1)  # category header

            # Scan forward to find all rows in this category block
            j = i + 1
            while j < n:
                next_cell = all_values[j][0].strip() if all_values[j] and all_values[j][0] else ""

                if next_cell == "":
                    # Blank row - part of the block (spacer after subtotal)
                    rows_to_delete.add(j + 1)
                    j += 1
                    # Check if next non-blank is another category to remove or GRAND TOTAL
                    continue
                elif "Subtotal" in next_cell:
                    # Subtotal row for this category
                    rows_to_delete.add(j + 1)
                    j += 1
                    continue
                elif next_cell in CATEGORIES_TO_REMOVE:
                    # Another category to remove starts here
                    rows_to_delete.add(j + 1)
                    j += 1
                    continue
                elif next_cell == "GRAND TOTAL":
                    # We've reached the grand total - stop
                    grand_total_info.append((j + 1, last_bb_subtotal))
                    break
                elif next_cell.startswith("BUSY BOARD"):
                    # Shouldn't happen but safety
                    break
                elif any(next_cell.startswith(h) for h in ["AMAZON MIS", "FLIPKART MIS", "FIRSTCRY MIS", "BLINKIT MIS", "Products"]):
                    # Section header - stop
                    break
                else:
                    # Product row under the category (e.g., "Ganesha", "(no orders this month)")
                    rows_to_delete.add(j + 1)
                    j += 1
                    continue

            i = j
            continue

        i += 1

    return sorted(rows_to_delete, reverse=True), grand_total_info


def cleanup_tab(ws, tab_name):
    """Clean up a single tab."""
    print(f"\n{'='*60}")
    print(f"Processing: {tab_name}")
    print(f"{'='*60}")

    all_values = ws.get_all_values()
    total_rows_before = len(all_values)
    num_cols = len(all_values[0]) if all_values else 0

    rows_to_delete, grand_total_info = find_rows_to_delete(all_values)

    if not rows_to_delete:
        print(f"  No SOFT TOY / STEM rows found. Skipping.")
        return

    # Print what we're deleting
    print(f"  Total rows before: {total_rows_before}")
    print(f"  Rows to delete ({len(rows_to_delete)}):")
    for r in sorted(rows_to_delete):
        cell_val = all_values[r-1][0][:60] if all_values[r-1][0] else "(empty)"
        print(f"    Row {r}: {cell_val}")

    # Delete rows from bottom to top so indices don't shift
    for row_idx in rows_to_delete:  # already sorted in reverse
        ws.delete_rows(row_idx)
        time.sleep(1)  # rate limit safety per delete

    print(f"  Deleted {len(rows_to_delete)} rows.")

    # Now update GRAND TOTAL formulas
    # Re-read the sheet after deletions
    time.sleep(3)
    all_values_new = ws.get_all_values()

    # Find GRAND TOTAL rows and their corresponding Busy Board subtotal rows
    for i, row in enumerate(all_values_new):
        cell0 = row[0].strip() if row[0] else ""
        if cell0 == "GRAND TOTAL":
            # Find the Busy Board subtotal above this GRAND TOTAL
            bb_sub_row = None
            for k in range(i - 1, -1, -1):
                kcell = all_values_new[k][0].strip() if all_values_new[k][0] else ""
                if "BUSY BOARD CATEGORY" in kcell and "Subtotal" in kcell:
                    bb_sub_row = k + 1  # 1-based
                    break

            if bb_sub_row:
                gt_row = i + 1  # 1-based
                # Copy the values from the Busy Board subtotal row to GRAND TOTAL
                # (since there's only one category left, GRAND TOTAL = BB subtotal)
                bb_values = all_values_new[bb_sub_row - 1]
                # Update GRAND TOTAL: set col A label, copy numeric cols from BB subtotal
                updates = []
                for col_idx in range(1, num_cols):
                    val = bb_values[col_idx]
                    if val:
                        # Convert column index to letter
                        col_letter = chr(65 + col_idx) if col_idx < 26 else chr(64 + col_idx // 26) + chr(65 + col_idx % 26)
                        # Set formula referencing BB subtotal
                        cell_ref = f"{col_letter}{bb_sub_row}"
                        updates.append({
                            "range": f"{col_letter}{gt_row}",
                            "values": [[f"={cell_ref}"]]
                        })

                if updates:
                    ws.batch_update(updates)
                    print(f"  Updated GRAND TOTAL (row {gt_row}) to reference BB Subtotal (row {bb_sub_row})")

    print(f"  Total rows after: {len(all_values_new)}")


def main():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    for tab_name in FY24_25_TABS:
        try:
            ws = sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            print(f"\n  Tab '{tab_name}' not found. Skipping.")
            continue

        cleanup_tab(ws, tab_name)
        time.sleep(3)  # pause between tabs to avoid rate limits

    print(f"\n{'='*60}")
    print("CLEANUP COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
