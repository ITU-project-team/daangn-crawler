"""
export.py - CSV/Excel export helper.

Loads daangn_seoul.csv, prints summary counts, and exports all or filtered
rows to CSV or Excel.
"""

import csv
import os
import sys
from collections import Counter


CSV_PATH = "daangn_seoul.csv"


def load_rows():
    if not os.path.exists(CSV_PATH):
        print(f"{CSV_PATH} does not exist. Run the crawler first.")
        sys.exit(1)
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows):,} rows from {CSV_PATH}")
    return rows


def show_summary(rows):
    gus = Counter(r["gu"] for r in rows)
    subjects = Counter(r["subject"] for r in rows)
    statuses = Counter(r["status"] for r in rows)

    dates = [r["createdAt"][:10] for r in rows if r.get("createdAt")]
    date_range = f"{min(dates)} ~ {max(dates)}" if dates else "-"

    print(f"\n{'─'*50}")
    print(f"  total {len(rows):,} rows | date range: {date_range}")
    print(f"{'─'*50}")

    print(f"\n  district distribution:")
    for gu, cnt in gus.most_common():
        print(f"    {gu:<10} {cnt:>6,}cases")

    print(f"\n  subject distribution:")
    for subj, cnt in subjects.most_common(10):
        print(f"    {subj:<14} {cnt:>6,}cases")

    print(f"\n  status: {dict(statuses)}")


def save_csv(rows, path):
    if not rows:
        print("  No rows to save.")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✅ CSV save: {path} ({len(rows):,}cases)")


def save_excel(rows, path):
    try:
        import openpyxl
    except ImportError:
        print("  ❌ openpyxl required: pip install openpyxl")
        return

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Seoul local-life"

    # Header row.
    headers = list(rows[0].keys())
    ws.append(headers)

    # Data rows.
    for r in rows:
        ws.append([r.get(h, "") for h in headers])

    # Set simple column widths based on header length.
    for i, h in enumerate(headers, 1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = max(len(h) + 2, 12)

    wb.save(path)
    print(f"  ✅ Excel save: {path} ({len(rows):,}cases)")


def filter_rows(rows):
    print(f"\n  Filter rows (blank input = all)")
    gu = input("  district (example: Gangnam-gu): ").strip()
    dong = input("  dong (example: Yeoksam-dong): ").strip()
    subject = input("  subject: ").strip()

    filtered = rows
    if gu:
        filtered = [r for r in filtered if r.get("gu") == gu]
    if dong:
        filtered = [r for r in filtered if r.get("regionName") == dong]
    if subject:
        filtered = [r for r in filtered if r.get("subject") == subject]

    print(f"  -> {len(filtered):,} rows selected")
    return filtered


def main():
    rows = load_rows()
    show_summary(rows)

    while True:
        print(f"\n{'─'*50}")
        print("  1) CSV export all")
        print("  2) Excel export all")
        print("  3) Export filtered CSV")
        print("  4) Export filtered Excel")
        print("  5) Show summary again")
        print("  q) exit")
        print(f"{'─'*50}")

        choice = input("  choice: ").strip().lower()

        if choice == "1":
            path = input("  file (default: daangn_export.csv): ").strip() or "daangn_export.csv"
            save_csv(rows, path)
        elif choice == "2":
            path = input("  file (default: daangn_export.xlsx): ").strip() or "daangn_export.xlsx"
            save_excel(rows, path)
        elif choice == "3":
            filtered = filter_rows(rows)
            path = input("  file (default: daangn_filtered.csv): ").strip() or "daangn_filtered.csv"
            save_csv(filtered, path)
        elif choice == "4":
            filtered = filter_rows(rows)
            path = input("  file (default: daangn_filtered.xlsx): ").strip() or "daangn_filtered.xlsx"
            save_excel(filtered, path)
        elif choice == "5":
            show_summary(rows)
        elif choice == "q":
            break


if __name__ == "__main__":
    main()
