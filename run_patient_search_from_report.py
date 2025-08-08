import os
import sys
import time
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd

from selenium_patient_search import download_latest_485_for_patient


def _strip(value: Optional[str]) -> str:
    return (str(value).strip() if value is not None else "").strip()


def _split_full_name(name: str) -> Tuple[str, str]:
    name = _strip(name)
    if not name:
        return "", ""
    # Handle "Last, First" and possible middle names
    if "," in name:
        last, rest = name.split(",", 1)
        parts = rest.strip().split()
        first = parts[0] if parts else ""
        return _strip(last), _strip(first)
    # Handle "First Last" with optional middle
    parts = name.split()
    if len(parts) == 1:
        return "", parts[0]
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        return _strip(last), _strip(first)
    return "", ""


def extract_names_from_report(path: str) -> List[Tuple[str, str]]:
    df = pd.read_excel(path)
    cols = {c.lower(): c for c in df.columns}

    # Preferred explicit columns
    candidates = [
        ("patient last name", "patient first name"),
        ("last name", "first name"),
        ("lastname", "firstname"),
        ("patient_last_name", "patient_first_name"),
    ]

    for last_key, first_key in candidates:
        if last_key in cols and first_key in cols:
            last_col, first_col = cols[last_key], cols[first_key]
            pairs = [
                (_strip(row[last_col]), _strip(row[first_col]))
                for _, row in df.iterrows()
            ]
            pairs = [p for p in pairs if p[0] or p[1]]
            return pairs

    # Fallback: single full name column
    full_name_keys = [
        "patient name", "patient", "name", "patient_name",
    ]
    for key in full_name_keys:
        if key in cols:
            full_col = cols[key]
            pairs = []
            for _, row in df.iterrows():
                last, first = _split_full_name(_strip(row[full_col]))
                if last or first:
                    pairs.append((last, first))
            return pairs

    # As a last resort, try to infer from any two columns containing name hints
    possible_first = [c for c in df.columns if "first" in c.lower()]
    possible_last = [c for c in df.columns if "last" in c.lower()]
    if possible_first and possible_last:
        first_col = possible_first[0]
        last_col = possible_last[0]
        pairs = [
            (_strip(row[last_col]), _strip(row[first_col]))
            for _, row in df.iterrows()
        ]
        pairs = [p for p in pairs if p[0] or p[1]]
        return pairs

    raise ValueError("Could not detect name columns in the report. Please provide explicit mappings.")


def run_search_from_file(
    report_path: str,
    *,
    da_login: str = "rpabot",
    da_password: str = "Dallas@1234",
    headless: bool = True,
    max_count: Optional[int] = None,
    dedupe: bool = False,
) -> str:
    print(f"Reading report: {report_path}")
    pairs = extract_names_from_report(report_path)

    total_detected = len(pairs)
    if dedupe:
        # Deduplicate while preserving order
        seen = set()
        processed = []
        for last, first in pairs:
            key = (last.lower(), first.lower())
            if key not in seen and (last or first):
                seen.add(key)
                processed.append((last, first))
        print(f"Detected names: {total_detected} | Unique: {len(processed)} (dedupe=True)")
    else:
        processed = [(last, first) for (last, first) in pairs]
        print(f"Detected names: {total_detected} | Processing all rows including duplicates (dedupe=False)")

    # Treat max_count <= 0 as no limit
    if max_count is not None and max_count > 0:
        print(f"Limiting to first {max_count} rows")
        processed = processed[:max_count]
    else:
        print("No limit applied (processing entire list)")

    results = []
    print(f"Starting Selenium run for {len(processed)} rows (headless={headless}, dedupe={dedupe}) — will open Documents and download latest 485 via API...")
    for idx, (last, first) in enumerate(processed, start=1):
        if idx == 1 or idx % 10 == 0:
            print(f"[{idx}/{len(processed)}] {last}, {first}")
        try:
            ok, doc_id, pdf_path = download_latest_485_for_patient(
                da_url="https://backoffice.doctoralliance.com",
                da_login=da_login,
                da_password=da_password,
                last_name=last,
                first_name=first,
                headless=headless,
                save_dir="Downloads_485",
            )
            results.append({
                "last_name": last,
                "first_name": first,
                "success": ok,
                "doc_id": doc_id or "",
                "pdf_path": pdf_path or "",
            })
        except Exception as e:
            results.append({
                "last_name": last,
                "first_name": first,
                "success": False,
                "doc_id": "",
                "pdf_path": "",
                "error": str(e),
            })
        # Small pacing delay
        time.sleep(0.2)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = f"patient_search_and_485_results_{ts}.csv"
    pd.DataFrame(results).to_csv(out_path, index=False)
    print(f"Completed. Saved results to: {out_path}")
    return out_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_patient_search_from_report.py <REPORT_XLSX_PATH> [MAX_COUNT (0=all)] [HEADLESS True|False] [LOGIN] [PASSWORD] [DEDUPE True|False]")
        sys.exit(1)

    report = sys.argv[1]
    if len(sys.argv) >= 3 and sys.argv[2].isdigit():
        _val = int(sys.argv[2])
        max_count = _val if _val > 0 else None
    else:
        max_count = None
    headless = (sys.argv[3].lower() == "true") if len(sys.argv) >= 4 else True
    da_login = sys.argv[4] if len(sys.argv) >= 5 else "rpabot"
    da_password = sys.argv[5] if len(sys.argv) >= 6 else "Dallas@1234"
    dedupe = (sys.argv[6].lower() == "true") if len(sys.argv) >= 7 else False

    out = run_search_from_file(
        report_path=report,
        da_login=da_login,
        da_password=da_password,
        headless=headless,
        max_count=max_count,
        dedupe=dedupe,
    )
    print(f"Saved: {out}")


