import sys
from datetime import datetime
from typing import Optional

import pandas as pd

from run_patient_search_from_report import extract_names_from_report
from selenium_patient_search import download_latest_485_for_patient


def run(report_xlsx_path: str, *, headless: bool = True, save_dir: str = "Downloads_485", max_count: Optional[int] = None,
        da_login: str = "rpabot", da_password: str = "Dallas@1234") -> str:
    pairs = extract_names_from_report(report_xlsx_path)

    # Deduplicate preserve order
    seen = set()
    names = []
    for last, first in pairs:
        key = (last.lower(), first.lower())
        if key not in seen:
            seen.add(key)
            names.append((last, first))

    if max_count and max_count > 0:
        names = names[:max_count]

    results = []
    for idx, (last, first) in enumerate(names, start=1):
        if idx == 1 or idx % 10 == 0:
            print(f"[{idx}/{len(names)}] {last}, {first} -> latest 485")
        try:
            ok, doc_id, path = download_latest_485_for_patient(
                da_url="https://backoffice.doctoralliance.com",
                da_login=da_login,
                da_password=da_password,
                last_name=last,
                first_name=first,
                headless=headless,
                save_dir=save_dir,
            )
            results.append({
                "last_name": last,
                "first_name": first,
                "success": ok,
                "doc_id": doc_id or "",
                "pdf_path": path or "",
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

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_csv = f"latest_485_downloads_{ts}.csv"
    pd.DataFrame(results).to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")
    return out_csv


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_download_485_from_report.py <REPORT_XLSX_PATH> [MAX_COUNT] [HEADLESS True|False] [SAVE_DIR] [LOGIN] [PASSWORD]")
        sys.exit(1)

    report = sys.argv[1]
    max_count = int(sys.argv[2]) if len(sys.argv) >= 3 and sys.argv[2].isdigit() and int(sys.argv[2]) > 0 else None
    headless = (sys.argv[3].lower() == "true") if len(sys.argv) >= 4 else True
    save_dir = sys.argv[4] if len(sys.argv) >= 5 else "Downloads_485"
    da_login = sys.argv[5] if len(sys.argv) >= 6 else "rpabot"
    da_password = sys.argv[6] if len(sys.argv) >= 7 else "Dallas@1234"

    run(report, headless=headless, save_dir=save_dir, max_count=max_count, da_login=da_login, da_password=da_password)


