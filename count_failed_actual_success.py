import os
import re
import sys
import json
from typing import List, Dict, Tuple, Optional


def normalize_id(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    if s.endswith('.0'):
        s = s[:-2]
    return s


def read_failed_docids(excel_path: str) -> List[str]:
    import pandas as pd
    # Try specific sheet first
    try:
        xls = pd.ExcelFile(excel_path)
        sheet = 'Failed_Records' if 'Failed_Records' in xls.sheet_names else None
        df = pd.read_excel(excel_path, sheet_name=sheet) if sheet else pd.read_excel(excel_path)
    except Exception:
        import pandas as pd  # ensure scope
        df = pd.read_excel(excel_path)

    doc_col = 'Document ID' if 'Document ID' in df.columns else ('docId' if 'docId' in df.columns else None)
    if not doc_col:
        return []
    ids = [normalize_id(v) for v in df[doc_col].tolist()]
    return [i for i in ids if i]


def collect_log_paths(args: List[str]) -> List[str]:
    if not args:
        # default to logs directory
        if os.path.isdir('logs'):
            return [os.path.join('logs', f) for f in os.listdir('logs') if f.lower().endswith('.log')]
        return []
    paths: List[str] = []
    for a in args:
        if os.path.isdir(a):
            for f in os.listdir(a):
                if f.lower().endswith('.log'):
                    paths.append(os.path.join(a, f))
        elif os.path.isfile(a):
            paths.append(a)
    return sorted(paths)


def parse_logs(log_paths: List[str]):
    # Patterns
    re_docid_ctx = re.compile(r"DocID\s*[=:]\s*(\d+)")
    re_docid_any = re.compile(r"(Document ID|docId)\s*[:=]\s*(\d+)")

    re_order_success_any = re.compile(r"(\[ORDER_CREATE\].*?Success|Order created and PDF uploaded successfully|Order created)\b", re.IGNORECASE)
    re_order_dup = re.compile(r"\[ORDER_CREATE\].*Duplicate detected|treated as success", re.IGNORECASE)
    re_patient_success = re.compile(r"\[PATIENT_CREATE\].*?Success.*?DocID\s*[=:]\s*(\d+)")
    re_patient_failure = re.compile(r"\[PATIENT_CREATE\].*?(❌|Failure)", re.IGNORECASE)

    docid_to_order_status: Dict[str, Tuple[str, Optional[str]]] = {}
    docid_to_patient_status: Dict[str, Tuple[str, Optional[str]]] = {}

    for path in log_paths:
        last_docid: Optional[str] = None
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    m = re_docid_ctx.search(line) or re_docid_any.search(line)
                    if m:
                        last_docid = m.group(1) if m.lastindex == 1 else m.group(2)
                    # patient
                    mps = re_patient_success.search(line)
                    if mps:
                        did = mps.group(1)
                        docid_to_patient_status[did] = ("SUCCESS", None)
                        continue
                    if re_patient_failure.search(line):
                        if last_docid:
                            docid_to_patient_status[last_docid] = ("FAIL", line.strip())
                        continue
                    # order
                    if re_order_success_any.search(line) or re_order_dup.search(line):
                        if last_docid:
                            docid_to_order_status[last_docid] = ("SUCCESS", None)
                        continue
        except Exception:
            continue

    return docid_to_order_status, docid_to_patient_status


def main():
    if len(sys.argv) < 2:
        print("Usage: python count_failed_actual_success.py <failed_records.xlsx> [logs_dir_or_files...]")
        sys.exit(2)

    excel_path = sys.argv[1]
    log_args = sys.argv[2:]
    log_paths = collect_log_paths(log_args)
    if not log_paths:
        print("No log files found. Provide a logs directory or log files.")
        sys.exit(1)

    failed_docids = set(read_failed_docids(excel_path))
    order_status, patient_status = parse_logs(log_paths)

    actually_success = set()
    still_failed = set()
    unknown = set()

    for did in failed_docids:
        ostat = order_status.get(did, ("UNKNOWN", None))[0]
        pstat = patient_status.get(did, ("UNKNOWN", None))[0]
        if ostat == "SUCCESS":
            actually_success.add(did)
        elif pstat == "SUCCESS":
            # patient created but no order success seen
            unknown.add(did)
        elif pstat == "FAIL":
            still_failed.add(did)
        else:
            unknown.add(did)

    summary = {
        'total_in_failed_excel': len(failed_docids),
        'actual_success_count': len(actually_success),
        'still_failed_count': len(still_failed),
        'not_found_in_logs_count': len(unknown),
    }

    print("==== SUMMARY ====")
    print(json.dumps(summary, indent=2))

    # Write details file
    try:
        import pandas as pd
        rows = []
        for did in sorted(failed_docids):
            rows.append({
                'Document ID': did,
                'OrderSuccessInLogs': order_status.get(did, ("UNKNOWN", None))[0],
                'PatientSuccessInLogs': patient_status.get(did, ("UNKNOWN", None))[0],
                'Classification': (
                    'Actually SUCCESS (order created)' if did in actually_success else (
                        'Failed (patient failure)' if did in still_failed else 'Not found in logs'
                    )
                )
            })
        pd.DataFrame(rows).to_excel('failed_records_actual_success.xlsx', index=False)
        print("Wrote failed_records_actual_success.xlsx")
    except Exception as e:
        print(f"Could not write Excel details: {e}")


if __name__ == '__main__':
    main()


