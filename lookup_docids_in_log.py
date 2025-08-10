import re
import sys
import json
from typing import Dict, Tuple, Optional, List


def parse_log_for_docids(log_path: str) -> tuple[Dict[str, tuple[str, Optional[str]]], Dict[str, tuple[str, Optional[str]]]]:
    """
    Returns:
      order_status_by_docid: {docid: ("SUCCESS"|"FAIL"|"UNKNOWN", reason)}
      patient_status_by_docid: {docid: ("SUCCESS"|"FAIL"|"UNKNOWN", reason)}
    """
    re_docid_ctx = re.compile(r"DocID\s*[=:]\s*(\d+)")
    re_docid_any = re.compile(r"(Document ID|docId)\s*[:=]\s*(\d+)")

    re_order_success_any = re.compile(r"(\[ORDER_CREATE\].*?Success|Order created and PDF uploaded successfully|Order created)\b", re.IGNORECASE)
    re_order_dup = re.compile(r"\[ORDER_CREATE\].*Duplicate detected|treated as success", re.IGNORECASE)
    re_order_fail = re.compile(r"\[ORDER_CREATE\].*(❌|Failure|HTTP\s+4\d\d|HTTP\s+5\d\d)", re.IGNORECASE)

    re_patient_success = re.compile(r"\[PATIENT_CREATE\].*?Success.*?DocID\s*[=:]\s*(\d+)")
    re_patient_failure = re.compile(r"\[PATIENT_CREATE\].*?(❌|Failure)", re.IGNORECASE)

    order_status: Dict[str, Tuple[str, Optional[str]]] = {}
    patient_status: Dict[str, Tuple[str, Optional[str]]] = {}

    last_docid: Optional[str] = None

    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            m = re_docid_ctx.search(line) or re_docid_any.search(line)
            if m:
                last_docid = m.group(1) if m.lastindex == 1 else m.group(2)

            # Patient success includes explicit DocID in line
            mps = re_patient_success.search(line)
            if mps:
                did = mps.group(1)
                patient_status[did] = ("SUCCESS", None)
                continue

            # Patient failure attributed to last_docid
            if re_patient_failure.search(line):
                if last_docid:
                    patient_status[last_docid] = ("FAIL", line.strip())
                continue

            # Order success/duplicate attributed to last_docid
            if re_order_success_any.search(line) or re_order_dup.search(line):
                if last_docid:
                    order_status[last_docid] = ("SUCCESS", None)
                continue

            # Order failure attributed to last_docid
            if re_order_fail.search(line):
                if last_docid:
                    order_status[last_docid] = ("FAIL", line.strip())
                continue

    return order_status, patient_status


def main():
    if len(sys.argv) < 3:
        print("Usage: python lookup_docids_in_log.py <log_path> <docid1> [<docid2> ...]")
        sys.exit(2)

    import os
    log_path = sys.argv[1]
    # Tolerate accidental wrapping quotes in the path and normalize separators
    log_path = os.path.normpath(log_path.strip().strip('"').strip("'"))
    doc_ids: List[str] = [s.strip() for s in sys.argv[2:] if s.strip()]

    order_status, patient_status = parse_log_for_docids(log_path)

    results = []
    for did in doc_ids:
        o = order_status.get(did, ("UNKNOWN", None))
        p = patient_status.get(did, ("UNKNOWN", None))
        if o[0] == "SUCCESS":
            classification = "Actually SUCCESS (order created)"
        elif p[0] == "SUCCESS":
            classification = "Patient created (no order success in this log)"
        elif p[0] == "FAIL" or o[0] == "FAIL":
            classification = "Failed (see reason)"
        else:
            classification = "Not found in log"

        results.append({
            'Document ID': did,
            'OrderStatus': o[0],
            'OrderReason': o[1] or '',
            'PatientStatus': p[0],
            'PatientReason': p[1] or '',
            'Classification': classification,
        })

    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()


