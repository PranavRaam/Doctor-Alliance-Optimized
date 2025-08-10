import re
import sys
import json
from collections import defaultdict, Counter
from typing import Dict, Optional, Tuple


def normalize_id(value: Optional[str]) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if s.endswith('.0') and s.replace('.', '', 1).isdigit():
        s = s[:-2]
    return s


def read_failed_records(path: str):
    import pandas as pd

    try:
        # Try to load specific sheet if present
        xls = pd.ExcelFile(path)
        sheet = 'Failed_Records' if 'Failed_Records' in xls.sheet_names else None
        df = pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
    except Exception:
        # Fallback: simple read
        import pandas as pd  # ensure available in this scope
        df = pd.read_excel(path)

    # Keep only relevant columns; tolerate different casings/names
    candidates = {
        'Document ID': ['Document ID', 'docId', 'docid', 'doc_id'],
        'DABackOfficeID': ['DABackOfficeID', 'DABackOfficeId', 'backOfficeId', 'backofficeid'],
        'orderno': ['orderno', 'orderNo', 'order_number', 'OrderNo', 'Order Number'],
        'patientName': ['patientName', 'patient_name', 'patient'],
        'PATIENTUPLOAD_STATUS': ['PATIENTUPLOAD_STATUS', 'patientupload_status'],
        'ORDERUPLOAD_STATUS': ['ORDERUPLOAD_STATUS', 'orderupload_status'],
        'ORDER_RESPONSE': ['ORDER_RESPONSE', 'ORDER_CREATION_REMARK'],
    }

    def pick(row, keys):
        for k in keys:
            if k in row and str(row[k]).strip() not in ("", "nan", "NaN", "None"):
                return row[k]
        return ""

    rows = []
    for _, r in df.iterrows():
        rd = dict(r)
        rows.append({
            'Document ID': normalize_id(pick(rd, candidates['Document ID'])),
            'DABackOfficeID': normalize_id(pick(rd, candidates['DABackOfficeID'])),
            'orderno': normalize_id(pick(rd, candidates['orderno'])),
            'patientName': str(pick(rd, candidates['patientName'])),
            'PATIENTUPLOAD_STATUS': str(pick(rd, candidates['PATIENTUPLOAD_STATUS'])).upper(),
            'ORDERUPLOAD_STATUS': str(pick(rd, candidates['ORDERUPLOAD_STATUS'])).upper(),
            'ORDER_RESPONSE': str(pick(rd, candidates['ORDER_RESPONSE'])),
        })

    return rows


def parse_log(log_path: str):
    # Patterns
    re_docid_ctx = re.compile(r"DocID\s*[=:]\s*(\d+)")
    re_docid_any = re.compile(r"(Document ID|docId)\s*[:=]\s*(\d+)")
    re_backoffice_ctx = re.compile(r"DABackOfficeID\s*[=:]\s*([\d\.]+)")

    # Success/failure markers
    re_patient_success = re.compile(r"\[PATIENT_CREATE\].*?Success.*?DocID\s*[=:]\s*(\d+)")
    re_patient_failure = re.compile(r"\[PATIENT_CREATE\].*?\u274c|\[PATIENT_CREATE\].*?Failure", re.IGNORECASE)  # ❌

    re_order_success_any = re.compile(r"(\[ORDER_CREATE\].*?Success|Order created and PDF uploaded successfully|Order created)\b", re.IGNORECASE)
    re_order_failure_any = re.compile(r"(\[ORDER_CREATE\].*?[\u274cF]ailure|Order creation failed|Order API returned|ORDER CREATE FAILED)", re.IGNORECASE)

    # State
    last_docid: Optional[str] = None
    last_backoffice: Optional[str] = None

    # Outputs
    docid_to_patient_status: Dict[str, Tuple[str, Optional[str]]] = {}  # docid -> ("SUCCESS"|"FAIL", reason)
    docid_to_order_status: Dict[str, Tuple[str, Optional[str]]] = {}
    backoffice_to_docid: Dict[str, str] = {}

    # Keep recent line buffer to attach evidence
    docid_to_evidence: Dict[str, Dict[str, str]] = defaultdict(dict)  # {docid: {patient_line, order_line}}

    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Track context IDs
            m = re_docid_ctx.search(line) or re_docid_any.search(line)
            if m:
                last_docid = m.group(1) if m.lastindex == 1 else m.group(2)
            m2 = re_backoffice_ctx.search(line)
            if m2:
                last_backoffice = normalize_id(m2.group(1))
                if last_backoffice and last_docid:
                    backoffice_to_docid[last_backoffice] = last_docid

            # Patient success
            mps = re_patient_success.search(line)
            if mps:
                did = mps.group(1)
                docid_to_patient_status[did] = ("SUCCESS", None)
                docid_to_evidence[did]['patient_line'] = line.strip()
                continue

            # Patient failure: tie to last_docid
            if re_patient_failure.search(line):
                rid = last_docid or ""
                if rid:
                    reason = line.strip()
                    docid_to_patient_status[rid] = ("FAIL", reason)
                    docid_to_evidence[rid]['patient_line'] = line.strip()
                continue

            # Order success/failure - attribute to last_docid context
            if re_order_success_any.search(line):
                rid = last_docid or ""
                if rid:
                    docid_to_order_status[rid] = ("SUCCESS", None)
                    docid_to_evidence[rid]['order_line'] = line.strip()
                continue

            if re_order_failure_any.search(line):
                rid = last_docid or ""
                if rid:
                    docid_to_order_status[rid] = ("FAIL", line.strip())
                    docid_to_evidence[rid]['order_line'] = line.strip()
                continue

    return {
        'patient': docid_to_patient_status,
        'order': docid_to_order_status,
        'by_backoffice': backoffice_to_docid,
        'evidence': docid_to_evidence,
    }


def main():
    if len(sys.argv) < 3:
        print("Usage: python analyze_failed_vs_log.py <failed_records.xlsx> <log_path>")
        sys.exit(2)

    failed_path = sys.argv[1]
    log_path = sys.argv[2]

    failed_rows = read_failed_records(failed_path)
    log_data = parse_log(log_path)

    order_status = log_data['order']
    patient_status = log_data['patient']
    backoffice_map = log_data['by_backoffice']
    evidence = log_data['evidence']

    results = []
    buckets = Counter()

    for row in failed_rows:
        docid = normalize_id(row.get('Document ID'))
        backid = normalize_id(row.get('DABackOfficeID'))
        if not docid and backid and backid in backoffice_map:
            docid = backoffice_map[backid]

        pstat, preason = patient_status.get(docid, ("UNKNOWN", None)) if docid else ("UNKNOWN", None)
        ostat, oreason = order_status.get(docid, ("UNKNOWN", None)) if docid else ("UNKNOWN", None)

        classification = ""
        if ostat == "SUCCESS":
            classification = "Actually SUCCESS (order created)"
        elif pstat == "SUCCESS" and ostat in ("UNKNOWN", "FAIL"):
            classification = "Patient created, Order not created"
        elif pstat == "FAIL" and 'DocumentName' in (preason or ''):
            classification = "Failed due to missing DocumentName"
        elif pstat == "UNKNOWN" and ostat == "UNKNOWN":
            classification = "Not found in log"
        elif pstat == "FAIL" or ostat == "FAIL":
            classification = "Failed (see reason)"
        else:
            classification = "Undetermined"

        buckets[classification] += 1

        results.append({
            'Document ID': docid,
            'DABackOfficeID': backid,
            'orderno': row.get('orderno', ''),
            'patientName': row.get('patientName', ''),
            'Excel_PATIENTUPLOAD_STATUS': row.get('PATIENTUPLOAD_STATUS', ''),
            'Excel_ORDERUPLOAD_STATUS': row.get('ORDERUPLOAD_STATUS', ''),
            'Excel_ORDER_RESPONSE': row.get('ORDER_RESPONSE', ''),
            'Log_PATIENT_STATUS': pstat,
            'Log_PATIENT_REASON': preason or '',
            'Log_ORDER_STATUS': ostat,
            'Log_ORDER_REASON': oreason or '',
            'Classification': classification,
            'Evidence_Patient': evidence.get(docid, {}).get('patient_line', ''),
            'Evidence_Order': evidence.get(docid, {}).get('order_line', ''),
        })

    summary = {
        'total_in_excel': len(failed_rows),
        'buckets': buckets,
    }

    print("==== SUMMARY ====")
    print(json.dumps({k: (v if not isinstance(v, Counter) else dict(v)) for k, v in summary.items()}, indent=2))
    print("\n==== SAMPLE (first 30) ====")
    for item in results[:30]:
        print(json.dumps(item, ensure_ascii=False))

    # Also save detailed CSV for review
    try:
        import pandas as pd
        pd.DataFrame(results).to_excel('failed_records_analysis.xlsx', index=False)
        print("\nWrote detailed analysis to failed_records_analysis.xlsx")
    except Exception as e:
        print(f"Could not write Excel output: {e}")


if __name__ == '__main__':
    main()


