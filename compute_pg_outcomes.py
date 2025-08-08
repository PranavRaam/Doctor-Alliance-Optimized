import os
import sys
import csv
import pandas as pd
from datetime import datetime

MATCH_SUFFIXES = [
    '_with_patient_and_order_upload.xlsx',
    '_with_patient_and_order_upload_FIXED.xlsx',
]


def load_pg_mapping(path: str = 'pg_ids.csv') -> dict:
    mapping = {}
    if not os.path.exists(path):
        return mapping
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Expect Id,Name
        for row in reader:
            pid = row.get('Id') or row.get('ID') or row.get('id')
            name = row.get('Name') or row.get('name')
            if pid and name:
                mapping[pid.strip()] = name.strip()
    return mapping


def normalize_status(val):
    if pd.isna(val):
        return ''
    return str(val).strip().upper()


def collect_files() -> list:
    files = []
    for fn in os.listdir('.'):
        if fn.startswith('supreme_excel_') and any(fn.endswith(sfx) for sfx in MATCH_SUFFIXES):
            files.append(fn)
    return sorted(files, key=os.path.getctime)


def pg_name_for(row, pg_map: dict) -> str:
    raw = row.get('Pgcompanyid', '')
    if pd.isna(raw) or not str(raw).strip():
        return 'Unknown'
    # Normalize to dashed UUID string if possible (allow pass-through)
    s = str(raw).strip()
    # many rows already dashed; mapping keys are dashed
    return pg_map.get(s, s)


def analyze_file(path: str, pg_map: dict) -> pd.DataFrame:
    df = pd.read_excel(path)

    if len(df) == 0:
        return pd.DataFrame()

    # Ensure columns exist
    for col in ['PATIENTUPLOAD_STATUS', 'ORDERUPLOAD_STATUS', 'ORDER_CREATION_REMARK']:
        if col not in df.columns:
            df[col] = ''

    df['PATIENT_N'] = df['PATIENTUPLOAD_STATUS'].map(normalize_status)
    df['ORDER_N'] = df['ORDERUPLOAD_STATUS'].map(normalize_status)
    df['REMARK'] = df['ORDER_CREATION_REMARK'].fillna('')

    # Success (relaxed): order TRUE and patient TRUE or SKIPPED
    df['SUCCESS_RELAXED'] = (df['ORDER_N'] == 'TRUE') & (df['PATIENT_N'].isin(['TRUE', 'SKIPPED']))

    # Order skipped due to patient missing
    df['ORDER_SKIPPED_NO_PATIENT'] = (df['ORDER_N'] == 'SKIPPED') & df['REMARK'].str.contains('patient does not exist', case=False, na=False)

    # Map pg name
    df['PG_NAME'] = df.apply(lambda r: pg_name_for(r, pg_map), axis=1)

    # Aggregate per PG
    agg = df.groupby('PG_NAME').agg(
        total_rows=('PG_NAME', 'size'),
        success_relaxed=('SUCCESS_RELAXED', 'sum'),
        order_true=('ORDER_N', lambda s: (s == 'TRUE').sum()),
        order_false=('ORDER_N', lambda s: (s == 'FALSE').sum()),
        order_skipped=('ORDER_N', lambda s: (s == 'SKIPPED').sum()),
        order_skipped_no_patient=('ORDER_SKIPPED_NO_PATIENT', 'sum'),
        patient_true=('PATIENT_N', lambda s: (s == 'TRUE').sum()),
        patient_false=('PATIENT_N', lambda s: (s == 'FALSE').sum()),
        patient_skipped=('PATIENT_N', lambda s: (s == 'SKIPPED').sum()),
    ).reset_index()

    agg.insert(0, 'file', os.path.basename(path))
    return agg


def main():
    pg_map = load_pg_mapping()
    files = collect_files()
    if not files:
        print('No matching files found to analyze.')
        sys.exit(0)

    all_rows = []
    for f in files:
        print(f'Analyzing: {f}')
        part = analyze_file(f, pg_map)
        if not part.empty:
            all_rows.append(part)

    if not all_rows:
        print('No data to summarize.')
        return

    out = pd.concat(all_rows, ignore_index=True)

    # Print concise stdout summary
    for fname in out['file'].unique():
        print(f"\nFile: {fname}")
        fdf = out[out['file'] == fname].copy()
        for _, r in fdf.iterrows():
            print(f"  PG: {r['PG_NAME']}")
            print(f"    total: {int(r['total_rows'])}, success(relaxed): {int(r['success_relaxed'])}")
            print(f"    order: TRUE {int(r['order_true'])}, FALSE {int(r['order_false'])}, SKIPPED {int(r['order_skipped'])} (no patient: {int(r['order_skipped_no_patient'])})")
            print(f"    patient: TRUE {int(r['patient_true'])}, FALSE {int(r['patient_false'])}, SKIPPED {int(r['patient_skipped'])}")

    # Save to CSV
    ts = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    out_path = f'pg_outcomes_summary_{ts}.csv'
    out.to_csv(out_path, index=False)
    print(f"\nWrote summary: {out_path}")


if __name__ == '__main__':
    main()
