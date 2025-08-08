import pandas as pd
import os
import sys

FILE_DEFAULT = "supreme_excel_restore_family_medical_clinic_with_patient_and_order_upload.xlsx"


def load_df(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_excel(file_path)


def normalize_status(val):
    if pd.isna(val):
        return ""
    return str(val).strip().upper()


def compute_counts(df: pd.DataFrame) -> dict:
    # Ensure columns exist
    for col in [
        'PATIENTUPLOAD_STATUS', 'ORDERUPLOAD_STATUS',
        'PDF_UPLOAD_STATUS'
    ]:
        if col not in df.columns:
            df[col] = ""

    # Normalize
    df['PATIENTUPLOAD_STATUS_N'] = df['PATIENTUPLOAD_STATUS'].map(normalize_status)
    df['ORDERUPLOAD_STATUS_N'] = df['ORDERUPLOAD_STATUS'].map(normalize_status)
    df['PDF_UPLOAD_STATUS_N'] = df['PDF_UPLOAD_STATUS'].map(normalize_status)

    # Overall success (classic) = both TRUE
    mask_success_classic = (df['PATIENTUPLOAD_STATUS_N'] == 'TRUE') & (df['ORDERUPLOAD_STATUS_N'] == 'TRUE')

    # Overall success (relaxed) = order TRUE and patient TRUE or SKIPPED (already exists)
    mask_success_relaxed = (df['ORDERUPLOAD_STATUS_N'] == 'TRUE') & (
        df['PATIENTUPLOAD_STATUS_N'].isin(['TRUE', 'SKIPPED'])
    )

    total = len(df)
    overall_success_classic = int(mask_success_classic.sum())
    overall_failed_classic = int((~mask_success_classic).sum())

    overall_success_relaxed = int(mask_success_relaxed.sum())
    overall_failed_relaxed = int((~mask_success_relaxed).sum())

    # Patient stats
    patient_true = int((df['PATIENTUPLOAD_STATUS_N'] == 'TRUE').sum())
    patient_false = int((df['PATIENTUPLOAD_STATUS_N'] == 'FALSE').sum())
    patient_skipped = int((df['PATIENTUPLOAD_STATUS_N'] == 'SKIPPED').sum())
    patient_blank = int((df['PATIENTUPLOAD_STATUS_N'] == '').sum())

    # Order stats
    order_true = int((df['ORDERUPLOAD_STATUS_N'] == 'TRUE').sum())
    order_false = int((df['ORDERUPLOAD_STATUS_N'] == 'FALSE').sum())
    order_skipped = int((df['ORDERUPLOAD_STATUS_N'] == 'SKIPPED').sum())
    order_blank = int((df['ORDERUPLOAD_STATUS_N'] == '').sum())

    # PDF stats
    pdf_true = int((df['PDF_UPLOAD_STATUS_N'] == 'TRUE').sum())
    pdf_false = int((df['PDF_UPLOAD_STATUS_N'] == 'FALSE').sum())
    pdf_skipped = int((df['PDF_UPLOAD_STATUS_N'] == 'SKIPPED').sum())
    pdf_blank = int((df['PDF_UPLOAD_STATUS_N'] == '').sum())

    return {
        'total': total,
        'overall_classic': {
            'success': overall_success_classic,
            'failed': overall_failed_classic,
        },
        'overall_relaxed': {
            'success': overall_success_relaxed,
            'failed': overall_failed_relaxed,
        },
        'patient': {
            'TRUE': patient_true,
            'FALSE': patient_false,
            'SKIPPED': patient_skipped,
            'BLANK': patient_blank,
        },
        'order': {
            'TRUE': order_true,
            'FALSE': order_false,
            'SKIPPED': order_skipped,
            'BLANK': order_blank,
        },
        'pdf': {
            'TRUE': pdf_true,
            'FALSE': pdf_false,
            'SKIPPED': pdf_skipped,
            'BLANK': pdf_blank,
        }
    }


def main():
    file_path = sys.argv[1] if len(sys.argv) > 1 else FILE_DEFAULT
    df = load_df(file_path)
    stats = compute_counts(df)

    print("\nCounts for:", file_path)
    print("=" * 60)
    print(f"Total rows:                      {stats['total']}")
    print(f"Overall successful (classic):    {stats['overall_classic']['success']}")
    print(f"Overall failed   (classic):      {stats['overall_classic']['failed']}")
    print(f"Overall successful (relaxed):    {stats['overall_relaxed']['success']}  <-- treating patient SKIPPED as success if order TRUE")
    print(f"Overall failed   (relaxed):      {stats['overall_relaxed']['failed']}")

    print("\nPatient status counts:")
    print(f"  TRUE:   {stats['patient']['TRUE']}  | FALSE:  {stats['patient']['FALSE']}  | SKIPPED: {stats['patient']['SKIPPED']}  | BLANK: {stats['patient']['BLANK']}")
    print("Order status counts:")
    print(f"  TRUE:   {stats['order']['TRUE']}  | FALSE:  {stats['order']['FALSE']}  | SKIPPED: {stats['order']['SKIPPED']}  | BLANK: {stats['order']['BLANK']}")
    print("PDF status counts:")
    print(f"  TRUE:   {stats['pdf']['TRUE']}  | FALSE:  {stats['pdf']['FALSE']}  | SKIPPED: {stats['pdf']['SKIPPED']}  | BLANK: {stats['pdf']['BLANK']}")


if __name__ == "__main__":
    main()
