import pandas as pd
import json
import csv
from datetime import datetime
import os

def load_company_mapping():
    """Load company mapping from Company IDs.csv (ID,Name)."""
    mapping = {}
    try:
        with open('Company IDs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Expect headers: ID,Name
            for row in reader:
                cid = row.get('ID') or row.get('Id') or row.get('id')
                name = row.get('Name') or row.get('name')
                if cid and name:
                    mapping[cid.strip()] = name.strip()
        if not mapping:
            print("WARN: Company IDs.csv loaded but no rows mapped. Check headers are ID,Name")
    except FileNotFoundError:
        print("WARN: Company IDs.csv not found")
    return mapping

def load_pg_mapping():
    """Load PG mapping from pg_ids.csv."""
    try:
        pg_mapping = {}
        with open('pg_ids.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                pg_mapping[row['Id']] = row['Name']
        return pg_mapping
    except FileNotFoundError:
        print("WARN: pg_ids.csv not found")
        return {}

def format_uuid(uuid_str):
    """Add hyphens to UUID string to match mapping format."""
    if pd.isna(uuid_str):
        return None
    
    uuid_str = str(uuid_str).strip()
    # Remove any existing hyphens first
    uuid_str = uuid_str.replace('-', '')
    
    # Add hyphens in the correct positions (8-4-4-4-12 format)
    if len(uuid_str) == 32:
        return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
    else:
        return uuid_str  # Return as is if not 32 characters

def get_pg_company_name(pg_id):
    """Convert PG ID to company name using pg_ids.csv."""
    if pd.isna(pg_id):
        return "Unknown"
    
    # Format the UUID with hyphens
    formatted_pg_id = format_uuid(pg_id)
    if formatted_pg_id:
        return pg_mapping.get(formatted_pg_id, f"Unknown PG Company ({pg_id})")
    else:
        return f"Invalid PG ID ({pg_id})"

def get_company_name(company_id):
    """Convert company ID to company name using company.json."""
    if pd.isna(company_id):
        return "Unknown"
    
    # Format the UUID with hyphens
    formatted_company_id = format_uuid(company_id)
    if formatted_company_id:
        return company_mapping.get(formatted_company_id, f"Unknown Company ({company_id})")
    else:
        return f"Invalid Company ID ({company_id})"

def clean_company_name(name):
    """Clean company name for use in filename."""
    if pd.isna(name) or name == "Unknown" or str(name).strip() == "":
        return "Unknown_Company"
    
    # Remove special characters and replace spaces with underscores
    cleaned = str(name).replace('/', '_').replace('\\', '_').replace(':', '_')
    cleaned = cleaned.replace(' ', '_').replace('-', '_').replace('.', '_')
    # Remove any remaining special characters
    cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')
    
    # Ensure we have at least one character
    if not cleaned or len(cleaned.strip()) == 0:
        return "Unknown_Company"
    
    return cleaned

# Load mappings
company_mapping = load_company_mapping()
pg_mapping = load_pg_mapping()

import sys
import os


def process_file(input_file: str):
    print(f"\nProcessing file: {input_file}")
    df = pd.read_excel(input_file)

    # Process all records and identify data quality issues (not upload outcomes)
    print(f"Processing {len(df)} total records for data quality issues...")

    if len(df) == 0:
        print("No records found. Skipping.")
        return

    # Create output dataframe with selected columns
    df_out = pd.DataFrame()
    df_out["docid"] = df["Document ID"]
    df_out["patient_name"] = df["patientName"]
    df_out["dob"] = df["dob"]
    df_out["dabackofficeid"] = df["DABackOfficeID"]
    df_out["mrn_number"] = df["mrn"]

    # Apply company name conversion
    df_out["pg name"] = df["Pgcompanyid"].apply(get_pg_company_name)
    # Prefer the agency name captured during payload building
    if "nameOfAgency" in df.columns:
        df_out["agency name"] = df["nameOfAgency"].fillna("")
    else:
        # Fallback to companyId -> name mapping
        df_out["agency name"] = df["companyId"].apply(get_company_name)

    # Add reason field based on missing data logic
    def get_reason(row):
        # Check for insufficient data (missing patient name or MRN)
        missing_patient_name = pd.isna(row["patientName"]) or str(row["patientName"]).strip() == ""
        missing_mrn = pd.isna(row["mrn"]) or str(row["mrn"]).strip() == ""

        if missing_patient_name or missing_mrn:
            return "Insufficient Data"

        # Check for missing required fields
        missing_doc_id = pd.isna(row["Document ID"]) or str(row["Document ID"]).strip() == ""
        missing_dabackofficeid = pd.isna(row["DABackOfficeID"]) or str(row["DABackOfficeID"]).strip() == ""

        if missing_doc_id or missing_dabackofficeid:
            return "Missing Required Fields"

        # If all checks pass, return Success (will be filtered out)
        return "Success"

    df_out["reason"] = df.apply(get_reason, axis=1)

    # Filter for records with issues only (exclude successful ones)
    df_out = df_out[df_out["reason"] != "Success"]

    print(f"Found {len(df_out)} records with issues out of {len(df)} total records")

    if len(df_out) == 0:
        print("No records with issues found. Skipping output.")
        return

    # Group by PG company only
    grouped = df_out.groupby("pg name")

    print(f"Found {len(grouped)} unique PG companies:")
    for pg_name, group in grouped:
        print(f"   - {pg_name}: {len(group)} records with issues")

    # Create one Excel file with multiple sheets (include source file stem in name)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_stem = os.path.splitext(os.path.basename(input_file))[0]
    safe_stem = clean_company_name(base_stem)
    output_filename = f"failed_records_by_pg_{safe_stem}_{timestamp}.xlsx"

    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        for pg_name, group in grouped:
            clean_pg_name = clean_company_name(pg_name)
            sheet_name = clean_pg_name[:31] if len(clean_pg_name) > 31 else clean_pg_name
            group.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Added sheet: {sheet_name} ({len(group)} records)")

    print(f"\nCreated file: {output_filename}")
    print(f"Total sheets: {len(grouped)}")
    print(f"Total records with issues: {len(df_out)}")
    print(f"\nData quality analysis complete!")
    print(f"Note: PatientExist=FALSE records are valid new patient creation scenarios, not failures.")


def main():
    # If a specific file is passed, process that. If --all, process all matching. Otherwise most recent.
    args = sys.argv[1:]
    candidates = []

    def is_match(filename: str) -> bool:
        return (
            filename.startswith('supreme_excel_') and (
                filename.endswith('_with_patient_and_order_upload.xlsx') or
                filename.endswith('_with_patient_and_order_upload_FIXED.xlsx')
            )
        )

    if args and args[0] != '--all':
        file_path = args[0]
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            sys.exit(1)
        candidates = [file_path]
    else:
        # collect matching files in cwd
        for file in os.listdir('.'):
            if is_match(file):
                candidates.append(file)
        if not candidates:
            # fallback to any supreme excel if none match strict pattern
            for file in os.listdir('.'):
                if file.startswith('supreme_excel_') and file.endswith('.xlsx'):
                    candidates.append(file)
        # if not --all, reduce to most recent
        if args != ['--all']:
            if not candidates:
                print("No supreme excel files found. Please ensure a supreme excel file exists.")
                sys.exit(1)
            most_recent = max(candidates, key=os.path.getctime)
            print(f"Using most recent file: {most_recent}")
            candidates = [most_recent]

    if not candidates:
        print("No candidate files found to process.")
        sys.exit(1)

    print(f"Files to process ({len(candidates)}):")
    for c in candidates:
        print(f"  - {c}")

    for c in candidates:
        process_file(c)


if __name__ == '__main__':
    main()
