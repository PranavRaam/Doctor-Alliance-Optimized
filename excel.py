import pandas as pd
import json
import csv
from datetime import datetime
import os

def load_company_mapping():
    """Load company mapping from company.json."""
    try:
        with open('company.json', 'r') as f:
            company_data = json.load(f)
        # Create reverse mapping (company ID to company name)
        company_mapping = {v: k for k, v in company_data.items()}
        return company_mapping
    except FileNotFoundError:
        print("⚠️  company.json not found")
        return {}

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
        print("⚠️  pg_ids.csv not found")
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

# Read the Excel file
input_file = "supreme_excel_internal_medicine_associates_okc.xlsx"
df = pd.read_excel(input_file)

# Process all records and identify data quality issues (not patient creation status)
print(f"Processing {len(df)} total records for data quality issues...")

# Check if we have any records to process
if len(df) == 0:
    print("❌ No records found. Exiting.")
    exit()

# Create output dataframe with selected columns
df_out = pd.DataFrame()
df_out["docid"] = df["Document ID"]
df_out["patient_name"] = df["patientName"]
df_out["dob"] = df["dob"]
df_out["dabackofficeid"] = df["DABackOfficeID"]
df_out["mrn_number"] = df["mrn"]

# Apply company name conversion
df_out["pg name"] = df["Pgcompanyid"].apply(get_pg_company_name)
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

# Check if we have any records with issues
if len(df_out) == 0:
    print("❌ No records with issues found. Exiting.")
    exit()

# Group by PG company only
grouped = df_out.groupby("pg name")

print(f"🏢 Found {len(grouped)} unique PG companies:")
for pg_name, group in grouped:
    print(f"   - {pg_name}: {len(group)} records with issues")

# Create one Excel file with multiple sheets
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_filename = f"failed_records_by_pg_{timestamp}.xlsx"

with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
    for pg_name, group in grouped:
        # Clean the PG name for sheet name
        clean_pg_name = clean_company_name(pg_name)
        
        # Create sheet name (Excel has 31 character limit for sheet names)
        sheet_name = clean_pg_name
        if len(sheet_name) > 31:
            sheet_name = sheet_name[:31]
        
        # Save to Excel sheet
        group.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Added sheet: {sheet_name} ({len(group)} records)")

print(f"\n📁 Created single file: {output_filename}")
print(f"📋 Total sheets: {len(grouped)}")
print(f"Total records with issues: {len(df_out)}")

print(f"\nData quality analysis complete!")
print(f"Note: PatientExist=FALSE records are valid new patient creation scenarios, not failures.")
