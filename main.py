import subprocess
import sys
import os
import glob
import pandas as pd
import requests
import json
import csv
from datetime import datetime

def run_script(script, input_args=None):
    args = [sys.executable, script]
    if input_args:
        args += input_args
    result = subprocess.run(args)
    if result.returncode != 0:
        print(f"❌ Error running {script}, exiting.")
        sys.exit(1)

def get_latest_file(directory, pattern):
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def get_existing_document_ids(company_key=None):
    from config import get_company_api_url, get_active_company
    
    # Use active company if no specific company provided
    if company_key is None:
        # Get the active company key from config
        from config import ACTIVE_COMPANY
        company_key = ACTIVE_COMPANY
    
    url = get_company_api_url(company_key)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        orders = response.json()
        existing_doc_ids = set(str(order["documentID"]) for order in orders if "documentID" in order)
        print(f"[INFO] Found {len(existing_doc_ids)} existing Document IDs on platform.")
        return existing_doc_ids
    except Exception as e:
        print(f"❌ Error fetching existing Document IDs: {e}")
        return set()

def cleanup_old_excels():
    files_to_delete = [
        "supreme_excel_with_patient_upload.xlsx",
        "supreme_excel_with_patient_and_order_upload.xlsx",
        "doctoralliance_orders_final.xlsx",
        "doctoralliance_orders_accuracy_focused.xlsx",
        "doctoralliance_combined_output.xlsx",
        "supreme_excel.xlsx"
    ]
    
    # Delete specific files
    for f in files_to_delete:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"[CLEANUP] Deleted old file: {f}")
            except Exception as e:
                print(f"[CLEANUP] Could not delete {f}: {e}")
    
    # Delete company-specific files using glob patterns
    import glob
    patterns = [
        "doctoralliance_combined_output_*.xlsx",
        "supreme_excel_*.xlsx"
    ]
    for pattern in patterns:
        for f in glob.glob(pattern):
            try:
                os.remove(f)
                print(f"[CLEANUP] Deleted old file: {f}")
            except Exception as e:
                print(f"[CLEANUP] Could not delete {f}: {e}")

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

def clean_company_name(name):
    """Clean company name for use in filename."""
    if pd.isna(name) or name == "Unknown":
        return "Unknown_Company"
    
    # Remove special characters and replace spaces with underscores
    cleaned = str(name).replace('/', '_').replace('\\', '_').replace(':', '_')
    cleaned = cleaned.replace(' ', '_').replace('-', '_').replace('.', '_')
    # Remove any remaining special characters
    cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')
    return cleaned

def create_failed_records_excel(supreme_excel_path, company_key, start_date, end_date):
    """Create failed records Excel file from supreme Excel output."""
    print(f"📊 Creating failed records report from: {supreme_excel_path}")
    
    # Load mappings
    company_mapping = load_company_mapping()
    pg_mapping = load_pg_mapping()
    
    # Read the supreme Excel file
    df = pd.read_excel(supreme_excel_path)
    
    # Filter for only failed and skipped records
    df = df[df["PATIENTUPLOAD_STATUS"].isin(["FALSE", "SKIPPED"])]
    
    print(f"📊 Processing {len(df)} failed/skipped records...")
    
    # Check if we have any records to process
    if len(df) == 0:
        print("❌ No failed/skipped records found.")
        return None
    
    # Create output dataframe with selected columns
    df_out = pd.DataFrame()
    df_out["docid"] = df["Document ID"]
    df_out["patient_name"] = df["patientName"]
    df_out["dob"] = df["dob"]
    df_out["dabackofficeid"] = df["DABackOfficeID"]
    df_out["mrn_number"] = df["mrn"]
    
    # Apply company name conversion
    def get_pg_company_name(pg_id):
        """Convert PG ID to company name using pg_ids.csv."""
        if pd.isna(pg_id):
            return ""
        
        # Format the UUID with hyphens
        formatted_pg_id = format_uuid(pg_id)
        if formatted_pg_id:
            return pg_mapping.get(formatted_pg_id, "")
        else:
            return ""
    
    def get_company_name(company_id):
        """Convert company ID to company name using company.json."""
        if pd.isna(company_id):
            return ""
        
        # Format the UUID with hyphens
        formatted_company_id = format_uuid(company_id)
        if formatted_company_id:
            return company_mapping.get(formatted_company_id, "")
        else:
            return ""
    
    df_out["pg name"] = df["Pgcompanyid"].apply(get_pg_company_name)
    df_out["agency name"] = df["companyId"].apply(get_company_name)
    
    # Add reason field based on missing data logic
    def get_reason(row):
        # Check if patient doesn't exist
        if not row.get("PatientExist", True):
            return "Patient Does Not Exist"
        
        # Check for insufficient data (missing patient name or MRN)
        missing_patient_name = pd.isna(row["patientName"]) or str(row["patientName"]).strip() == ""
        missing_mrn = pd.isna(row["mrn"]) or str(row["mrn"]).strip() == ""
        
        if missing_patient_name or missing_mrn:
            return "Insufficient Data"
        
        # If all checks pass, return Success (will be filtered out)
        return "Success"
    
    df_out["reason"] = df.apply(get_reason, axis=1)
    
    # Filter for failed/skipped records only (exclude successful ones)
    df_out = df_out[df_out["reason"] != "Success"]
    
    print(f"📊 Found {len(df_out)} failed/skipped records with issues out of {len(df)} total failed/skipped records")
    
    # Check if we have any records with issues
    if len(df_out) == 0:
        print("❌ No failed/skipped records with issues found.")
        return None
    
    # Group by PG company only
    grouped = df_out.groupby("pg name")
    
    print(f"🏢 Found {len(grouped)} unique PG companies:")
    for pg_name, group in grouped:
        print(f"   - {pg_name}: {len(group)} failed records")
    
    # Create filename with PG company name and date range
    # Get the most common PG company name (primary company for this run)
    pg_company_names = df_out["pg name"].value_counts()
    if len(pg_company_names) > 0 and pg_company_names.index[0] != "":
        primary_pg_name = pg_company_names.index[0]
        # Clean the PG name for filename
        clean_pg_name = clean_company_name(primary_pg_name)
        # Format dates (MM-DD-YYYY)
        start_date_formatted = start_date.replace("/", "-")
        end_date_formatted = end_date.replace("/", "-")
        output_filename = f"{clean_pg_name}_{start_date_formatted}_{end_date_formatted}.xlsx"
    else:
        # Fallback if no PG company found
        output_filename = f"Failed_Records_{start_date.replace('/', '-')}_{end_date.replace('/', '-')}.xlsx"
    
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
    
    print(f"\n📁 Created failed records file: {output_filename}")
    print(f"📋 Total sheets: {len(grouped)}")
    print(f"📊 Total failed records: {len(df_out)}")
    
    return output_filename

def process_single_company(company_key, start_date, end_date):
    """Process a single company with the given date range."""
    from config import get_company_config
    
    company = get_company_config(company_key)
    print(f"\n🏢 Processing Company: {company['name']} ({company_key})")
    print(f"   PG Company ID: {company['pg_company_id']}")
    print(f"   Helper ID: {company['helper_id']}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    
    # Set the active company for this processing
    from config import set_active_company
    set_active_company(company_key)
    
    # Step 1: Run Selenium extractor with date parameters
    print(f"\nStep 1: Extracting Document IDs & NPIs via Selenium for {company['name']}...")
    run_script("selenium_extractor.py", [start_date, end_date, company_key])
    
    # Step 2: Get latest DocumentID_NPI_*.xlsx file from Combined folder
    # Look for company-specific file first, then fallback to general pattern
    latest_npi_excel = get_latest_file("Combined", f"DocumentID_NPI_{company_key}_*.xlsx")
    if not latest_npi_excel:
        latest_npi_excel = get_latest_file("Combined", "DocumentID_NPI_*.xlsx")
    
    if not latest_npi_excel:
        print(f"❌ No NPI output found from selenium_extractor.py for {company['name']}, skipping.")
        return False
    print(f"✅ Found latest NPI file: {latest_npi_excel}")
    
    # Step 3: Run pipeline_main.py
    print(f"\nStep 2: Extracting PDF data with enhanced accuracy for {company['name']}...")
    run_script("pipeline_main.py", [latest_npi_excel])
    
    # Step 4: Find the output Excel from pipeline_main.py
    pdf_output_excel = "doctoralliance_orders_accuracy_focused.xlsx"
    if not os.path.exists(pdf_output_excel):
        print(f"❌ Enhanced PDF extractor output not found for {company['name']}, skipping.")
        return False
    
    # Step 5: Merge/join both Excels into a final combined output
    print(f"\nStep 3: Merging both outputs for {company['name']}...")
    df_npi = pd.read_excel(latest_npi_excel)
    df_pdf = pd.read_excel(pdf_output_excel)
    
    # Try both int and str merge for docId/Document ID
    try:
        merged = df_pdf.merge(
            df_npi,
            how='left',
            left_on='docId',
            right_on='Document ID'
        )
    except Exception as e:
        print("❗ Merge on docId/Document ID failed, attempting type-casting and merge again.")
        df_pdf['docId'] = df_pdf['docId'].astype(str)
        df_npi['Document ID'] = df_npi['Document ID'].astype(str)
        merged = df_pdf.merge(
            df_npi,
            how='left',
            left_on='docId',
            right_on='Document ID'
        )
    
    # REMOVE DUPLICATES (orders already on platform)
    print(f"\nStep 3.1: Removing orders already present on platform for {company['name']}...")
    existing_doc_ids = get_existing_document_ids(company_key)
    before_rows = len(merged)
    merged = merged[~merged["Document ID"].astype(str).isin(existing_doc_ids)].copy()
    after_rows = len(merged)
    print(f"✅ Removed {before_rows - after_rows} rows with existing Document IDs already present in the platform.")
    
    # Save company-specific combined output
    combined_excel = f"doctoralliance_combined_output_{company_key}.xlsx"
    merged.to_excel(combined_excel, index=False)
    print(f"\n✅ Combined output written to {combined_excel}")
    
    # Step 6: Run supremesheet.py using the combined Excel as input
    print(f"\nStep 4: Running supremesheet.py on combined output for {company['name']}...")
    supremesheet_output = f"supreme_excel_{company_key}.xlsx"
    run_script("supremesheet.py", [combined_excel, supremesheet_output])
    
    # Step 7: Confirm output
    if os.path.exists(supremesheet_output):
        print(f"\n🎉 Supreme sheet is ready for {company['name']}: {supremesheet_output}")
        
        # Step 8: Create failed records Excel file
        print(f"\nStep 5: Creating failed records report for {company['name']}...")
        failed_records_output = create_failed_records_excel(supremesheet_output, company_key, start_date, end_date)
        
        # Step 9: Run Upload_Patients_Orders.py on the supreme Excel output
        print(f"\nStep 6: Uploading Patients and Orders for {company['name']}...")
        try:
            run_script("Upload_Patients_Orders.py", [supremesheet_output, company_key])
            
            # Verify that the expected output files were created
            expected_files = [
                supremesheet_output.replace('.xlsx', '_with_patient_upload.xlsx'),
                supremesheet_output.replace('.xlsx', '_with_patient_and_order_upload.xlsx')
            ]
            
            created_files = []
            for expected_file in expected_files:
                if os.path.exists(expected_file):
                    created_files.append(expected_file)
                    print(f"✅ Created: {expected_file}")
                else:
                    print(f"❌ Missing: {expected_file}")
            
            if len(created_files) == 2:
                print(f"\n✅ Upload_Patients_Orders.py finished successfully for {company['name']}.")
                print(f"   Created files: {', '.join(created_files)}")
            else:
                print(f"\n⚠️  Upload_Patients_Orders.py completed but some files are missing for {company['name']}.")
                print(f"   Expected: {len(expected_files)} files, Created: {len(created_files)} files")
            
        except Exception as e:
            print(f"\n❌ Error in Upload_Patients_Orders.py for {company['name']}: {e}")
            return False
        
        print(f"\n✅ Upload_Patients_Orders.py finished for {company['name']}.")
        return True
    else:
        print(f"\n❌ supremesheet.py did not produce {supremesheet_output} for {company['name']}, check logs.")
        return False

if __name__ == "__main__":
    # Import config functions
    from config import (
        get_active_company, show_active_company, set_active_company, 
        list_companies, get_companies_to_process, get_date_range, 
        show_current_config, PROCESS_MULTIPLE_COMPANIES
    )
    
    # Show current configuration
    show_current_config()
    print()
    
    # Get date range from config
    start_date, end_date = get_date_range()
    
    # Get companies to process
    companies_to_process = get_companies_to_process()
    
    # CLEANUP old Excel outputs
    cleanup_old_excels()
    
    print("========== DoctorAlliance: Full Pipeline ==========")
    
    # Process companies
    if PROCESS_MULTIPLE_COMPANIES:
        print(f"🔄 Processing {len(companies_to_process)} companies...")
        successful_companies = []
        
        for company_key in companies_to_process:
            try:
                success = process_single_company(company_key, start_date, end_date)
                if success:
                    successful_companies.append(company_key)
                    
                    # Send email for each successful company
                    supremesheet_output = f"supreme_excel_{company_key}.xlsx"
                    if os.path.exists(supremesheet_output):
                        print(f"\n📧 Sending email for {company_key}...")
                        run_script("SendMail.py", [supremesheet_output])
                        print(f"✅ Email sent for {company_key}")
                    
                    # Also send failed records Excel if it exists
                    failed_records_pattern = f"*_{start_date.replace('/', '-')}_{end_date.replace('/', '-')}.xlsx"
                    failed_records_files = glob.glob(failed_records_pattern)
                    if failed_records_files:
                        latest_failed_records = max(failed_records_files, key=os.path.getmtime)
                        print(f"\n📧 Sending failed records report for {company_key}: {latest_failed_records}")
                        run_script("SendMail.py", [latest_failed_records])
                        print(f"✅ Failed records report sent for {company_key}")
                        
            except Exception as e:
                print(f"❌ Error processing {company_key}: {e}")
        
        print(f"\n✅ Processing complete! Successfully processed {len(successful_companies)} out of {len(companies_to_process)} companies.")
        if successful_companies:
            print(f"Successful companies: {', '.join(successful_companies)}")
            
            # Send summary email for all companies
            print(f"\n📧 Sending summary email for all processed companies...")
            try:
                # Create a summary of all successful companies
                summary_file = f"processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(summary_file, 'w') as f:
                    f.write(f"Processing Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Date Range: {start_date} to {end_date}\n")
                    f.write(f"Successfully processed: {len(successful_companies)} out of {len(companies_to_process)} companies\n")
                    f.write(f"Successful companies: {', '.join(successful_companies)}\n")
                    f.write(f"Failed companies: {', '.join(set(companies_to_process) - set(successful_companies))}\n")
                
                # Send summary email
                run_script("SendMail.py", [summary_file])
                print(f"✅ Summary email sent")
            except Exception as e:
                print(f"❌ Error sending summary email: {e}")
    else:
        # Single company processing
        company_key = companies_to_process[0]
        success = process_single_company(company_key, start_date, end_date)
        
        if success:
            # Step 9: Run mail.py to send email with output Excel
            supremesheet_output = f"supreme_excel_{company_key}.xlsx"
            print(f"\nStep 6: Sending email with the output Excel (mail.py)...")
            run_script("SendMail.py", [supremesheet_output])
            print("\n✅ All steps finished. Check your mail for the report!")
            
            # Also send failed records Excel if it exists
            # Look for files with PG company name and date range pattern
            failed_records_pattern = f"*_{start_date.replace('/', '-')}_{end_date.replace('/', '-')}.xlsx"
            failed_records_files = glob.glob(failed_records_pattern)
            if failed_records_files:
                latest_failed_records = max(failed_records_files, key=os.path.getmtime)
                print(f"\n📧 Sending failed records report: {latest_failed_records}")
                run_script("SendMail.py", [latest_failed_records])
                print("✅ Failed records report sent!")
        else:
            print("\n❌ Processing failed. Check logs for details.")
    
    print("\n🎉 Pipeline execution complete!")
