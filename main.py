import subprocess
import sys
import os
import glob
import pandas as pd
import requests

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
    for f in files_to_delete:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"[CLEANUP] Deleted old file: {f}")
            except Exception as e:
                print(f"[CLEANUP] Could not delete {f}: {e}")

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
    run_script("supremesheet.py", [combined_excel])
    
    # Step 7: Confirm output
    supremesheet_output = f"supreme_excel_{company_key}.xlsx"
    if os.path.exists(supremesheet_output):
        print(f"\n🎉 Supreme sheet is ready for {company['name']}: {supremesheet_output}")
        
        # Step 8: Run Upload_Patients_Orders.py on the supreme Excel output
        print(f"\nStep 5: Uploading Patients and Orders for {company['name']}...")
        run_script("Upload_Patients_Orders.py", [supremesheet_output])
        
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
            except Exception as e:
                print(f"❌ Error processing {company_key}: {e}")
        
        print(f"\n✅ Processing complete! Successfully processed {len(successful_companies)} out of {len(companies_to_process)} companies.")
        if successful_companies:
            print(f"Successful companies: {', '.join(successful_companies)}")
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
        else:
            print("\n❌ Processing failed. Check logs for details.")
    
    print("\n🎉 Pipeline execution complete!")
