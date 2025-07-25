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

if __name__ == "__main__":
    # Import config functions
    from config import get_active_company, show_active_company, set_active_company, list_companies
    
    # Initialize date variables
    start_date = None
    end_date = None
    
    # Show current active company
    print("🏢 Current Active Company:")
    show_active_company()
    print()
    
    # Check command line arguments
    import sys
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--list-companies" or sys.argv[i] == "-l":
            list_companies()
            sys.exit(0)
        elif sys.argv[i] == "--set-company" or sys.argv[i] == "-s":
            if i + 1 < len(sys.argv):
                try:
                    set_active_company(sys.argv[i + 1])
                    print()
                    i += 2  # Skip the company key
                    continue
                except ValueError as e:
                    print(f"❌ {e}")
                    sys.exit(1)
            else:
                print("❌ Please provide a company key. Use --list-companies to see options.")
                sys.exit(1)
        elif sys.argv[i] == "--help" or sys.argv[i] == "-h":
            print("DoctorAlliance PDF Processing Pipeline")
            print("=" * 50)
            print("Usage:")
            print("  python main.py                    # Run with current active company")
            print("  python main.py --list-companies   # List all available companies")
            print("  python main.py --set-company <key> # Change active company")
            print("  python main.py --june             # Process June 2024 documents")
            print("  python main.py --date <start> <end> # Process specific date range")
            print("  python main.py --help             # Show this help")
            print()
            print("Available company keys: housecall_md, los_cerros, rocky_mountain")
            print("Date format: MM/DD/YYYY (e.g., 06/01/2024)")
            sys.exit(0)
        elif sys.argv[i] == "--june":
            # Set June 2024 date range
            start_date = "06/01/2024"
            end_date = "06/30/2024"
            print(f"📅 Processing June 2024 documents: {start_date} to {end_date}")
            i += 1
        elif sys.argv[i] == "--date":
            if i + 2 < len(sys.argv):
                start_date = sys.argv[i + 1]
                end_date = sys.argv[i + 2]
                print(f"📅 Processing documents from {start_date} to {end_date}")
                i += 3  # Skip the date arguments
                continue
            else:
                print("❌ Please provide start and end dates. Format: MM/DD/YYYY")
                sys.exit(1)
        else:
            # Unknown argument
            print(f"❌ Unknown argument: {sys.argv[i]}")
            sys.exit(1)
        i += 1
    
    # Get active company info
    company_info = get_active_company()
    
    # CLEANUP old Excel outputs
    cleanup_old_excels()

    print("========== DoctorAlliance: Full Pipeline ==========")
    print("Step 1: Extracting Document IDs & NPIs via Selenium...")

    # Step 1: Run Selenium extractor with date parameters if specified
    if start_date and end_date:
        print(f"📅 Running Selenium extractor for date range: {start_date} to {end_date}")
        run_script("selenium_extractor.py", [start_date, end_date])
    else:
        print("📅 Running Selenium extractor with default date range (30 days ago)")
        run_script("selenium_extractor.py")

    # Step 2: Get latest DocumentID_NPI_*.xlsx file from Combined folder
    latest_npi_excel = get_latest_file("Combined", "DocumentID_NPI_*.xlsx")
    if not latest_npi_excel:
        print("❌ No NPI output found from selenium_extractor.py, exiting.")
        sys.exit(1)
    print(f"✅ Found latest NPI file: {latest_npi_excel}")

    # Step 3: Run pipeline_main.py (enhanced PDF extractor) and pass the NPI Excel as argument
    print("\nStep 2: Extracting PDF data with enhanced accuracy and producing final Excel...")
    run_script("pipeline_main.py", [latest_npi_excel])

    # Step 4: Find the output Excel from pipeline_main.py
    pdf_output_excel = "doctoralliance_orders_accuracy_focused.xlsx"
    if not os.path.exists(pdf_output_excel):
        print("❌ Enhanced PDF extractor output not found, exiting.")
        sys.exit(1)

    # Step 5: Merge/join both Excels into a final combined output
    print("\nStep 3: Merging both outputs...")
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
    print("\nStep 3.1: Removing orders already present on platform...")
    existing_doc_ids = get_existing_document_ids()  # Uses active company from config
    before_rows = len(merged)
    merged = merged[~merged["Document ID"].astype(str).isin(existing_doc_ids)].copy()
    after_rows = len(merged)
    print(f"✅ Removed {before_rows - after_rows} rows with existing Document IDs already present in the platform.")

    combined_excel = "doctoralliance_combined_output.xlsx"
    merged.to_excel(combined_excel, index=False)
    print(f"\n✅ Combined output written to {combined_excel}")

    # Step 6: Run supremesheet.py using the combined Excel as input
    print("\nStep 4: Running supremesheet.py on combined output...")
    run_script("supremesheet.py", [combined_excel])

    # Step 7: Confirm output
    supremesheet_output = "supreme_excel.xlsx"
    if os.path.exists(supremesheet_output):
        print(f"\n🎉 ALL DONE! Supreme sheet is ready: {supremesheet_output}")
    else:
        print("\n❌ supremesheet.py did not produce supreme_excel.xlsx, check logs.")
        sys.exit(1)

    # Step 8: Run Upload_Patients_Orders.py on the supreme Excel output
    print("\nStep 5: Uploading Patients and Orders using Upload_Patients_Orders.py ...")
    run_script("Upload_Patients_Orders.py", [supremesheet_output])

    print(f"\n✅ Upload_Patients_Orders.py finished. Check your output or logs for details.")

    # Step 9: Run mail.py to send email with output Excel
    print("\nStep 6: Sending email with the output Excel (mail.py)...")
    run_script("SendMail.py", [supreme_excel_with_patient_and_order_upload])
    print("\n✅ All steps finished. Check your mail for the report!")
