import subprocess
import sys
import os
import glob
import shutil
import pandas as pd
import requests
import json
import csv
import base64
import tempfile
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
        # Merge CSV fallback (prefers json entries)
        csv_map = load_company_ids_csv()
        for cid, name in csv_map.items():
            company_mapping.setdefault(cid, name)
        return company_mapping
    except FileNotFoundError:
        print("⚠️  company.json not found")
        # Fallback to CSV only
        return load_company_ids_csv()



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



def prefill_document_names(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DocumentName/documentType is populated for all rows using the document API."""
    if df is None or len(df) == 0:
        return df
    # Prefer to fill 'documentType' if missing/blank, also add 'DocumentName' column for traceability
    if 'DocumentName' not in df.columns:
        df['DocumentName'] = ""
    for i, row in df.iterrows():
        current_type = str(row.get('documentType', '') or '').strip()
        current_name = str(row.get('DocumentName', '') or '').strip()
        if current_type and current_name:
            continue
        doc_id = row.get('Document ID') or row.get('docId')
        if pd.isna(doc_id) or not str(doc_id).strip():
            continue
        info = get_document_info(str(doc_id).strip())
        if info.get('success') and info.get('document_name'):
            doc_name = info['document_name']
            if not current_type:
                df.at[i, 'documentType'] = doc_name
            if not current_name:
                df.at[i, 'DocumentName'] = doc_name
    return df



def clean_company_name(name):
    """Clean company name for use in filename."""
    if pd.isna(name) or name == "Unknown" or str(name).strip() == "":
        return "Unknown_Company"
    
    # Remove special characters and replace spaces with underscores
    cleaned = str(name).replace('/', '_').replace('\\', '_').replace(':', '_')
    cleaned = cleaned.replace(' ', '_').replace('-', '_').replace('.', '_')
    # Remove any remaining special characters
    cleaned = ''.join(c for c in cleaned if c.isalnum() or c == '_')
    
    # Ensure we have at least one character after cleaning
    if not cleaned or len(cleaned.strip()) == 0:
        return "Unknown_Company"
    
    return cleaned



def get_output_dirs(company_name, start_date, end_date):
    """Return a dictionary of output directories for a company and date range."""
    base = os.path.join(
        "outputs",
        clean_company_name(company_name),
        f"{start_date.replace('/', '-')}__to__{end_date.replace('/', '-')}"
    )
    dirs = {
        'base': base,
        'intermediate': os.path.join(base, 'intermediate'),
        'uploads': os.path.join(base, 'uploads'),
        'reports': os.path.join(base, 'reports'),
    }
    for d in dirs.values():
        os.makedirs(d, exist_ok=True)
    return dirs


def move_if_exists(src_path, dst_dir):
    """Move a file to destination dir if it exists; return new path or None."""
    try:
        if src_path and os.path.exists(src_path):
            os.makedirs(dst_dir, exist_ok=True)
            filename = os.path.basename(src_path)
            dst_path = os.path.join(dst_dir, filename)
            # If destination already exists, append numeric suffix
            if os.path.exists(dst_path):
                name, ext = os.path.splitext(filename)
                i = 1
                while os.path.exists(os.path.join(dst_dir, f"{name}({i}){ext}")):
                    i += 1
                dst_path = os.path.join(dst_dir, f"{name}({i}){ext}")
            shutil.move(src_path, dst_path)
            print(f"[ORGANIZE] Moved {src_path} -> {dst_path}")
            return dst_path
    except Exception as e:
        print(f"[ORGANIZE] Could not move {src_path} to {dst_dir}: {e}")
    return None


def organize_company_outputs(company_key, start_date, end_date):
    """Move generated Excel files into a professional folder structure and return moved report path if available."""
    try:
        from config import get_company_config
        company = get_company_config(company_key)
        company_name = company.get('name', company_key)
    except Exception:
        company_name = company_key

    dirs = get_output_dirs(company_name, start_date, end_date)

    # Known generated files (filenames only; may exist in cwd or already in dirs)
    candidates_intermediate = [
        f"doctoralliance_combined_output_{company_key}.xlsx",
        "doctoralliance_orders_accuracy_focused.xlsx",
        f"supreme_excel_{company_key}.xlsx",
    ]
    candidates_uploads = [
        f"supreme_excel_{company_key}_with_patient_upload.xlsx",
        f"supreme_excel_{company_key}_with_patient_and_order_upload.xlsx",
    ]
    candidates_reports = [
        f"supreme_excel_{company_key}_with_patient_and_order_upload_FIXED.xlsx",
        f"{clean_company_name(company_name)}_processing_report_{start_date.replace('/', '-')}_{end_date.replace('/', '-')}.xlsx",
    ]

    moved_report_path = None

    # Try moving from both cwd and the intermediate dir (when already written there)
    for fname in candidates_intermediate:
        # If file exists in cwd, move it
        moved = move_if_exists(fname, dirs['intermediate'])
        # If not in cwd, ensure it's in the right place already
        src_in_intermediate = os.path.join(dirs['intermediate'], fname)
        if not moved and os.path.exists(src_in_intermediate):
            print(f"[ORGANIZE] Found already in intermediate: {src_in_intermediate}")
    for fname in candidates_uploads:
        # Prefer moving from cwd
        moved = move_if_exists(fname, dirs['uploads'])
        if not moved:
            # Try moving from intermediate -> uploads
            src_in_intermediate = os.path.join(dirs['intermediate'], fname)
            if os.path.exists(src_in_intermediate):
                try:
                    moved = move_if_exists(src_in_intermediate, dirs['uploads'])
                except Exception as e:
                    print(f"[ORGANIZE] Could not move from intermediate: {src_in_intermediate} -> {dirs['uploads']}: {e}")
    for fname in candidates_reports:
        # Prefer moving from cwd
        newp = move_if_exists(fname, dirs['reports'])
        if not newp:
            # Try moving from intermediate -> reports
            src_in_intermediate = os.path.join(dirs['intermediate'], fname)
            if os.path.exists(src_in_intermediate):
                try:
                    newp = move_if_exists(src_in_intermediate, dirs['reports'])
                except Exception as e:
                    print(f"[ORGANIZE] Could not move report from intermediate: {src_in_intermediate} -> {dirs['reports']}: {e}")
        if newp and newp.endswith('.xlsx') and 'processing_report' in os.path.basename(newp):
            moved_report_path = newp

    return moved_report_path

def create_success_failed_excels(final_excel_path, company_key, start_date, end_date):
    """Create a single Excel file with two sheets: successful and failed records from final upload file."""
    print(f"Creating success/failed Excel sheets from: {final_excel_path}")
    
    if not os.path.exists(final_excel_path):
        print(f"❌ Final Excel file not found: {final_excel_path}")
        return None, None
    
    # Read the final Excel file
    df = pd.read_excel(final_excel_path)
    print(f"Processing {len(df)} total records...")
    
    # Define success criteria
    # Consider order success as primary, and patient TRUE/SKIPPED acceptable
    order_ok = df['ORDERUPLOAD_STATUS'] == 'TRUE'
    patient_ok = df['PATIENTUPLOAD_STATUS'].isin(['TRUE', 'SKIPPED'])

    # Additional success: order creation remark indicates duplicate/already exists
    remark_col = 'ORDER_CREATION_REMARK' if 'ORDER_CREATION_REMARK' in df.columns else None
    if remark_col:
        dup_markers = ['already exist', 'already exists', 'duplicate', 'exists', 'conflict']
        dup_mask = df[remark_col].astype(str).str.lower().apply(lambda x: any(m in x for m in dup_markers))
        success_mask = (order_ok & patient_ok) | dup_mask
    else:
        success_mask = (order_ok & patient_ok)

    successful_records = df[success_mask].copy()
    failed_records = df[~success_mask].copy()
    
    print(f"✅ Successful records: {len(successful_records)}")
    print(f"❌ Failed records: {len(failed_records)}")
    
    # Create filename with date range
    start_date_formatted = start_date.replace("/", "-")
    end_date_formatted = end_date.replace("/", "-")
    
    # Get company name for filename
    from config import get_company_config
    try:
        company = get_company_config(company_key)
        company_name = clean_company_name(company['name'])
    except:
        company_name = company_key
    
    # Create single Excel file with both sheets
    combined_filename = f"{company_name}_processing_report_{start_date_formatted}_{end_date_formatted}.xlsx"
    
    # Process failed records with specific columns and failure reasons
    if len(failed_records) > 0:
        # Create failed records with specific columns
        failed_output = pd.DataFrame()
        
        # Map columns to required format
        failed_output["docid"] = failed_records.get("Document ID", "")
        failed_output["patient_name"] = failed_records.get("patientName", "")
        failed_output["dob"] = failed_records.get("dob", "")
        failed_output["dabackofficeid"] = failed_records.get("DABackOfficeID", "")
        failed_output["mrn_number"] = failed_records.get("mrn", "")
        
        # Load mappings for PG name and agency name
        company_mapping = load_company_mapping()
        pg_mapping = load_pg_mapping()
        
        def get_pg_company_name(pg_id):
            if pd.isna(pg_id):
                return ""
            formatted_pg_id = format_uuid(pg_id)
            if formatted_pg_id:
                return pg_mapping.get(formatted_pg_id, "")
            return ""
        
        def get_company_name(company_id):
            """Convert company ID to company name using company.json."""
            if pd.isna(company_id):
                return "Unknown"
            
            # Format the UUID with hyphens
            formatted_company_id = format_uuid(company_id)
            if formatted_company_id:
                company_name = company_mapping.get(formatted_company_id, "")
                if company_name:
                    return company_name
                else:
                    # If company ID not found in mapping, use a fallback approach
                    print(f"⚠️  Company ID not found in mapping: {formatted_company_id}")
                    # Try to get company name from active company config as fallback
                    try:
                        from config import get_active_company
                        active_company = get_active_company()
                        return active_company['name']
                    except:
                        return f"Unknown Company ({formatted_company_id})"
            else:
                return f"Invalid Company ID ({company_id})"
        
        failed_output["pg name"] = failed_records.get("Pgcompanyid", "").apply(get_pg_company_name)
        if "nameOfAgency" in failed_records.columns:
            failed_output["agency name"] = failed_records["nameOfAgency"].fillna("")
        else:
            failed_output["agency name"] = failed_records.get("companyId", "").apply(get_company_name)
        
        # Determine failure reason
        def get_failure_reason(row):
            patient_status = row.get("PATIENTUPLOAD_STATUS", "")
            order_status = row.get("ORDERUPLOAD_STATUS", "")
            patient_remark = row.get("PATIENTUPLOAD_REMARKS", "")
            order_remark = row.get("ORDER_CREATION_REMARK", "")
            
            reasons = []
            
            # Check patient upload failure
            if patient_status != "TRUE":
                if patient_status == "SKIPPED":
                    reasons.append("Patient upload skipped")
                elif patient_status == "FALSE":
                    if patient_remark:
                        reasons.append(f"Patient upload failed: {patient_remark}")
                    else:
                        reasons.append("Patient upload failed")
                else:
                    reasons.append("Patient upload status unclear")
            
            # Check order upload failure
            if order_status != "TRUE":
                if order_status == "SKIPPED":
                    reasons.append("Order upload skipped")
                elif order_status == "FALSE":
                    if order_remark:
                        reasons.append(f"Order upload failed: {order_remark}")
                    else:
                        reasons.append("Order upload failed")
                else:
                    reasons.append("Order upload status unclear")
            
            return "; ".join(reasons) if reasons else "Unknown failure reason"
        
        failed_output["reason"] = failed_records.apply(get_failure_reason, axis=1)
        
        # Show failure reason summary
        if len(failed_output) > 0:
            print(f"📊 Failure reasons summary:")
            reason_counts = failed_output["reason"].value_counts()
            for reason, count in reason_counts.head(5).items():
                print(f"   - {reason}: {count} records")
    else:
        print("✅ No failed records!")
        failed_output = pd.DataFrame()  # Empty DataFrame for failed records
    
    # Create Excel file with both sheets
    with pd.ExcelWriter(combined_filename, engine='openpyxl') as writer:
        # Write successful records sheet (all columns)
        if len(successful_records) > 0:
            successful_records.to_excel(writer, sheet_name='Successful_Records', index=False)
            print(f"📊 Added successful records sheet: {len(successful_records)} records")
        else:
            # Create empty successful sheet with headers
            empty_successful = pd.DataFrame(columns=df.columns)
            empty_successful.to_excel(writer, sheet_name='Successful_Records', index=False)
            print("📊 Added empty successful records sheet")
        
        # Write failed records sheet (specific columns)
        if len(failed_output) > 0:
            failed_output.to_excel(writer, sheet_name='Failed_Records', index=False)
            print(f"📊 Added failed records sheet: {len(failed_output)} records")
        else:
            # Create empty failed sheet with headers
            empty_failed = pd.DataFrame(columns=["docid", "patient_name", "dob", "dabackofficeid", "mrn_number", "pg name", "agency name", "reason"])
            empty_failed.to_excel(writer, sheet_name='Failed_Records', index=False)
            print("📊 Added empty failed records sheet")
    
    print(f"📁 Created combined Excel file: {combined_filename}")
    
    # Add Drive links for failed records if any failed records exist
    if len(failed_records) > 0:
        print(f"\n☁️  Uploading failed PDFs to Google Drive...")
        add_drive_links_to_failed_records(failed_output, combined_filename)
    
    return combined_filename, combined_filename, None  # Return same filename for both successful and failed



def get_document_info(doc_id):
    """Get document information including name from the working document API."""
    # Import API credentials from supremesheet
    from supremesheet import API_BASE, AUTH_HEADER
    
    # Use the working document API (not the broken getstatus API)
    url = f"{API_BASE}{doc_id}"
    try:
        r = requests.get(url, headers=AUTH_HEADER, timeout=30)
        data = r.json()
        
        if data.get("isSuccess") and 'value' in data:
            value = data['value']
            document_name = ""
            
            # Debug: Print the full response structure for the first few calls
            if doc_id in ['9431342', '9431476', '9433593']:  # Debug first few calls
                print(f"  [DOC_INFO_DEBUG] Full response for {doc_id}:")
                print(f"  [DOC_INFO_DEBUG] {json.dumps(data, indent=2)}")
                
                # Check for documentType structure
                if 'documentType' in value:
                    doc_type = value['documentType']
                    print(f"  [DOC_INFO_DEBUG] documentType structure: {json.dumps(doc_type, indent=2)}")
                else:
                    print(f"  [DOC_INFO_DEBUG] No documentType found in value")
            
            # Extract document name from documentType
            if 'documentType' in value:
                doc_type = value['documentType']
                if isinstance(doc_type, dict):
                    # If documentType is an object with displayName/code
                    if 'displayName' in doc_type:
                        document_name = doc_type['displayName']
                    elif 'code' in doc_type:
                        document_name = doc_type['code']
                elif isinstance(doc_type, str):
                    # If documentType is a string (like "485RECERT")
                    document_name = doc_type
            
            if document_name:
                print(f"  [DOC_INFO] ✅ Found document name: {document_name}")
                return {
                    'document_name': document_name,
                    'document_type': value.get('documentType', {}),
                    'status': value.get('status', ''),
                    'success': True
                }
            else:
                print(f"  [DOC_INFO] ❌ No document name found in response")
                return {'success': False, 'error': 'No document name in response'}
        else:
            print(f"  [DOC_INFO] ❌ API call failed. isSuccess={data.get('isSuccess')}")
            print(f"  [DOC_INFO] Error message: {data.get('errorMessage', 'No error message')}")
            return {'success': False, 'error': 'API call failed'}
            
    except Exception as e:
        print(f"  [DOC_INFO] Exception for doc_id={doc_id}: {e}")
        return {'success': False, 'error': str(e)}



def download_pdf_for_docid(doc_id):
    """Download PDF for a specific document ID using existing API."""
    # Import API credentials from supremesheet
    from supremesheet import API_BASE, AUTH_HEADER
    
    url = f"{API_BASE}{doc_id}"
    try:
        r = requests.get(url, headers=AUTH_HEADER, timeout=30)
        data = r.json()
        
        if not data.get("isSuccess"):
            print(f"  [PDF_DOWNLOAD] Failed for doc_id={doc_id}. isSuccess={data.get('isSuccess')}")
            return None
        
        # Extract PDF buffer from response
        if 'value' in data:
            value_obj = data['value']
            if isinstance(value_obj, str):
                # Sometimes value is a JSON string
                try:
                    value_obj = json.loads(value_obj)
                except:
                    print(f"  [PDF_DOWNLOAD] Could not parse value as JSON for {doc_id}")
                    return None
            
            if isinstance(value_obj, dict) and 'documentBuffer' in value_obj:
                document_buffer = value_obj['documentBuffer']
                if document_buffer:
                    print(f"  [PDF_DOWNLOAD] ✅ Downloaded PDF for {doc_id}")
                    return document_buffer
                else:
                    print(f"  [PDF_DOWNLOAD] Empty document buffer for {doc_id}")
                    return None
            else:
                print(f"  [PDF_DOWNLOAD] No documentBuffer in response for {doc_id}")
                return None
        else:
            print(f"  [PDF_DOWNLOAD] No value in response for {doc_id}")
            return None
            
    except Exception as e:
        print(f"  [PDF_DOWNLOAD] Exception for doc_id={doc_id}: {e}")
        return None



def upload_pdf_to_drive(pdf_bytes, doc_id):
    """Upload PDF to Google Drive and return the Drive link."""
    import tempfile
    
    # Create temporary file for upload
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
        temp_file.write(pdf_bytes)
        temp_file_path = temp_file.name
    
    try:
        # Prepare the upload request
        upload_url = "https://dawavadmin-djb0f9atf8e6cwgx.eastus-01.azurewebsites.net/api/Gmail/upload-to-drive-url"
        
        with open(temp_file_path, 'rb') as pdf_file:
            files = {
                'email': (None, 'admin_mydaplatform@doctoralliance.com'),
                'file': (f'{doc_id}.pdf', pdf_file, 'application/pdf')
            }
            
            response = requests.post(upload_url, files=files, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                drive_url = result.get('driveFileUrl', '')
                if drive_url:
                    print(f"    ✅ Uploaded {doc_id}.pdf to Drive: {drive_url}")
                    return drive_url
                else:
                    print(f"    ❌ No Drive URL in response for {doc_id}")
                    return None
            else:
                print(f"    ❌ Drive upload failed for {doc_id}: {response.status_code} - {response.text}")
                return None
    
    except Exception as e:
        print(f"    ❌ Exception uploading {doc_id} to Drive: {e}")
        return None
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file_path)
        except:
            pass



def add_drive_links_to_failed_records(failed_records_df, excel_filename):
    """Add Google Drive links for PDFs to the failed records sheet in the Excel file."""
    if len(failed_records_df) == 0:
        print("No failed records to add Drive links for")
        return
    
    # Get document IDs from failed records
    doc_ids = failed_records_df['docid'].dropna().unique()
    print(f"📄 Uploading {len(doc_ids)} failed PDFs to Google Drive...")
    
    if len(doc_ids) == 0:
        print("No valid document IDs in failed records")
        return
    
    # Create a mapping of doc_id to Drive URL
    doc_id_to_drive_url = {}
    uploaded_count = 0
    total_docs = len(doc_ids)
    
    for i, doc_id in enumerate(doc_ids, 1):
        try:
            print(f"  📥 Processing PDF {i}/{total_docs}: {doc_id}")
            
            # Download PDF buffer
            pdf_buffer = download_pdf_for_docid(str(doc_id))
            
            if pdf_buffer:
                # Decode base64 PDF content
                try:
                    pdf_bytes = base64.b64decode(pdf_buffer)
                    
                    # Validate it's a PDF
                    if pdf_bytes.startswith(b'%PDF-'):
                        # Upload to Google Drive
                        drive_url = upload_pdf_to_drive(pdf_bytes, str(doc_id))
                        if drive_url:
                            doc_id_to_drive_url[str(doc_id)] = drive_url
                            uploaded_count += 1
                        else:
                            doc_id_to_drive_url[str(doc_id)] = "Upload failed"
                    else:
                        print(f"    ❌ Invalid PDF data for {doc_id}")
                        doc_id_to_drive_url[str(doc_id)] = "Invalid PDF data"
                except Exception as e:
                    print(f"    ❌ Error processing PDF data for {doc_id}: {e}")
                    doc_id_to_drive_url[str(doc_id)] = f"Processing error: {str(e)}"
            else:
                print(f"    ❌ Could not download PDF for {doc_id}")
                doc_id_to_drive_url[str(doc_id)] = "Download failed"
                
        except Exception as e:
            print(f"    ❌ Exception processing {doc_id}: {e}")
            doc_id_to_drive_url[str(doc_id)] = f"Exception: {str(e)}"
            continue
    
    # Add Drive URL column to the failed records DataFrame
    failed_records_df['PDF_Drive_Link'] = failed_records_df['docid'].astype(str).map(doc_id_to_drive_url)
    
    # Update the Excel file by reading existing sheets and updating the failed records sheet
    try:
        # Read existing sheets
        successful_sheet = pd.read_excel(excel_filename, sheet_name='Successful_Records')
        
        # Write back to Excel with updated failed records sheet
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            successful_sheet.to_excel(writer, sheet_name='Successful_Records', index=False)
            failed_records_df.to_excel(writer, sheet_name='Failed_Records', index=False)
        
        print(f"📊 Drive upload summary:")
        print(f"   📄 Total PDFs processed: {total_docs}")
        print(f"   ✅ Successfully uploaded: {uploaded_count}")
        print(f"   ❌ Failed uploads: {total_docs - uploaded_count}")
        print(f"   📁 Updated Excel file: {excel_filename}")
        print(f"   🔗 Drive links added to 'PDF_Drive_Link' column in Failed_Records sheet")
        
    except Exception as e:
        print(f"❌ Error updating Excel file with Drive links: {e}")
        # Fallback: save just the failed records with Drive links
        failed_records_df.to_excel(excel_filename.replace('.xlsx', '_failed_with_drive_links.xlsx'), index=False)
        print(f"📁 Saved failed records with Drive links to: {excel_filename.replace('.xlsx', '_failed_with_drive_links.xlsx')}")



def fix_failed_records_with_document_names(final_excel_path, company_key, start_date, end_date):
    """Fix failed records by adding document names and re-uploading."""
    print(f"🔧 Fixing failed records with document names from: {final_excel_path}")
    
    if not os.path.exists(final_excel_path):
        print(f"❌ Final Excel file not found: {final_excel_path}")
        return None
    
    # Read the final Excel file
    df = pd.read_excel(final_excel_path)
    print(f"Processing {len(df)} total records...")
    
    # Get failed records
    failed_records = df[
        ~((df['PATIENTUPLOAD_STATUS'] == 'TRUE') & 
          (df['ORDERUPLOAD_STATUS'] == 'TRUE'))
    ].copy()
    
    print(f"❌ Found {len(failed_records)} failed records to fix...")
    
    if len(failed_records) == 0:
        print("✅ No failed records to fix!")
        return None
    
    # Get document names for failed records
    print(f"📄 Getting document names for {len(failed_records)} failed records...")
    
    fixed_count = 0
    for idx, row in failed_records.iterrows():
        doc_id = row.get('Document ID')
        if pd.notna(doc_id):
            print(f"  🔍 Getting document info for {doc_id}...")
            
            # Get document information
            doc_info = get_document_info(str(doc_id))
            
            if doc_info.get('success') and doc_info.get('document_name'):
                # Update the document name in the DataFrame
                df.loc[idx, 'DocumentName'] = doc_info['document_name']
                print(f"    ✅ Added document name: {doc_info['document_name']}")
                fixed_count += 1
            else:
                print(f"    ❌ Could not get document name for {doc_id}")
    
    print(f"📊 Document name fix summary:")
    print(f"   📄 Total failed records: {len(failed_records)}")
    print(f"   ✅ Fixed with document names: {fixed_count}")
    print(f"   ❌ Could not fix: {len(failed_records) - fixed_count}")
    
    # Save the fixed Excel file
    fixed_filename = final_excel_path.replace('.xlsx', '_FIXED.xlsx')
    df.to_excel(fixed_filename, index=False)
    print(f"📁 Saved fixed file: {fixed_filename}")
    
    return fixed_filename



def load_company_ids_csv():
    """Load company IDs from Company IDs.csv as a fallback mapping (ID -> Name)."""
    mapping = {}
    try:
        if os.path.exists('Company IDs.csv'):
            df = pd.read_csv('Company IDs.csv')
            for _, row in df.iterrows():
                cid = str(row.get('ID', '')).strip()
                name = str(row.get('Name', '')).strip()
                if not cid or not name:
                    continue
                # Normalize to hyphenated UUID if 32 chars
                cid_nohyphen = cid.replace('-', '')
                if len(cid_nohyphen) == 32:
                    cid = f"{cid_nohyphen[:8]}-{cid_nohyphen[8:12]}-{cid_nohyphen[12:16]}-{cid_nohyphen[16:20]}-{cid_nohyphen[20:]}"
                mapping[cid] = name
    except Exception:
        pass
    return mapping



def process_single_company(company_key, start_date, end_date):
    """Process a single company with the given date range."""
    from config import get_company_config
    
    company = get_company_config(company_key)
    print(f"\n🏢 Processing Company: {company['name']} ({company_key})")
    print(f"   PG Company ID: {company['pg_company_id']}")
    print(f"   Helper ID: {company['helper_id']}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    
    # Prepare structured output directories up-front and write files directly there
    dirs = get_output_dirs(company['name'], start_date, end_date)
    
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
    
    # Check if the NPI file has any actual data (not just headers)
    try:
        df_npi_check = pd.read_excel(latest_npi_excel)
        if len(df_npi_check) <= 1:  # Only header row or empty
            print(f"⚠️  No documents found for {company['name']} in the specified date range, skipping to next company.")
            return False
    except Exception as e:
        print(f"❌ Error reading NPI file for {company['name']}: {e}")
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
    
    # Check if the PDF output file has any actual data
    try:
        df_pdf_check = pd.read_excel(pdf_output_excel)
        if len(df_pdf_check) <= 1:  # Only header row or empty
            print(f"⚠️  No PDF data extracted for {company['name']}, skipping to next company.")
            return False
    except Exception as e:
        print(f"❌ Error reading PDF output file for {company['name']}: {e}")
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

    # Safety filter: if any rows slipped past due to type mismatch, clean both sides and re-filter
    try:
        before_rows_2 = len(merged)
        cleaned_doc_ids = set(str(x).strip() for x in existing_doc_ids)
        merged["__docid_str__"] = merged["Document ID"].astype(str).str.strip()
        merged = merged[~merged["__docid_str__"].isin(cleaned_doc_ids)].copy()
        merged.drop(columns=["__docid_str__"], errors='ignore', inplace=True)
        print(f"✅ Additional safety filter removed {before_rows_2 - len(merged)} rows (post-cleaning).")
    except Exception as e:
        print(f"⚠️  Safety filter error (ignored): {e}")
    
    # Prefill document names so downstream order creation has DocumentName
    merged = prefill_document_names(merged)

    # Filter out excluded document types (e.g., CONVERSION)
    try:
        from config import should_filter_document_types, get_document_type_filter
        if should_filter_document_types(company_key):
            filter_cfg = get_document_type_filter(company_key)
            excluded_types = [t.strip().upper() for t in filter_cfg.get('excluded_types', []) if isinstance(t, str)]
            if excluded_types and 'documentType' in merged.columns:
                before_excl = len(merged)
                merged['__doctype_upper__'] = merged['documentType'].astype(str).str.upper()
                mask_excl = merged['__doctype_upper__'].apply(lambda t: any(ex in t for ex in excluded_types))
                merged = merged[~mask_excl].copy()
                merged.drop(columns=['__doctype_upper__'], errors='ignore', inplace=True)
                print(f"✅ Excluded {before_excl - len(merged)} rows by excluded document types: {', '.join(excluded_types)}")
    except Exception as e:
        print(f"⚠️  Error applying excluded document type filter: {e}")

    # Save company-specific combined output (write directly to structured folder)
    combined_excel = os.path.join(dirs['intermediate'], f"doctoralliance_combined_output_{company_key}.xlsx")
    merged.to_excel(combined_excel, index=False)
    print(f"\n✅ Combined output written to {combined_excel}")
    
    # Step 6: Run supremesheet.py using the combined Excel as input
    print(f"\nStep 4: Running supremesheet.py on combined output for {company['name']}...")
    # Write supreme output directly to structured folder
    supremesheet_output = os.path.join(dirs['intermediate'], f"supreme_excel_{company_key}.xlsx")
    run_script("supremesheet.py", [combined_excel, supremesheet_output])
    
    # Step 7: Confirm output
    if os.path.exists(supremesheet_output):
        print(f"\n🎉 Supreme sheet is ready for {company['name']}: {supremesheet_output}")
        
        # Step 8: Skip old failed records creation - will be done after upload processing
        print(f"\nStep 5: Skipping old failed records creation for {company['name']}...")
        
        # Step 9: Run Upload_Patients_Orders.py on the supreme Excel output
        print(f"\nStep 6: Uploading Patients and Orders for {company['name']}...")
        print(f"   Input file: {supremesheet_output}")
        print(f"   Company key: {company_key}")
        
        try:
            # Check if input file exists before calling Upload_Patients_Orders.py
            if not os.path.exists(supremesheet_output):
                print(f"❌ Input file {supremesheet_output} does not exist!")
                return False
            
            print(f"   Calling Upload_Patients_Orders.py...")
            run_script("Upload_Patients_Orders.py", [supremesheet_output, company_key])
            
            # Verify that the expected output files were created
            expected_files = [
                supremesheet_output.replace('.xlsx', '_with_patient_upload.xlsx'),
                supremesheet_output.replace('.xlsx', '_with_patient_and_order_upload.xlsx')
            ]
            
            print(f"   Checking for expected files...")
            created_files = []
            for expected_file in expected_files:
                if os.path.exists(expected_file):
                    created_files.append(expected_file)
                    print(f"✅ Created: {expected_file}")
                    # Get file size
                    file_size = os.path.getsize(expected_file)
                    print(f"   File size: {file_size} bytes")
                else:
                    print(f"❌ Missing: {expected_file}")
            
            if len(created_files) == 2:
                print(f"\n✅ Upload_Patients_Orders.py finished successfully for {company['name']}.")
                print(f"   Created files: {', '.join(created_files)}")
            else:
                print(f"\n⚠️  Upload_Patients_Orders.py completed but some files are missing for {company['name']}.")
                print(f"   Expected: {len(expected_files)} files, Created: {len(created_files)} files")
                print(f"   Available files in directory:")
                out_dir = os.path.dirname(supremesheet_output) or '.'
                try:
                    for f in os.listdir(out_dir):
                        if f.endswith('.xlsx') and company_key in f:
                            print(f"     - {os.path.join(out_dir, f)}")
                except Exception as e:
                    print(f"   [LIST] Could not list directory {out_dir}: {e}")
            
        except Exception as e:
            print(f"\n❌ Error in Upload_Patients_Orders.py for {company['name']}: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        print(f"\n✅ Upload_Patients_Orders.py finished for {company['name']}.")
        # Organize outputs even on success
        try:
            organize_company_outputs(company_key, start_date, end_date)
        except Exception as e:
            print(f"[ORGANIZE] Skipped organizing outputs for {company_key}: {e}")
        return True
    else:
        print(f"\n❌ supremesheet.py did not produce {supremesheet_output} for {company['name']}, check logs.")
        # Attempt to organize any partial outputs for traceability
        try:
            organize_company_outputs(company_key, start_date, end_date)
        except Exception as e:
            print(f"[ORGANIZE] Skipped organizing outputs for {company_key}: {e}")
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
        
        for i, company_key in enumerate(companies_to_process, 1):
            print(f"\n{'='*60}")
            print(f"🏢 Processing Company {i}/{len(companies_to_process)}: {company_key}")
            print(f"{'='*60}")
            
            try:
                success = process_single_company(company_key, start_date, end_date)
                if success:
                    successful_companies.append(company_key)
                    print(f"✅ Successfully processed: {company_key}")
                else:
                    print(f"⚠️  Skipped or failed: {company_key} (no documents found or processing failed)")
                    
                # New workflow: Create and send successful and failed Excel files
                final_upload_file = f"supreme_excel_{company_key}_with_patient_and_order_upload.xlsx"
                if os.path.exists(final_upload_file):
                    print(f"\n📊 Creating success/failed Excel files for {company_key}...")
                    
                    # First, try to fix failed records with document names
                    print(f"\n🔧 Attempting to fix failed records with document names...")
                    fixed_file = fix_failed_records_with_document_names(
                        final_upload_file, company_key, start_date, end_date
                    )
                    
                    # Use fixed file if available, otherwise use original
                    excel_to_process = fixed_file if fixed_file and os.path.exists(fixed_file) else final_upload_file
                    
                    # Create combined Excel file with successful and failed sheets
                    combined_file, _, _ = create_success_failed_excels(
                        excel_to_process, company_key, start_date, end_date
                    )
                    
                    # Send combined Excel file if it exists
                    if combined_file and os.path.exists(combined_file):
                        # Organize outputs into structured folders
                        organized_report = organize_company_outputs(company_key, start_date, end_date)
                        report_to_send = organized_report if organized_report and os.path.exists(organized_report) else combined_file
                        # Also attach the main supreme sheet (non-upload) from outputs/intermediate
                        try:
                            from config import get_company_config
                            company = get_company_config(company_key)
                            dirs = get_output_dirs(company['name'], start_date, end_date)
                            supreme_main = os.path.join(dirs['intermediate'], f"supreme_excel_{company_key}.xlsx")
                        except Exception:
                            supreme_main = None
                        attachments = [p for p in [report_to_send, supreme_main] if p and os.path.exists(p)]
                        print(f"\n📧 Sending deliverables: {', '.join(attachments)}")
                        run_script("SendMail.py", attachments)
                        print(f"✅ Combined processing report sent: {report_to_send}")
                    else:
                        print(f"⚠️  No processing report to send for {company_key}")
                    
                else:
                    print(f"❌ Final upload file not found: {final_upload_file}")
                    print(f"   Cannot create success/failed Excel files")
                        
            except Exception as e:
                print(f"❌ Error processing {company_key}: {e}")
        
        print(f"\n✅ Processing complete! Successfully processed {len(successful_companies)} out of {len(companies_to_process)} companies.")
        
        # Show detailed summary
        skipped_companies = [company for company in companies_to_process if company not in successful_companies]
        
        if successful_companies:
            print(f"✅ Successful companies: {', '.join(successful_companies)}")
        
        if skipped_companies:
            print(f"⚠️  Skipped companies (no documents found): {', '.join(skipped_companies)}")
        
        print(f"📊 Summary: {len(successful_companies)} processed, {len(skipped_companies)} skipped")
            
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
            # New workflow: Create and send successful and failed Excel files
            final_upload_file = f"supreme_excel_{company_key}_with_patient_and_order_upload.xlsx"
            if os.path.exists(final_upload_file):
                print(f"\n📊 Creating success/failed Excel files for {company_key}...")
                
                # First, try to fix failed records with document names
                print(f"\n🔧 Attempting to fix failed records with document names...")
                fixed_file = fix_failed_records_with_document_names(
                    final_upload_file, company_key, start_date, end_date
                )
                
                # Use fixed file if available, otherwise use original
                excel_to_process = fixed_file if fixed_file and os.path.exists(fixed_file) else final_upload_file
                
                # Create combined Excel file with successful and failed sheets
                combined_file, _, _ = create_success_failed_excels(
                    excel_to_process, company_key, start_date, end_date
                )
                
                # Send combined Excel file if it exists
                if combined_file and os.path.exists(combined_file):
                    # Organize outputs into structured folders
                    organized_report = organize_company_outputs(company_key, start_date, end_date)
                    report_to_send = organized_report if organized_report and os.path.exists(organized_report) else combined_file
                    # Also attach the main supreme sheet (non-upload) from outputs/intermediate
                    try:
                        from config import get_company_config
                        company = get_company_config(company_key)
                        dirs = get_output_dirs(company['name'], start_date, end_date)
                        supreme_main = os.path.join(dirs['intermediate'], f"supreme_excel_{company_key}.xlsx")
                    except Exception:
                        supreme_main = None
                    attachments = [p for p in [report_to_send, supreme_main] if p and os.path.exists(p)]
                    print(f"\n📧 Sending deliverables: {', '.join(attachments)}")
                    run_script("SendMail.py", attachments)
                    print(f"✅ Combined processing report sent: {report_to_send}")
                else:
                    print(f"⚠️  No processing report to send for {company_key}")
            
            print("\n✅ All steps finished. Check your mail for the reports!")
        else:
            print(f"❌ Final upload file not found: {final_upload_file}")
            print(f"   Cannot create success/failed Excel files")
    
    print("\n🎉 Pipeline execution complete!")
