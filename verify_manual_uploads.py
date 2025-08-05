import pandas as pd
import requests
import json
import csv
from datetime import datetime
import os
import sys
from typing import Dict, List, Tuple, Optional
import time

# Import config functions
from config import get_company_config, get_company_api_url

def load_company_mapping():
    """Load company mapping from company.json."""
    try:
        with open('company.json', 'r') as f:
            company_data = json.load(f)
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
    uuid_str = uuid_str.replace('-', '')
    
    if len(uuid_str) == 32:
        return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
    else:
        return uuid_str

def get_existing_orders_from_platform(company_key):
    """Fetch all existing orders from the platform for a company."""
    try:
        url = get_company_api_url(company_key)
        print(f"🔍 Fetching existing orders from: {url}")
        
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        orders = response.json()
        
        print(f"✅ Found {len(orders)} existing orders on platform")
        return orders
    except Exception as e:
        print(f"❌ Error fetching existing orders: {e}")
        return []

def create_platform_lookup_maps(platform_orders):
    """Create lookup maps for fast searching."""
    doc_id_map = {}
    mrn_map = {}
    patient_name_map = {}
    dabackofficeid_map = {}
    
    for order in platform_orders:
        doc_id = str(order.get("documentID", "")).strip()
        mrn = str(order.get("mrn", "")).strip()
        patient_name = str(order.get("patientName", "")).strip()
        dabackofficeid = str(order.get("dabackOfficeID", "")).strip()
        
        if doc_id:
            doc_id_map[doc_id] = order
        if mrn:
            mrn_map[mrn] = order
        if patient_name:
            patient_name_map[patient_name] = order
        if dabackofficeid:
            dabackofficeid_map[dabackofficeid] = order
    
    return {
        'doc_id_map': doc_id_map,
        'mrn_map': mrn_map,
        'patient_name_map': patient_name_map,
        'dabackofficeid_map': dabackofficeid_map
    }

def check_if_record_exists_on_platform(failed_record, platform_maps):
    """Check if a failed record now exists on the platform using multiple methods."""
    doc_id = str(failed_record.get('docid', '')).strip()
    mrn = str(failed_record.get('mrn_number', '')).strip()
    patient_name = str(failed_record.get('patient_name', '')).strip()
    dabackofficeid = str(failed_record.get('dabackofficeid', '')).strip()
    
    matches = []
    
    # Method 1: Direct Document ID match
    if doc_id and doc_id in platform_maps['doc_id_map']:
        matches.append({
            'method': 'Document ID',
            'platform_record': platform_maps['doc_id_map'][doc_id]
        })
    
    # Method 2: MRN match
    if mrn and mrn in platform_maps['mrn_map']:
        platform_record = platform_maps['mrn_map'][mrn]
        # Additional check: same patient name or DOB
        platform_patient = str(platform_record.get('patientName', '')).strip()
        if patient_name and platform_patient and patient_name.lower() == platform_patient.lower():
            matches.append({
                'method': 'MRN + Patient Name',
                'platform_record': platform_record
            })
    
    # Method 3: Patient Name + DOB match
    if patient_name and patient_name in platform_maps['patient_name_map']:
        platform_record = platform_maps['patient_name_map'][patient_name]
        # Check if DOB matches (if available)
        failed_dob = str(failed_record.get('dob', '')).strip()
        platform_dob = str(platform_record.get('dob', '')).strip()
        if failed_dob and platform_dob and failed_dob == platform_dob:
            matches.append({
                'method': 'Patient Name + DOB',
                'platform_record': platform_record
            })
    
    # Method 4: DABackOfficeID match
    if dabackofficeid and dabackofficeid in platform_maps['dabackofficeid_map']:
        matches.append({
            'method': 'DABackOfficeID',
            'platform_record': platform_maps['dabackofficeid_map'][dabackofficeid]
        })
    
    return matches

def verify_failed_records_manual_uploads(failed_records_file, company_key):
    """Main function to verify if failed records were manually uploaded."""
    print(f"🔍 VERIFYING MANUAL UPLOADS")
    print(f"Company: {company_key}")
    print(f"Failed Records File: {failed_records_file}")
    print("=" * 60)
    
    # Load company configuration
    try:
        company = get_company_config(company_key)
        print(f"🏢 Company: {company['name']}")
        print(f"   PG Company ID: {company['pg_company_id']}")
    except Exception as e:
        print(f"❌ Error loading company config: {e}")
        return None
    
    # Load mappings
    company_mapping = load_company_mapping()
    pg_mapping = load_pg_mapping()
    
    # Read failed records file
    try:
        print(f"📖 Reading failed records from: {failed_records_file}")
        failed_df = pd.read_excel(failed_records_file)
        print(f"✅ Loaded {len(failed_df)} failed records")
    except Exception as e:
        print(f"❌ Error reading failed records file: {e}")
        return None
    
    # Get existing orders from platform
    platform_orders = get_existing_orders_from_platform(company_key)
    if not platform_orders:
        print("❌ No platform orders found. Cannot verify.")
        return None
    
    # Create platform lookup maps
    platform_maps = create_platform_lookup_maps(platform_orders)
    
    # Verify each failed record
    verification_results = []
    found_count = 0
    not_found_count = 0
    
    print(f"\n🔍 Verifying {len(failed_df)} failed records...")
    
    for idx, failed_record in failed_df.iterrows():
        doc_id = str(failed_record.get('docid', '')).strip()
        patient_name = str(failed_record.get('patient_name', '')).strip()
        mrn = str(failed_record.get('mrn_number', '')).strip()
        
        print(f"  [{idx+1}/{len(failed_df)}] Checking: DocID={doc_id}, Patient={patient_name}, MRN={mrn}")
        
        # Check if record exists on platform
        matches = check_if_record_exists_on_platform(failed_record, platform_maps)
        
        if matches:
            found_count += 1
            best_match = matches[0]  # Take the first/best match
            print(f"    ✅ FOUND via {best_match['method']}")
            
            verification_results.append({
                'docid': doc_id,
                'patient_name': patient_name,
                'mrn_number': mrn,
                'dob': failed_record.get('dob', ''),
                'dabackofficeid': failed_record.get('dabackofficeid', ''),
                'pg_name': failed_record.get('pg name', ''),
                'agency_name': failed_record.get('agency name', ''),
                'original_reason': failed_record.get('reason', ''),
                'verification_status': 'FOUND',
                'verification_method': best_match['method'],
                'platform_doc_id': best_match['platform_record'].get('documentID', ''),
                'platform_patient_name': best_match['platform_record'].get('patientName', ''),
                'platform_mrn': best_match['platform_record'].get('mrn', ''),
                'platform_order_id': best_match['platform_record'].get('id', ''),
                'platform_created_date': best_match['platform_record'].get('createdDate', '')
            })
        else:
            not_found_count += 1
            print(f"    ❌ NOT FOUND")
            
            verification_results.append({
                'docid': doc_id,
                'patient_name': patient_name,
                'mrn_number': mrn,
                'dob': failed_record.get('dob', ''),
                'dabackofficeid': failed_record.get('dabackofficeid', ''),
                'pg_name': failed_record.get('pg name', ''),
                'agency_name': failed_record.get('agency name', ''),
                'original_reason': failed_record.get('reason', ''),
                'verification_status': 'NOT_FOUND',
                'verification_method': 'N/A',
                'platform_doc_id': '',
                'platform_patient_name': '',
                'platform_mrn': '',
                'platform_order_id': '',
                'platform_created_date': ''
            })
    
    # Create verification report
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_filename = f"manual_upload_verification_{company_key}_{timestamp}.xlsx"
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(verification_results)
    
    # Save to Excel with multiple sheets
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = {
            'Metric': [
                'Total Failed Records',
                'Found on Platform',
                'Not Found on Platform',
                'Success Rate (%)',
                'Verification Date',
                'Company'
            ],
            'Value': [
                len(failed_df),
                found_count,
                not_found_count,
                round((found_count / len(failed_df)) * 100, 2) if len(failed_df) > 0 else 0,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                company['name']
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # All results sheet
        results_df.to_excel(writer, sheet_name='All_Results', index=False)
        
        # Found records sheet
        found_df = results_df[results_df['verification_status'] == 'FOUND']
        if len(found_df) > 0:
            found_df.to_excel(writer, sheet_name='Found_Records', index=False)
        
        # Not found records sheet
        not_found_df = results_df[results_df['verification_status'] == 'NOT_FOUND']
        if len(not_found_df) > 0:
            not_found_df.to_excel(writer, sheet_name='Not_Found_Records', index=False)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"📊 VERIFICATION SUMMARY")
    print(f"{'='*60}")
    print(f"🏢 Company: {company['name']}")
    print(f"📅 Verification Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📋 Total Failed Records: {len(failed_df)}")
    print(f"✅ Found on Platform: {found_count}")
    print(f"❌ Not Found on Platform: {not_found_count}")
    print(f"📈 Success Rate: {round((found_count / len(failed_df)) * 100, 2) if len(failed_df) > 0 else 0}%")
    print(f"📁 Report saved to: {output_filename}")
    
    if not_found_count > 0:
        print(f"\n⚠️  {not_found_count} records still need manual attention!")
        print(f"   Check the 'Not_Found_Records' sheet in the report.")
    
    return output_filename

def main():
    """Main function to run the verification."""
    if len(sys.argv) < 3:
        print("Usage: python verify_manual_uploads.py <failed_records_file> <company_key>")
        print("Example: python verify_manual_uploads.py failed_records_by_pg_2025-01-15.xlsx grace_at_home")
        return
    
    failed_records_file = sys.argv[1]
    company_key = sys.argv[2]
    
    if not os.path.exists(failed_records_file):
        print(f"❌ Failed records file not found: {failed_records_file}")
        return
    
    # Run verification
    output_file = verify_failed_records_manual_uploads(failed_records_file, company_key)
    
    if output_file:
        print(f"\n🎉 Verification complete! Check: {output_file}")

if __name__ == "__main__":
    main() 