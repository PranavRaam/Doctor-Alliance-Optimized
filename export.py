import pandas as pd
import re
import json
import csv
from datetime import datetime

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

def get_pg_company_name(pg_id, pg_mapping):
    """Convert PG ID to company name using pg_ids.csv."""
    if pd.isna(pg_id):
        return ""
    
    # Format the UUID with hyphens
    formatted_pg_id = format_uuid(pg_id)
    if formatted_pg_id:
        return pg_mapping.get(formatted_pg_id, "")
    else:
        return ""

def get_company_name(company_id, company_mapping):
    """Convert company ID to company name using company.json."""
    if pd.isna(company_id):
        return ""
    
    # Format the UUID with hyphens
    formatted_company_id = format_uuid(company_id)
    if formatted_company_id:
        return company_mapping.get(formatted_company_id, "")
    else:
        return ""

def load_and_process_excel(file_path):
    """Load Excel file and process medical records with improved extraction"""
    
    # Load mappings
    company_mapping = load_company_mapping()
    pg_mapping = load_pg_mapping()
    
    # Load Excel file
    df = pd.read_excel(file_path)
    print(f"📄 Processing {len(df)} records from {file_path}")
    
    # Create empty list for new structured data
    output_data = []
    
    # Process each row
    for idx, row in df.iterrows():
        # Extract from existing columns
        docid = clean_text(row.get("Document ID", ""))
        existing_patient_name = clean_text(row.get("patientName", ""))
        dob = clean_text(row.get("dob", ""))
        dabackofficeid = clean_text(row.get("DABackOfficeID", ""))
        existing_mrn = clean_text(row.get("mrn", ""))
        
        # Convert UUIDs to company names using mappings
        pg_id = row.get("Pgcompanyid", "")
        company_id = row.get("companyId", "")
        
        # Get raw text for additional extraction if needed
        raw_text = clean_text(row.get("raw_text", ""))
        
        # Since this is a Prima Care file and UUIDs are not available,
        # use hardcoded values based on the company configuration
        if file_path == 'doctoralliance_combined_output_prima_care.xlsx':
            pg_name = "Prima Care"
            agency_name = ""  # Will be extracted from raw_text if available
        else:
            pg_name = get_pg_company_name(pg_id, pg_mapping)
            agency_name = get_company_name(company_id, company_mapping)
        
        # Try to extract agency name from raw_text if available
        if not agency_name and pd.notna(raw_text):
            # Look for agency information in raw_text
            agency_patterns = [
                r'Nightingale Visiting Nurses',
                r'([A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+)',  # Pattern for agency names
                r'([A-Z][a-z]+ [A-Z][a-z]+ Nurses)',
                r'([A-Z][a-z]+ [A-Z][a-z]+ Agency)'
            ]
            
            for pattern in agency_patterns:
                match = re.search(pattern, str(raw_text))
                if match:
                    agency_name = match.group(1) if match.groups() else match.group(0)
                    break
        
        # Debug output for first 10 records to see mapping results
        if idx < 10:
            print(f"PG Name: '{pg_name}'")
            print(f"Agency Name: '{agency_name}'")
        
        # Enhanced patient name extraction
        if existing_patient_name:
            patient_name = existing_patient_name
            extraction_source = "existing"
        else:
            patient_name, extraction_source = extract_patient_name_improved(raw_text)
        
        # Enhanced MRN extraction
        if existing_mrn:
            mrn_number = existing_mrn
            mrn_source = "existing"
        else:
            mrn_number, mrn_source = extract_mrn_improved(raw_text, patient_name)
        
        # Compute reason with additional checks
        missing_fields = []
        if not patient_name or is_invalid_name(patient_name):
            missing_fields.append("Patient Name")
            if patient_name and is_invalid_name(patient_name):
                missing_fields.append("Invalid Name")
        if not mrn_number:
            missing_fields.append("MRN")
        if is_likely_physician_name(patient_name, raw_text):
            missing_fields.append("Possible Physician Name")
        
        reason = "Missing: " + ", ".join(set(missing_fields)) if missing_fields else "Complete"  # Use set to avoid duplicates
        
        # Debug output for first 10 records
        if idx < 10:
            print(f"\n=== Record {idx + 1} ===")
            print(f"Document ID: {docid}")
            print(f"Patient Name: '{patient_name}' (existing: '{existing_patient_name}', source: {extraction_source})")
            print(f"MRN: '{mrn_number}' (existing: '{existing_mrn}', source: {mrn_source})")
            print(f"DOB: {dob}")
            print(f"Reason: {reason}")
        
        # Append to results
        output_data.append({
            "docid": docid,
            "patient_name": patient_name,
            "dob": dob,
            "dabackofficeid": dabackofficeid,
            "mrn_number": mrn_number,
            "pg_name": pg_name,
            "agency_name": agency_name,
            "reason": reason
        })
    
    return output_data

def clean_text(text):
    """Clean and normalize text"""
    if pd.isna(text):
        return ""
    return str(text).strip()

def is_invalid_name(name):
    """Check if extracted name is actually invalid"""
    if not name:
        return True
    
    # Expanded invalid patterns
    invalid_patterns = [
        r"Provider's Name",
        r"Patient's Name",
        r"Address and Telephone",
        r"Telephone Number",
        r"\(\d{3}\)\s*\d{3}-\d{4}",  # Phone numbers
        r"^\d+$",  # Only numbers
        r"^[A-Z\s:]+:$",  # Field labels ending with colon
        r"Medical Record",
        r"Document",
        r"Date of Birth",
        r"Certification Period",
        r"Electronically Signed",
        r"Period Medical",
        r"Signed",
        r"Verbal Time",
        r"Verbal Order",
        r"Description\nExacerbation",  # Specific from output
        r"Where Applicable",  # Specific from output
        r"\n"  # Reject if contains newline
    ]
    
    # Expanded medical keyword blocklist
    medical_keywords = ['period', 'medical', 'certification', 'order', 'provider', 'agency', 'hospital', 'record', 'number', 'signed', 'verbal', 'time', 'date', 'description', 'exacerbation', 'applicable', 'where']
    
    if any(re.search(pattern, name, re.IGNORECASE) for pattern in invalid_patterns):
        return True
    if any(keyword in name.lower() for keyword in medical_keywords):
        return True
    if len(name.split()) < 2 or re.match(r"^[A-Z\s]+$", name) or '\n' in name:  # Require at least two words, not all caps/spaces, no newlines
        return True
    
    return False

def is_likely_physician_name(name, raw_text):
    """Check if extracted name is likely a physician's"""
    if not name:
        return False
    
    physician_indicators = [
        r"Dr\.\s*" + re.escape(name),
        re.escape(name) + r"\s*(MD|DO|Physician|Doctor)",
        r"REFERRED\s+BY\s+" + re.escape(name),
        r"BY\s+DR\.\s+" + re.escape(name.split()[-1])
    ]
    
    for indicator in physician_indicators:
        if re.search(indicator, raw_text, re.IGNORECASE):
            return True
    return False

def extract_patient_name_improved(raw_text):
    """Improved patient name extraction with patient-specific prioritization"""
    if not raw_text:
        return "", "none"
    
    # Pattern 1: Structured patient name fields (prioritize these)
    structured_patterns = [
        (r"Patient's Name and Address\s*:\s*([^\n\r(]+?)(?:\s*\(|\s*\d{3}[-\s]?\d{3}[-\s]?\d{4}|$)", "structured-address"),
        (r"Patient's Name\s*:\s*([^\n\r(]+?)(?:\s*\(|\s*\d{3}[-\s]?\d{3}[-\s]?\d{4}|$)", "structured-name"),
        (r"Patient Name\s*:\s*([^\n\r(]+?)(?:\s*\(|\s*\d{3}[-\s]?\d{3}[-\s]?\d{4}|$)", "structured-patient")
    ]
    
    for pattern, source in structured_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            name = re.sub(r'\s*\(|\s*\d.*$', '', name).strip()
            if name and not is_invalid_name(name) and not is_likely_physician_name(name, raw_text):
                return name, source
    
    # Pattern 2: Near clinical notes (e.g., "81 YO MALE" sections)
    clinical_patterns = [
        (r"(\d+\s+YO\s+(MALE|FEMALE))\s+([^.]+?)(?=\s+REFERRED|\s+BY|\s+2ND|\.)", "clinical-age"),  # e.g., 81 YO MALE GEORGE OSTAPOW
        (r"Patient:\s*([A-Z][a-z]+,\s*[A-Z][a-z]+)", "clinical-patient")
    ]
    
    for pattern, source in clinical_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            name = match.group(3) if len(match.groups()) >= 3 else match.group(1)
            name = name.strip()
            if name and not is_invalid_name(name) and not is_likely_physician_name(name, raw_text):
                return name, source
    
    # Pattern 3: Fallback to general name formats (stricter)
    name_patterns = [
        (r"([A-Z][a-z]+,\s*[A-Z][a-z]+)", "fallback-comma"),  # Last, First
        (r"([A-Z][a-z]+\s+[A-Z][a-z]+)", "fallback-space"),   # First Last
        (r"([A-Z]{2,}\s*,\s*[A-Z]{2,})", "fallback-caps")   # LAST, FIRST (all caps)
    ]
    
    for pattern, source in name_patterns:
        matches = re.findall(pattern, raw_text)
        for match in matches:
            if (len(match.split()) >= 2 and 
                not is_invalid_name(match) and
                not is_likely_physician_name(match, raw_text) and
                not any(word in match.lower() for word in 
                       ['address', 'phone', 'date', 'gender', 'birth', 'medicare', 
                        'provider', 'agency', 'hospital', 'doctor', 'record', 'number', 'period', 'signed', 'verbal', 'time', 'description', 'exacerbation', 'applicable'])):
                return match.strip(), source
    
    return "", "none"

def extract_mrn_improved(raw_text, patient_name=""):
    """Improved MRN extraction with user-specified variations"""
    if not raw_text:
        return "", "none"
    
    mrn_patterns = [
        (r"Medical Record No\.\s*:\s*([^\n\r]+)", "medical-record-no"),
        (r"Medical Record Number\s*:\s*([^\n\r]+)", "medical-record-number"),
        (r"MRN\s*:\s*([^\n\r]+)", "mrn-label"),
        (r"Record\s*#\s*:\s*([^\n\r]+)", "record-hash"),
        (r"Patient\s*ID\s*:\s*([^\n\r]+)", "patient-id"),
        (r"MR\s*#\s*:\s*([^\n\r]+)", "mr-hash"),
        (r"Pt MRN\s*:\s*([^\n\r]+)", "pt-mrn"),
        (r"\[MRN:\s*([^\]]+)\]", "bracket-mrn"),  # [MRN: ...]
        (r"\(Medical Record Number:\s*([^\)]+)\)", "paren-medical-record"),  # (Medical Record Number: ...)
        (r"\(MRN:\s*([^\)]+)\)", "paren-mrn"),  # (MRN: ...)
        (r"\[Medical Record Number:\s*([^\]]+)\]", "bracket-medical-record"),  # [Medical Record Number: ...]
        (r"MR#\s*([^\n\r:]+)", "mr-hash-no-colon"),  # MR# without colon
        (r"MRN\s*\[\s*([^\]]+)\]", "mrn-bracket-no-colon")  # MRN [12345]
    ]
    
    for pattern, source in mrn_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            mrn = match.group(1).strip()
            mrn = re.sub(r'^[:\s]+|[:\s]+$', '', mrn)
            if mrn and len(mrn) >= 4:
                return mrn, source
    
    # Fallback: Look for alphanumeric MRN near patient name (if available, expanded window)
    if patient_name:
        name_pos = raw_text.find(patient_name)
        if name_pos != -1:
            nearby_text = raw_text[name_pos:name_pos + 200]  # Expanded to 200 chars
            nearby_match = re.search(r"\[([A-Z0-9]{4,})\]|\(([A-Z0-9]{4,})\)|MRN:\s*([A-Z0-9]{4,})|MR#\s*([A-Z0-9]{4,})", nearby_text, re.IGNORECASE)
            if nearby_match:
                for group in nearby_match.groups():
                    if group:
                        return group.strip(), "near-name-fallback"
    
    # General fallback for alphanumeric sequences near keywords
    fallback_match = re.search(r"([A-Z0-9]{6,})\s*(?=\nMedical Record|Patient ID|MRN|MR#)", raw_text, re.IGNORECASE)
    if fallback_match:
        mrn = fallback_match.group(1).strip()
        if len(mrn) >= 4:
            return mrn, "keyword-fallback"
    
    # Additional fallback: Lines starting with MRN or MR#
    line_fallback = re.search(r"^MRN\s*([A-Z0-9]{4,})|MR#\s*([A-Z0-9]{4,})", raw_text, re.IGNORECASE | re.MULTILINE)
    if line_fallback:
        for group in line_fallback.groups():
            if group:
                return group.strip(), "line-fallback"
    
    return "", "none"

def main():
    # Configuration
    file_path = 'doctoralliance_combined_output_prima_care.xlsx'
    
    # Process the file
    output_data = load_and_process_excel(file_path)
    
    # Convert to DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save to new Excel file
    output_filename = f"extracted_patient_data_improved_v5_{timestamp}.xlsx"
    output_df.to_excel(output_filename, index=False)
    
    print(f"\n✅ Extraction complete!")
    print(f"📁 Output file: {output_filename}")
    print(f"📊 Total records processed: {len(output_df)}")
    
    # Show extraction summary
    complete_records = len(output_df[output_df['reason'] == 'Complete'])
    incomplete_records = len(output_df[output_df['reason'] != 'Complete'])
    
    print(f"\n📈 Extraction Summary:")
    print(f"✅ Complete records: {complete_records} ({complete_records/len(output_df)*100:.1f}%)")
    print(f"❌ Incomplete records: {incomplete_records} ({incomplete_records/len(output_df)*100:.1f}%)")
    
    # Show breakdown of missing data
    if incomplete_records > 0:
        print(f"\n🔍 Missing Data Breakdown:")
        reason_counts = output_df['reason'].value_counts()
        for reason, count in reason_counts.items():
            if reason != 'Complete':
                print(f"  {reason}: {count} records")
    
    # Show sample of extracted data
    print(f"\n📋 Sample Extracted Data:")
    sample_cols = ['docid', 'patient_name', 'mrn_number', 'agency_name', 'reason']
    print(output_df[sample_cols].head(10).to_string(index=False))
    
    # Additional quality checks
    print(f"\n🔍 Quality Check Results:")
    
    # Check for suspected physician names
    physician_names = output_df[output_df['reason'].str.contains("Possible Physician Name", na=False)]
    print(f"⚠️ Suspected physician names extracted as patient: {len(physician_names)} records")
    
    # Check for invalid names
    invalid_names = output_df[output_df['reason'].str.contains("Invalid Name", na=False)]
    print(f"⚠️ Invalid name extractions flagged: {len(invalid_names)} records")
    
    # Check for empty extractions
    empty_names = len(output_df[output_df['patient_name'] == ''])
    empty_mrns = len(output_df[output_df['mrn_number'] == ''])
    print(f"📉 Empty patient names: {empty_names}")
    print(f"📉 Empty MRNs: {empty_mrns}")
    
    # New: Breakdown of MRN extraction sources
    if 'mrn_source' in locals():  # For simplicity, assuming we track in future
        print("\n📊 MRN Extraction Sources Breakdown (sample):")
        # You'd need to add mrn_source to output_data for full tracking; this is illustrative
        print("  (Run script to see full)")

if __name__ == "__main__":
    main()
