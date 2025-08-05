import pandas as pd
import requests
import datetime
import json
import re
import math
import base64
import tempfile
import os
import sys

# These will be set dynamically based on company configuration
PATIENT_CREATE_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/create"
PATIENT_API = None  # Will be set dynamically
ORDER_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Order"
ORDER_PDF_UPLOAD_API = "https://dawavadmin-djb0f9atf8e6cwgx.eastus-01.azurewebsites.net/api/OrderPdfUpload/upload"
HEADERS = {'accept': '*/*', 'Content-Type': 'application/json'}


def get_api_urls_for_company(company_key=None):
    """Get API URLs for a specific company."""
    from config import get_company_config
    
    if company_key is None:
        from config import ACTIVE_COMPANY
        company_key = ACTIVE_COMPANY
    
    company = get_company_config(company_key)
    pg_company_id = company['pg_company_id']
    
    return {
        "patient_api": f"https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/company/pg/{pg_company_id}",
        "order_api": ORDER_API,
        "patient_create_api": PATIENT_CREATE_API
    }


def set_company_api_urls(company_key=None):
    """Set the global API URLs for the specified company."""
    global PATIENT_API
    urls = get_api_urls_for_company(company_key)
    PATIENT_API = urls["patient_api"]
    print(f"[CONFIG] Set API URLs for company: {company_key}")
    print(f"   Patient API: {PATIENT_API}")
    print(f"   Order API: {urls['order_api']}")


def clean_order_number_for_upload(val):
    """Clean order number for upload - modular function."""
    if not val:
        return ""
    
    # Remove all non-alphanumeric characters
    cleaned = re.sub(r'[^A-Za-z0-9]', '', str(val))
    
    # Ensure minimum length
    if len(cleaned) < 3:
        return ""
    
    return cleaned


def clean_mrn_for_upload(val):
    """Clean MRN for upload - modular function."""
    if not val:
        return ""
    
    # Remove all non-alphanumeric characters  
    cleaned = re.sub(r'[^A-Za-z0-9]', '', str(val))
    
    # Must be more than 3 characters and contain at least one digit
    if len(cleaned) <= 3 or not any(c.isdigit() for c in cleaned):
        return ""
    
    return cleaned


def clean_id(val):
    """Enhanced clean_id with alphanumeric validation."""
    if pd.isna(val) or val is None:
        return ""
    
    if isinstance(val, float) and val.is_integer():
        cleaned = str(int(val))
    else:
        cleaned = str(val)
        if cleaned.endswith('.0'):
            cleaned = cleaned[:-2]
    
    # Remove non-alphanumeric characters
    cleaned = re.sub(r'[^A-Za-z0-9]', '', cleaned)
    
    return cleaned


def clean_uuid(val):
    """Clean UUID while preserving hyphens for GUID format."""
    if pd.isna(val) or val is None:
        return ""
    
    cleaned = str(val).strip()
    
    # If it looks like a UUID (contains hyphens), preserve the format
    if '-' in cleaned:
        # Remove any non-alphanumeric characters except hyphens
        cleaned = re.sub(r'[^A-Za-z0-9-]', '', cleaned)
        # Ensure proper UUID format (8-4-4-4-12)
        parts = cleaned.split('-')
        if len(parts) == 5:
            return f"{parts[0]}-{parts[1]}-{parts[2]}-{parts[3]}-{parts[4]}"
        elif len(parts) == 1 and len(parts[0]) == 32:
            # Convert 32-char string to UUID format
            uuid_str = parts[0]
            return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
    
    # If not a UUID format, use regular clean_id
    return clean_id(val)


def clean_payload_for_json(obj):
    """Recursively replace NaN, inf, -inf with empty string."""
    if isinstance(obj, dict):
        return {k: clean_payload_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_payload_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return ""
        return obj
    elif pd.isna(obj):
        return ""
    return obj


def split_name(full_name):
    if not isinstance(full_name, str):
        full_name = "" if pd.isna(full_name) else str(full_name)
    parts = (full_name or '').split()
    if len(parts) == 0:
        return "", "", ""
    elif len(parts) == 1:
        return parts[0], "", ""
    elif len(parts) == 2:
        return parts[0], "", parts[1]
    else:
        return parts[0], " ".join(parts[1:-1]), parts[-1]


def get_age(dob):
    try:
        birth = pd.to_datetime(dob)
        today = pd.to_datetime("today")
        return str(today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day)))
    except Exception:
        return ""


def search_patientid_by_name_dob(patients, name, dob):
    if not isinstance(name, str):
        name = "" if pd.isna(name) else str(name)
    if not isinstance(dob, str):
        dob = "" if pd.isna(dob) else str(dob)
    name = name.strip().lower()
    dob = dob.strip()
    for p in patients:
        p_name = ""
        p_dob = ""
        agency = p.get("agencyInfo", {})
        # Try to construct the patient name as in your Excel
        # (you may need to adjust key names)
        pfname = agency.get("patientFName", "") or ""
        plname = agency.get("patientLName", "") or ""
        p_name = f"{pfname} {plname}".strip().lower()
        p_dob = agency.get("dob", "") or ""
        if p_name == name and p_dob == dob:
            return p.get("id", "")
    return ""


def parse_address(address):
    state, city, zipc = "", "", ""
    if not isinstance(address, str):
        address = "" if pd.isna(address) else str(address)
    if not address:
        return state, city, zipc
    zipm = re.search(r'(\d{5})(?:-\d{4})?$', address)
    if zipm:
        zipc = zipm.group(1)
    parts = address.split(',')
    if len(parts) >= 2:
        city = parts[-2].strip()
        state = parts[-1].strip().split()[0] if len(parts[-1].split()) > 0 else ""
    return state, city, zipc


def standardize_patient_sex(val):
    if not val:
        return ""
    val = str(val).strip().upper()
    if val in ["MALE", "M"]:
        return "MALE"
    if val in ["FEMALE", "F"]:
        return "FEMALE"
    return ""


def now_iso():
    return datetime.datetime.now().isoformat()


def get_order_id_with_fallback(row):
    """Generate order ID with enhanced validation and fallback."""
    order_id = row.get("orderno", "")
    
    # Clean the order ID
    if order_id:
        cleaned_order = clean_order_number_for_upload(order_id)
        if cleaned_order:
            return cleaned_order
    
    # Fallback to NOF-{DocumentID}
    doc_id = clean_id(row.get("docId", row.get("Document ID", "")))
    if doc_id:
        fallback_order = f"NOF{doc_id}"  # Remove hyphen for pure alphanumeric
        return clean_order_number_for_upload(fallback_order)
    
    return ""


def get_order_date_with_fallback(row):
    """Get order date with sendDate fallback"""
    orderdate = row.get("orderdate")
    sendDate = row.get("sendDate")
    
    # Clean and validate orderdate
    if pd.isna(orderdate) or orderdate is None or str(orderdate).strip() == "":
        orderdate = None
    else:
        orderdate = str(orderdate).strip()
    
    # Clean and validate sendDate
    if pd.isna(sendDate) or sendDate is None or str(sendDate).strip() == "":
        sendDate = None
    else:
        sendDate = str(sendDate).strip()
    
    return orderdate or sendDate or ""


def get_episode_data_from_patient(row, patients):
    """Get SOC/SOE/EOE from patient data when missing in order"""
    patient_id = clean_id(row.get("patientid", ""))
    if not patient_id:
        return row.get("soc", ""), row.get("cert_period_soe", ""), row.get("cert_period_eoe", "")
    
    # Get the reference date (sendDate or orderDate)
    ref_date = get_order_date_with_fallback(row)
    if not ref_date:
        return row.get("soc", ""), row.get("cert_period_soe", ""), row.get("cert_period_eoe", "")
    
    try:
        ref_date_parsed = pd.to_datetime(ref_date)
    except:
        return row.get("soc", ""), row.get("cert_period_soe", ""), row.get("cert_period_eoe", "")
    
    # Find matching patient and episode
    for patient in patients:
        if clean_id(patient.get("id", "")) == patient_id:
            agency_info = patient.get("agencyInfo", {})
            episodes = agency_info.get("episodeDiagnoses", [])
            
            for episode in episodes:
                try:
                    soe = episode.get("startOfEpisode", "")
                    eoe = episode.get("endOfEpisode", "")
                    if soe and eoe:
                        soe_parsed = pd.to_datetime(soe)
                        eoe_parsed = pd.to_datetime(eoe)
                        if soe_parsed <= ref_date_parsed <= eoe_parsed:
                            return (
                                episode.get("startOfCare", row.get("soc", "")),
                                soe,
                                eoe
                            )
                except:
                    continue
    
    # Return original values if no match found
    return row.get("soc", ""), row.get("cert_period_soe", ""), row.get("cert_period_eoe", "")


def build_patient_payload(row, company_key=None):
    """Enhanced patient payload with cleaned fields."""
    # Get the authoritative PG ID from config, not from Excel data
    from config import get_company_config, ACTIVE_COMPANY
    
    if company_key is None:
        company_key = ACTIVE_COMPANY
    
    company = get_company_config(company_key)
    authoritative_pg_id = company['pg_company_id']
    required = ['patientName', 'dob', 'mrn', 'soc', 'cert_period_soe', 'cert_period_eoe', 'Diagnosis 1', 'companyId', 'Pgcompanyid','patient_sex']
    
    remarks = []
    for r in required:
        if not row.get(r):
            remarks.append(f"{r} absent")
    
    # Clean MRN specifically
    cleaned_mrn = clean_mrn_for_upload(row.get("mrn", ""))
    if not cleaned_mrn:
        remarks.append("MRN invalid (must be >3 chars, alphanumeric with at least one digit)")
    
    # Handle both patientName and patient_name fields
    patient_name = row.get("patientName", "") or row.get("patient_name", "")
    fname, mname, lname = split_name(patient_name)
    age = get_age(row.get("dob"))
    state, city, zipc = parse_address(row.get("address", ""))
    
    payload = {
        "filterStatus": "",
        "patientEHRRecId": "",
        "patientEHRType": "",
        "patientFName": fname,
        "patientMName": mname,
        "patientLName": lname,
        "dob": row.get("dob", ""),
        "age": age,
        "patientSex": standardize_patient_sex(row.get("patient_sex", "")),
        "patientStatus": "Active",
        "maritalStatus": "",
        "ssn": "",
        "startOfCare": row.get("soc", ""),
        "careManagement": [{"careManagementType": "CPO"}],
        "medicalRecordNo": cleaned_mrn,  # Use cleaned MRN
        "serviceLine": "",
        "patientAddress": row.get("address", ""),
        "state": state,
        "patientCity": city,
        "patientState": state,
        "zip": zipc,
        "email": "",
        "phoneNumber": "",
        "fax": "",
        "payorSource": "",
        "billingProvider": "",
        "billingProviderPhoneNo": "",
        "billingProviderAddress": "",
        "billingProviderZip": "",
        "npi": "",
        "line1DOSFrom": "",
        "line1DOSTo": "",
        "line1POS": "",
        "physicianNPI": clean_id(row.get("NPI", "")),
        "supervisingProvider": "",
        "supervisingProviderNPI": "",
        "physicianGroup": "",
        "physicianGroupNPI": "",
        "physicianGroupAddress": "",
        "physicianPhone": "",
        "physicianAddress": "",
        "cityStateZip": "",
        "patientAccountNo": "",
        "agencyNPI": "",
        "nameOfAgency": "",
        "insuranceId": "",
        "primaryInsuranceName": "",
        "secondaryInsuranceName": "",
        "secondaryInsuranceID": "",
        "tertiaryInsuranceName": "",
        "tertiaryInsuranceID": "",
        "nextofKin": "",
        "patientCaretaker": "",
        "patientCaretakerContactNumber": "",
        "remarks": "",
        "daBackofficeID": clean_id(row.get("DABackOfficeID", "")),
        "companyId": clean_id(row.get("companyId", "")),
        "pgcompanyID": authoritative_pg_id,  # Use authoritative PG ID from config
        "createdBy": "PatientScript",
        "createdOn": now_iso(),
        "updatedBy": "",
        "updatedOn": now_iso(),
        "episodeDiagnoses": [{
            "id": "",
            "startOfCare": row.get("soc", ""),
            "startOfEpisode": row.get("cert_period_soe", ""),
            "endOfEpisode": row.get("cert_period_eoe", ""),
            "firstDiagnosis": row.get("Diagnosis 1", ""),
            "secondDiagnosis": row.get("Diagnosis 2", ""),
            "thirdDiagnosis": row.get("Diagnosis 3", ""),
            "fourthDiagnosis": row.get("Diagnosis 4", ""),
            "fifthDiagnosis": row.get("Diagnosis 5", ""),
            "sixthDiagnosis": row.get("Diagnosis 6", "")
        }]
    }
    
    return payload, remarks


def create_patient(row, company_key=None):
    payload, remarks = build_patient_payload(row, company_key)
    payload = clean_payload_for_json(payload)
    print("\n--- [PATIENT_CREATE] Request Payload ---")
    print(json.dumps(payload, indent=2, default=str))
    try:
        resp = requests.post(PATIENT_CREATE_API, headers=HEADERS, json=payload, timeout=20)
        print("--- [PATIENT_CREATE] Response ---")
        print(f"Status: {resp.status_code}\n{resp.text}\n")
        # Use original working success criteria for patient creation - check for 'id' in response
        try:
            resp_json = resp.json()
        except Exception:
            resp_json = {}
        success = resp.status_code in (200, 201) and (isinstance(resp_json, dict) and resp_json.get("id"))
        return success, "; ".join(remarks)
    except Exception as e:
        print(f"  [PATIENT_CREATE] Error: {e}")
        return False, "; ".join(remarks) + f"; Exception: {e}"


def refill_patient_info(df):
    try:
        resp = requests.get(PATIENT_API, timeout=30)
        patients = resp.json()
    except Exception as e:
        print(f"  [PATIENT_REFILL] Download error: {e}")
        return df
    for i, row in df.iterrows():
        if row['PatientExist']: continue
        mrn = str(row['mrn']).strip()
        dabackid = clean_id(row['DABackOfficeID'])
        found = False
        for p in patients:
            agency = p.get("agencyInfo", {})
            if clean_id(agency.get("medicalRecordNo", "")) == clean_id(mrn) or \
               (agency.get("daBackofficeID", "") and dabackid and clean_id(agency["daBackofficeID"]) == dabackid):
                df.at[i, 'PatientExist'] = True
                df.at[i, 'patientid'] = p.get("id", "")
                df.at[i, 'companyId'] = clean_id(agency.get("companyId", ""))
                df.at[i, 'Pgcompanyid'] = clean_uuid(agency.get("pgcompanyID", ""))
                found = True
                break
        if not found:
            # Try by patientName + dob
            excel_name = row.get("patientName", "")
            excel_dob = row.get("dob", "")
            patientid = search_patientid_by_name_dob(patients, excel_name, excel_dob)
            if patientid:
                df.at[i, 'PatientExist'] = True
                df.at[i, 'patientid'] = patientid
                # Optionally update companyId/Pgcompanyid from this patient
    return df


def build_order_payload(row, patients=None, company_key=None):
    """Build order payload with enhanced field cleaning."""
    # Get the authoritative PG ID from config, not from Excel data
    from config import get_company_config, ACTIVE_COMPANY
    
    if company_key is None:
        company_key = ACTIVE_COMPANY
    
    company = get_company_config(company_key)
    authoritative_pg_id = company['pg_company_id']
    # Get episode data with patient lookup if needed
    if patients:
        soc, soe, eoe = get_episode_data_from_patient(row, patients)
    else:
        soc, soe, eoe = row.get("soc", ""), row.get("cert_period_soe", ""), row.get("cert_period_eoe", "")
    
    # Handle both patientName and patient_name fields
    patient_name = row.get("patientName", "") or row.get("patient_name", "")
    
    return {
        "orderNo": get_order_id_with_fallback(row),  # Uses enhanced cleaning
        "orderDate": get_order_date_with_fallback(row),
        "startOfCare": soc,
        "episodeStartDate": soe,
        "episodeEndDate": eoe,
        "documentID": clean_id(row.get("docId", row.get("Document ID", ""))),
        "mrn": clean_mrn_for_upload(row.get("mrn", "")),  # Use cleaned MRN
        "patientName": patient_name,
        "sentToPhysicianDate": row.get("sendDate", ""),
        "sentToPhysicianStatus": True,
        "signedByPhysicianDate": row.get("physicianSigndate", ""),
        "signedByPhysicianStatus": True,
        "uploadedSignedOrderDate": "",
        "uploadedSignedOrderStatus": True,
        "uploadedSignedPgOrderDate": "",
        "uploadedSignedPgOrderStatus": True,
        "cpoMinutes": "",
        "orderUrl": "",
        "documentName": row.get("documentType", ""),
        "ehr": "",
        "account": "",
        "location": "",
        "remarks": "",
        "patientId": clean_id(row.get("patientid", "")),
        "companyId": clean_id(row.get("companyId", "")),
        "pgCompanyId": authoritative_pg_id,  # Use authoritative PG ID from config
        "entityType": "ORDER",
        "clinicalJustification": "",
        "billingProvider": "",
        "billingProviderNPI": clean_id(row.get("NPI", "")),
        "supervisingProvider": "",
        "supervisingProviderNPI": "",
        "bit64Url": "",
        "daOrderType": "",
        "daUploadSuccess": True,
        "daResponseStatusCode": 0,
        "daResponseDetails": "",
        "createdBy": "PatientScript",
        "createdOn": now_iso(),
        "updatedBy": "",
        "updatedOn": now_iso(),
        "cpoUpdatedBy": "",
        "cpoUpdatedOn": now_iso()
    }


def get_document_data(doc_id):
    """Get document data from DoctorAlliance API."""
    from supremesheet import API_BASE, AUTH_HEADER
    
    url = f"{API_BASE}{doc_id}"
    try:
        r = requests.get(url, headers=AUTH_HEADER, timeout=20)
        data = r.json()
        if not data.get("isSuccess"):
            print(f"  [DOC_API] Failed for doc_id={doc_id}. isSuccess={data.get('isSuccess')}")
            return None
        return data
    except Exception as e:
        print(f"  [DOC_API] Exception for doc_id={doc_id}: {e}")
        return None

def create_order(row, patients=None, company_key=None):
    payload = build_order_payload(row, patients, company_key)
    payload = clean_payload_for_json(payload)
    print("\n--- [ORDER_CREATE] Request Payload ---")
    print(json.dumps(payload, indent=2, default=str))
    
    order_guid = None
    try:
        resp = requests.post(ORDER_API, headers=HEADERS, json=payload, timeout=20)
        print("--- [ORDER_CREATE] Response ---")
        print(f"Status: {resp.status_code}\n{resp.text}\n")
        try:
            resp_json = resp.json()
        except Exception:
            resp_json = {}
        success = resp.status_code in (200, 201) and (isinstance(resp_json, dict) and 'orderNo' in resp_json)
        
        if success:
            # Extract order GUID for PDF upload
            order_guid = resp_json.get('id') or resp_json.get('orderId') or resp_json.get('guid')
            print(f"  [ORDER_CREATE] Order created successfully. Order GUID: {order_guid}")
            
            # Upload PDF to the order
            if order_guid:
                doc_id = row.get('Document ID') or row.get('docId')
                if doc_id:
                    print(f"  [PDF_UPLOAD] Starting PDF upload for Document ID: {doc_id}")
                    doc_data = get_document_data(doc_id)
                    if doc_data:
                        pdf_success, pdf_remark = upload_pdf_from_document_data(doc_data, order_guid)
                        if pdf_success:
                            print(f"  [PDF_UPLOAD] ✅ PDF uploaded successfully to order {order_guid}")
                            return True, f"Order created and PDF uploaded successfully"
                        else:
                            print(f"  [PDF_UPLOAD] ❌ PDF upload failed: {pdf_remark}")
                            return True, f"Order created but PDF upload failed: {pdf_remark}"
                    else:
                        print(f"  [PDF_UPLOAD] ❌ Could not fetch document data for PDF upload")
                        return True, f"Order created but could not fetch document data for PDF upload"
                else:
                    print(f"  [PDF_UPLOAD] ❌ No Document ID found for PDF upload")
                    return True, f"Order created but no Document ID found for PDF upload"
            else:
                print(f"  [PDF_UPLOAD] ❌ No order GUID received for PDF upload")
                return True, f"Order created but no order GUID received for PDF upload"
        else:
            # Simplify error extraction
            if isinstance(resp_json, dict) and 'errors' in resp_json:
                error_details = json.dumps(resp_json['errors'])
            else:
                error_details = resp.text
            return False, error_details
            
    except Exception as e:
        print(f"  [ORDER_CREATE] Error for {row.get('Document ID', '')}: {e}")
        return False, f"Exception: {e}"


def upload_pdf_to_order(document_buffer, order_guid):
    """
    Upload PDF content to the OrderPdfUpload API.
    
    Args:
        document_buffer (str): Base64 encoded PDF content
        order_guid (str): The order GUID to upload to
    
    Returns:
        tuple: (success: bool, response_text: str)
    """
    try:
        # Decode base64 PDF content
        pdf_bytes = base64.b64decode(document_buffer)
        
        # Create temporary PDF file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
            temp_file.write(pdf_bytes)
            temp_file_path = temp_file.name
        
        try:
            # Prepare the upload URL with order GUID
            upload_url = f"{ORDER_PDF_UPLOAD_API}/{order_guid}"
            
            # Prepare multipart form data
            with open(temp_file_path, 'rb') as pdf_file:
                files = {
                    'file': ('document.pdf', pdf_file, 'application/pdf')
                }
                
                # Upload the PDF
                response = requests.post(
                    upload_url,
                    files=files,
                    headers={'accept': '*/*'},
                    timeout=30
                )
                
                print(f"--- [PDF_UPLOAD] Response for Order {order_guid} ---")
                print(f"Status: {response.status_code}")
                print(f"Response: {response.text}\n")
                
                success = response.status_code in (200, 201)
                return success, response.text
                
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                
    except Exception as e:
        print(f"  [PDF_UPLOAD] Error for Order {order_guid}: {e}")
        return False, f"Exception: {e}"


def upload_pdf_from_document_data(doc_data, order_guid):
    try:
        # Extract PDF buffer from document data
        if 'value' in doc_data and isinstance(doc_data['value'], dict):
            value_obj = doc_data['value']
            if 'documentBuffer' in value_obj and value_obj['documentBuffer']:
                return upload_pdf_to_order(value_obj['documentBuffer'], order_guid)
            else:
                return False, "No documentBuffer found in document data"
        else:
            return False, "Invalid document data structure"
            
    except Exception as e:
        print(f"  [PDF_UPLOAD] Error processing document data for Order {order_guid}: {e}")
        return False, f"Exception: {e}"


def main():
    # Get input file name from command line arguments
    input_file = "supreme_excel.xlsx"  # default
    company_key = None  # default
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    if len(sys.argv) > 2:
        company_key = sys.argv[2]
    
    print(f"🔧 Upload_Patients_Orders.py started")
    print(f"   Input file: {input_file}")
    print(f"   Company key: {company_key}")
    print(f"   Arguments received: {sys.argv}")
    
    # Check if input file exists, if not try to find it with company key
    if not os.path.exists(input_file) and company_key:
        # Try to find the company-specific file
        possible_files = [
            f"supreme_excel_{company_key}.xlsx",
            "supreme_excel.xlsx"
        ]
        print(f"   Looking for input file. Tried: {possible_files}")
        for file in possible_files:
            if os.path.exists(file):
                input_file = file
                print(f"✅ Found input file: {input_file}")
                break
        else:
            print(f"❌ Error: Could not find input file. Tried: {possible_files}")
            print(f"   Available files in directory:")
            for f in os.listdir('.'):
                if f.endswith('.xlsx'):
                    print(f"     - {f}")
            return
    elif os.path.exists(input_file):
        print(f"✅ Input file exists: {input_file}")
    else:
        print(f"❌ Input file does not exist: {input_file}")
        return
    
    # Set company API URLs based on active company or provided company key
    set_company_api_urls(company_key)
    
    df = pd.read_excel(input_file)
    if 'PATIENTUPLOAD_STATUS' not in df.columns:
        df['PATIENTUPLOAD_STATUS'] = ""
    if 'PATIENTUPLOAD_REMARKS' not in df.columns:
        df['PATIENTUPLOAD_REMARKS'] = ""
    if 'PATIENT_CREATION_REMARK' not in df.columns:
        df['PATIENT_CREATION_REMARK'] = ""
    if 'ORDER_CREATION_REMARK' not in df.columns:
        df['ORDER_CREATION_REMARK'] = ""

    created_patients = set()
    # 1. First pass: Create patients for 485CERT and 485RECERT where PatientExist==False
    for idx, row in df.iterrows():
        dabackid = str(row.get('DABackOfficeID', '')).strip()
        if (
            not row.get('PatientExist', False)
            and str(row.get('documentType', '')).upper() in ["485RECERT", "485CERT"]
            and dabackid not in created_patients
        ):
            success, remarks = create_patient(row, company_key)
            df.at[idx, 'PATIENTUPLOAD_STATUS'] = "TRUE" if success else "FALSE"
            df.at[idx, 'PATIENTUPLOAD_REMARKS'] = remarks
            if success:
                df.at[idx, 'PATIENT_CREATION_REMARK'] = ""
                created_patients.add(dabackid)
            else:
                df.at[idx, 'PATIENT_CREATION_REMARK'] = remarks  # Already includes error if any
        else:
            df.at[idx, 'PATIENTUPLOAD_STATUS'] = "SKIPPED"
            df.at[idx, 'PATIENT_CREATION_REMARK'] = "Patient creation skipped: already exists or document type not eligible."

    # Refill patient info and update PatientExist if found
    df = refill_patient_info(df)
    output_file_with_patients = input_file.replace('.xlsx', '_with_patient_upload.xlsx')
    df.to_excel(output_file_with_patients, index=False)
    print(f"✅ Created patient upload file: {output_file_with_patients}")
    print(f"   Total records: {len(df)}")
    print(f"   Patients created: {len(created_patients)}")

    # 2. Second pass: Convert OTHER_SIGNABLE to OTHER, and create patients for ALL PatientExist==False rows (if not already created)
    for idx, row in df.iterrows():
        # Fix document type
        if str(row.get('documentType', '')).upper() == "OTHER_SIGNABLE":
            df.at[idx, 'documentType'] = "OTHER"
    for idx, row in df.iterrows():
        dabackid = str(row.get('DABackOfficeID', '')).strip()
        if (
            not row.get('PatientExist', False)
            and dabackid not in created_patients
        ):
            success, remarks = create_patient(row, company_key)
            df.at[idx, 'PATIENTUPLOAD_STATUS'] = "TRUE" if success else "FALSE"
            df.at[idx, 'PATIENTUPLOAD_REMARKS'] = remarks
            if success:
                df.at[idx, 'PATIENT_CREATION_REMARK'] = ""
                created_patients.add(dabackid)
            else:
                df.at[idx, 'PATIENT_CREATION_REMARK'] = remarks
        else:
            df.at[idx, 'PATIENT_CREATION_REMARK'] = "Patient creation skipped: already exists."

    # Refill patient info again after second pass
    df = refill_patient_info(df)
    df.to_excel(output_file_with_patients, index=False)

    # Download patients once for episode lookup
    try:
        resp = requests.get(PATIENT_API, timeout=30)
        patients_for_orders = resp.json()
    except Exception as e:
        print(f"Warning: Could not download patients for episode lookup: {e}")
        patients_for_orders = []

    # 3. Upload orders for PatientExist==TRUE
    df['ORDERUPLOAD_STATUS'] = ""
    df['ORDER_CREATION_REMARK'] = ""
    df['PDF_UPLOAD_STATUS'] = ""
    df['PDF_UPLOAD_REMARK'] = ""
    
    for idx, row in df.iterrows():
        if row.get('PatientExist', False):
            try:
                order_success, order_remark = create_order(row, patients_for_orders, company_key)
                df.at[idx, 'ORDERUPLOAD_STATUS'] = "TRUE" if order_success else "FALSE"
                df.at[idx, 'ORDER_CREATION_REMARK'] = order_remark
                
                # Track PDF upload status
                if order_success and "PDF uploaded successfully" in order_remark:
                    df.at[idx, 'PDF_UPLOAD_STATUS'] = "TRUE"
                    df.at[idx, 'PDF_UPLOAD_REMARK'] = "PDF uploaded successfully"
                elif order_success and "PDF upload failed" in order_remark:
                    df.at[idx, 'PDF_UPLOAD_STATUS'] = "FALSE"
                    df.at[idx, 'PDF_UPLOAD_REMARK'] = order_remark
                elif order_success:
                    df.at[idx, 'PDF_UPLOAD_STATUS'] = "SKIPPED"
                    df.at[idx, 'PDF_UPLOAD_REMARK'] = "PDF upload skipped - no document data available"
                else:
                    df.at[idx, 'PDF_UPLOAD_STATUS'] = "SKIPPED"
                    df.at[idx, 'PDF_UPLOAD_REMARK'] = "PDF upload skipped - order creation failed"
                    
            except Exception as e:
                df.at[idx, 'ORDERUPLOAD_STATUS'] = "FALSE"
                df.at[idx, 'ORDER_CREATION_REMARK'] = f"Exception: {e}"
                df.at[idx, 'PDF_UPLOAD_STATUS'] = "SKIPPED"
                df.at[idx, 'PDF_UPLOAD_REMARK'] = "PDF upload skipped - order creation exception"
        else:
            df.at[idx, 'ORDERUPLOAD_STATUS'] = "SKIPPED"
            df.at[idx, 'ORDER_CREATION_REMARK'] = "Order skipped: Patient does not exist for this row."
            df.at[idx, 'PDF_UPLOAD_STATUS'] = "SKIPPED"
            df.at[idx, 'PDF_UPLOAD_REMARK'] = "PDF upload skipped: Patient does not exist for this row."

    output_file_final = input_file.replace('.xlsx', '_with_patient_and_order_upload.xlsx')
    df.to_excel(output_file_final, index=False)
    print(f"✅ Created final upload file: {output_file_final}")
    print(f"   Total records: {len(df)}")
    print(f"   Orders processed: {len(df[df['ORDERUPLOAD_STATUS'].isin(['TRUE', 'FALSE'])])}")
    print(f"   PDFs uploaded successfully: {len(df[df['PDF_UPLOAD_STATUS'] == 'TRUE'])}")
    print(f"   PDFs upload failed: {len(df[df['PDF_UPLOAD_STATUS'] == 'FALSE'])}")
    print(f"   PDFs upload skipped: {len(df[df['PDF_UPLOAD_STATUS'] == 'SKIPPED'])}")
    print(f"Upload process complete. Check {output_file_final}")


if __name__ == "__main__":
    main()
