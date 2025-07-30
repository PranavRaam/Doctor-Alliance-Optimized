import pandas as pd
import requests
from datetime import datetime
import os
import re
import sys
from fuzzywuzzy import fuzz
import openai
import concurrent.futures
import time
from functools import lru_cache
import threading

API_BASE = "https://api.doctoralliance.com/document/getfile?docId.id="
AUTH_HEADER = {
    "Accept": "application/json",
    "Authorization": "Bearer BwmWBqhXAEvG70Irt_1J8kJM8_4p81dStSUAeWXFho6d-Fu2Ymsox3qFLaQgZcX_EA-JjYi_MpiDS5FzulJ6hw2Qne5DearMdRfkkS_E8GaG5fy82RI_YhwM1cn-VtTQG5FSAjUPukOuJri8lPjQUZS1vzh9bRd3f3FQQlJxwzMKDfrqkt_03SR70bjDsKA9KYdJibMr5DBpaUkyJNzATdlewBwkeGEnX4EfzRj_mn_gm_G7Pjdo2qCCXbDhGeuH5lLuKvqFciQy_Wb8TEOR7Q"
}

# PATIENT_API will be set dynamically based on company configuration
PATIENT_API = None
ENTITY_API = "https://dawaventity-g5a6apetdkambpcu.eastus-01.azurewebsites.net/api/Entity?EntityType=ANCILLIARY"

# Global cache for API responses
api_cache = {}
cache_lock = threading.Lock()

# Gender cache to avoid repeated GPT calls
gender_cache = {}
gender_cache_lock = threading.Lock()

# Azure OpenAI configuration
AZURE_OPENAI_KEY = "EVtCfEbXd2pvVrkOaByfss3HBMJy9x0FvwXdFhCmenum0RLvHCZNJQQJ99BDACYeBjFXJ3w3AAABACOGe7zr"
AZURE_OPENAI_ENDPOINT = "https://daplatformai.openai.azure.com/"
AZURE_OPENAI_DEPLOYMENT = "gpt-35-turbo"

def clean_doc_id(val):
    if pd.isna(val) or val is None:
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    s = str(val)
    if s.endswith('.0'):
        return s[:-2]
    return s

def try_date(dtstr):
    if not dtstr: return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(dtstr[:19], fmt).strftime("%m/%d/%Y")
        except: continue
    try:
        return pd.to_datetime(dtstr).strftime("%m/%d/%Y")
    except:
        return dtstr[:10]

def get_order_doc_api(doc_id):
    # Check cache first
    with cache_lock:
        if doc_id in api_cache:
            return api_cache[doc_id]
    
    url = f"{API_BASE}{doc_id}"
    try:
        r = requests.get(url, headers=AUTH_HEADER, timeout=20)
        data = r.json()
        if not data.get("isSuccess"):
            print(f"  [DOC_API] Failed for doc_id={doc_id}. isSuccess={data.get('isSuccess')}. Raw: {data}")
            result = {}
        else:
            value = data.get("value", {})
            print(f"  [DOC_API] Success for doc_id={doc_id}. Type: {value.get('documentType', '')}, PatientName: {value.get('patientName', '')}")
            result = value
        
        # Cache the result
        with cache_lock:
            api_cache[doc_id] = result
        return result
    except Exception as e:
        print(f"  [DOC_API] Exception for doc_id={doc_id}: {e}")
        result = {}
        # Cache the error result too to avoid retrying
        with cache_lock:
            api_cache[doc_id] = result
        return result

def set_patient_api_for_company(company_key=None):
    """Set the PATIENT_API URL for a specific company."""
    global PATIENT_API
    from config import get_company_config, get_companies_to_process
    
    # If no company key provided, get the first company from the processing list
    if company_key is None:
        companies_to_process = get_companies_to_process()
        if companies_to_process:
            company_key = companies_to_process[0]
            print(f"  [CONFIG] Using first company from processing list: {company_key}")
        else:
            print("  [ERROR] No companies configured for processing")
            return
    
    try:
        company = get_company_config(company_key)
        pg_company_id = company['pg_company_id']
        PATIENT_API = f"https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/company/pg/{pg_company_id}"
        print(f"  [CONFIG] Set PATIENT_API for company {company_key} ({company['name']}): {PATIENT_API}")
    except Exception as e:
        print(f"  [ERROR] Failed to set PATIENT_API for company {company_key}: {e}")
        return

def get_all_patients():
    if PATIENT_API is None:
        print("  [ERROR] PATIENT_API not set. Please set company configuration first.")
        return []
    
    try:
        r = requests.get(PATIENT_API, timeout=20)
        data = r.json()
        if isinstance(data, list):
            print(f"  [PATIENT_API] Downloaded {len(data)} patients")
            return data
        print(f"  [PATIENT_API] Unexpected result: {data}")
        return []
    except Exception as e:
        print(f"  [PATIENT_API] Error: {e}")
        return []

def get_valid_icds(icd_codes_validated):
    if not isinstance(icd_codes_validated, list):
        try:
            icd_codes_validated = eval(icd_codes_validated)
        except:
            return []
    return [icd for icd in icd_codes_validated if icd and str(icd).strip()]

def extract_pgcompanyid_from_url(url):
    if not url: return ""
    match = re.search(r'/pg/(\d+)', url)
    return match.group(1) if match else ""

def get_companyid_by_careprovider_name(care_provider_name):
    if not care_provider_name:
        return ""
    try:
        r = requests.get(ENTITY_API, timeout=10)
        entities = r.json()
        for entity in entities:
            if entity.get("name", "").lower() == care_provider_name.lower():
                return str(entity.get("id", ""))
    except Exception as e:
        print(f"  [ENTITY_API] Error: {e}")
    return ""

def match_patient(row, patients):
    mrn = row.get("mrn", "")
    dabackofficeid = row.get("DABackOfficeID", "")
    
    for patient in patients:
        patient_mrn = patient.get("mrn", "")
        patient_id = str(patient.get("id", ""))
        
        if mrn and patient_mrn and mrn == patient_mrn:
            return patient, True
        if dabackofficeid and patient_id and dabackofficeid == patient_id:
            return patient, True
    
    return None, False

def fill_episode_dates(patient, orderdate):
    episode_diagnoses = patient.get("episodeDiagnoses", [])
    if not episode_diagnoses:
        return "", "", ""
    
    # Find the episode that matches the order date
    for episode in episode_diagnoses:
        episode_date = episode.get("episodeDate")
        if episode_date and orderdate:
            # Simple date comparison - you might want to improve this
            if str(episode_date)[:10] == str(orderdate)[:10]:
                return (
                    try_date(episode.get("soc")),
                    try_date(episode.get("certPeriodSoe")),
                    try_date(episode.get("certPeriodEoe"))
                )
    
    # If no exact match, return the first episode
    first_episode = episode_diagnoses[0]
    return (
        try_date(first_episode.get("soc")),
        try_date(first_episode.get("certPeriodSoe")),
        try_date(first_episode.get("certPeriodEoe"))
    )

def guess_gender_with_gpt(name):
    if not name:
        return ""
    
    # Check cache first
    with gender_cache_lock:
        if name in gender_cache:
            return gender_cache[name]
    
    openai.api_type = "azure"
    openai.api_key = AZURE_OPENAI_KEY
    openai.api_base = AZURE_OPENAI_ENDPOINT
    openai.api_version = "2024-02-15-preview"
    prompt = (
        f"What is the most likely gender for the first name '{name}'? "
        "Reply with MALE or FEMALE only. If the name could be both, make your best educated guess and still reply MALE or FEMALE. "
        "Do NOT reply with any other word, phrase, or empty string."
    )
    try:
        response = openai.ChatCompletion.create(
            engine=AZURE_OPENAI_DEPLOYMENT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1,
            temperature=0.1  # Reduce randomness for more consistent answers
        )
        ans = response["choices"][0]["message"]["content"].strip().upper()
        if ans in ["MALE", "FEMALE"]:
            # Cache the result
            with gender_cache_lock:
                gender_cache[name] = ans
            return ans
    except Exception as e:
        print(f"[GENDER_GPT] Error guessing gender: {e}")
    
    # Default/fallback if GPT fails
    result = "FEMALE"
    with gender_cache_lock:
        gender_cache[name] = result
    return result

def extract_first_name(full_name):
    if not full_name or not isinstance(full_name, str):
        return ""
    full_name = full_name.strip()
    if ',' in full_name:
        parts = [p.strip() for p in full_name.split(',')]
        if len(parts) > 1:
            candidates = re.findall(r"[A-Za-z'-]+", parts[1])
            if candidates:
                return candidates[0].capitalize()
        candidates = re.findall(r"[A-Za-z'-]+", parts[0])
        if candidates:
            return candidates[0].capitalize()
    candidates = re.findall(r"[A-Za-z'-]+", full_name)
    if candidates:
        return candidates[0].capitalize()
    return ""

def process_document_batch(doc_ids, patients):
    """Process a batch of documents in parallel"""
    results = []
    
    def process_single_doc(doc_id):
        try:
            # Get API info
            doc_api = get_order_doc_api(doc_id)
            return doc_id, doc_api
        except Exception as e:
            print(f"  [ERROR] Failed to process doc_id={doc_id}: {e}")
            return doc_id, {}
    
    # Use ThreadPoolExecutor for parallel API calls
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_doc = {executor.submit(process_single_doc, doc_id): doc_id for doc_id in doc_ids}
        
        for future in concurrent.futures.as_completed(future_to_doc):
            doc_id, doc_api = future.result()
            results.append((doc_id, doc_api))
    
    return results

def process_row(row, patients, i):
    """Process a single row - optimized version"""
    doc_id = clean_doc_id(row.get("Document ID"))
    if not doc_id:
        print(f"[{i+1}] Skipped: No Document ID")
        return None
    
    print(f"\n[{i+1}] Processing Document ID: {doc_id}")
    
    npi = row.get("NPI")
    orderno = row.get("orderno", "")
    orderdate = row.get("orderdate", "")
    mrn = row.get("mrn", "")
    soc = row.get("soc", "")
    cert_period_soe = row.get("cert_period_soe", "")
    cert_period_eoe = row.get("cert_period_eoe", "")
    icd_codes_validated = row.get("icd_codes_validated", "[]")
    dob = row.get("dob", "")
    address = row.get("address", "")

    # Get API info (this will use cache if available)
    doc_api = get_order_doc_api(doc_id)
    documentType = doc_api.get("documentType", "")
    physicianSigndate = try_date(doc_api.get("physicianSigndate"))
    dabackofficeid = str(doc_api.get("patientId", {}).get("id", ""))
    patient_name = row.get("patientName") or doc_api.get("patientName", "")
    sendDate = try_date(doc_api.get("sendDate"))
    care_provider_name = doc_api.get("careProvider", "")
    
    # Use sendDate as fallback when orderdate is empty
    if not orderdate or str(orderdate).strip() == "" or str(orderdate).lower() in ["nan", "none"]:
        orderdate = sendDate

    # SEX/GENDER LOGIC - only call GPT if necessary
    patient_sex = row.get("patient_sex", "")
    if isinstance(patient_sex, str):
        if patient_sex.strip().upper() in ["MALE", "M"]:
            patient_sex = "MALE"
        elif patient_sex.strip().upper() in ["FEMALE", "F"]:
            patient_sex = "FEMALE"
        else:
            patient_sex = ""
    else:
        patient_sex = ""
    
    # Only call GPT if we don't have gender and have a first name
    first_name = extract_first_name(patient_name)
    if not patient_sex and first_name:
        guessed = guess_gender_with_gpt(first_name)
        if guessed:
            patient_sex = guessed
        else:
            print(f"  [GENDER_GPT] Could not determine gender for name: {first_name} ({patient_name})")
    elif not first_name:
        print(f"  [GENDER] No first name could be extracted from '{patient_name}'")

    # ICDs
    if isinstance(icd_codes_validated, str):
        try:
            icd_codes_validated = eval(icd_codes_validated)
        except: 
            print("  [ICD] Could not parse icd_codes_validated, using empty list")
            icd_codes_validated = []
    valid_icds = get_valid_icds(icd_codes_validated)
    diag_dict = {}
    for idx in range(6):
        diag_dict[f"Diagnosis {idx+1}"] = valid_icds[idx] if idx < len(valid_icds) else ""

    out_row = {
        "Document ID": doc_id,
        "NPI": npi,
        "orderno": orderno,
        "orderdate": orderdate,
        "mrn": mrn,
        "dob": dob,
        "address": address,
        "soc": soc,
        "cert_period_soe": cert_period_soe,
        "cert_period_eoe": cert_period_eoe,
        **diag_dict,
        "documentType": documentType,
        "physicianSigndate": physicianSigndate,
        "DABackOfficeID": dabackofficeid,
        "patientName": patient_name,
        "sendDate": sendDate,
        "patient_sex": patient_sex,
        "PDF_Available": "YES" if doc_api.get("documentBuffer") else "NO",
        "PDF_Upload_Ready": "YES" if (doc_api.get("documentBuffer") and doc_api.get("documentType")) else "NO",
        "PDF_Size_KB": len(doc_api.get("documentBuffer", "")) // 1024 if doc_api.get("documentBuffer") else 0
    }

    patient, found = match_patient(
        {
            "mrn": mrn,
            "DABackOfficeID": dabackofficeid
        },
        patients
    )
    companyid = ""
    if care_provider_name:
        companyid = get_companyid_by_careprovider_name(care_provider_name)
    if found and patient:
        agency = patient.get("agencyInfo", {})
        out_row["PatientExist"] = True
        out_row["patientid"] = patient.get("id", "")
        out_row["Pgcompanyid"] = extract_pgcompanyid_from_url(PATIENT_API)
        if not companyid:
            companyid = agency.get("companyId", "")
        out_row["companyId"] = companyid
        # If soc, cert_period_soe, cert_period_eoe are missing, fill from episodeDiagnoses
        missing_soc = not soc or str(soc).strip() == "" or str(soc).lower() in ["nan", "none"]
        missing_soe = not cert_period_soe or str(cert_period_soe).strip() == "" or str(cert_period_soe).lower() in ["nan", "none"]
        missing_eoe = not cert_period_eoe or str(cert_period_eoe).strip() == "" or str(cert_period_eoe).lower() in ["nan", "none"]
        if (missing_soc or missing_soe or missing_eoe):
            soc2, soe2, eoe2 = fill_episode_dates(patient, orderdate)
            if missing_soc and soc2:
                out_row["soc"] = soc2
            if missing_soe and soe2:
                out_row["cert_period_soe"] = soe2
            if missing_eoe and eoe2:
                out_row["cert_period_eoe"] = eoe2
        print(f"  [OUTPUT] Patient found: patientid={out_row['patientid']} companyId={out_row['companyId']}")
    else:
        out_row["PatientExist"] = False
        out_row["patientid"] = ""
        out_row["Pgcompanyid"] = extract_pgcompanyid_from_url(PATIENT_API)
        out_row["companyId"] = companyid
        print(f"  [OUTPUT] Patient NOT found for doc_id={doc_id}")

    return out_row

def main():
    start_time = time.time()
    # Get input and output file names from command line arguments
    excel_file = "doctoralliance_combined_output.xlsx"  # default
    output_file = "supreme_excel.xlsx"  # default
    max_workers = 5  # default parallel workers
    pre_fetch = True  # default to pre-fetching
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    if len(sys.argv) > 3:
        try:
            max_workers = int(sys.argv[3])
        except ValueError:
            print(f"Invalid max_workers value: {sys.argv[3]}, using default: 5")
    if len(sys.argv) > 4:
        pre_fetch = sys.argv[4].lower() in ['true', '1', 'yes']
    
    print(f"[CONFIG] Excel file: {excel_file}")
    print(f"[CONFIG] Output file: {output_file}")
    print(f"[CONFIG] Max workers: {max_workers}")
    print(f"[CONFIG] Pre-fetch APIs: {pre_fetch}")
    
    if not os.path.exists(excel_file):
        print(f"Input file {excel_file} not found!")
        return
    
    # Set the PATIENT_API for the company using config
    set_patient_api_for_company()
    
    df = pd.read_excel(excel_file)
    print(f"[INFO] Loaded {len(df)} rows from {excel_file}")
    patients = get_all_patients()
    print(f"[INFO] Fetched {len(patients)} patients from API.")
    
    # Pre-fetch all document APIs in parallel to populate cache
    if pre_fetch:
        print("[INFO] Pre-fetching document APIs in parallel...")
        doc_ids = [clean_doc_id(row.get("Document ID")) for _, row in df.iterrows() if clean_doc_id(row.get("Document ID"))]
        process_document_batch(doc_ids, patients)
        print(f"[INFO] Pre-fetched {len(api_cache)} document APIs")
    
    # Process rows in parallel for better performance
    print(f"[INFO] Processing rows in parallel with {max_workers} workers...")
    output_rows = []
    
    def process_row_wrapper(args):
        row, patients, i = args
        return process_row(row, patients, i)
    
    # Prepare arguments for parallel processing
    row_args = [(row, patients, i) for i, (_, row) in enumerate(df.iterrows())]
    
    # Use ThreadPoolExecutor for parallel row processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_row = {executor.submit(process_row_wrapper, args): args[2] for args in row_args}
        
        # Collect results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_row):
            i = future_to_row[future]
            completed += 1
            if completed % 10 == 0:
                print(f"[PROGRESS] Completed {completed}/{len(row_args)} rows ({(completed/len(row_args)*100):.1f}%)")
            try:
                result = future.result()
                if result:
                    output_rows.append(result)
            except Exception as e:
                print(f"[ERROR] Failed to process row {i}: {e}")

    out_df = pd.DataFrame(output_rows)
    out_df.to_excel(output_file, index=False)
    
    # Print PDF statistics
    total_records = len(out_df)
    pdf_available = len(out_df[out_df['PDF_Available'] == 'YES'])
    pdf_upload_ready = len(out_df[out_df['PDF_Upload_Ready'] == 'YES'])
    
    print(f"\n✅ Supreme Excel written: {output_file}")
    print(f"📊 PDF Statistics:")
    print(f"   Total records: {total_records}")
    print(f"   PDFs available: {pdf_available}")
    print(f"   PDFs ready for upload: {pdf_upload_ready}")
    print(f"   PDF upload success rate: {(pdf_upload_ready/total_records*100):.1f}%" if total_records > 0 else "   PDF upload success rate: 0%")
    print(f"   API calls cached: {len(api_cache)}")
    print(f"   Gender guesses cached: {len(gender_cache)}")
    print(f"\n🚀 Performance Summary:")
    print(f"   Total processing time: {time.time() - start_time:.2f} seconds")
    print(f"   Average time per record: {(time.time() - start_time)/total_records:.2f} seconds" if total_records > 0 else "   Average time per record: 0.00 seconds")

if __name__ == "__main__":
    main()
