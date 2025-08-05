import pandas as pd
import requests
from datetime import datetime
import os
import re
import sys
from fuzzywuzzy import fuzz
import openai
import asyncio
import aiohttp
from typing import Dict, List, Tuple, Optional
import time
from functools import lru_cache
from performance_monitor import start_monitoring, update_progress, stop_monitoring

API_BASE = "https://api.doctoralliance.com/document/getfile?docId.id="
AUTH_HEADER = {
    "Accept": "application/json",
    "Authorization": "Bearer BwmWBqhXAEvG70Irt_1J8kJM8_4p81dStSUAeWXFho6d-Fu2Ymsox3qFLaQgZcX_EA-JjYi_MpiDS5FzulJ6hw2Qne5DearMdRfkkS_E8GaG5fy82RI_YhwM1cn-VtTQG5FSAjUPukOuJri8lPjQUZS1vzh9bRd3f3FQQlJxwzMKDfrqkt_03SR70bjDsKA9KYdJibMr5DBpaUkyJNzATdlewBwkeGEnX4EfzRj_mn_gm_G7Pjdo2qCCXbDhGeuH5lLuKvqFciQy_Wb8TEOR7Q"
}

# PATIENT_API will be set dynamically based on company configuration
PATIENT_API = None
ENTITY_API = "https://dawaventity-g5a6apetdkambpcu.eastus-01.azurewebsites.net/api/Entity?EntityType=ANCILLIARY"

# Performance optimization: Global caches
DOC_API_CACHE = {}
PATIENT_CACHE = {}
ENTITY_CACHE = {}
GENDER_CACHE = {}

# Performance settings
MAX_CONCURRENT_REQUESTS = 10
BATCH_SIZE = 50

# Performance optimization: Global caches
DOC_API_CACHE = {}
PATIENT_CACHE = {}
ENTITY_CACHE = {}
GENDER_CACHE = {}

# Performance settings
MAX_CONCURRENT_REQUESTS = 10
BATCH_SIZE = 50

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

async def get_order_doc_api_async(session: aiohttp.ClientSession, doc_id: str) -> Dict:
    """Async version of get_order_doc_api with caching."""
    if doc_id in DOC_API_CACHE:
        return DOC_API_CACHE[doc_id]
    
    url = f"{API_BASE}{doc_id}"
    try:
        async with session.get(url, headers=AUTH_HEADER, timeout=aiohttp.ClientTimeout(total=20)) as r:
            data = await r.json()
            if not data.get("isSuccess"):
                print(f"  [DOC_API] Failed for doc_id={doc_id}. isSuccess={data.get('isSuccess')}. Raw: {data}")
                result = {}
            else:
                value = data.get("value", {})
                print(f"  [DOC_API] Success for doc_id={doc_id}. Type: {value.get('documentType', '')}, PatientName: {value.get('patientName', '')}")
                result = value
            DOC_API_CACHE[doc_id] = result
            return result
    except Exception as e:
        print(f"  [DOC_API] Exception for doc_id={doc_id}: {e}")
        result = {}
        DOC_API_CACHE[doc_id] = result
        return result

def get_order_doc_api(doc_id):
    """Synchronous fallback for get_order_doc_api."""
    if doc_id in DOC_API_CACHE:
        return DOC_API_CACHE[doc_id]
    
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
        DOC_API_CACHE[doc_id] = result
        return result
    except Exception as e:
        print(f"  [DOC_API] Exception for doc_id={doc_id}: {e}")
        result = {}
        DOC_API_CACHE[doc_id] = result
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
    
    # Check cache first
    if PATIENT_API in PATIENT_CACHE:
        print(f"  [PATIENT_API] Using cached {len(PATIENT_CACHE[PATIENT_API])} patients")
        return PATIENT_CACHE[PATIENT_API]
    
    try:
        r = requests.get(PATIENT_API, timeout=20)
        data = r.json()
        if isinstance(data, list):
            print(f"  [PATIENT_API] Downloaded {len(data)} patients")
            PATIENT_CACHE[PATIENT_API] = data
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
        except: return []
    out = [
        d['code'] for d in icd_codes_validated
        if d.get("code") and d.get("desc") and str(d["desc"]).strip().upper() not in ["NOT FOUND", "NOTFOUND", "NULL", ""]
    ]
    print(f"  [ICD] Valid codes: {out}")
    return out

def extract_pgcompanyid_from_url(url):
    m = re.search(r"/company/pg/([a-f0-9\-]+)", url)
    return m.group(1) if m else ""

@lru_cache(maxsize=1000)
def get_companyid_by_careprovider_name(care_provider_name):
    """Cached version of company ID lookup."""
    if not care_provider_name:
        return ""
    
    # Check cache first
    if care_provider_name in ENTITY_CACHE:
        return ENTITY_CACHE[care_provider_name]
    
    try:
        r = requests.get(ENTITY_API, timeout=30)
        entities = r.json()
        if isinstance(entities, dict) and "value" in entities:
            entities = entities["value"]
        best_score = 0
        best_id = ""
        for entity in entities:
            entity_name_raw = entity.get("name", "")
            entity_name = entity_name_raw.strip().lower() if entity_name_raw else ""
            input_name_raw = care_provider_name
            input_name = input_name_raw.strip().lower() if input_name_raw else ""
            score = fuzz.token_set_ratio(entity_name, input_name)
            if score > best_score:
                best_score = score
                best_id = entity.get('id', "")
            if score >= 90:
                print(f"  [COMPANYID] 90%+ match for '{care_provider_name}' <-> '{entity.get('name', '')}' (score {score}): {entity.get('id')}")
                result = entity.get("id", "")
                ENTITY_CACHE[care_provider_name] = result
                return result
        if best_score >= 80:
            print(f"  [COMPANYID] Best fuzzy match ({best_score}%) for '{care_provider_name}': {best_id}")
            ENTITY_CACHE[care_provider_name] = best_id
            return best_id
        print(f"  [COMPANYID] No 90% match for careProvider='{care_provider_name}'")
    except Exception as e:
        print(f"  [COMPANYID] Error: {e}")
    return ""

def create_patient_lookup_maps(patients):
    """Create efficient lookup maps for patient matching."""
    mrn_map = {}
    dabackid_map = {}
    
    for patient in patients:
        agency = patient.get("agencyInfo", {})
        
        # MRN mapping
        mrn_raw = agency.get("medicalRecordNo")
        mrn = str(mrn_raw).strip() if mrn_raw is not None else ""
        if mrn:
            mrn_map[mrn.upper()] = patient
        
        # DABackOfficeID mapping
        dabackid_raw = agency.get("daBackofficeID")
        dabackid = str(dabackid_raw).strip() if dabackid_raw is not None else ""
        if dabackid:
            dabackid_map[dabackid] = patient
    
    return mrn_map, dabackid_map

def match_patient_fast(row, mrn_map, dabackid_map):
    """Fast patient matching using lookup maps."""
    mrn_raw = row.get("mrn")
    mrn = str(mrn_raw).strip() if mrn_raw is not None else ""
    dabackid_raw = row.get("DABackOfficeID") if "DABackOfficeID" in row else None
    dabackid = str(dabackid_raw).strip() if dabackid_raw is not None else ""
    
    # Try MRN first
    if mrn and mrn.upper() in mrn_map:
        print(f"  [PAT_MATCH] MRN matched for {mrn}")
        return mrn_map[mrn.upper()], True
    
    # Try DABackOfficeID
    if dabackid and dabackid in dabackid_map:
        print(f"  [PAT_MATCH] DABackOfficeID matched for {dabackid}")
        return dabackid_map[dabackid], True
    
    print(f"  [PAT_MATCH] No match for MRN='{mrn}' or DABackOfficeID='{dabackid}'")
    return None, False

def fill_episode_dates(patient, orderdate):
    if not patient or not orderdate:
        return "", "", ""
    agency = patient.get("agencyInfo", {})
    episode_diag = agency.get("episodeDiagnoses", [])
    try:
        odt = pd.to_datetime(orderdate)
    except:
        return "", "", ""
    for ep in episode_diag:
        try:
            start_ep = pd.to_datetime(ep.get("startOfEpisode", ""))
            end_ep = pd.to_datetime(ep.get("endOfEpisode", ""))
        except:
            continue
        if pd.notna(start_ep) and pd.notna(end_ep) and start_ep <= odt <= end_ep:
            soc = ep.get("startOfCare", "")
            soe = ep.get("startOfEpisode", "")
            eoe = ep.get("endOfEpisode", "")
            print(f"  [EPISODE] Found match: SOC={soc}, SOE={soe}, EOE={eoe}")
            return soc, soe, eoe
    return "", "", ""

AZURE_OPENAI_KEY = "EVtCfEbXd2pvVrkOaByfss3HBMJy9x0FvwXdFhCmenum0RLvHCZNJQQJ99BDACYeBjFXJ3w3AAABACOGe7zr"
AZURE_OPENAI_ENDPOINT = "https://daplatformai.openai.azure.com/"
AZURE_OPENAI_DEPLOYMENT = "gpt-35-turbo"

@lru_cache(maxsize=1000)
def guess_gender_with_gpt(name):
    """Cached gender guessing to avoid repeated API calls."""
    if not name:
        return ""
    
    # Check cache first
    if name in GENDER_CACHE:
        return GENDER_CACHE[name]
    
    prompt = (
        f"What is the most likely gender for the first name '{name}'? "
        "Reply with MALE or FEMALE only. If the name could be both, make your best educated guess and still reply MALE or FEMALE. "
        "Do NOT reply with any other word, phrase, or empty string."
    )
    try:
        from openai import AzureOpenAI
        
        client = AzureOpenAI(
            api_key=AZURE_OPENAI_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version="2024-02-15-preview"
        )
        
        response = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1,
            temperature=0.1  # Reduce randomness for more consistent answers
        )
        ans = response.choices[0].message.content.strip().upper()
        if ans in ["MALE", "FEMALE"]:
            GENDER_CACHE[name] = ans
            return ans
    except Exception as e:
        print(f"[GENDER_GPT] Error guessing gender: {e}")
    # Default/fallback if GPT fails (optional: choose your own default)
    result = "FEMALE"
    GENDER_CACHE[name] = result
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

async def process_batch_async(session: aiohttp.ClientSession, batch_rows, patients, mrn_map, dabackid_map):
    """Process a batch of rows asynchronously."""
    tasks = []
    for row in batch_rows:
        task = process_single_row_async(session, row, patients, mrn_map, dabackid_map)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]

async def process_single_row_async(session: aiohttp.ClientSession, row, patients, mrn_map, dabackid_map):
    """Process a single row asynchronously."""
    doc_id = clean_doc_id(row.get("Document ID"))
    if not doc_id:
        return None
    
    # Get API info asynchronously
    doc_api = await get_order_doc_api_async(session, doc_id)
    
    # Process the row (rest of the logic remains the same)
    return process_row_data(row, doc_api, patients, mrn_map, dabackid_map)

def process_row_data(row, doc_api, patients, mrn_map, dabackid_map):
    """Process row data (extracted from original logic)."""
    doc_id = clean_doc_id(row.get("Document ID"))
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

    documentType = doc_api.get("documentType", "")
    physicianSigndate = try_date(doc_api.get("physicianSigndate"))
    dabackofficeid = str(doc_api.get("patientId", {}).get("id", ""))
    patient_name = row.get("patientName") or doc_api.get("patientName", "")
    sendDate = try_date(doc_api.get("sendDate"))
    care_provider_name = doc_api.get("careProvider", "")
    
    if not orderdate or (isinstance(orderdate, str) and orderdate.strip() == "") or str(orderdate).lower() in ["nan", "none"]:
        orderdate = sendDate

    # SEX/GENDER LOGIC
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

    patient, found = match_patient_fast(
        {
            "mrn": mrn,
            "DABackOfficeID": dabackofficeid
        },
        mrn_map, dabackid_map
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
        missing_soc = not soc or (isinstance(soc, str) and soc.strip() == "") or str(soc).lower() in ["nan", "none"]
        missing_soe = not cert_period_soe or (isinstance(cert_period_soe, str) and cert_period_soe.strip() == "") or str(cert_period_soe).lower() in ["nan", "none"]
        missing_eoe = not cert_period_eoe or (isinstance(cert_period_eoe, str) and cert_period_eoe.strip() == "") or str(cert_period_eoe).lower() in ["nan", "none"]
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

async def main_async():
    """Async version of main function for better performance."""
    # Get input and output file names from command line arguments
    excel_file = "doctoralliance_combined_output.xlsx"  # default
    output_file = "supreme_excel.xlsx"  # default
    
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    if not os.path.exists(excel_file):
        print(f"Input file {excel_file} not found!")
        return
    
    # Set the PATIENT_API for the company using config
    set_patient_api_for_company()
    
    df = pd.read_excel(excel_file)
    print(f"[INFO] Loaded {len(df)} rows from {excel_file}")
    
    # Start performance monitoring
    start_monitoring(len(df))
    
    # Pre-load all patients and create lookup maps
    patients = get_all_patients()
    print(f"[INFO] Fetched {len(patients)} patients from API.")
    mrn_map, dabackid_map = create_patient_lookup_maps(patients)
    
    # Pre-load entity data
    print("[INFO] Pre-loading entity data...")
    get_companyid_by_careprovider_name("dummy")  # This will load entities into cache
    
    # Process in batches asynchronously
    output_rows = []
    total_rows = len(df)
    
    # Setup async session
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS,
        limit_per_host=MAX_CONCURRENT_REQUESTS,
        ttl_dns_cache=300,
        use_dns_cache=True,
        keepalive_timeout=30,
        enable_cleanup_closed=True,
        ssl=False
    )
    
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=AUTH_HEADER,
        trust_env=True
    ) as session:
        
        for i in range(0, total_rows, BATCH_SIZE):
            batch_end = min(i + BATCH_SIZE, total_rows)
            batch_df = df.iloc[i:batch_end]
            print(f"\n[INFO] Processing batch {i//BATCH_SIZE + 1}/{(total_rows + BATCH_SIZE - 1)//BATCH_SIZE} ({len(batch_df)} rows)")
            
            batch_results = await process_batch_async(session, batch_df.to_dict('records'), patients, mrn_map, dabackid_map)
            output_rows.extend([r for r in batch_results if r is not None])
            
            # Progress update
            processed = len(output_rows)
            update_progress(processed)
            print(f"[PROGRESS] Processed {processed}/{total_rows} rows ({processed/total_rows*100:.1f}%)")

    # Stop performance monitoring
    stop_monitoring()
    
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

def main():
    """Main function with fallback to async processing."""
    try:
        # Try async processing first
        asyncio.run(main_async())
    except Exception as e:
        print(f"[WARNING] Async processing failed, falling back to synchronous: {e}")
        # Fallback to original synchronous processing
        main_sync()

def main_sync():
    """Original synchronous main function as fallback."""
    # Get input and output file names from command line arguments
    excel_file = "doctoralliance_combined_output.xlsx"  # default
    output_file = "supreme_excel.xlsx"  # default
    
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    if not os.path.exists(excel_file):
        print(f"Input file {excel_file} not found!")
        return
    
    # Set the PATIENT_API for the company using config
    set_patient_api_for_company()
    
    df = pd.read_excel(excel_file)
    print(f"[INFO] Loaded {len(df)} rows from {excel_file}")
    
    # Start performance monitoring
    start_monitoring(len(df))
    
    patients = get_all_patients()
    print(f"[INFO] Fetched {len(patients)} patients from API.")
    
    # Create lookup maps for faster patient matching
    mrn_map, dabackid_map = create_patient_lookup_maps(patients)
    
    output_rows = []
    for i, row in df.iterrows():
        doc_id = clean_doc_id(row.get("Document ID"))
        if not doc_id:
            print(f"[{i+1}] Skipped: No Document ID")
            continue
        print(f"\n[{i+1}] Processing Document ID: {doc_id}")
        
        # Get API info
        doc_api = get_order_doc_api(doc_id)
        
        # Process row data
        result = process_row_data(row, doc_api, patients, mrn_map, dabackid_map)
        if result:
            output_rows.append(result)
        
        # Update progress
        update_progress(len(output_rows))

    # Stop performance monitoring
    stop_monitoring()
    
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

if __name__ == "__main__":
    main()
