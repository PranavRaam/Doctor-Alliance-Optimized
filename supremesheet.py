import pandas as pd
import requests
from datetime import datetime
import os
import re
import sys
from fuzzywuzzy import fuzz
import openai

API_BASE = "https://api.doctoralliance.com/document/getfile?docId.id="
AUTH_HEADER = {
    "Accept": "application/json",
    "Authorization": "Bearer BwmWBqhXAEvG70Irt_1J8kJM8_4p81dStSUAeWXFho6d-Fu2Ymsox3qFLaQgZcX_EA-JjYi_MpiDS5FzulJ6hw2Qne5DearMdRfkkS_E8GaG5fy82RI_YhwM1cn-VtTQG5FSAjUPukOuJri8lPjQUZS1vzh9bRd3f3FQQlJxwzMKDfrqkt_03SR70bjDsKA9KYdJibMr5DBpaUkyJNzATdlewBwkeGEnX4EfzRj_mn_gm_G7Pjdo2qCCXbDhGeuH5lLuKvqFciQy_Wb8TEOR7Q"
}

# PATIENT_API will be set dynamically based on company configuration
PATIENT_API = None
ENTITY_API = "https://dawaventity-g5a6apetdkambpcu.eastus-01.azurewebsites.net/api/Entity?EntityType=ANCILLIARY"

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
    url = f"{API_BASE}{doc_id}"
    try:
        r = requests.get(url, headers=AUTH_HEADER, timeout=20)
        data = r.json()
        if not data.get("isSuccess"):
            print(f"  [DOC_API] Failed for doc_id={doc_id}. isSuccess={data.get('isSuccess')}. Raw: {data}")
            return {}
        value = data.get("value", {})
        print(f"  [DOC_API] Success for doc_id={doc_id}. Type: {value.get('documentType', '')}, PatientName: {value.get('patientName', '')}")
        return value
    except Exception as e:
        print(f"  [DOC_API] Exception for doc_id={doc_id}: {e}")
        return {}

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

def get_companyid_by_careprovider_name(care_provider_name):
    if not care_provider_name:
        return ""
    try:
        r = requests.get(ENTITY_API, timeout=30)
        entities = r.json()
        if isinstance(entities, dict) and "value" in entities:
            entities = entities["value"]
        best_score = 0
        best_id = ""
        for entity in entities:
            entity_name = entity.get("name", "").strip().lower()
            input_name = care_provider_name.strip().lower()
            score = fuzz.token_set_ratio(entity_name, input_name)
            if score > best_score:
                best_score = score
                best_id = entity.get('id', "")
            if score >= 90:
                print(f"  [COMPANYID] 90%+ match for '{care_provider_name}' <-> '{entity.get('name', '')}' (score {score}): {entity.get('id')}")
                return entity.get("id", "")
        if best_score >= 80:
            print(f"  [COMPANYID] Best fuzzy match ({best_score}%) for '{care_provider_name}': {best_id}")
            return best_id
        print(f"  [COMPANYID] No 90% match for careProvider='{care_provider_name}'")
    except Exception as e:
        print(f"  [COMPANYID] Error: {e}")
    return ""

def match_patient(row, patients):
    mrn = str(row.get("mrn", "")).strip()
    dabackid = str(row.get("DABackOfficeID", "")).strip() if "DABackOfficeID" in row else None
    for pat in patients:
        agency = pat.get("agencyInfo", {})
        if agency.get("medicalRecordNo", "").strip() and mrn and agency["medicalRecordNo"].strip().upper() == mrn.upper():
            print(f"  [PAT_MATCH] MRN matched for {mrn}")
            return pat, True
        if agency.get("daBackofficeID", "") and dabackid and str(agency["daBackofficeID"]).strip() == dabackid:
            print(f"  [PAT_MATCH] DABackOfficeID matched for {dabackid}")
            return pat, True
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

def guess_gender_with_gpt(name):
    if not name:
        return ""
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
            return ans
    except Exception as e:
        print(f"[GENDER_GPT] Error guessing gender: {e}")
    # Default/fallback if GPT fails (optional: choose your own default)
    return "FEMALE"


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

def main():
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
    patients = get_all_patients()
    print(f"[INFO] Fetched {len(patients)} patients from API.")
    output_rows = []
    for i, row in df.iterrows():
        doc_id = clean_doc_id(row.get("Document ID"))
        if not doc_id:
            print(f"[{i+1}] Skipped: No Document ID")
            continue
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

        # 2. Get API info
        doc_api = get_order_doc_api(doc_id)
        documentType = doc_api.get("documentType", "")
        physicianSigndate = try_date(doc_api.get("physicianSigndate"))
        dabackofficeid = str(doc_api.get("patientId", {}).get("id", ""))  # numeric id!
        patient_name = row.get("patientName") or doc_api.get("patientName", "")
        sendDate = try_date(doc_api.get("sendDate"))
        care_provider_name = doc_api.get("careProvider", "")
        
        # Use sendDate as fallback when orderdate is empty
        if not orderdate or str(orderdate).strip() == "" or str(orderdate).lower() in ["nan", "none"]:
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
            "patient_sex": patient_sex
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

        output_rows.append(out_row)

    out_df = pd.DataFrame(output_rows)
    out_df.to_excel(output_file, index=False)
    print(f"\n✅ Supreme Excel written: {output_file}")

if __name__ == "__main__":
    main()
