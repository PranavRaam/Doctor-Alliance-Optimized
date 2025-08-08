import pandas as pd
import requests
try:
    import fitz  # PyMuPDF for PDF text extraction
except Exception:
    fitz = None
try:
    from selenium_patient_search import download_latest_485_for_patient
except Exception:
    download_latest_485_for_patient = None
import datetime
import json
import re
import math
import base64
import tempfile
import os
import sys
import time
from typing import Tuple, Optional

# These will be set dynamically based on company configuration
PATIENT_CREATE_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/create"
PATIENT_API = None  # Will be set dynamically
ORDER_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Order"
ORDER_PDF_UPLOAD_API = "https://dawavadmin-djb0f9atf8e6cwgx.eastus-01.azurewebsites.net/api/OrderPdfUpload/upload"
HEADERS = {'accept': '*/*', 'Content-Type': 'application/json'}

# Global cache for company ID lookups
COMPANY_ID_CACHE = {}
COMPANY_IDS_CSV_DATA = None

# Ultra-verbose debugging toggle
DEBUG_VERBOSE = True

def debug_log(tag: str, message: str):
    if DEBUG_VERBOSE:
        try:
            print(f"[{tag}] {message}")
        except Exception:
            pass


# ============================
# Logging: duplicate stdout/err to file
# ============================
class _TeeIO:
    def __init__(self, stream, logfile_handle):
        self._stream = stream
        self._log = logfile_handle

    def write(self, data):
        try:
            self._stream.write(data)
        except Exception:
            # Best-effort console write
            pass
        try:
            self._log.write(data)
        except Exception:
            pass

    def flush(self):
        try:
            self._stream.flush()
        except Exception:
            pass
        try:
            self._log.flush()
        except Exception:
            pass


def setup_run_logging(company_key: Optional[str]) -> str:
    """Create logs directory and tee stdout/stderr to a timestamped log file.

    Returns the path to the log file.
    """
    os.makedirs("logs", exist_ok=True)
    ts = time.strftime("%Y-%m-%d_%H-%M-%S")
    suffix = company_key or "default"
    logfile = os.path.join("logs", f"Upload_Patients_Orders_{suffix}_{ts}.log")
    # Open in text mode, utf-8
    fh = open(logfile, "w", encoding="utf-8", buffering=1)
    # Tee
    sys.stdout = _TeeIO(sys.__stdout__, fh)
    sys.stderr = _TeeIO(sys.__stderr__, fh)
    print(f"[LOG] Duplicating console output to {logfile}")
    return logfile

def load_company_ids_csv():
    """Load company IDs from CSV file for fallback lookup."""
    global COMPANY_IDS_CSV_DATA
    
    if COMPANY_IDS_CSV_DATA is not None:
        return COMPANY_IDS_CSV_DATA
    
    try:
        if os.path.exists('Company IDs.csv'):
            df = pd.read_csv('Company IDs.csv')
            # Create a mapping from company name to ID
            company_mapping = {}
            for _, row in df.iterrows():
                company_name = str(row['Name']).strip()
                company_id = str(row['ID']).strip()
                if company_name and company_id:
                    # Ensure company ID is in proper UUID format with hyphens
                    formatted_id = format_uuid_for_csv(company_id)
                    if formatted_id:
                        company_mapping[company_name.lower()] = formatted_id
            COMPANY_IDS_CSV_DATA = company_mapping
            print(f"✅ Loaded {len(company_mapping)} company IDs from CSV")
            return company_mapping
        else:
            print("⚠️  Company IDs.csv not found")
            COMPANY_IDS_CSV_DATA = {}
            return {}
    except Exception as e:
        print(f"❌ Error loading Company IDs.csv: {e}")
        COMPANY_IDS_CSV_DATA = {}
        return {}

def format_uuid_for_csv(uuid_str):
    """Format UUID string to ensure proper hyphenated format for CSV data."""
    if pd.isna(uuid_str):
        return None
    
    uuid_str = str(uuid_str).strip()
    
    # If already has hyphens, validate format
    if '-' in uuid_str:
        # Remove any extra spaces or characters, keep hyphens
        cleaned = re.sub(r'[^A-Za-z0-9-]', '', uuid_str)
        # Ensure proper UUID format (8-4-4-4-12)
        parts = cleaned.split('-')
        if len(parts) == 5:
            return f"{parts[0]}-{parts[1]}-{parts[2]}-{parts[3]}-{parts[4]}"
        else:
            # If wrong number of parts, try to fix
            cleaned = cleaned.replace('-', '')
            if len(cleaned) == 32:
                return f"{cleaned[:8]}-{cleaned[8:12]}-{cleaned[12:16]}-{cleaned[16:20]}-{cleaned[20:]}"
    
    # If no hyphens, add them
    uuid_str = re.sub(r'[^A-Za-z0-9]', '', uuid_str)
    if len(uuid_str) == 32:
        return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
    
    return None

def get_entity_api_url():
    """Get the entity API URL for company lookup."""
    from config import API_BASE
    return API_BASE.replace('/document/getfile?docId.id=', '/entity/')

def lookup_company_id_hybrid(company_name, pg_company_id=None):
    """
    Hybrid company ID lookup:
    1. First try entity API with company name
    2. If not found, try entity API with PG company ID
    3. If still not found, try CSV lookup
    """
    global COMPANY_ID_CACHE
    
    # Check cache first
    cache_key = f"{company_name}_{pg_company_id}"
    if cache_key in COMPANY_ID_CACHE:
        return COMPANY_ID_CACHE[cache_key]
    
    # For PG (Practice Group) companies, use the pg_company_id as the company_id
    if company_name == "Chickasaw Nation Medical Center" and pg_company_id == "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7":
        print(f"  📋 PG Company detected: Chickasaw Nation Medical Center")
        print(f"  ✅ Using PG Company ID as Company ID: {pg_company_id}")
        COMPANY_ID_CACHE[cache_key] = pg_company_id
        return pg_company_id
    elif company_name == "Southeast Oklahoma Medical Clinic" and pg_company_id == "108bbba4-5d5d-41d9-b1c6-0eaac5538f6c":
        print(f"  📋 PG Company detected: Southeast Oklahoma Medical Clinic")
        print(f"  ✅ Using PG Company ID as Company ID: {pg_company_id}")
        COMPANY_ID_CACHE[cache_key] = pg_company_id
        return pg_company_id
    elif company_name == "Triton Health PLLC" and pg_company_id == "d09df8cc-a549-4229-a03a-ce29fb09aea2":
        print(f"  📋 PG Company detected: Triton Health PLLC")
        print(f"  ✅ Using PG Company ID as Company ID: {pg_company_id}")
        COMPANY_ID_CACHE[cache_key] = pg_company_id
        return pg_company_id
    
    company_id = None
    
    # Step 1: Try entity API with company name
    if company_name:
        company_id = lookup_company_id_via_entity_api(company_name)
        if company_id:
            print(f"  ✅ Found company ID via entity API (name): {company_id}")
            COMPANY_ID_CACHE[cache_key] = company_id
            return company_id
    
    # Step 2: Try entity API with PG company ID
    if pg_company_id and not company_id:
        company_id = lookup_company_id_via_entity_api(pg_company_id)
        if company_id:
            print(f"  ✅ Found company ID via entity API (PG ID): {company_id}")
            COMPANY_ID_CACHE[cache_key] = company_id
            return company_id
    
    # Step 3: Try CSV lookup
    if not company_id:
        company_id = lookup_company_id_via_csv(company_name)
        if company_id:
            print(f"  ✅ Found company ID via CSV lookup: {company_id}")
            COMPANY_ID_CACHE[cache_key] = company_id
            return company_id
    
    if not company_id:
        print(f"  ❌ Company ID not found for: {company_name} (PG ID: {pg_company_id})")
        COMPANY_ID_CACHE[cache_key] = None
        return None
    
    return company_id

def lookup_company_id_via_entity_api(search_term):
    """Lookup company ID via entity API using the existing supremesheet approach."""
    try:
        from supremesheet import ENTITY_API, AUTH_HEADER, get_companyid_by_careprovider_name
        
        print(f"  🔍 Searching entity API for: {search_term}")
        
        # Use the existing function from supremesheet
        company_id = get_companyid_by_careprovider_name(search_term)
        
        if company_id:
            print(f"  ✅ Found company ID via entity API: {company_id}")
            return company_id
        else:
            print(f"  ⚠️  No company found in entity API for: {search_term}")
            return None
        
    except Exception as e:
        print(f"  ❌ Entity API lookup error: {e}")
        return None

def lookup_company_id_via_csv(company_name):
    """Lookup company ID via CSV file."""
    if not company_name:
        return None
    
    company_mapping = load_company_ids_csv()
    if not company_mapping:
        return None
    
    # Try exact match first
    company_name_lower = company_name.lower().strip()
    if company_name_lower in company_mapping:
        company_id = company_mapping[company_name_lower]
        print(f"  📋 Exact match found: '{company_name}' -> {company_id}")
        return company_id
    
    # Try partial matches with better logic
    best_match = None
    best_score = 0
    
    for csv_name, csv_id in company_mapping.items():
        # Check if any word from company_name appears in csv_name
        company_words = company_name_lower.split()
        csv_words = csv_name.split()
        
        # Count matching words
        matches = 0
        for word in company_words:
            if len(word) > 2:  # Only consider words longer than 2 characters
                for csv_word in csv_words:
                    if word in csv_word or csv_word in word:
                        matches += 1
                        break
        
        # Calculate match score
        if matches > 0:
            score = matches / max(len(company_words), len(csv_words))
            if score > best_score and score > 0.3:  # Minimum 30% match
                best_score = score
                best_match = (csv_name, csv_id)
    
    if best_match:
        csv_name, csv_id = best_match
        print(f"  📋 Partial match found: '{company_name}' -> '{csv_name}' (score: {best_score:.2f}) -> {csv_id}")
        return csv_id
    
    return None


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


def extract_mrn_from_text(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    patterns = [
        r"\bMedical\s*Record\s*No\.?\s*[:#-]?\s*([A-Za-z0-9-]{4,})",
        r"\bMedical\s*Record\s*Number\s*[:#-]?\s*([A-Za-z0-9-]{4,})",
        r"\bMRN\s*[:#-]?\s*([A-Za-z0-9-]{4,})",
        r"\bMR\s*#\s*([A-Za-z0-9-]{4,})",
        r"\bMed(?:ical)?\s*Rec(?:ord)?\s*(?:No\.|#)?\s*[:#-]?\s*([A-Za-z0-9-]{4,})",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            cleaned = clean_mrn_for_upload(candidate)
            if cleaned:
                return cleaned
    return ""


def extract_mrn_from_pdf(pdf_path: str) -> str:
    if not pdf_path or not os.path.exists(pdf_path) or fitz is None:
        return ""
    try:
        doc = fitz.open(pdf_path)
        texts = []
        for page in doc:
            try:
                texts.append(page.get_text("text"))
            except Exception:
                continue
        doc.close()
        return extract_mrn_from_text("\n".join(texts))
    except Exception:
        return ""


def fetch_mrn_from_latest_485(patient_full_name: str) -> Tuple[str, Optional[str]]:
    """Download latest 485 for patient and extract MRN. Returns (mrn, pdf_path)."""
    if download_latest_485_for_patient is None:
        return "", None
    # Split into last, first
    last_name, first_name = "", ""
    if "," in (patient_full_name or ""):
        parts = [p.strip() for p in patient_full_name.split(",", 1)]
        last_name, rest = parts[0], parts[1] if len(parts) > 1 else ""
        first_name = rest.split()[0] if rest else ""
    else:
        parts = (patient_full_name or "").split()
        if len(parts) >= 2:
            first_name, last_name = parts[0], parts[-1]
        elif len(parts) == 1:
            first_name = parts[0]
    try:
        ok, doc_id, pdf_path = download_latest_485_for_patient(
            da_url="https://backoffice.doctoralliance.com",
            da_login="rpabot",
            da_password="Dallas@1234",
            last_name=last_name,
            first_name=first_name,
            headless=True,
            save_dir="Downloads_485",
        )
        if ok and pdf_path:
            mrn = extract_mrn_from_pdf(pdf_path)
            return mrn, pdf_path
    except Exception:
        pass
    return "", None


def clean_id(val):
    """Basic ID cleaning for non-UUID fields."""
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
    
    # If no hyphens but it's a 32-character string, add hyphens to make it a UUID
    if len(cleaned) == 32 and cleaned.replace('-', '').isalnum():
        return f"{cleaned[:8]}-{cleaned[8:12]}-{cleaned[12:16]}-{cleaned[16:20]}-{cleaned[20:]}"
    
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
    # Handle NaN and None values
    if pd.isna(full_name) or full_name is None:
        return "", "", ""
    
    # Convert to string if not already
    if not isinstance(full_name, str):
        full_name = str(full_name)
    
    # Clean the name
    full_name = full_name.strip()
    if not full_name or full_name.lower() == 'nan':
        return "", "", ""
    
    parts = full_name.split()
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


def get_existing_document_ids_for_company(company_key=None):
    """Fetch set of existing Document IDs already present on the platform for a company."""
    try:
        from config import get_company_api_url
        if company_key is None:
            from config import ACTIVE_COMPANY
            company_key = ACTIVE_COMPANY
        if not company_key:
            return set()
        url = get_company_api_url(company_key)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        orders = resp.json()
        return set(str(o["documentID"]).strip() for o in orders if isinstance(o, dict) and "documentID" in o and o["documentID"] is not None)
    except Exception as e:
        print(f"[EXISTING_DOCS] Error fetching existing Document IDs: {e}")
        return set()


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
    debug_log("PATIENT_PAYLOAD", f"Row keys: {list(row.keys())}")
    debug_log("PATIENT_PAYLOAD", f"Raw row snippet: DABackOfficeID={row.get('DABackOfficeID')} docId={row.get('Document ID') or row.get('docId')} docType={row.get('documentType')} patientName={row.get('patientName') or row.get('patient_name')}")
    for r in required:
        if not row.get(r):
            remarks.append(f"{r} absent")
    
    # Clean MRN specifically
    cleaned_mrn = clean_mrn_for_upload(row.get("mrn", ""))
    debug_log("PATIENT_PAYLOAD", f"MRN cleaned -> '{cleaned_mrn}' from '{row.get('mrn','')}'")
    if not cleaned_mrn:
        # MRN fallback via latest 485 PDF using our OpenAI/Ollama extractor
        try:
            from field_extraction import AccuracyFocusedFieldExtractor
            from text_extraction import extract_text_from_pdf
        except Exception:
            AccuracyFocusedFieldExtractor = None
            extract_text_from_pdf = None

        patient_name_for_fallback = row.get("patientName", "") or row.get("patient_name", "")
        if patient_name_for_fallback and AccuracyFocusedFieldExtractor and extract_text_from_pdf:
            # Download latest 485
            mrn_candidate = ""
            ok_pdf, pdf_path = False, None
            try:
                ok, doc_id, path = fetch_mrn_from_latest_485(patient_name_for_fallback)
                # fetch_mrn_from_latest_485 returns (mrn, pdf_path). If it found MRN already, use that.
                if ok:
                    # ok here is MRN string; adjust call site
                    pass
            except Exception:
                pass
            # If previous helper returned MRN directly, use it
            if isinstance(ok, str) and ok and ok.strip():
                mrn_candidate = ok.strip()
                pdf_path = path
            else:
                # If not, try to ensure we have a PDF via selenium helper
                if download_latest_485_for_patient and not pdf_path:
                    try:
                        last, first = "", ""
                        if "," in patient_name_for_fallback:
                            parts = [p.strip() for p in patient_name_for_fallback.split(",", 1)]
                            last, rest = parts[0], parts[1] if len(parts) > 1 else ""
                            first = rest.split()[0] if rest else ""
                        else:
                            parts = patient_name_for_fallback.split()
                            if len(parts) >= 2:
                                first, last = parts[0], parts[-1]
                        ok_pdf, _, pdf_path = download_latest_485_for_patient(
                            da_url="https://backoffice.doctoralliance.com",
                            da_login="rpabot",
                            da_password="Dallas@1234",
                            last_name=last,
                            first_name=first,
                            headless=True,
                            save_dir="Downloads_485",
                        )
                    except Exception:
                        ok_pdf = False
                # Extract text and call LLM extractor strictly for MRN
                if pdf_path and extract_text_from_pdf and AccuracyFocusedFieldExtractor:
                    try:
                        text = extract_text_from_pdf(pdf_path)
                        extractor = AccuracyFocusedFieldExtractor()
                        result = extractor.extract_fields_multi_approach(text, doc_id="MRN_ONLY")
                        fields = result.fields if hasattr(result, 'fields') else result
                        mrn_candidate = fields.get("mrn") if isinstance(fields, dict) else None
                    except Exception:
                        mrn_candidate = ""

            cleaned_from_llm = clean_mrn_for_upload(mrn_candidate)
            if cleaned_from_llm:
                cleaned_mrn = cleaned_from_llm
                print(f"  🤖 MRN filled from 485 via LLM extraction ({pdf_path}): {cleaned_mrn}")
    if not cleaned_mrn:
        remarks.append("MRN invalid (must be >3 chars, alphanumeric with at least one digit)")
    
    # Handle both patientName and patient_name fields
    patient_name = row.get("patientName", "") or row.get("patient_name", "")
    
    # If patient name is empty, try other possible columns
    if not patient_name or (isinstance(patient_name, str) and patient_name.strip() == "") or pd.isna(patient_name):
        name_columns = ['patient_name', 'patientName', 'name', 'full_name', 'patient_full_name']
        for col in name_columns:
            if col in row and row[col] and not pd.isna(row[col]):
                patient_name = str(row[col]).strip()
                if patient_name and patient_name.lower() != 'nan':
                    break
    
    fname, mname, lname = split_name(patient_name)
    debug_log("PATIENT_PAYLOAD", f"Name split -> fname='{fname}' mname='{mname}' lname='{lname}'")
    age = get_age(row.get("dob"))
    debug_log("PATIENT_PAYLOAD", f"DOB='{row.get('dob','')}' -> age='{age}'")
    state, city, zipc = parse_address(row.get("address", ""))
    debug_log("PATIENT_PAYLOAD", f"Address parsed -> state='{state}' city='{city}' zip='{zipc}'")
    
    # Hybrid company ID lookup
    excel_company_id = clean_uuid(row.get("companyId", ""))
    excel_pg_company_id = clean_uuid(row.get("Pgcompanyid", ""))
    debug_log("PATIENT_PAYLOAD", f"Excel IDs -> companyId='{excel_company_id}' pgCompanyId='{excel_pg_company_id}'")
    
    # Try to get company name from various possible columns
    company_name = None
    name_columns = ['agency name', 'company_name', 'agencyName', 'companyName', 'nameOfAgency', 'agency', 'company']
    for col in name_columns:
        if col in row and row[col]:
            company_name = str(row[col]).strip()
            if company_name and company_name.lower() != 'nan':
                break
    
    # If still no company name, try to get it from the PG company ID using config
    if not company_name or company_name.lower() == 'nan':
        try:
            from config import COMPANIES
            # Try to find company by PG ID
            for company_key, company_config in COMPANIES.items():
                if company_config.get('pg_company_id') == authoritative_pg_id:
                    company_name = company_config.get('name', '')
                    print(f"  📋 Found company name from config: {company_name}")
                    break
        except Exception as e:
            print(f"  ⚠️  Error getting company name from config: {e}")
    
    # If still no company name, use a default based on PG ID
    if not company_name or company_name.lower() == 'nan':
        if authoritative_pg_id == "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7":
            company_name = "Chickasaw Nation Medical Center"
        elif authoritative_pg_id == "108bbba4-5d5d-41d9-b1c6-0eaac5538f6c":
            company_name = "Southeast Oklahoma Medical Clinic"
        elif authoritative_pg_id == "d09df8cc-a549-4229-a03a-ce29fb09aea2":
            company_name = "Triton Health PLLC"
        else:
            company_name = f"Company_{authoritative_pg_id[:8]}"
        print(f"  📋 Using default company name: {company_name}")
    
    # Let the hybrid lookup handle company ID resolution
    final_company_id = None
    
    # Use hybrid lookup to find the correct company ID
    if not final_company_id:  # Only if not already set by known companies
        if excel_company_id:
            # If we have a company ID from Excel, try to validate/enhance it
            final_company_id = excel_company_id
            print(f"  📋 Using Excel company ID: {final_company_id}")
        else:
            # Try hybrid lookup
            final_company_id = lookup_company_id_hybrid(company_name, excel_pg_company_id or authoritative_pg_id)
            if final_company_id:
                print(f"  ✅ Found company ID via hybrid lookup: {final_company_id}")
            else:
                print(f"  ⚠️  Could not find company ID for: {company_name}")
                remarks.append(f"Company ID not found for: {company_name}")
    
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
        "companyId": final_company_id or "",
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
    debug_log("PATIENT_PAYLOAD", "Omitting createdOn/updatedOn and nameOfAgency per backend rules")
    debug_log("PATIENT_PAYLOAD", f"Final payload keys: {list(payload.keys())}")
    debug_log("PATIENT_PAYLOAD", f"Episode -> SOC='{payload['episodeDiagnoses'][0]['startOfCare']}' SOE='{payload['episodeDiagnoses'][0]['startOfEpisode']}' EOE='{payload['episodeDiagnoses'][0]['endOfEpisode']}' Dx1='{payload['episodeDiagnoses'][0]['firstDiagnosis']}'")
    
    return payload, remarks


def create_patient(row, company_key=None):
    payload, remarks = build_patient_payload(row, company_key)
    payload = clean_payload_for_json(payload)
    # Human-friendly summary of what is being sent
    try:
        epi = (payload.get("episodeDiagnoses") or [{}])[0]
        print("[PATIENT_SUMMARY]",
              f"Name={payload.get('patientFName','')} {payload.get('patientLName','')}",
              f"DOB={payload.get('dob','')}",
              f"MRN={payload.get('medicalRecordNo','')}",
              f"Sex={payload.get('patientSex','')}",
              f"SOC={payload.get('startOfCare','')}",
              f"SOE={epi.get('startOfEpisode','')}",
              f"EOE={epi.get('endOfEpisode','')}",
              f"Dx1={epi.get('firstDiagnosis','')}",
              f"CompanyId={payload.get('companyId','')}",
              f"PG={payload.get('pgcompanyID','')}")
        if remarks:
            print(f"[PATIENT_SUMMARY] Pre-check remarks: {'; '.join(remarks)}")
    except Exception:
        pass
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
        if success:
            created_id = resp_json.get("id") if isinstance(resp_json, dict) else None
            print(f"[PATIENT_CREATE] ✅ Success id={created_id} for DABackOfficeID={row.get('DABackOfficeID','')} DocID={row.get('Document ID') or row.get('docId')}")
        else:
            # Extract detailed error if available
            detailed = None
            if isinstance(resp_json, dict):
                detailed = (
                    resp_json.get("errors")
                    or resp_json.get("message")
                    or resp_json.get("title")
                )
            detail_text = json.dumps(detailed) if detailed else resp.text
            remarks.append(f"Patient API failure: HTTP {resp.status_code} - {detail_text}")
            print(f"[PATIENT_CREATE] ❌ Failure: HTTP {resp.status_code} - {detail_text}")
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
                patient_id = p.get("id", "")
                # Ensure patientid column can handle string values
                if 'patientid' in df.columns and df['patientid'].dtype == 'float64':
                    df['patientid'] = df['patientid'].astype('object')
                df.at[i, 'patientid'] = patient_id
                
                # Use hybrid lookup to get the best company ID
                agency_company_id = clean_uuid(agency.get("companyId", ""))
                agency_pg_company_id = clean_uuid(agency.get("pgcompanyID", ""))
                agency_name = agency.get("nameOfAgency", "")
                
                if agency_company_id:
                    df.at[i, 'companyId'] = agency_company_id
                    print(f"  ✅ Updated company ID from patient API: {agency_company_id}")
                else:
                    # Try hybrid lookup
                    hybrid_company_id = lookup_company_id_hybrid(agency_name, agency_pg_company_id)
                    if hybrid_company_id:
                        df.at[i, 'companyId'] = hybrid_company_id
                        print(f"  ✅ Updated company ID via hybrid lookup: {hybrid_company_id}")
                    else:
                        df.at[i, 'companyId'] = agency_company_id  # Keep original or empty
                
                df.at[i, 'Pgcompanyid'] = agency_pg_company_id
                found = True
                break
        if not found:
            # Try by patientName + dob
            excel_name = row.get("patientName", "")
            excel_dob = row.get("dob", "")
            patientid = search_patientid_by_name_dob(patients, excel_name, excel_dob)
            if patientid:
                df.at[i, 'PatientExist'] = True
                # Ensure patientid column can handle string values
                if 'patientid' in df.columns and df['patientid'].dtype == 'float64':
                    df['patientid'] = df['patientid'].astype('object')
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
    debug_log("ORDER_PAYLOAD", f"Row keys: {list(row.keys())}")
    debug_log("ORDER_PAYLOAD", f"Raw row snippet: DocID={row.get('Document ID') or row.get('docId')} docType={row.get('documentType')} PatientExist={row.get('PatientExist')} patientName={row.get('patientName') or row.get('patient_name')}")

    if patients:
        soc, soe, eoe = get_episode_data_from_patient(row, patients)
    else:
        soc, soe, eoe = row.get("soc", ""), row.get("cert_period_soe", ""), row.get("cert_period_eoe", "")
    debug_log("ORDER_PAYLOAD", f"Episode -> SOC='{soc}' SOE='{soe}' EOE='{eoe}'")
    
    # Handle both patientName and patient_name fields
    patient_name = row.get("patientName", "") or row.get("patient_name", "")
    
    # Hybrid company ID lookup for orders
    excel_company_id = clean_uuid(row.get("companyId", ""))
    excel_pg_company_id = clean_uuid(row.get("Pgcompanyid", ""))
    debug_log("ORDER_PAYLOAD", f"Excel IDs -> companyId='{excel_company_id}' pgCompanyId='{excel_pg_company_id}'")
    
    # Try to get company name from various possible columns
    company_name = None
    name_columns = ['agency name', 'company_name', 'agencyName', 'companyName', 'nameOfAgency', 'agency', 'company']
    for col in name_columns:
        if col in row and row[col]:
            company_name = str(row[col]).strip()
            if company_name and company_name.lower() != 'nan':
                break
    
    # If still no company name, try to get it from the PG company ID using config
    if not company_name or company_name.lower() == 'nan':
        try:
            from config import COMPANIES
            # Try to find company by PG ID
            for company_key, company_config in COMPANIES.items():
                if company_config.get('pg_company_id') == authoritative_pg_id:
                    company_name = company_config.get('name', '')
                    print(f"  📋 Found company name from config for order: {company_name}")
                    break
        except Exception as e:
            print(f"  ⚠️  Error getting company name from config for order: {e}")
    
    # If still no company name, use a default based on PG ID
    if not company_name or company_name.lower() == 'nan':
        if authoritative_pg_id == "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7":
            company_name = "Chickasaw Nation Medical Center"
        elif authoritative_pg_id == "108bbba4-5d5d-41d9-b1c6-0eaac5538f6c":
            company_name = "Southeast Oklahoma Medical Clinic"
        elif authoritative_pg_id == "d09df8cc-a549-4229-a03a-ce29fb09aea2":
            company_name = "Triton Health PLLC"
        else:
            company_name = f"Company_{authoritative_pg_id[:8]}"
        print(f"  📋 Using default company name for order: {company_name}")
    
    # Let the hybrid lookup handle company ID resolution
    final_company_id = None
    
    # Use hybrid lookup to find the correct company ID
    if not final_company_id:  # Only if not already set by known companies
        if excel_company_id:
            # If we have a company ID from Excel, use it
            final_company_id = excel_company_id
            print(f"  📋 Using Excel company ID for order: {final_company_id}")
        else:
            # Try hybrid lookup
            final_company_id = lookup_company_id_hybrid(company_name, excel_pg_company_id or authoritative_pg_id)
            if final_company_id:
                print(f"  ✅ Found company ID via hybrid lookup for order: {final_company_id}")
            else:
                print(f"  ⚠️  Could not find company ID for order: {company_name}")
    
    order_payload = {
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
        "patientId": clean_uuid(row.get("patientid", "")),
        "companyId": final_company_id or "",
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
    debug_log("ORDER_PAYLOAD", f"OrderNo='{order_payload['orderNo']}' OrderDate='{order_payload['orderDate']}' DocType='{order_payload['documentName']}' MRN='{order_payload['mrn']}' PatientId='{order_payload['patientId']}' CompanyId='{order_payload['companyId']}' PG='{order_payload['pgCompanyId']}'")
    return order_payload


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
        
        # Debug: Print the full response structure for the first few calls
        if doc_id in ['9431342', '9431476', '9433593']:  # Debug first few calls
            print(f"  [DOC_API_DEBUG] Full response for {doc_id}:")
            print(f"  [DOC_API_DEBUG] {json.dumps(data, indent=2)}")
            
            # Check for documentType structure
            if 'value' in data and 'documentType' in data['value']:
                doc_type = data['value']['documentType']
                print(f"  [DOC_API_DEBUG] documentType structure: {json.dumps(doc_type, indent=2)}")
        
        return data
    except Exception as e:
        print(f"  [DOC_API] Exception for doc_id={doc_id}: {e}")
        return None

def create_order(row, patients=None, company_key=None):
    payload = build_order_payload(row, patients, company_key)
    payload = clean_payload_for_json(payload)
    # Human-friendly summary of what is being sent
    try:
        print("[ORDER_SUMMARY]",
              f"DocID={payload.get('documentID','')}",
              f"OrderNo={payload.get('orderNo','')}",
              f"OrderDate={payload.get('orderDate','')}",
              f"PatientName={payload.get('patientName','')}",
              f"MRN={payload.get('mrn','')}",
              f"SOC={payload.get('startOfCare','')}",
              f"SOE={payload.get('episodeStartDate','')}",
              f"EOE={payload.get('episodeEndDate','')}",
              f"DocType={payload.get('documentName','')}",
              f"CompanyId={payload.get('companyId','')}",
              f"PG={payload.get('pgCompanyId','')}")
    except Exception:
        pass
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
            # Simplify error extraction with more context
            if isinstance(resp_json, dict):
                error_details = (
                    json.dumps(resp_json.get('errors'))
                    if resp_json.get('errors') is not None
                    else resp_json.get('message') or resp_json.get('title') or resp.text
                )
            else:
                error_details = resp.text

            # If backend reports duplicate/already exists, treat as success per business rule
            error_text = str(error_details or "")
            duplicate_markers = [
                "already exist",
                "already exists",
                "duplicate",
                "exists",
                "conflict",
            ]
            is_duplicate = (
                resp.status_code == 409
                or any(m in error_text.lower() for m in duplicate_markers)
            )
            if is_duplicate:
                print(f"[ORDER_CREATE] ⚠️ Duplicate detected (HTTP {resp.status_code}). Treating as success. Details: {error_text}")
                return True, "Order already exists on platform; treated as success"

            print(f"[ORDER_CREATE] ❌ Failure: HTTP {resp.status_code} - {error_details}")
            return False, f"Order API failure: HTTP {resp.status_code} - {error_details}"
            
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

    # Setup logging after parsing args so suffix is accurate
    log_path = setup_run_logging(company_key)
    print(f"   Log file: {log_path}")
    
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
    # Echo resolved company configuration for traceability
    try:
        from config import get_company_config
        cc = get_company_config(company_key) if company_key else get_company_config()
        print(f"[CONFIG] Active company name: {cc.get('name')}")
        print(f"[CONFIG] PG Company ID: {cc.get('pg_company_id')}")
        print(f"[CONFIG] Helper ID: {cc.get('helper_id')}")
    except Exception as e:
        print(f"[CONFIG] Warning: could not load company config details: {e}")
    
    df = pd.read_excel(input_file)
    if 'PATIENTUPLOAD_STATUS' not in df.columns:
        df['PATIENTUPLOAD_STATUS'] = ""
    if 'PATIENTUPLOAD_REMARKS' not in df.columns:
        df['PATIENTUPLOAD_REMARKS'] = ""
    if 'PATIENT_CREATION_REMARK' not in df.columns:
        df['PATIENT_CREATION_REMARK'] = ""
    if 'ORDER_CREATION_REMARK' not in df.columns:
        df['ORDER_CREATION_REMARK'] = ""

    # Filter out rows whose Document ID already exists on WAV (platform)
    try:
        existing_ids = get_existing_document_ids_for_company(company_key)
        if len(df) > 0 and existing_ids:
            doc_col = 'Document ID' if 'Document ID' in df.columns else ('docId' if 'docId' in df.columns else None)
            if doc_col:
                before = len(df)
                df = df[~df[doc_col].astype(str).isin(existing_ids)].copy()
                removed = before - len(df)
                print(f"[FILTER] Removed {removed} rows already on platform before upload.")
    except Exception as e:
        print(f"[FILTER] Error filtering existing Document IDs: {e}")

    created_patients = set()
    # 1. First pass: Create patients for 485CERT and 485RECERT where PatientExist==False
    for idx, row in df.iterrows():
        debug_log("PATIENT_PASS1", f"Row={idx} DocID={row.get('Document ID') or row.get('docId')} Name={row.get('patientName') or row.get('patient_name')} PatientExist={row.get('PatientExist')} DocType={row.get('documentType')} DABackOfficeID={row.get('DABackOfficeID')}")
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
            debug_log("PATIENT_PASS1", f"Row={idx} Skipped patient creation")

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
        debug_log("PATIENT_PASS2", f"Row={idx} DocID={row.get('Document ID') or row.get('docId')} Name={row.get('patientName') or row.get('patient_name')} PatientExist={row.get('PatientExist')} DocType={row.get('documentType')} DABackOfficeID={row.get('DABackOfficeID')}")
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
            debug_log("PATIENT_PASS2", f"Row={idx} Skipped patient creation")

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
        debug_log("ORDER", f"Row={idx} DocID={row.get('Document ID') or row.get('docId')} Name={row.get('patientName') or row.get('patient_name')} PatientExist={row.get('PatientExist')} DocType={row.get('documentType')}")
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
            print(f"[ORDER][Row {idx}] Skipped order creation (PatientExist=False)")

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
