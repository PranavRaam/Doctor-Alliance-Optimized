import pandas as pd
import requests
import datetime
import json
import re
import math

PATIENT_CREATE_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/create"
PATIENT_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/company/pg/bc3a6a28-dd03-4cf3-95ba-2c5976619818"
ORDER_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Order"
HEADERS = {'accept': '*/*', 'Content-Type': 'application/json'}

def clean_id(val):
    if pd.isna(val) or val is None:
        return ""
    if isinstance(val, float) and val.is_integer():
        return str(int(val))
    s = str(val)
    if s.endswith('.0'):
        return s[:-2]
    return s

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

def build_patient_payload(row):
    required = ['patientName', 'dob', 'mrn', 'soc', 'cert_period_soe', 'cert_period_eoe', 'Diagnosis 1', 'companyId', 'Pgcompanyid','patient_sex']
    remarks = []
    for r in required:
        if not row.get(r):
            remarks.append(f"{r} absent")
    fname, mname, lname = split_name(row.get("patientName", ""))
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
        "medicalRecordNo": clean_id(row.get("mrn", "")),
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
        "pgcompanyID": clean_id(row.get("Pgcompanyid", "")),
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

def create_patient(row):
    payload, remarks = build_patient_payload(row)
    payload = clean_payload_for_json(payload)
    print("\n--- [PATIENT_CREATE] Request Payload ---")
    print(json.dumps(payload, indent=2, default=str))
    try:
        resp = requests.post(PATIENT_CREATE_API, headers=HEADERS, json=payload, timeout=20)
        print("--- [PATIENT_CREATE] Response ---")
        print(f"Status: {resp.status_code}\n{resp.text}\n")
        success = resp.status_code in (200, 201) and (isinstance(resp.json(), dict) and resp.json().get("id"))

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
                df.at[i, 'Pgcompanyid'] = clean_id(agency.get("pgcompanyID", ""))
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


def build_order_payload(row):
    # Enhanced handling of orderdate and sendDate
    orderdate = row.get("orderdate")
    sendDate = row.get("sendDate")
    
    # Handle various empty value cases
    if pd.isna(orderdate) or orderdate is None or str(orderdate).strip() == "":
        orderdate = None
    else:
        orderdate = str(orderdate).strip()
    
    if pd.isna(sendDate) or sendDate is None or str(sendDate).strip() == "":
        sendDate = None
    else:
        sendDate = str(sendDate).strip()
    
    # Use sendDate as fallback when orderdate is empty
    final_order_date = orderdate or sendDate or ""
    
    return {
        "orderNo": row.get("orderno", ""),
        "orderDate": final_order_date,

        "startOfCare": row.get("soc", ""),
        "episodeStartDate": row.get("cert_period_soe", ""),
        "episodeEndDate": row.get("cert_period_eoe", ""),
        "documentID": clean_id(row.get("Document ID", "")),
        "mrn": clean_id(row.get("mrn", "")),
        "patientName": row.get("patientName", ""),
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
        "pgCompanyId": clean_id(row.get("Pgcompanyid", "")),
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

def create_order(row):
    payload = build_order_payload(row)
    payload = clean_payload_for_json(payload)
    print("\n--- [ORDER_CREATE] Request Payload ---")
    print(json.dumps(payload, indent=2, default=str))
    try:
        resp = requests.post(ORDER_API, headers=HEADERS, json=payload, timeout=20)
        print("--- [ORDER_CREATE] Response ---")
        print(f"Status: {resp.status_code}\n{resp.text}\n")
        try:
            resp_json = resp.json()
        except Exception:
            resp_json = {}
        success = resp.status_code in (200, 201) and (isinstance(resp_json, dict) and 'orderNo' in resp_json)
        if not success:
            # Simplify error extraction
            if isinstance(resp_json, dict) and 'errors' in resp_json:
                error_details = json.dumps(resp_json['errors'])
            else:
                error_details = resp.text
            return False, error_details
        return True, ""
    except Exception as e:
        print(f"  [ORDER_CREATE] Error for {row.get('Document ID', '')}: {e}")
        return False, f"Exception: {e}"

def main():
    df = pd.read_excel("supreme_excel.xlsx")
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
            success, remarks = create_patient(row)
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
    df.to_excel("supreme_excel_with_patient_upload.xlsx", index=False)

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
            success, remarks = create_patient(row)
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
    df.to_excel("supreme_excel_with_patient_upload.xlsx", index=False)

    # 3. Upload orders for PatientExist==TRUE
    df['ORDERUPLOAD_STATUS'] = ""
    df['ORDER_CREATION_REMARK'] = ""  # Ensure exists
    for idx, row in df.iterrows():
        if row.get('PatientExist', False):
            try:
                order_success, order_remark = create_order(row)  # Now expects a tuple!
                df.at[idx, 'ORDERUPLOAD_STATUS'] = "TRUE" if order_success else "FALSE"
                df.at[idx, 'ORDER_CREATION_REMARK'] = order_remark
            except Exception as e:
                df.at[idx, 'ORDERUPLOAD_STATUS'] = "FALSE"
                df.at[idx, 'ORDER_CREATION_REMARK'] = f"Exception: {e}"
        else:
            df.at[idx, 'ORDERUPLOAD_STATUS'] = "SKIPPED"
            df.at[idx, 'ORDER_CREATION_REMARK'] = "Order skipped: Patient does not exist for this row."

    df.to_excel("supreme_excel_with_patient_and_order_upload.xlsx", index=False)
    print("Upload process complete. Check supreme_excel_with_patient_and_order_upload.xlsx")



if __name__ == "__main__":
    main()
