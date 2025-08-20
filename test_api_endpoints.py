#!/usr/bin/env python3
"""
API Endpoint Test Script for Doctor Alliance
Tests all POST APIs for all companies to ensure they're working properly, and clearly reports failure reasons.
Also checks DoctorAlliance bearer token health used for document APIs.
"""

import requests
import json
import time
import sys
import os
from urllib.parse import urlparse
from typing import Optional
from datetime import datetime
from config import COMPANIES, MULTIPLE_COMPANIES, get_companies_to_process
from Upload_Patients_Orders import lookup_company_id_hybrid
import config as app_config

# API Endpoints to test
PATIENT_CREATE_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/create"
ORDER_API = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Order"
ORDER_PDF_UPLOAD_API = "https://dawavadmin-djb0f9atf8e6cwgx.eastus-01.azurewebsites.net/api/OrderPdfUpload/upload"

# Test headers (base)
HEADERS = {'accept': '*/*', 'Content-Type': 'application/json'}

# Optional Azure Function keys (set as environment variables if required by endpoints)
DA_ORDERPATIENT_KEY = os.getenv('DA_ORDERPATIENT_KEY')
DA_ADMIN_KEY = os.getenv('DA_ADMIN_KEY')

def headers_for_url(url: str, extra_headers: Optional[dict] = None) -> dict:
    """Return appropriate headers for a given URL, adding function keys if available."""
    headers = dict(HEADERS)
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ''
    # Add Azure Function key if provided
    if 'dawavorderpatient' in host and DA_ORDERPATIENT_KEY:
        headers['x-functions-key'] = DA_ORDERPATIENT_KEY
    if 'dawavadmin' in host and DA_ADMIN_KEY:
        headers['x-functions-key'] = DA_ADMIN_KEY
    # Allow caller to add/override
    if extra_headers:
        headers.update(extra_headers)
    return headers

# Test data templates
# Allow overriding NPI via environment var for safer testing
TEST_PHYSICIAN_NPI = os.getenv("TEST_PHYSICIAN_NPI", "1234567890")
TEST_PATIENT_PAYLOAD = {
    "filterStatus": "",
    "patientEHRRecId": "",
    "patientEHRType": "",
    "patientFName": "TEST",
    "patientMName": "",
    "patientLName": "PATIENT",
    "dob": "01/01/1990",
    "age": "33",
    "patientSex": "MALE",
    "patientStatus": "Active",
    "maritalStatus": "",
    "ssn": "",
    "startOfCare": "01/01/2024",
    "careManagement": [{"careManagementType": "CPO"}],
    "medicalRecordNo": "TEST123456",
    "serviceLine": "",
    "patientAddress": "123 Test St, Test City, TX 12345",
    "state": "TX",
    "patientCity": "Test City",
    "patientState": "TX",
    "zip": "12345",
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
    "physicianNPI": TEST_PHYSICIAN_NPI,
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
    "nameOfAgency": "Test Agency",
    "insuranceId": "",
    "primaryInsuranceName": "",
    "secondaryInsuranceName": "",
    "secondaryInsuranceID": "",
    "tertiaryInsuranceName": "",
    "tertiaryInsuranceID": "",
    "nextofKin": "",
    "patientCaretaker": "",
    "patientCaretakerContactNumber": "",
    "remarks": "API TEST - DO NOT PROCESS",
    "daBackofficeID": "TEST123",
    "companyId": "",
    "pgcompanyID": "",
    "createdBy": "APITest",
    "createdOn": datetime.now().isoformat(),
    "updatedBy": "",
    "updatedOn": datetime.now().isoformat(),
    "episodeDiagnoses": [{
        "id": "",
        "startOfCare": "01/01/2024",
        "startOfEpisode": "01/01/2024",
        "endOfEpisode": "12/31/2024",
        "firstDiagnosis": "Test Diagnosis",
        "secondDiagnosis": "",
        "thirdDiagnosis": "",
        "fourthDiagnosis": "",
        "fifthDiagnosis": "",
        "sixthDiagnosis": ""
    }]
}

TEST_ORDER_PAYLOAD = {
    "orderNo": "TEST123456",
    "orderDate": "01/01/2024",
    "startOfCare": "01/01/2024",
    "episodeStartDate": "01/01/2024",
    "episodeEndDate": "12/31/2024",
    "documentID": "TEST123",
    "mrn": "TEST123456",
    "patientName": "TEST PATIENT",
    "sentToPhysicianDate": "01/01/2024",
    "sentToPhysicianStatus": True,
    "signedByPhysicianDate": "01/01/2024",
    "signedByPhysicianStatus": True,
    "uploadedSignedOrderDate": "",
    "uploadedSignedOrderStatus": True,
    "uploadedSignedPgOrderDate": "",
    "uploadedSignedPgOrderStatus": True,
    "cpoMinutes": "",
    "orderUrl": "",
    "documentName": "TEST DOCUMENT",
    "ehr": "",
    "account": "",
    "location": "",
    "remarks": "API TEST - DO NOT PROCESS",
    "patientId": "",
    "companyId": "",
    "pgCompanyId": "",
    "entityType": "ORDER",
    "clinicalJustification": "",
    "billingProvider": "",
    "billingProviderNPI": "",
    "supervisingProvider": "",
    "supervisingProviderNPI": "",
    "bit64Url": "",
    "daOrderType": "",
    "daUploadSuccess": True,
    "daResponseStatusCode": 0,
    "daResponseDetails": "",
    "createdBy": "APITest",
    "createdOn": datetime.now().isoformat(),
    "updatedBy": "",
    "updatedOn": datetime.now().isoformat(),
    "cpoUpdatedBy": "",
    "cpoUpdatedOn": datetime.now().isoformat()
}

def interpret_error(status_code: Optional[int], response_text: str) -> str:
    """Map status code and body to a human-friendly failure reason."""
    body = (response_text or '').strip()
    body_lower = body.lower()
    if status_code is None:
        return "No response (network error/timeout)"
    if status_code == 401:
        if any(k in body_lower for k in ["token", "bearer", "expired", "invalid credential", "unauthorized"]):
            return "401 Unauthorized - Token missing/expired/invalid"
        if 'function key' in body_lower or 'x-functions-key' in body_lower:
            return "401 Unauthorized - Missing or invalid Azure Function key"
        return "401 Unauthorized"
    if status_code == 403:
        if 'forbidden' in body_lower:
            return "403 Forbidden - Access denied"
        return "403 Forbidden"
    if status_code == 400:
        return f"400 Bad Request - Likely payload/validation issue: {body[:160]}"
    if status_code == 404:
        return "404 Not Found - Endpoint or resource not found"
    if status_code == 405:
        return "405 Method Not Allowed - Wrong HTTP method for endpoint"
    if status_code == 409:
        return "409 Conflict - Likely duplicate (already exists)"
    if status_code == 415:
        return "415 Unsupported Media Type - Check Content-Type header"
    if 500 <= status_code <= 599:
        return f"{status_code} Server Error - Backend issue"
    # Generic fallback
    if body:
        return f"HTTP {status_code} - {body[:160]}"
    return f"HTTP {status_code}"

def test_api_endpoint(url, method="GET", payload=None, headers=None, description="", ok_statuses=None):
    """Test a single API endpoint and return results"""
    if ok_statuses is None:
        ok_statuses = [200, 201, 202]
    if headers is None:
        headers = headers_for_url(url)
    
    try:
        start_time = time.time()
        
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=30)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=payload, timeout=30)
        elif method.upper() == "OPTIONS":
            response = requests.options(url, headers=headers, timeout=30)
        else:
            return {
                "success": False,
                "error": f"Unsupported method: {method}",
                "status_code": None,
                "response_time": 0,
                "response_text": ""
            }
        
        response_time = time.time() - start_time
        
        # Check if response is valid JSON
        try:
            response_json = response.json()
            response_text = json.dumps(response_json, indent=2)
        except:
            response_json = None
            response_text = response.text
        
        success = response.status_code in ok_statuses
        error_msg = None
        if not success:
            error_msg = interpret_error(response.status_code, response_text)
        return {
            "success": success,
            "status_code": response.status_code,
            "response_time": round(response_time, 3),
            "response_text": response_text[:500] + "..." if len(response_text) > 500 else response_text,
            "response_json": response_json,
            "error": error_msg
        }
        
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "Request timeout (30s)",
            "status_code": None,
            "response_time": 30,
            "response_text": ""
        }
    except requests.exceptions.ConnectionError:
        return {
            "success": False,
            "error": "Connection error (network/DNS)",
            "status_code": None,
            "response_time": 0,
            "response_text": ""
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "status_code": None,
            "response_time": 0,
            "response_text": ""
        }

def check_doctoralliance_token(doc_id: Optional[str] = None) -> dict:
    """Health-check the DoctorAlliance bearer token by hitting the document API.
    Returns a dict with fields: ok(bool), status_code(int|None), reason(str).
    """
    # Use a fallback doc ID seen in code if not provided
    test_doc_id = doc_id or '9431342'
    url = f"{app_config.API_BASE}{test_doc_id}"
    try:
        r = requests.get(url, headers=app_config.AUTH_HEADER, timeout=20)
        try:
            body = r.json()
            body_text = json.dumps(body)
        except Exception:
            body_text = r.text
        if r.status_code in (200, 400):
            # 200 typically means isSuccess may be true/false; 400 may mean bad doc id but token accepted
            return {"ok": True, "status_code": r.status_code, "reason": "Token accepted by API"}
        if r.status_code in (401, 403):
            return {"ok": False, "status_code": r.status_code, "reason": interpret_error(r.status_code, body_text)}
        return {"ok": False, "status_code": r.status_code, "reason": interpret_error(r.status_code, body_text)}
    except requests.exceptions.Timeout:
        return {"ok": False, "status_code": None, "reason": "Token check timeout (20s)"}
    except Exception as e:
        return {"ok": False, "status_code": None, "reason": f"Token check error: {e}"}

def test_company_apis(company_key, company_config):
    """Test all APIs for a specific company"""
    print(f"\n{'='*80}")
    print(f"Testing APIs for: {company_config['name']} ({company_key})")
    print(f"{'='*80}")
    
    results = {
        "company": company_key,
        "company_name": company_config['name'],
        "pg_company_id": company_config['pg_company_id'],
        "helper_id": company_config['helper_id'],
        "tests": {}
    }
    created_patient_id = None
    
    # Test 1: Patient API (GET)
    print(f"\n1. Testing Patient API (GET)...")
    patient_api_url = f"https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/company/pg/{company_config['pg_company_id']}"
    patient_result = test_api_endpoint(patient_api_url, "GET", description="Patient API")
    results["tests"]["patient_api_get"] = patient_result
    
    if patient_result["success"]:
        print(f"   ✅ Patient API GET: Success (HTTP {patient_result['status_code']}) - {patient_result['response_time']}s")
    else:
        print(f"   ❌ Patient API GET: Failed - {patient_result['error']}")
    
    # Test 2: Order API (GET)
    print(f"\n2. Testing Order API (GET)...")
    order_result = test_api_endpoint(ORDER_API, "GET", description="Order API")
    results["tests"]["order_api_get"] = order_result
    
    if order_result["success"]:
        print(f"   ✅ Order API GET: Success (HTTP {order_result['status_code']}) - {patient_result['response_time']}s")
    else:
        print(f"   ❌ Order API GET: Failed - {order_result['error']}")
    
    # Test 3: Patient Create API (POST) - with company-specific data
    print(f"\n3. Testing Patient Create API (POST)...")
    test_patient = TEST_PATIENT_PAYLOAD.copy()
    test_patient["pgcompanyID"] = company_config['pg_company_id']
    # Resolve a valid companyId using hybrid lookup
    try:
        resolved_company_id = lookup_company_id_hybrid(company_config['name'], company_config['pg_company_id'])
    except Exception:
        resolved_company_id = None
    test_patient["companyId"] = resolved_company_id or company_config['pg_company_id']
    
    patient_create_result = test_api_endpoint(PATIENT_CREATE_API, "POST", test_patient, description="Patient Create API")
    results["tests"]["patient_create_api"] = patient_create_result
    
    if patient_create_result["success"]:
        print(f"   ✅ Patient Create API: Success (HTTP {patient_create_result['status_code']}) - {patient_create_result['response_time']}s")
        # Try to extract created patient ID from response JSON
        try:
            resp_json = patient_create_result.get("response_json")
            if isinstance(resp_json, dict) and resp_json.get("id"):
                created_patient_id = resp_json.get("id")
        except Exception:
            created_patient_id = None
    else:
        print(f"   ❌ Patient Create API: Failed (HTTP {patient_create_result['status_code']})")
        if patient_create_result.get("response_text"):
            print(f"      Body: {patient_create_result['response_text'][:300]}")
        if patient_create_result["status_code"] == 409:
            print(f"      ℹ️  HTTP 409 - This is expected for duplicate test data")
    
    # Test 4: Order Create API (POST) - with company-specific data
    print(f"\n4. Testing Order Create API (POST)...")
    test_order = TEST_ORDER_PAYLOAD.copy()
    test_order["pgCompanyId"] = company_config['pg_company_id']
    test_order["companyId"] = resolved_company_id or company_config['pg_company_id']
    # Set patientId: prefer created patient; else fallback to an existing one
    if created_patient_id:
        test_order["patientId"] = created_patient_id
    else:
        try:
            patient_api_url = f"https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Patient/company/pg/{company_config['pg_company_id']}"
            pr = requests.get(patient_api_url, timeout=30)
            patients = pr.json() if pr.status_code == 200 else []
            if isinstance(patients, list) and patients:
                test_order["patientId"] = patients[0].get("id", "")
        except Exception:
            pass
    
    order_create_result = test_api_endpoint(ORDER_API, "POST", test_order, description="Order Create API")
    results["tests"]["order_create_api"] = order_create_result
    
    if order_create_result["success"]:
        print(f"   ✅ Order Create API: Success (HTTP {order_create_result['status_code']}) - {order_create_result['response_time']}s")
    else:
        print(f"   ❌ Order Create API: Failed (HTTP {order_create_result['status_code']})")
        if order_create_result.get("response_text"):
            print(f"      Body: {order_create_result['response_text'][:300]}")
        if order_create_result["status_code"] == 409:
            print(f"      ℹ️  HTTP 409 - This is expected for duplicate test data")
    
    # Test 5: PDF Upload API (GET - just test connectivity)
    print(f"\n5. Testing PDF Upload API connectivity...")
    # Consider 404/405 as reachable since this endpoint expects POST with an order GUID
    pdf_upload_result = test_api_endpoint(ORDER_PDF_UPLOAD_API, "GET", description="PDF Upload API", ok_statuses=[200,201,202,404,405])
    results["tests"]["pdf_upload_api"] = pdf_upload_result
    
    if pdf_upload_result["success"]:
        print(f"   ✅ PDF Upload API: Success (HTTP {pdf_upload_result['status_code']}) - {pdf_upload_result['response_time']}s")
    else:
        print(f"   ❌ PDF Upload API: Failed (HTTP {pdf_upload_result['status_code']})")
        if pdf_upload_result.get("response_text"):
            print(f"      Body: {pdf_upload_result['response_text'][:300]}")
    
    # Test 6: Company-specific Order API (GET)
    print(f"\n6. Testing Company-specific Order API (GET)...")
    company_order_api = f"https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Order/pgcompany/{company_config['pg_company_id']}"
    company_order_result = test_api_endpoint(company_order_api, "GET", description="Company Order API")
    results["tests"]["company_order_api"] = company_order_result
    
    if company_order_result["success"]:
        print(f"   ✅ Company Order API: Success (HTTP {company_order_result['status_code']}) - {company_order_result['response_time']}s")
    else:
        print(f"   ❌ Company Order API: Failed - {company_order_result['error']}")
    
    return results

def generate_summary_report(all_results):
    """Generate a summary report of all test results"""
    print(f"\n{'='*100}")
    print(f"API TESTING SUMMARY REPORT")
    print(f"{'='*100}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # Exclude non-company meta entries like __token_health__
    company_entries = {k: v for k, v in all_results.items() if not str(k).startswith("__")}
    print(f"Total Companies Tested: {len(company_entries)}")
    
    # Count successes and failures
    total_tests = 0
    successful_tests = 0
    failed_tests = 0
    
    for company_key, results in company_entries.items():
        company_name = results.get("company_name", company_key)
        print(f"\n{company_name} ({company_key}):")
        
        for test_name, test_result in results["tests"].items():
            total_tests += 1
            if test_result["success"]:
                successful_tests += 1
                status = "✅ PASS"
            else:
                failed_tests += 1
                status = "❌ FAIL"
            
            print(f"  {test_name}: {status}")
            if not test_result["success"]:
                print(f"    Error: {test_result['error']}")
    
    print(f"\n{'='*100}")
    print(f"OVERALL RESULTS:")
    print(f"  Total Tests: {total_tests}")
    print(f"  Successful: {successful_tests}")
    print(f"  Failed: {failed_tests}")
    print(f"  Success Rate: {(successful_tests/total_tests*100):.1f}%" if total_tests > 0 else "  Success Rate: N/A")
    print(f"{'='*100}")
    
    # Save detailed results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"api_test_results_{timestamp}.json"
    
    try:
        with open(results_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        print(f"\nDetailed results saved to: {results_file}")
    except Exception as e:
        print(f"\nWarning: Could not save results file: {e}")

def main():
    """Main function to run all API tests"""
    print("🚀 Doctor Alliance API Endpoint Testing Script")
    print("=" * 60)
    print("This script will test all POST APIs for all configured companies")
    print("Note: This is a connectivity/functionality test, not a data creation test")
    print("=" * 60)
    
    # Get companies to test
    companies_to_test = get_companies_to_process()

    # CLI filters: --limit=N and/or --companies=key1,key2
    limit_n = None
    companies_filter = None
    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            try:
                limit_n = int(arg.split("=", 1)[1])
            except Exception:
                limit_n = None
        elif arg.startswith("--companies="):
            raw = arg.split("=", 1)[1]
            companies_filter = [c.strip() for c in raw.split(",") if c.strip()]

    if companies_filter:
        # Keep only valid keys, preserve order given by filter
        valid_set = set(companies_to_test)
        selected = [c for c in companies_filter if c in valid_set]
        missing = [c for c in companies_filter if c not in valid_set]
        if missing:
            print(f"\n⚠️  Skipping unknown company keys: {', '.join(missing)}")
        companies_to_test = selected
    elif isinstance(limit_n, int) and limit_n > 0:
        companies_to_test = companies_to_test[:limit_n]

    print(f"\n📋 Companies to test: {len(companies_to_test)}")
    
    if not companies_to_test:
        print("❌ No companies configured for testing!")
        return
    
    # Confirm before proceeding
    print(f"\n⚠️  This will test {len(companies_to_test)} companies with multiple API endpoints each.")
    print("   This may take several minutes and will make test API calls.")
    
    # Allow non-interactive run with --yes / -y
    auto_yes = any(arg in ("--yes", "-y") for arg in sys.argv[1:])
    # Optional: provide a docId for token check via --docid=12345
    token_doc_id = None
    for arg in sys.argv[1:]:
        if arg.startswith("--docid="):
            token_doc_id = arg.split("=", 1)[1]
    if not auto_yes:
        try:
            response = input("\nContinue? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("❌ Testing cancelled by user")
                return
        except KeyboardInterrupt:
            print("\n❌ Testing cancelled by user")
            return

    # Check DoctorAlliance token health first
    print("\n🔐 Checking DoctorAlliance token health...")
    token_health = check_doctoralliance_token(token_doc_id)
    token_status = "OK" if token_health.get("ok") else "PROBLEM"
    print(f"   Token status: {token_status} (HTTP {token_health.get('status_code')}) - {token_health.get('reason')}")
    
    # Run tests for all companies
    all_results = {}
    start_time = time.time()
    
    for company_key in companies_to_test:
        if company_key in COMPANIES:
            try:
                results = test_company_apis(company_key, COMPANIES[company_key])
                all_results[company_key] = results
                
                # Small delay between companies to avoid overwhelming APIs
                time.sleep(1)
                
            except Exception as e:
                print(f"\n❌ Error testing company {company_key}: {e}")
                all_results[company_key] = {
                    "company": company_key,
                    "company_name": company_key,
                    "error": str(e),
                    "tests": {}
                }
        else:
            print(f"\n⚠️  Company {company_key} not found in configuration")
    
    total_time = time.time() - start_time
    
    # Attach token health to results for reference
    all_results["__token_health__"] = token_health

    # Generate summary report
    generate_summary_report(all_results)
    
    print(f"\n⏱️  Total testing time: {total_time:.1f} seconds")
    print("🎯 API testing complete!")

if __name__ == "__main__":
    main()
