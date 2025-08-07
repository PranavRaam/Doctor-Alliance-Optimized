#!/usr/bin/env python3
"""
Test script to verify the fixes for company ID and patient name issues.
"""

import sys
import os
import pandas as pd

# Add the current directory to Python path to import from Upload_Patients_Orders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Upload_Patients_Orders import (
    build_patient_payload,
    split_name,
    lookup_company_id_hybrid
)

def test_split_name():
    """Test the improved split_name function."""
    print("🔍 Testing split_name function...")
    
    test_cases = [
        ("John Doe", "John", "", "Doe"),
        ("John", "John", "", "Patient"),
        ("", "Unknown", "", "Patient"),
        ("John A Doe", "John", "A", "Doe"),
        ("John A B Doe", "John", "A B", "Doe"),
        ("nan", "Unknown", "", "Patient"),
        (None, "Unknown", "", "Patient")
    ]
    
    for input_name, expected_fname, expected_mname, expected_lname in test_cases:
        fname, mname, lname = split_name(input_name)
        success = (fname == expected_fname and mname == expected_mname and lname == expected_lname)
        status = "✅" if success else "❌"
        print(f"   {status} '{input_name}' -> '{fname}' '{mname}' '{lname}'")

def test_company_lookup():
    """Test company ID lookup with PG company ID."""
    print("\n🔍 Testing company ID lookup...")
    
    # Test with Chickasaw Nation Medical Center PG ID
    pg_company_id = "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7"
    company_name = "Chickasaw Nation Medical Center"
    
    company_id = lookup_company_id_hybrid(company_name, pg_company_id)
    if company_id:
        print(f"   ✅ Found company ID: {company_id}")
    else:
        print(f"   ❌ Company ID not found for: {company_name}")

def test_patient_payload():
    """Test building patient payload with missing data."""
    print("\n🔍 Testing patient payload building...")
    
    # Create a test row with minimal data
    test_row = {
        "mrn": "12345",
        "dob": "01/01/1980",
        "soc": "08/01/2025",
        "cert_period_soe": "08/01/2025",
        "cert_period_eoe": "10/01/2025",
        "Diagnosis 1": "Test Diagnosis",
        "patient_sex": "MALE",
        "address": "123 Test St, Test City, OK 12345",
        "NPI": "1234567890",
        "DABackOfficeID": "TEST123",
        "Pgcompanyid": "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7"
        # Note: No patientName or companyId fields
    }
    
    try:
        payload, remarks = build_patient_payload(test_row, "chickasaw_nation_medical_center")
        
        # Check if required fields are present
        required_fields = ["patientFName", "patientLName", "companyId"]
        missing_fields = []
        
        for field in required_fields:
            if not payload.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            print(f"   ❌ Missing required fields: {missing_fields}")
            print(f"   Payload: {payload}")
        else:
            print(f"   ✅ All required fields present")
            print(f"   Patient Name: {payload.get('patientFName')} {payload.get('patientLName')}")
            print(f"   Company ID: {payload.get('companyId')}")
            print(f"   Remarks: {remarks}")
            
    except Exception as e:
        print(f"   ❌ Error building payload: {e}")

def main():
    """Main test function."""
    print("🧪 Testing Fixes for Company ID and Patient Name Issues")
    print("=" * 60)
    
    # Test split_name function
    test_split_name()
    
    # Test company lookup
    test_company_lookup()
    
    # Test patient payload building
    test_patient_payload()
    
    print("\n✅ All tests completed!")

if __name__ == "__main__":
    main()
