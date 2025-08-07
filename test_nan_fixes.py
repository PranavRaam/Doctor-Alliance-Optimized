#!/usr/bin/env python3
"""
Test script to verify NaN handling fixes.
"""

import sys
import os
import pandas as pd
import numpy as np

# Add the current directory to Python path to import from Upload_Patients_Orders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Upload_Patients_Orders import (
    split_name,
    build_patient_payload
)

def test_split_name_nan():
    """Test split_name function with NaN values."""
    print("🔍 Testing split_name with NaN values...")
    
    test_cases = [
        (np.nan, "", "", ""),
        (None, "", "", ""),
        ("John Doe", "John", "", "Doe"),
        ("", "", "", ""),
        ("John", "John", "", ""),
        (pd.NA, "", "", ""),
        (float('nan'), "", "", "")
    ]
    
    for input_name, expected_fname, expected_mname, expected_lname in test_cases:
        try:
            fname, mname, lname = split_name(input_name)
            success = (fname == expected_fname and mname == expected_mname and lname == expected_lname)
            status = "✅" if success else "❌"
            print(f"   {status} '{input_name}' -> '{fname}' '{mname}' '{lname}'")
        except Exception as e:
            print(f"   ❌ Error with '{input_name}': {e}")

def test_patient_payload_nan():
    """Test build_patient_payload with NaN values."""
    print("\n🔍 Testing build_patient_payload with NaN values...")
    
    # Create a test row with NaN values
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
        "Pgcompanyid": "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7",
        "patientName": np.nan,  # NaN value
        "patient_name": np.nan,  # NaN value
        "name": np.nan,  # NaN value
        "full_name": np.nan,  # NaN value
        "patient_full_name": np.nan  # NaN value
    }
    
    try:
        payload, remarks = build_patient_payload(test_row, "chickasaw_nation_medical_center")
        
        # Check if the function handled NaN values properly
        fname = payload.get("patientFName", "")
        lname = payload.get("patientLName", "")
        company_id = payload.get("companyId", "")
        
        print(f"   Patient Name: '{fname}' '{lname}'")
        print(f"   Company ID: {company_id}")
        print(f"   Remarks: {remarks}")
        
        if fname == "" and lname == "":
            print("   ✅ NaN values handled correctly - empty names")
        else:
            print("   ❌ NaN values not handled correctly")
            
    except Exception as e:
        print(f"   ❌ Error building payload: {e}")

def test_dataframe_nan():
    """Test DataFrame operations with NaN values."""
    print("\n🔍 Testing DataFrame operations with NaN values...")
    
    # Create a DataFrame with NaN values
    df = pd.DataFrame({
        'patientid': [np.nan, np.nan, np.nan],
        'PatientExist': [False, False, False],
        'mrn': ['123', '456', '789'],
        'DABackOfficeID': ['ID1', 'ID2', 'ID3']
    })
    
    print(f"   Original patientid dtype: {df['patientid'].dtype}")
    
    # Simulate the operation that was causing the warning
    try:
        # Check if column is float64 and convert to object if needed
        if df['patientid'].dtype == 'float64':
            df['patientid'] = df['patientid'].astype('object')
            print(f"   ✅ Converted patientid to object dtype")
        
        # Now set string values
        df.at[0, 'patientid'] = "test-patient-id-1"
        df.at[1, 'patientid'] = "test-patient-id-2"
        df.at[2, 'patientid'] = "test-patient-id-3"
        
        print(f"   Final patientid dtype: {df['patientid'].dtype}")
        print(f"   Values: {df['patientid'].tolist()}")
        print("   ✅ DataFrame operations successful")
        
    except Exception as e:
        print(f"   ❌ DataFrame operation error: {e}")

def main():
    """Main test function."""
    print("🧪 Testing NaN Handling Fixes")
    print("=" * 50)
    
    # Test split_name with NaN values
    test_split_name_nan()
    
    # Test patient payload with NaN values
    test_patient_payload_nan()
    
    # Test DataFrame operations with NaN values
    test_dataframe_nan()
    
    print("\n✅ All tests completed!")

if __name__ == "__main__":
    main()
