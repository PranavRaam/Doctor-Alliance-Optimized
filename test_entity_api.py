#!/usr/bin/env python3
"""
Test script to verify the entity API lookup functionality.
"""

import sys
import os

# Add the current directory to Python path to import from Upload_Patients_Orders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Upload_Patients_Orders import (
    lookup_company_id_via_entity_api,
    lookup_company_id_hybrid
)

def test_entity_api_lookup():
    """Test entity API lookup for company IDs."""
    print("🔍 Testing Entity API Lookup...")
    
    # Test cases with company names
    test_companies = [
        "Chickasaw Nation Medical Center",
        "Southeast Oklahoma Medical Clinic", 
        "Triton Health PLLC",
        "AccentCare Fall River"
    ]
    
    for company_name in test_companies:
        print(f"\n   Testing: {company_name}")
        company_id = lookup_company_id_via_entity_api(company_name)
        if company_id:
            print(f"   ✅ Found company ID: {company_id}")
        else:
            print(f"   ❌ Company ID not found")

def test_hybrid_lookup():
    """Test hybrid lookup with PG company IDs."""
    print("\n🔍 Testing Hybrid Lookup...")
    
    # Test cases with company names and their PG IDs
    test_cases = [
        ("Chickasaw Nation Medical Center", "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7"),
        ("Southeast Oklahoma Medical Clinic", "108bbba4-5d5d-41d9-b1c6-0eaac5538f6c"),
        ("Triton Health PLLC", "d09df8cc-a549-4229-a03a-ce29fb09aea2")
    ]
    
    for company_name, pg_company_id in test_cases:
        print(f"\n   Testing: {company_name} (PG ID: {pg_company_id})")
        company_id = lookup_company_id_hybrid(company_name, pg_company_id)
        if company_id:
            print(f"   ✅ Found company ID: {company_id}")
        else:
            print(f"   ❌ Company ID not found")

def main():
    """Main test function."""
    print("🧪 Testing Entity API Lookup")
    print("=" * 50)
    
    # Test entity API lookup
    test_entity_api_lookup()
    
    # Test hybrid lookup
    test_hybrid_lookup()
    
    print("\n✅ All tests completed!")

if __name__ == "__main__":
    main()
