#!/usr/bin/env python3
"""
Test script for hybrid company ID lookup functionality.
This script demonstrates how the hybrid lookup works with both entity API and CSV fallback.
"""

import sys
import os

# Add the current directory to Python path to import from Upload_Patients_Orders
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Upload_Patients_Orders import (
    load_company_ids_csv, 
    lookup_company_id_hybrid, 
    lookup_company_id_via_csv,
    lookup_company_id_via_entity_api
)

def test_csv_loading():
    """Test loading company IDs from CSV."""
    print("🔍 Testing CSV loading...")
    company_mapping = load_company_ids_csv()
    print(f"   Loaded {len(company_mapping)} company mappings")
    
    # Show some examples
    sample_companies = list(company_mapping.items())[:5]
    print("   Sample companies:")
    for name, cid in sample_companies:
        print(f"     {name} -> {cid}")
    
    return company_mapping

def test_csv_lookup():
    """Test CSV-based company ID lookup."""
    print("\n🔍 Testing CSV lookup...")
    
    # Test cases with companies that exist in CSV
    test_cases = [
        "AccentCare Fall River",
        "Synergy Care Southeast, LLC",
        "BOS - Enhabit OF SOUTHEAST OKLAHOMA",
        "AccentCare - Westwood",
        "Unknown Company"
    ]
    
    for company_name in test_cases:
        company_id = lookup_company_id_via_csv(company_name)
        if company_id:
            print(f"   ✅ Found: '{company_name}' -> {company_id}")
        else:
            print(f"   ❌ Not found: '{company_name}'")

def test_hybrid_lookup():
    """Test hybrid company ID lookup."""
    print("\n🔍 Testing hybrid lookup...")
    
    # Test cases with company names that exist in CSV
    test_cases = [
        ("AccentCare Fall River", "c7cc6389-e15a-468f-bfbc-bc60ac4d4e81"),
        ("Synergy Care Southeast, LLC", "6e9932be-db4d-4a8c-a738-2574e7af98dd"),
        ("BOS - Enhabit OF SOUTHEAST OKLAHOMA", "96852979-1b4a-409f-881e-ce236f54d73c"),
        ("Unknown Company", None),
        ("AccentCare - Westwood", "b0cb1a6d-af9a-439d-b07a-657d718d1ae4")
    ]
    
    for company_name, pg_company_id in test_cases:
        print(f"\n   Testing: {company_name} (PG ID: {pg_company_id})")
        company_id = lookup_company_id_hybrid(company_name, pg_company_id)
        if company_id:
            print(f"   ✅ Found company ID: {company_id}")
        else:
            print(f"   ❌ Company ID not found")

def test_entity_api_placeholder():
    """Test entity API placeholder function."""
    print("\n🔍 Testing entity API placeholder...")
    
    test_terms = [
        "AccentCare Fall River",
        "c7cc6389-e15a-468f-bfbc-bc60ac4d4e81"
    ]
    
    for term in test_terms:
        result = lookup_company_id_via_entity_api(term)
        print(f"   Entity API lookup for '{term}': {result}")

def main():
    """Main test function."""
    print("🧪 Hybrid Company ID Lookup Test")
    print("=" * 50)
    
    # Test CSV loading
    company_mapping = test_csv_loading()
    
    # Test CSV lookup
    test_csv_lookup()
    
    # Test entity API placeholder
    test_entity_api_placeholder()
    
    # Test hybrid lookup
    test_hybrid_lookup()
    
    print("\n✅ All tests completed!")
    print("\n📝 Notes:")
    print("   - Entity API lookup is currently a placeholder")
    print("   - CSV lookup is fully functional")
    print("   - Hybrid lookup combines both approaches")
    print("   - You can implement the actual entity API call in lookup_company_id_via_entity_api()")

if __name__ == "__main__":
    main()
