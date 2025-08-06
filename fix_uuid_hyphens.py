#!/usr/bin/env python3
"""
Script to fix missing hyphens in patientId and companyId fields for existing orders.
Just paste your JSON data and run!
"""

import json
import requests
import time
from typing import List, Dict

# Configuration
API_BASE_URL = "https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net"
HEADERS = {
    "Content-Type": "application/json",
    "accept": "*/*"
}

def add_hyphens_to_uuid(uuid_str: str) -> str:
    """Convert 32-character UUID string to proper hyphenated format."""
    if not uuid_str or len(uuid_str) != 32:
        return uuid_str  # Return as-is if not 32 characters
    
    # Convert: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    # To:      xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"

def fix_order_uuids(order: Dict) -> Dict:
    """Fix UUID formatting in a single order object."""
    fixed_order = order.copy()
    
    # Fix patientId if it's 32 characters without hyphens
    patient_id = order.get("patientId", "")
    if patient_id and len(patient_id) == 32 and "-" not in patient_id:
        fixed_order["patientId"] = add_hyphens_to_uuid(patient_id)
        print(f"  Fixed patientId: {patient_id} → {fixed_order['patientId']}")
    
    # Fix companyId if it's 32 characters without hyphens  
    company_id = order.get("companyId", "")
    if company_id and len(company_id) == 32 and "-" not in company_id:
        fixed_order["companyId"] = add_hyphens_to_uuid(company_id)
        print(f"  Fixed companyId: {company_id} → {fixed_order['companyId']}")
    
    # pgCompanyId should already be correct, but check anyway
    pg_company_id = order.get("pgCompanyId", "")
    if pg_company_id and len(pg_company_id) == 32 and "-" not in pg_company_id:
        fixed_order["pgCompanyId"] = add_hyphens_to_uuid(pg_company_id)
        print(f"  Fixed pgCompanyId: {pg_company_id} → {fixed_order['pgCompanyId']}")
    
    return fixed_order

def update_order_via_api(order: Dict) -> bool:
    """Update a single order via PUT API."""
    order_id = order.get("id")
    if not order_id:
        print(f"  ❌ No ID found for order")
        return False
    
    try:
        url = f"{API_BASE_URL}/api/Order/{order_id}"
        response = requests.put(url, json=order, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            print(f"  ✅ Updated order {order_id}")
            return True
        else:
            print(f"  ❌ Failed to update order {order_id}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"  ❌ Error updating order {order_id}: {e}")
        return False

def main():
    """Main function - loads data from JSON file automatically!"""
    
    # Load JSON data from file
    json_filename = "response_1754505828480.json"
    try:
        with open(json_filename, 'r', encoding='utf-8') as f:
            orders_data = json.load(f)
        print(f"✅ Loaded {len(orders_data)} orders from {json_filename}")
    except FileNotFoundError:
        print(f"❌ JSON file '{json_filename}' not found! Please make sure the file exists in the current directory.")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON format in '{json_filename}': {e}")
        return
    except Exception as e:
        print(f"❌ Error loading '{json_filename}': {e}")
        return
    
    if not orders_data:
        print("❌ No orders data found in the JSON file!")
        return
    
    print(f"🔧 Processing {len(orders_data)} orders...")
    print(f"🌐 API Base URL: {API_BASE_URL}")
    print("=" * 60)
    
    # First, preview what changes will be made
    print("\n🔍 PREVIEW MODE - Checking what needs to be fixed...")
    orders_needing_fixes = []
    
    for i, order in enumerate(orders_data, 1):
        order_no = order.get("orderNo", "Unknown")
        patient_name = order.get("patientName", "Unknown")
        
        # Check what needs fixing
        patient_id = order.get("patientId", "")
        company_id = order.get("companyId", "")
        pg_company_id = order.get("pgCompanyId", "")
        
        needs_patient_fix = patient_id and len(patient_id) == 32 and "-" not in patient_id
        needs_company_fix = company_id and len(company_id) == 32 and "-" not in company_id
        needs_pg_company_fix = pg_company_id and len(pg_company_id) == 32 and "-" not in pg_company_id
        
        if needs_patient_fix or needs_company_fix or needs_pg_company_fix:
            orders_needing_fixes.append(order)
            print(f"  [{i}] Order {order_no} - {patient_name}")
            if needs_patient_fix:
                print(f"      🔧 patientId: {patient_id} → {add_hyphens_to_uuid(patient_id)}")
            if needs_company_fix:
                print(f"      🔧 companyId: {company_id} → {add_hyphens_to_uuid(company_id)}")
            if needs_pg_company_fix:
                print(f"      🔧 pgCompanyId: {pg_company_id} → {add_hyphens_to_uuid(pg_company_id)}")
    
    if not orders_needing_fixes:
        print("✅ All UUIDs are already properly formatted! No fixes needed.")
        return
    
    print(f"\n📊 Summary: {len(orders_needing_fixes)} out of {len(orders_data)} orders need UUID fixes.")
    
    # Ask for confirmation
    response = input("\n❓ Do you want to proceed with updating these orders? (y/n): ").lower().strip()
    if response not in ['y', 'yes']:
        print("❌ Operation cancelled by user.")
        return
    
    print("\n🚀 Starting updates...")
    print("=" * 60)
    
    successful_updates = 0
    failed_updates = 0
    
    for i, order in enumerate(orders_needing_fixes, 1):
        order_no = order.get("orderNo", "Unknown")
        patient_name = order.get("patientName", "Unknown")
        
        print(f"\n[{i}/{len(orders_needing_fixes)}] Processing Order {order_no} - {patient_name}")
        
        # Fix UUID formatting
        fixed_order = fix_order_uuids(order)
        
        # Update via API
        if update_order_via_api(fixed_order):
            successful_updates += 1
        else:
            failed_updates += 1
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.5)
    
    print("\n" + "=" * 60)
    print(f"🎉 Processing complete!")
    print(f"✅ Successful updates: {successful_updates}")
    print(f"❌ Failed updates: {failed_updates}")
    print(f"📊 Orders needing fixes: {len(orders_needing_fixes)}")
    print(f"📈 Total orders in file: {len(orders_data)}")

if __name__ == "__main__":
    # Quick test of UUID conversion
    print("Testing UUID conversion...")
    test_uuid = "bd70d19f85fc4ab4b71a6bd4534e64a1"
    converted = add_hyphens_to_uuid(test_uuid)
    print(f"Test: {test_uuid} → {converted}")
    print()
    
    main()