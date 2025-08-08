#!/usr/bin/env python3
"""
Utility script to identify and fix missing company IDs in company.json
This script helps resolve the issue where some company IDs from Excel files
are not found in company.json, causing empty payloads and skipped records.
"""

import json
import pandas as pd
import os
import sys
from datetime import datetime

def load_company_mapping():
    """Load company mapping from company.json."""
    try:
        with open('company.json', 'r') as f:
            company_data = json.load(f)
        return company_data
    except FileNotFoundError:
        print("⚠️  company.json not found")
        return {}

def find_missing_company_ids():
    """Find company IDs that appear in Excel files but are missing from company.json."""
    print("🔍 Scanning for missing company IDs...")
    
    # Load existing company mapping
    company_mapping = load_company_mapping()
    existing_ids = set(company_mapping.values())
    
    # Find all Excel files in current directory
    excel_files = []
    for file in os.listdir('.'):
        if file.endswith('.xlsx') and ('supreme_excel' in file or 'doctoralliance' in file):
            excel_files.append(file)
    
    if not excel_files:
        print("❌ No Excel files found to scan")
        return set()
    
    # Collect all company IDs from Excel files
    excel_company_ids = set()
    for file in excel_files:
        try:
            print(f"📄 Scanning {file}...")
            df = pd.read_excel(file)
            
            # Check for companyId column
            if 'companyId' in df.columns:
                company_ids = df['companyId'].dropna().unique()
                for cid in company_ids:
                    if pd.notna(cid) and str(cid).strip():
                        # Format UUID with hyphens
                        formatted_id = format_uuid(str(cid).strip())
                        if formatted_id:
                            excel_company_ids.add(formatted_id)
                            if formatted_id not in existing_ids:
                                print(f"  ⚠️  Missing: {formatted_id}")
            
            # Also check Pgcompanyid column
            if 'Pgcompanyid' in df.columns:
                pg_ids = df['Pgcompanyid'].dropna().unique()
                for pgid in pg_ids:
                    if pd.notna(pgid) and str(pgid).strip():
                        formatted_id = format_uuid(str(pgid).strip())
                        if formatted_id:
                            excel_company_ids.add(formatted_id)
                            if formatted_id not in existing_ids:
                                print(f"  ⚠️  Missing PG ID: {formatted_id}")
                                
        except Exception as e:
            print(f"  ❌ Error reading {file}: {e}")
    
    # Find missing IDs
    missing_ids = excel_company_ids - existing_ids
    
    print(f"\n📊 Summary:")
    print(f"   Total company IDs found in Excel: {len(excel_company_ids)}")
    print(f"   Existing IDs in company.json: {len(existing_ids)}")
    print(f"   Missing IDs: {len(missing_ids)}")
    
    return missing_ids

def format_uuid(uuid_str):
    """Add hyphens to UUID string to match mapping format."""
    if pd.isna(uuid_str):
        return None
    
    uuid_str = str(uuid_str).strip()
    # Remove any existing hyphens first
    uuid_str = uuid_str.replace('-', '')
    
    # Add hyphens in the correct positions (8-4-4-4-12 format)
    if len(uuid_str) == 32:
        return f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
    else:
        return None

def suggest_company_names(missing_ids):
    """Suggest company names for missing IDs based on Excel data."""
    print(f"\n💡 Suggestions for missing company IDs:")
    
    # Find Excel files to extract company names
    excel_files = []
    for file in os.listdir('.'):
        if file.endswith('.xlsx') and ('supreme_excel' in file or 'doctoralliance' in file):
            excel_files.append(file)
    
    suggestions = {}
    
    for file in excel_files:
        try:
            df = pd.read_excel(file)
            
            # Look for company name columns
            name_columns = ['agency name', 'company_name', 'agencyName', 'companyName']
            name_col = None
            for col in name_columns:
                if col in df.columns:
                    name_col = col
                    break
            
            if name_col and 'companyId' in df.columns:
                # Group by companyId and get unique company names
                for cid in missing_ids:
                    # Find rows with this company ID
                    matching_rows = df[df['companyId'].apply(lambda x: format_uuid(str(x)) == cid)]
                    if len(matching_rows) > 0:
                        company_names = matching_rows[name_col].dropna().unique()
                        if len(company_names) > 0:
                            suggestions[cid] = company_names[0]  # Use first name found
                            print(f"  {cid}: {company_names[0]}")
                            
        except Exception as e:
            print(f"  ❌ Error processing {file}: {e}")
    
    return suggestions

def backup_company_json():
    """Create a backup of company.json."""
    if os.path.exists('company.json'):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f'company_backup_{timestamp}.json'
        try:
            with open('company.json', 'r') as src:
                with open(backup_file, 'w') as dst:
                    dst.write(src.read())
            print(f"✅ Backup created: {backup_file}")
            return backup_file
        except Exception as e:
            print(f"❌ Failed to create backup: {e}")
            return None
    return None

def add_missing_companies(missing_ids, suggestions):
    """Add missing company IDs to company.json."""
    if not missing_ids:
        print("✅ No missing company IDs to add")
        return
    
    # Create backup
    backup_file = backup_company_json()
    
    # Load current company.json
    company_data = load_company_mapping()
    
    # Add missing companies
    added_count = 0
    for cid in missing_ids:
        company_name = suggestions.get(cid, f"Unknown Company ({cid})")
        company_data[company_name] = cid
        added_count += 1
        print(f"  ➕ Added: {company_name} -> {cid}")
    
    # Save updated company.json
    try:
        with open('company.json', 'w') as f:
            json.dump(company_data, f, indent=2)
        print(f"\n✅ Added {added_count} missing companies to company.json")
        print(f"📁 Backup saved as: {backup_file}")
    except Exception as e:
        print(f"❌ Failed to save company.json: {e}")

def main():
    """Main function to identify and fix missing company IDs."""
    print("🔧 Company ID Mapping Fix Tool")
    print("=" * 50)
    
    # Find missing company IDs
    missing_ids = find_missing_company_ids()
    
    if not missing_ids:
        print("✅ All company IDs are properly mapped!")
        return
    
    # Get suggestions for company names
    suggestions = suggest_company_names(missing_ids)
    
    # Ask user if they want to add missing companies
    print(f"\n❓ Found {len(missing_ids)} missing company IDs.")
    response = input("Do you want to add them to company.json? (y/n): ").lower().strip()
    
    if response in ['y', 'yes']:
        add_missing_companies(missing_ids, suggestions)
        print("\n✅ Company mapping updated! You can now run your pipeline again.")
    else:
        print("\n⚠️  No changes made. You may need to manually add missing company IDs to company.json")

if __name__ == "__main__":
    main()

