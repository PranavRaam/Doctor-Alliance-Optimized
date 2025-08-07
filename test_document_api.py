#!/usr/bin/env python3
"""
Test script to check document API response structure.
"""

import sys
import os
import json

# Add the current directory to Python path to import from main
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import get_document_info

def test_document_api():
    """Test document API response structure."""
    print("🔍 Testing Document API Response Structure...")
    
    # Test with a few document IDs
    test_doc_ids = [
        "9431342",
        "9431476", 
        "9433593"
    ]
    
    for doc_id in test_doc_ids:
        print(f"\n📄 Testing Document ID: {doc_id}")
        print("=" * 50)
        
        try:
            result = get_document_info(doc_id)
            
            if result.get('success'):
                print(f"✅ Success: {result.get('document_name', 'No name')}")
                print(f"📋 Document Type: {result.get('document_type', {})}")
                print(f"📊 Status: {result.get('status', 'No status')}")
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
    
    print("\n✅ Document API testing completed!")

if __name__ == "__main__":
    test_document_api()
