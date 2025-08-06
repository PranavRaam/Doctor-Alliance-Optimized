import json

def readConfig():
    config_file_path = './config.json'
    with open(config_file_path, 'r') as file:
        config_data = json.load(file)
    configuration_dict = config_data.get('configuration', {})
    return configuration_dict

#print(readConfig()["AgencyTemplatePath"])

# --- API CONFIG ---
API_BASE = "https://api.doctoralliance.com/document/getfile?docId.id="
AUTH_HEADER = {
    "Accept": "application/json",
    "Authorization": "Bearer zbs0Sj0CL-7JGE39N60iVdG-w7ZVNOrJHCjYrq8DA4uatgxI0gD0_niGYl72ynOsxkA72V4lQHWJ1lrOPWvmRZDXv0AevDIgKiPjICG_wdsk1qIHz8n_b2Fz7rIQqwCexi8sMz2NeoUxXyFOibxIm2HLUNZGoAOGInowvfiVErnF4RFHjUEdU1DrK8KREo67B7jbhQ91EEkxyZAFSSeU-AA0YPUlbJAIjgeq6rrzzjwyqKAvTmkC3T0Hc0Q_jCVMcQuNm2nZQdxj1nOBkq8V2Q"
}

api_key = "EVtCfEbXd2pvVrkOaByfss3HBMJy9x0FvwXdFhCmenum0RLvHCZNJQQJ99BDACYeBjFXJ3w3AAABACOGe7zr"
azure_endpoint = "https://daplatformai.openai.azure.com/"
deployment_name = "gpt-35-turbo"

OLLAMA_LLM_MODEL = "llama3"

# Your Qdrant Configuration - Accuracy Focused
QDRANT_HOST = "adc8fc0d-3f8e-4373-87d0-b3df7d2417d0.us-west-1-0.aws.cloud.qdrant.io"
QDRANT_PORT = 6333
QDRANT_API_KEY = "adc8fc0d-3f8e-4373-87d0-b3df7d2417d0"
COLLECTION_NAME = "medical_document"
QDRANT_USE_HTTPS = True

# Performance-Optimized Configuration for VM
QDRANT_CONFIG = {
    "hnsw_ef_construct": 200,    # Reduced for speed
    "hnsw_m": 32,                # Reduced connections for speed
    "hnsw_ef_search": 100,       # Reduced search quality for speed
    "quantization_enabled": True, # Enable quantization for speed
    "oversampling": 1.0,         # No oversampling needed
    "rescore": False,            # Disable rescore for speed
    "top_k_retrieval": 5         # Retrieve less context for speed
}

# Optimized Download Configuration for VM performance
DOWNLOAD_CONFIG = {
    "max_concurrent_downloads": 15,  # Increased for VM performance
    "chunk_size": 8192,
    "timeout": 30,                   # Reduced timeout for faster failure detection
    "max_retries": 3,                # Reduced retries for speed
    "retry_backoff": 2,              # Shorter backoff
    "connection_pool_size": 25,      # Increased pool size
    "use_async": True,
}

# Optimized Text Extraction Configuration for VM performance
EXTRACTION_CONFIG = {
    "max_concurrent_extractions": 8,    # Increased concurrency for speed
    "quality_threshold": 80,            # Slightly lower threshold for speed
    "comprehensive_testing": False,     # Disabled for speed
    "ocr_fallback_threshold": 70,       # Higher threshold to avoid OCR
    "multi_pass_extraction": False,     # Disabled for speed
    "text_validation_enabled": False,   # Disabled for speed
    "medical_field_validation": False,  # Disabled for speed
    "extraction_timeout": 60,          # Reduced timeout for speed
}

# Optimized Field Extraction Configuration for VM performance
FIELD_EXTRACTION_CONFIG = {
    "max_retries": 3,               # Reduced retries for speed
    "chunk_overlap": 200,           # Reduced overlap for speed
    "multi_model_validation": False, # Disabled for speed
    "field_confidence_threshold": 0.6, # Lower threshold for speed
    "cross_validation_enabled": False,  # Disabled for speed
    "medical_context_enhancement": False, # Disabled for speed
    "structured_prompting": False,   # Disabled for speed
}

# ===========================================
# DATE RANGE CONFIGURATION
# ===========================================

# Date range for processing (MM/DD/YYYY format)
DATE_RANGE = {
    "start_date": "08/01/2025",
    "end_date": "08/06/2025",
}

# Multiple companies to process (leave empty list for single company)
MULTIPLE_COMPANIES = [
    "southeast_oklahoma_medical_clinic",
    "terry_draper_restore_family_medical_clinic",
    "tpch_practice_dr_tradewell"
]

# If MULTIPLE_COMPANIES is empty, use the active company
# If MULTIPLE_COMPANIES has entries, process all of them
PROCESS_MULTIPLE_COMPANIES = len(MULTIPLE_COMPANIES) > 0

# ===========================================
# CONFIGURATION EXAMPLES
# ===========================================

# Example 1: Single Company Processing
# DATE_RANGE = {"start_date": "06/15/2024", "end_date": "06/30/2024"}
# MULTIPLE_COMPANIES = []  # Empty list = single company
# ACTIVE_COMPANY = "los_cerros"

# Example 2: Multiple Companies Processing
# DATE_RANGE = {"start_date": "06/01/2024", "end_date": "06/30/2024"}
# MULTIPLE_COMPANIES = ["housecall_md", "los_cerros", "rocky_mountain"]
# ACTIVE_COMPANY = "los_cerros"  # This will be ignored when MULTIPLE_COMPANIES is not empty

# Example 3: Different Date Range
# DATE_RANGE = {"start_date": "05/01/2024", "end_date": "05/31/2024"}
# MULTIPLE_COMPANIES = ["los_cerros"]
# ACTIVE_COMPANY = "los_cerros"

# ===========================================
# USAGE INSTRUCTIONS
# ===========================================

# To use this system:
# 1. Set your desired date range in DATE_RANGE above
# 2. For single company: leave MULTIPLE_COMPANIES empty and set ACTIVE_COMPANY
# 3. For multiple companies: add company keys to MULTIPLE_COMPANIES list
# 4. Run: python main.py
# 5. No command line arguments needed!

# Available company keys: "triton_health_pllc_dr_sullivan", "chickasaw_nation_medical_center", "southeast_oklahoma_medical_clinic", "terry_draper_restore_family_medical_clinic", "tpch_practice_dr_tradewell"

# ===========================================
# COMPANY CONFIGURATIONS
# ===========================================

# Company configurations with their pg company IDs and helper IDs
# Updated with the 3 new companies for current pipeline processing
COMPANIES = {
"southeast_oklahoma_medical_clinic": {
    "name": "Southeast Oklahoma Medical Clinic - Dr. Richard Helton",
    "pg_company_id": "108bbba4-5d5d-41d9-b1c6-0eaac5538f6c",
    "helper_id": "handrewph14",
    "description": "Southeast Oklahoma Medical Clinic - Dr. Richard Helton - Medical clinic services"
},
"terry_draper_restore_family_medical_clinic": {
    "name": "Terry Draper / Restore Family Medical clinic",
    "pg_company_id": "be52e9cc-f825-4ff2-b336-508d6b9ad63b",
    "helper_id": "acooperph1020",
    "description": "Terry Draper / Restore Family Medical clinic - Family medical services"
},
"tpch_practice_dr_tradewell": {
    "name": "TPCH Practice/ Dr. Tradewell",
    "pg_company_id": "8e53f8ea-bb0b-472f-8560-0b9b4808c0fa",
    "helper_id": "handrewph12",
    "description": "TPCH Practice/ Dr. Tradewell - Medical practice services"
}
}


# Default company to use
DEFAULT_COMPANY = ""

# Active company setting - change this to switch companies
ACTIVE_COMPANY = "southeast_oklahoma_medical_clinic"  # Change this for each company

# Function to get companies to process
def get_companies_to_process():
    """Get list of companies to process based on configuration."""
    if PROCESS_MULTIPLE_COMPANIES and MULTIPLE_COMPANIES:
        return MULTIPLE_COMPANIES
    else:
        return [ACTIVE_COMPANY]

# Function to get date range
def get_date_range():
    """Get the configured date range."""
    return DATE_RANGE["start_date"], DATE_RANGE["end_date"]

# Function to set date range
def set_date_range(start_date, end_date):
    """Set the date range for processing."""
    global DATE_RANGE
    DATE_RANGE["start_date"] = start_date
    DATE_RANGE["end_date"] = end_date
    print(f"✅ Date range set to: {start_date} to {end_date}")

# Function to show current configuration
def show_current_config():
    """Show the current processing configuration."""
    print("🔧 Current Processing Configuration:")
    print("=" * 50)
    
    # Show date range
    start_date, end_date = get_date_range()
    print(f"📅 Date Range: {start_date} to {end_date}")
    
    # Show companies to process
    companies_to_process = get_companies_to_process()
    if len(companies_to_process) == 1:
        company = get_company_config(companies_to_process[0])
        company_key = companies_to_process[0]
        print(f"🏢 Single Company: {company['name']}")
        print(f"   PG Company ID: {company['pg_company_id']}")
        print(f"   Helper ID: {company['helper_id']}")
        
        # Show document type filtering status
        filter_config = get_document_type_filter(company_key)
        if filter_config['enabled']:
            print(f"   📄 Document Type Filter: ENABLED")
            print(f"      Allowed Types: {', '.join(filter_config['allowed_types'])}")
        else:
            print(f"   📄 Document Type Filter: DISABLED (process all types)")
    else:
        print(f"🏢 Multiple Companies ({len(companies_to_process)}):")
        for company_key in companies_to_process:
            company = get_company_config(company_key)
            print(f"   • {company['name']} ({company_key})")
            print(f"     PG Company ID: {company['pg_company_id']}")
            print(f"     Helper ID: {company['helper_id']}")
            
            # Show document type filtering status for each company
            filter_config = get_document_type_filter(company_key)
            if filter_config['enabled']:
                print(f"     📄 Document Type Filter: ENABLED")
                print(f"        Allowed Types: {', '.join(filter_config['allowed_types'])}")
            else:
                print(f"     📄 Document Type Filter: DISABLED")
    
    print("=" * 50)

# Function to get company configuration
def get_company_config(company_key=None):
    """Get configuration for a specific company or default company."""
    if company_key is None:
        company_key = DEFAULT_COMPANY
    
    if company_key not in COMPANIES:
        raise ValueError(f"Company '{company_key}' not found. Available companies: {list(COMPANIES.keys())}")
    
    return COMPANIES[company_key]

# Function to list all available companies
def list_companies():
    """List all available companies with their details."""
    print("Available Companies:")
    print("=" * 80)
    for key, company in COMPANIES.items():
        print(f"Key: {key}")
        print(f"Name: {company['name']}")
        print(f"PG Company ID: {company['pg_company_id']}")
        print(f"Helper ID: {company['helper_id']}")
        print(f"Description: {company['description']}")
        print("-" * 40)
    print(f"Default Company: {DEFAULT_COMPANY}")

# Function to get API URL for a specific company
def get_company_api_url(company_key=None):
    """Get the API URL for fetching existing orders for a specific company."""
    company = get_company_config(company_key)
    return f"https://dawavorderpatient-hqe2apddbje9gte0.eastus-01.azurewebsites.net/api/Order/pgcompany/{company['pg_company_id']}"

# Function to validate company key
def validate_company_key(company_key):
    """Validate if a company key exists."""
    return company_key in COMPANIES

# Function to get active company configuration
def get_active_company():
    """Get the currently active company configuration."""
    return get_company_config(ACTIVE_COMPANY)

# Function to set active company
def set_active_company(company_key):
    """Set the active company for processing."""
    global ACTIVE_COMPANY
    if validate_company_key(company_key):
        ACTIVE_COMPANY = company_key
        print(f"✅ Active company set to: {COMPANIES[company_key]['name']}")
        print(f"   PG Company ID: {COMPANIES[company_key]['pg_company_id']}")
        print(f"   Helper ID: {COMPANIES[company_key]['helper_id']}")
    else:
        raise ValueError(f"Invalid company key: {company_key}. Available: {list(COMPANIES.keys())}")

# Function to show current active company
def show_active_company():
    """Show the currently active company."""
    company = get_active_company()
    print(f"🏢 Active Company: {company['name']}")
    print(f"   PG Company ID: {company['pg_company_id']}")
    print(f"   Helper ID: {company['helper_id']}")
    print(f"   Description: {company['description']}")

# ===========================================
# DOCUMENT TYPE FILTERING CONFIGURATION
# ===========================================

# Document type filtering for the 3 companies in current pipeline
DOCUMENT_TYPE_FILTERS = {
    "infectious_diseases_consultants_oklahoma_city": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Infectious Diseases Consultants of Oklahoma City"
    },
    "pushmataha_family_medical_center": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Pushmataha Family Medical Center"
    },
    "crescent_infectious_diseases": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Crescent Infectious Diseases"
    }
}

def get_document_type_filter(company_key=None):
    """Get document type filter configuration for a specific company."""
    if company_key is None:
        company_key = ACTIVE_COMPANY
    
    return DOCUMENT_TYPE_FILTERS.get(company_key, {
        "enabled": False,
        "allowed_types": [],
        "description": "No filtering configured"
    })

def should_filter_document_types(company_key=None):
    """Check if document type filtering is enabled for a company."""
    filter_config = get_document_type_filter(company_key)
    return filter_config.get("enabled", False)

def get_allowed_document_types(company_key=None):
    """Get list of allowed document types for a company."""
    filter_config = get_document_type_filter(company_key)
    return filter_config.get("allowed_types", [])