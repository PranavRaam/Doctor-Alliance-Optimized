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
    "Authorization": "Bearer BwmWBqhXAEvG70Irt_1J8kJM8_4p81dStSUAeWXFho6d-Fu2Ymsox3qFLaQgZcX_EA-JjYi_MpiDS5FzulJ6hw2Qne5DearMdRfkkS_E8GaG5fy82RI_YhwM1cn-VtTQG5FSAjUPukOuJri8lPjQUZS1vzh9bRd3f3FQQlJxwzMKDfrqkt_03SR70bjDsKA9KYdJibMr5DBpaUkyJNzATdlewBwkeGEnX4EfzRj_mn_gm_G7Pjdo2qCCXbDhGeuH5lLuKvqFciQy_Wb8TEOR7Q"
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

# ACCURACY-FIRST Configuration
QDRANT_CONFIG = {
    "hnsw_ef_construct": 400,    # Higher for better index quality
    "hnsw_m": 64,                # More connections for better accuracy
    "hnsw_ef_search": 200,       # Higher search quality
    "quantization_enabled": False, # Disable quantization for max accuracy
    "oversampling": 1.0,         # No oversampling needed without quantization
    "rescore": True,             # Always rescore for accuracy
    "top_k_retrieval": 10        # Retrieve more context
}

# Balanced Download Configuration (reasonable speed)
DOWNLOAD_CONFIG = {
    "max_concurrent_downloads": 6,   # Reduced for stability
    "chunk_size": 8192,
    "timeout": 45,                   # Longer timeout for reliability
    "max_retries": 5,                # More retries for robustness
    "retry_backoff": 3,              # Longer backoff
    "connection_pool_size": 15,
    "use_async": True,
}

# ACCURACY-FOCUSED Text Extraction Configuration
EXTRACTION_CONFIG = {
    "max_concurrent_extractions": 3,    # Lower concurrency for accuracy
    "quality_threshold": 85,            # High quality threshold
    "comprehensive_testing": True,      # Test all methods thoroughly
    "ocr_fallback_threshold": 60,       # Lower threshold for OCR fallback
    "multi_pass_extraction": True,      # Multiple extraction attempts
    "text_validation_enabled": True,   # Validate extracted text
    "medical_field_validation": True,   # Medical-specific validation
    "extraction_timeout": 120,         # Longer timeout per document
}

# Enhanced Field Extraction Configuration
FIELD_EXTRACTION_CONFIG = {
    "max_retries": 8,               # More retries for field extraction
    "chunk_overlap": 300,           # More overlap for context
    "multi_model_validation": True, # Use multiple models for validation
    "field_confidence_threshold": 0.7, # Confidence threshold for fields
    "cross_validation_enabled": True,  # Cross-validate extracted fields
    "medical_context_enhancement": True, # Enhanced medical context
    "structured_prompting": True,   # Use structured prompting techniques
}

# ===========================================
# DATE RANGE CONFIGURATION
# ===========================================

# Date range for processing (MM/DD/YYYY format)
DATE_RANGE = {
    "start_date": "07/01/2025",
    "end_date": "07/26/2025",
}

# Multiple companies to process (leave empty list for single company)
MULTIPLE_COMPANIES = [
    # Uncomment and modify the companies you want to process
    # "housecall_md",
    # "los_cerros", 
    # "rocky_mountain"
    # "prima_care"
    "clinics_of_north_texas",
    "doctors_at_home",
    "spectrum_health_solutions",
    "royal_vp",
    "citywide_housecalls"
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

# Available company keys: "clinics_of_north_texas", "doctors_at_home", "spectrum_health_solutions", "royal_vp", "citywide_housecalls"

# ===========================================
# COMPANY CONFIGURATIONS
# ===========================================

# Company configurations with their pg company IDs and helper IDs
COMPANIES = {
    "clinics_of_north_texas": {
        "name": "CLINICS OF NORTH TEXAS LLP",
        "pg_company_id": "398b825d-5e6d-4a86-88df-79992a36f536",
        "helper_id": "sbensonph924",
        "description": "CLINICS OF NORTH TEXAS LLP - Medical clinics and healthcare services"
    },
    "doctors_at_home": {
        "name": "Doctors at Home - Mary Snellings MD",
        "pg_company_id": "de385408-1cd6-46a2-be58-ff4b8eeeddc7",
        "helper_id": "handrewph27",
        "description": "Doctors at Home - Mary Snellings MD - Home healthcare services"
    },
    "spectrum_health_solutions": {
        "name": "Spectrum Health Solutions (Community First Primary Care)",
        "pg_company_id": "6f4180aa-b472-4d5c-b7aa-98e06bb4fd6f",
        "helper_id": "ihelperph12232",
        "description": "Spectrum Health Solutions (Community First Primary Care) - Primary care services"
    },
    "royal_vp": {
        "name": "Royal V.P., LLC",
        "pg_company_id": "eaba3c1c-217f-458d-aa2f-172e3ffbab1e",
        "helper_id": "ihelperph4214",
        "description": "Royal V.P., LLC - Healthcare and medical services"
    },
    "citywide_housecalls": {
        "name": "CityWide Housecalls, LLC",
        "pg_company_id": "534ca7a5-2db0-4c75-8988-89f73064c5e5",
        "helper_id": "ihelperph1021",
        "description": "CityWide Housecalls, LLC - House call medical services"
    }
}

# Default company to use
DEFAULT_COMPANY = ""

# Active company setting - change this to switch companies
ACTIVE_COMPANY = ""  # Options: "housecall_md", "los_cerros", "rocky_mountain"

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

# Document type filtering for specific companies
DOCUMENT_TYPE_FILTERS = {
    "grace_at_home": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Grace at Home"
    },
    "covenant_care": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Covenant Care"
    },
    "md_primary_care": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for MD Primary Care"
    },
    "prima_care": {
        "enabled": True,
        "allowed_types": ["485", "CERT", "RECERT", "485CERT", "485RECERT", "485CERT", "485 CERT"],
        "description": "Only process 485 document types for Prima Care"
    },
    "housecall_md": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Housecall MD"
    },
    "los_cerros": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Los Cerros"
    },
    "rocky_mountain": {
        "enabled": False,
        "allowed_types": [],
        "description": "Process all document types for Rocky Mountain"
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