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
# COMPANY CONFIGURATIONS
# ===========================================

# Company configurations with their pg company IDs and helper IDs
COMPANIES = {
    "housecall_md": {
        "name": "Housecall MD",
        "pg_company_id": "bc3a6a28-dd03-4cf3-95ba-2c5976619818",
        "helper_id": "dhelperph621",
        "description": "Housecall MD - Primary care and home health services"
    },
    "los_cerros": {
        "name": "Los Cerros Medical LLC",
        "pg_company_id": "9d8d2765-0b51-489b-868c-a217b4283c62",
        "helper_id": "ihelperph7221",
        "description": "Los Cerros Medical LLC - Medical services and consultations"
    },
    "rocky_mountain": {
        "name": "Rocky Mountain Medical and Healthcare",
        "pg_company_id": "4e594a84-7340-469e-82fb-b41b91930db5",
        "helper_id": "ihelperph4215",
        "description": "Rocky Mountain Medical and Healthcare - Comprehensive healthcare services"
    }
}

# Default company to use
DEFAULT_COMPANY = "housecall_md"

# Active company setting - change this to switch companies
ACTIVE_COMPANY = "housecall_md"  # Options: "housecall_md", "los_cerros", "rocky_mountain"

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