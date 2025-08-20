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
    "Authorization": "Bearer mlZUpFXHI8n35CZ7Coi5bjnAAg1czkvQpx0ofQ7rqqM3WPFQd5hgOOAfluxwlAYMCBGvNjeHrjiNcMDuKtheZYX7KUG_e4pT9k7cLJxD5YVdteKMp2tkwl402UGdPAV1Eqw05E0Vs19SLQYv-LLJ3jMELRUu5b5LNQYLkWWzaPsIRonDu1IFm5ulvXHzSmmQIbc7vnr17pq46VnXPoF5X1HhnqTyopYqFkl-3rEIlP-0JaWAJvMOF2QF77kztQVKwCkjoWmpNjxNv-bjgLm0vw"
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

# Supreme sheet (Excel builder) bulk processing configuration
SUPREME_SHEET_CONFIG = {
    "max_concurrent_requests": 10,
    "batch_size": 50,
    "request_timeout": 30,
    "max_retries": 3,
    "retry_backoff": 1.5
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
    "end_date": "08/18/2025", 
}

# Multiple companies to process (leave empty list for single company)
MULTIPLE_COMPANIES = [
    # Process ALL companies
    "housecallmd",
    "los_cerros",
    "paragon_medical_associates",
    "rocky_mountain",
    "brownfield_family_physicians",
    "applemd",
    "anand_balasubrimunium",
    "woundcentrics_llc",
    "visiting_practitioners_and_palliative",
    "responsive_infectious_diseases",
    "doctor_at_your_service",
    "san_antonio_family_physician",
    "ut_health_geriatrics",
    "centric_physicians_group",
    "goldstein_alasdair_md",
    "bsz_medical_pa",
    "boyer_family_practice",
    "diverse_care",
    "doctors_at_home_mary_snellings",
    "morning_star_healthcare",
    "spectrum_community_first",
    "royal_vp_llc",
    "citywide_housecalls",
    "prime_md_geriatrics",
    "americare_medical_group",
    "texas_infectious_disease",
    "atrium_housecall",
    "traveling_at_doctors",
    "ssm_health_bone_joint",
    "clinic_central_oklahoma",
    "ssm_health_shawnee",
    "community_physician_group",
    "infectious_diseases_okc",
    "pushmataha_family_medical",
    "crescent_infectious_diseases",
    "norman_regional_ortho_central",
    "triton_health_dr_sullivan",
    "internal_medicine_associates_okc",
    "chickasaw_nation_medical",
    "southeast_oklahoma_medical_clinic",
    "terry_draper_restore_family",
    "tpch_practice_dr_tradewell",
    "community_health_centers_oklahoma",
    "kates_lindsay_primary_care",
    "anibal_avila",
    "doctors_2_u",
    "covenant_care",
    "md_primary_care",
    "prima_care",
    "hawthorn",
    "trucare",
    "acohealth",
    "carney_hospital",
    "dr_resil_claude",
    "health_quality_primary_care",
    "caring",
    "bestself_primary_care",
    "care_dimension",
    "riverside_medical_group",
    "family_medical_associates",
    "upham",
    "orthopaedic_specialists_of_massachusetts",
    "lowell",
    "associates_in_internal_medicine_norwood",
    "northeast_medical_group",
    "new_bedford_internal_medicine",
    "boston_senior_medicine",
    "bidmc",
    "bowdoin",
    "saint_elizabeth_medical_centre",
    "neurology_center_of_new_england",
    "Total_Family_Healthcare"
]

# If MULTIPLE_COMPANIES is empty, use the active company
# If MULTIPLE_COMPANIES has entries, process all of them
PROCESS_MULTIPLE_COMPANIES = len(MULTIPLE_COMPANIES) > 0

# Selenium extractor limits for bulk page handling
EXTRACTOR_LIMITS = {
    "signed_max_pages": 200,
    "inbox_max_pages": 200,
    "max_consecutive_no_new": 8,
    "npi_batch_size": 50
}

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

# Available company keys: "housecall_md", "los_cerros", "paragon_medical_associates"

# ===========================================
# COMPANY CONFIGURATIONS
# ===========================================

# Company configurations with their pg company IDs and helper IDs
COMPANIES = {
    "housecallmd": {
        "name": "HousecallMD",
        "pg_company_id": "bc3a6a28-dd03-4cf3-95ba-2c5976619818",
        "helper_id": "dhelperph621",
        "description": "HousecallMD",
        "extraction_sources": ["inbox", "signed"]
    },
    "los_cerros": {
        "name": "Los Cerros",
        "pg_company_id": "9d8d2765-0b51-489b-868c-a217b4283c62",
        "helper_id": "ihelperph7221",
        "description": "Los Cerros",
        "extraction_sources": ["inbox", "signed"]
    },
    "paragon_medical_associates": {
        "name": "Paragon Medical Associates",
        "pg_company_id": "84e35202-3422-4de4-b5cb-efe5461b1312",
        "helper_id": "ihelperph4215",
        "description": "Paragon Medical Associates",
        "extraction_sources": ["inbox", "signed"]
    },
    "rocky_mountain": {
        "name": "Rocky Mountain",
        "pg_company_id": "4e594a84-7340-469e-82fb-b41b91930db5",
        "helper_id": "ihelperph4215",
        "description": "Rocky Mountain",
        "extraction_sources": ["inbox", "signed"]
    },
    "brownfield_family_physicians": {
        "name": "Brownfield Family Physicians",
        "pg_company_id": "b62429db-642c-4fdb-9bf3-29e381d20e22",
        "helper_id": "dallianceph7218",
        "description": "Brownfield Family Physicians",
        "extraction_sources": ["inbox", "signed"]
    },
    "applemd": {
        "name": "APPLEMD",
        "pg_company_id": "83de8c79-1a28-4d0b-90bc-5deaf95949e5",
        "helper_id": "ihelperph1024",
        "description": "APPLEMD",
        "extraction_sources": ["inbox", "signed"]
    },
    "anand_balasubrimunium": {
        "name": "ANAND BALASUBRIMUNIUM",
        "pg_company_id": "1726e467-f4b0-4c11-b2b7-a39eb1328d91",
        "helper_id": "jmonroyph",
        "description": "ANAND BALASUBRIMUNIUM",
        "extraction_sources": ["inbox", "signed"]
    },
    "woundcentrics_llc": {
        "name": "WoundCentrics, LLC",
        "pg_company_id": "0367ce80-57a9-45e7-8afe-18f665a6a640",
        "helper_id": "tthriftph1024",
        "description": "WoundCentrics, LLC",
        "extraction_sources": ["inbox", "signed"]
    },
    "visiting_practitioners_and_palliative": {
        "name": "Visiting Practitioners And Palliative Care LLC",
        "pg_company_id": "f6464e98-d46b-4c7a-a9bc-254c02aa8e1c",
        "helper_id": "ihelperph1242",
        "description": "Visiting Practitioners And Palliative Care LLC",
        "extraction_sources": ["inbox", "signed"]
    },
    "responsive_infectious_diseases": {
        "name": "Responsive Infectious Diseases Solutions",
        "pg_company_id": "ee74f247-b46e-480c-a4e4-9ae6b8a5dc35",
        "helper_id": "ihelperph10201",
        "description": "Responsive Infectious Diseases Solutions",
        "extraction_sources": ["inbox", "signed"]
    },
    "doctor_at_your_service": {
        "name": "Doctor at your service",
        "pg_company_id": "e749dda4-60ab-48d3-afc6-728a15d74182",
        "helper_id": "ihelperph421",
        "description": "Doctor at your service",
        "extraction_sources": ["inbox", "signed"]
    },
    "san_antonio_family_physician": {
        "name": "san antonio family phsician",
        "pg_company_id": "6c2414e8-b2d3-4d94-953e-967a64c31488",
        "helper_id": "dallianceph11211",
        "description": "san antonio family phsician",
        "extraction_sources": ["inbox", "signed"]
        
    },
    "ut_health_geriatrics": {
        "name": "UT Health Geriatrics & Supportive Care Clinic",
        "pg_company_id": "b50483ad-042c-4d64-96d7-4427c7862f9e",
        "helper_id": "mramirezph821",
        "description": "UT Health Geriatrics & Supportive Care Clinic",
        "extraction_sources": ["inbox", "signed"]
    },
    "centric_physicians_group": {
        "name": "Centric Physicians Group",
        "pg_company_id": "5bce99a2-a71f-48e4-9c06-c16d9ab78ad5",
        "helper_id": "ihelperph6221",
        "description": "Centric Physicians Group",
        "extraction_sources": ["inbox", "signed"]
    },
    "goldstein_alasdair_md": {
        "name": "Goldstein Alasdair MD",
        "pg_company_id": "ea4c3d61-28fa-473f-8bdd-685075343711",
        "helper_id": "ihelperph3243",
        "description": "Goldstein Alasdair MD",
        "extraction_sources": ["inbox", "signed"]
    },
    "bsz_medical_pa": {
        "name": "BSZ Medical PA",
        "pg_company_id": "3e387a9f-5535-4984-9419-483bed5e63f1",
        "helper_id": "ihelperph1020",
        "description": "BSZ Medical PA",
        "extraction_sources": ["inbox", "signed"]
    },
    "boyer_family_practice": {
        "name": "Boyer family practice",
        "pg_company_id": "8b15ea65-269e-412f-88ce-785959be023f",
        "helper_id": "aboyerph724",
        "description": "Boyer family practice",
        "extraction_sources": ["inbox", "signed"]
    },
    "diverse_care": {
        "name": "Diverse care",
        "pg_company_id": "daf14002-92e8-4024-b6bf-62cd1a2f8606",
        "helper_id": "ihelperph32412",
        "description": "Diverse care",
        "extraction_sources": ["inbox", "signed"]
    },
    "doctors_at_home_mary_snellings": {
        "name": "Doctors at Home - Mary Snellings MD",
        "pg_company_id": "de385408-1cd6-46a2-be58-ff4b8eeeddc7",
        "helper_id": "handrewph27",
        "description": "Doctors at Home - Mary Snellings MD",
        "extraction_sources": ["inbox", "signed"]
    },
    "morning_star_healthcare": {
        "name": "Morning Star Healthcare Services PA",
        "pg_company_id": "9c9bd7d0-bd70-4197-98f7-a77b4e781ab1",
        "helper_id": "ihelperph722",
        "description": "Morning Star Healthcare Services PA",
        "extraction_sources": ["inbox", "signed"]
    },
    "spectrum_community_first": {
        "name": "Spectrum (Community First Primary Care)",
        "pg_company_id": "6f4180aa-b472-4d5c-b7aa-98e06bb4fd6f",
        "helper_id": "ihelperph12232",
        "description": "Spectrum (Community First Primary Care)",
        "extraction_sources": ["inbox", "signed"]
    },
    "royal_vp_llc": {
        "name": "Royal V.P., LLC",
        "pg_company_id": "eaba3c1c-217f-458d-aa2f-172e3ffbab1e",
        "helper_id": "ihelperph4214",
        "description": "Royal V.P., LLC",
        "extraction_sources": ["inbox", "signed"]
    },
    "citywide_housecalls": {
        "name": "CityWide Housecalls, LLC",
        "pg_company_id": "534ca7a5-2db0-4c75-8988-89f73064c5e5",
        "helper_id": "ihelperph1021",
        "description": "CityWide Housecalls, LLC",
        "extraction_sources": ["inbox", "signed"]
    },
    "prime_md_geriatrics": {
        "name": "Prime MD Geriatrics",
        "pg_company_id": "ef8847e7-ed2a-4dc0-a08b-49b1d6b2b5f7",
        "helper_id": "ihelperph22476",
        "description": "Prime MD Geriatrics",               
        "extraction_sources": ["inbox", "signed"]
    },
    "americare_medical_group": {
        "name": "Americare Medical Group",
        "pg_company_id": "c147e1f1-ccdb-4e22-8526-60a93ad4a678",
        "helper_id": "ihelperph11203",
        "description": "Americare Medical Group",
        "extraction_sources": ["inbox", "signed"]
    },
    "texas_infectious_disease": {
        "name": "Texas Infectious Disease Institute",
        "pg_company_id": "a3b8a6c5-db61-42b4-8eee-64e1098c0336",
        "helper_id": "ihelperph32413",
        "description": "Texas Infectious Disease Institute",
        "extraction_sources": ["inbox", "signed"]
    },
    "atrium_housecall": {
        "name": "Atrium HouseCall",
        "pg_company_id": "bb158a70-b51a-4008-9600-e94484485b61",
        "helper_id": "ihelperph10221",
        "description": "Atrium HouseCall",
        "extraction_sources": ["inbox", "signed"]
    },
    "traveling_at_doctors": {
        "name": "Traveling at doctors",
        "pg_company_id": "8cd766e5-6e19-492e-a1a9-6595d81d20ee",
        "helper_id": "ihelperph525",
        "description": "Traveling at doctors",
        "extraction_sources": ["inbox", "signed"]
    },
    "ssm_health_bone_joint": {
        "name": "SSM Health Bone & Joint Hospital.",
        "pg_company_id": "3bc728e7-6839-4807-92ed-bb6c712020de",
        "helper_id": "ihelperph3232",
        "description": "SSM Health Bone & Joint Hospital.",
        "extraction_sources": ["inbox", "signed"]
    },
    "clinic_central_oklahoma": {
        "name": "The Clinic @ Central Oklahoma Family Medical Center",
        "pg_company_id": "3642cb84-6d4f-492c-8be1-4dd388bcea19",
        "helper_id": "dallianceph9212",
        "description": "The Clinic @ Central Oklahoma Family Medical Center",
        "extraction_sources": ["inbox", "signed"]
    },
    "ssm_health_shawnee": {
        "name": "SSM Health Shawnee",
        "pg_company_id": "ee54c7f2-a7ba-4b9a-90b0-7df96330b9f7",
        "helper_id": "ihelperph323",
        "description": "SSM Health Shawnee",
        "extraction_sources": ["inbox", "signed"]
    },
    "community_physician_group": {
        "name": "Community Physician Group-CPG Clinics",
        "pg_company_id": "45d72b92-6c6c-4bef-84f0-a36852d5f868",
        "helper_id": "ihelperph11201",
        "description": "Community Physician Group-CPG Clinics",
        "extraction_sources": ["inbox", "signed"]
    },
    "infectious_diseases_okc": {
        "name": "Infectious Diseases Consultants of Oklahoma City- (Idcokc)",
        "pg_company_id": "198e2b2d-c22a-415d-9ebd-9656091d0308",
        "helper_id": "ihelperph9223",
        "description": "Infectious Diseases Consultants of Oklahoma City- (Idcokc)",
        "extraction_sources": ["inbox", "signed"]
    },
    "pushmataha_family_medical": {
        "name": "Pushmataha Family Medical Center",
        "pg_company_id": "ecad2da6-91a7-4e26-8152-58d588eab134",
        "helper_id": "ihelperph9221",
        "description": "Pushmataha Family Medical Center",
        "extraction_sources": ["inbox", "signed"]
    },
    "crescent_infectious_diseases": {
        "name": "Crescent Infectious Diseases",
        "pg_company_id": "f86dc96a-777c-4bdc-ae87-f147b1e5568e",
        "helper_id": "handrewph7202",
        "description": "Crescent Infectious Diseases",
        "extraction_sources": ["inbox", "signed"]
    },
    "norman_regional_ortho_central": {
        "name": "Norman Regional - Ortho Central",
        "pg_company_id": "3c002ed5-f9b5-4d07-914a-4856c268c977",
        "helper_id": "ihelperph22459",
        "description": "Norman Regional - Ortho Central",               
        "extraction_sources": ["inbox", "signed"]
    },
    "triton_health_dr_sullivan": {
        "name": "Triton Health PLLC Dr. Sullivan, Cary",
        "pg_company_id": "d09df8cc-a549-4229-a03a-ce29fb09aea2",
        "helper_id": "handrewph19",
        "description": "Triton Health PLLC Dr. Sullivan, Cary",
        "extraction_sources": ["inbox", "signed"]
    },
    "internal_medicine_associates_okc": {
        "name": "Internal Medicine Associates OKC",
        "pg_company_id": "c6ad87d9-79de-49bd-aa0a-6ef01400a83d",
        "helper_id": "ihelperph7215",
        "description": "Internal Medicine Associates OKC",
        "extraction_sources": ["inbox", "signed"]
    },
    "chickasaw_nation_medical": {
        "name": "Chickasaw Nation Medical Center",
        "pg_company_id": "e8f2df67-c5a5-4c74-9daa-d9b41d8eb5d7",
        "helper_id": "michaelph1",
        "description": "Chickasaw Nation Medical Center",
        "extraction_sources": ["inbox", "signed"]
    },
    "southeast_oklahoma_medical_clinic": {
        "name": "Southeast Oklahoma Medical Clinic - Dr. Richard Helton",
        "pg_company_id": "108bbba4-5d5d-41d9-b1c6-0eaac5538f6c",
        "helper_id": "handrewph14",
        "description": "Southeast Oklahoma Medical Clinic - Dr. Richard Helton",
        "extraction_sources": ["inbox", "signed"]
    },
    "terry_draper_restore_family": {
        "name": "Terry Draper / Restore Family Medical clinic",
        "pg_company_id": "be52e9cc-f825-4ff2-b336-508d6b9ad63b",
        "helper_id": "acooperph1020",
        "description": "Terry Draper / Restore Family Medical clinic",
        "extraction_sources": ["inbox", "signed"]
    },
    "tpch_practice_dr_tradewell": {
        "name": "TPCH Practice/ Dr. Tradewell",
        "pg_company_id": "8e53f8ea-bb0b-472f-8560-0b9b4808c0fa",
        "helper_id": "handrewph12",
        "description": "TPCH Practice/ Dr. Tradewell",
        "extraction_sources": ["inbox", "signed"]
    },
    "community_health_centers_oklahoma": {
        "name": "Community Health Centers,Inc Oklahoma",
        "pg_company_id": "69f909d4-b4c5-4d8a-8d2e-eb52d467ef3c",
        "helper_id": "ihelperph22478",
        "description": "Community Health Centers,Inc Oklahoma",
        "extraction_sources": ["inbox", "signed"]
    },
    "kates_lindsay_primary_care": {
        "name": "KATES, LINDSAY / Primary care of Ada",
        "pg_company_id": "2aeb18f5-4461-496d-8f74-66ba6f269cd3",
        "helper_id": "handrewph44",
        "description": "KATES, LINDSAY / Primary care of Ada",
        "extraction_sources": ["inbox", "signed"]
    },
    "anibal_avila": {
        "name": "Anibal Avila MA P,C",
        "pg_company_id": "13c9e1d2-fbde-498a-b384-f530c29d0745",
        "helper_id": "handrewph8",
        "description": "Anibal Avila MA P,C",
        "extraction_sources": ["inbox", "signed"]
    },
    "doctors_2_u": {
        "name": "Doctors 2 U",
        "pg_company_id": "ced25ca7-8e1e-401b-b8fe-d181f688ac90",
        "helper_id": "dallianceph9213",
        "description": "Doctors 2 U",
        "extraction_sources": ["inbox", "signed"]
    },
    "covenant_care": {
        "name": "Covenant care",
        "pg_company_id": "ec35b120-0883-4d1f-b63d-89bd43d6d89e",
        "helper_id": "ihelperph7241",
        "description": "Covenant care",
        "extraction_sources": ["inbox", "signed"]
    },
    "md_primary_care": {
        "name": "MD Primary care",
        "pg_company_id": "29e46ad6-8ca8-400b-b049-48c17c0b831d",
        "helper_id": "ihelperph5211",
        "description": "MD Primary care",
        "extraction_sources": ["inbox", "signed"]
    },
    "prima_care": {
        "name": "Prima CARE",
        "pg_company_id": "d10f46ad-225d-4ba2-882c-149521fcead5",
        "helper_id": "ihelperph6233",
        "description": "Prima CARE",
        "extraction_sources": ["inbox", "signed"]
    },
    "hawthorn": {
        "name": "Hawthorn",
        "pg_company_id": "4b51c8b7-c8c4-4779-808c-038c057f026b",
        "helper_id": "ihelperph7243",
        "description": "Hawthorn",      
        "extraction_sources": ["inbox", "signed"]
    },
    "trucare": {
        "name": "Trucare",
        "pg_company_id": "7c40b6f6-5874-4ab8-96d4-e03b0d2f8201",
        "helper_id": "ihelperph7244",
        "description": "Trucare",
        "extraction_sources": ["inbox", "signed"]
    },
    "acohealth": {
        "name": "AcoHealth",
        "pg_company_id": "d074279d-8ff6-47ab-b340-04f21c0f587e",
        "helper_id": "dallianceph125",
        "description": "AcoHealth",
        "extraction_sources": ["inbox", "signed"]
    },
    "carney_hospital": {
        "name": "Carney Hospital",
        "pg_company_id": "14761337-cd76-4e76-8bdd-18a96465624e",
        "helper_id": "ihelperph6231",
        "description": "Carney Hospital",
        "extraction_sources": ["inbox", "signed"]
    },
    "dr_resil_claude": {
        "name": "Dr. Resil Claude",
        "pg_company_id": "042a7278-25b6-4a9b-a18d-1981ab0daf11",
        "helper_id": "ihelperph8244",
        "description": "Dr. Resil Claude",
        "extraction_sources": ["inbox", "signed"]
    },
    "health_quality_primary_care": {
        "name": "Health Quality Primary Care",
        "pg_company_id": "f0d98fdc-c432-4e05-b75e-af146aa0e27d",
        "helper_id": "ihelperph7245",
        "description": "Health Quality Primary Care",
        "extraction_sources": ["inbox", "signed"]
    },
    "caring": {
        "name": "Caring",
        "pg_company_id": "03657233-8677-4c81-92c8-c19c3f64fc84",
        "helper_id": "ihelperph524",
        "description": "Caring",
        "extraction_sources": ["inbox", "signed"]
    },
    "bestself_primary_care": {
        "name": "BestSelf Primary Care",
        "pg_company_id": "c5c1a894-08ac-4cb9-bfd1-0ad1384b890e",
        "helper_id": "ihelperph125",
        "description": "BestSelf Primary Care",
        "extraction_sources": ["inbox", "signed"]
    },
    "care_dimension": {
        "name": "CARE DIMENSION",
        "pg_company_id": "da7d760b-e3a8-4c92-9006-eca464ce8e1e",
        "helper_id": "ihelperph225",
        "description": "CARE DIMENSION",
        "extraction_sources": ["inbox", "signed"]
    },
    "riverside_medical_group": {
        "name": "Riverside Medical Group",
        "pg_company_id": "ca5314fe-cf71-42e5-9482-81507666328c",
        "helper_id": "ihelperph2232",
        "description": "Riverside Medical Group",
        "extraction_sources": ["inbox", "signed"]
    },
    "family_medical_associates": {
        "name": "Family medical associates",
        "pg_company_id": "38511e46-cc15-4856-92bc-718c5ec56cbf",
        "helper_id": "ihelperph5236",
        "description": "Family medical associates",
        "extraction_sources": ["inbox", "signed"]
    },
    "upham": {
        "name": "Upham",
        "pg_company_id": "acfcd97b-0533-4c95-9f5d-4744c5f9c64c",
        "helper_id": "ihelperph2236",
        "description": "Upham",
        "extraction_sources": ["inbox", "signed"]
    },
    "orthopaedic_specialists_of_massachusetts": {
        "name": "Orthopaedic Specialists of Massachusetts",
        "pg_company_id": "cdabc85a-9c13-4fae-9dbf-d2e22e12f466",
        "helper_id": "ihelperph2234",
        "description": "Orthopaedic Specialists of Massachusetts",          
        "extraction_sources": ["inbox", "signed"]
    },
    "lowell": {
        "name": "Lowell",
        "pg_company_id": "b92e8240-61f7-475f-8cbe-f1442b6389b5",
        "helper_id": "ihelperph22499",
        "description": "Lowell",
        "extraction_sources": ["inbox", "signed"]
    },
    "associates_in_internal_medicine_norwood": {
        "name": "Associates in Internal Medicine - Norwood",
        "pg_company_id": "0245a889-31da-445b-9f1e-51f97ea6d37e",
        "helper_id": "ihelperph5237",
        "description": "Associates in Internal Medicine - Norwood",
        "extraction_sources": ["inbox", "signed"]
    },
    "northeast_medical_group": {
        "name": "Northeast Medical Group",
        "pg_company_id": "e7ca529f-bc5e-4706-b61f-0f682a3f6e23",
        "helper_id": "ihelperph325",
        "description": "Northeast Medical Group",
        "extraction_sources": ["inbox", "signed"]
    },
    "new_bedford_internal_medicine": {
        "name": "New Bedford Internal Medicine and Geriatrics",
        "pg_company_id": "716be0f8-9710-4fee-90b2-09dc30f229c9",
        "helper_id": "ihelperph10231",
        "description": "New Bedford Internal Medicine and Geriatrics",
        "extraction_sources": ["inbox", "signed"]
    },
    "boston_senior_medicine": {
        "name": "Boston Senior Medicine",
        "pg_company_id": "61e6dd93-452b-41b0-aca4-8d67fbe71e78",
        "helper_id": "ihelperph523",
        "description": "Boston Senior Medicine",
        "extraction_sources": ["inbox", "signed"]
    },
    "bidmc": {
        "name": "BIDMC",
        "pg_company_id": "0c2c11e0-ce99-4282-9172-7d06c7a12dda",
        "helper_id": "ihelperph112326",
        "description": "BIDMC",
        "extraction_sources": ["inbox", "signed"]
    },
    "bowdoin": {
        "name": "Bowdoin",
        "pg_company_id": "5f173aaa-338d-4510-9d2d-c856d8771aa8",
        "helper_id": "ihelperph2233",
        "description": "Bowdoin",
        "extraction_sources": ["inbox", "signed"]
    },
    "saint_elizabeth_medical_centre": {
        "name": "St. Elizabeth Medical Center Orthopedics",
        "pg_company_id": "ceece087-093e-421d-92c0-b1aff03405e6",
        "helper_id": "ihelperph5238",
        "description": "St. Elizabeth Medical Center Orthopedics",
        "extraction_sources": ["inbox", "signed"]
    },
    "neurology_center_of_new_england": {
        "name": "Neurology Center Of New England, PC",
        "pg_company_id": "c0926069-e956-4ed5-8775-1f462f6cff36",
        "helper_id": "ihelperph7247",
        "description": "Neurology Center Of New England, PC",
        "extraction_sources": ["inbox", "signed"]
    },
    "Total_Family_Healthcare": {
        "name": "Total Family Healthcare Clinic PLLC",
        "pg_company_id": "7ec965fe-9777-4d52-8124-b056b4d90224",
        "helper_id": "ihelperph825",
        "description": "Total Family Healthcare Clinic PLLC",
        "extraction_sources": ["inbox", "signed"]
    },

}


# Default company to use
DEFAULT_COMPANY = ""

# Active company setting - change this to switch companies  
ACTIVE_COMPANY = "hawthorn"  # Hawthorn

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

# Document type filtering for the companies in current pipeline
# Defaults: enable filtering and exclude CONVERSION for all companies
DOCUMENT_TYPE_FILTERS = {
    key: {
        "enabled": True,
        "allowed_types": [],
        "excluded_types": ["CONVERSATION"],
        "description": f"Default document type filter for {COMPANIES[key]['name']}"
    }
    for key in COMPANIES.keys()
}

# Overrides for specific companies (if present)
DOCUMENT_TYPE_FILTERS["visiting_practitioners_and_palliative"] = {
    "enabled": True,
    "allowed_types": ["485", "485CERT", "RECERT"],
    "excluded_types": ["CONVERSION"],
    "description": "Focus on 485 family documents for Visiting Practitioners And Palliative Care LLC"
}

DOCUMENT_TYPE_FILTERS["anibal_avila"] = {
    "enabled": True,
    "allowed_types": ["485", "485CERT", "RECERT"],
    "excluded_types": ["CONVERSION"],
    "description": "Focus on 485 family documents for Anibal Avila PG"
}

# Grace At Home: focus on 485 family documents as well
DOCUMENT_TYPE_FILTERS["grace_at_home"] = {
    "enabled": True,
    "allowed_types": [],
    "excluded_types": ["CONVERSATION"],
    "description": "Focus on 485 family documents for Grace At Home"
}

def get_document_type_filter(company_key=None):
    """Get document type filter configuration for a specific company."""
    if company_key is None:
        company_key = ACTIVE_COMPANY
    
    return DOCUMENT_TYPE_FILTERS.get(company_key, {
        "enabled": False,
        "allowed_types": [],
        "excluded_types": [],
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

def get_excluded_document_types(company_key=None):
    """Get list of excluded document types for a company."""
    filter_config = get_document_type_filter(company_key)
    return filter_config.get("excluded_types", [])

def get_extraction_sources(company_key=None):
    """Get list of extraction sources (inbox/signed) for a company."""
    if company_key is None:
        company_key = ACTIVE_COMPANY
    
    company_config = COMPANIES.get(company_key, {})
    return company_config.get("extraction_sources", ["inbox", "signed"])  # Default to both

def should_extract_from_inbox(company_key=None):
    """Check if extraction from inbox is enabled for a company."""
    sources = get_extraction_sources(company_key)
    return "inbox" in sources

def should_extract_from_signed(company_key=None):
    """Check if extraction from signed is enabled for a company."""
    sources = get_extraction_sources(company_key)
    return "signed" in sources