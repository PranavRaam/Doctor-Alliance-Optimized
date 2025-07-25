import re
import requests
from datetime import datetime
from typing import Tuple, Optional, List, Dict, Any
from enum import Enum
from dataclasses import dataclass

class ExtractionQuality(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    FAILED = "failed"

@dataclass
class ExtractionResult:
    text: str
    method: str
    quality_score: float
    confidence: float
    error: str = ""
    metrics: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metrics is None:
            self.metrics = {}

@dataclass
class FieldExtractionResult:
    fields: Dict[str, Any]
    confidence: float
    method: str
    validation_errors: List[str] = None
    quality: ExtractionQuality = ExtractionQuality.FAILED
    
    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []

def is_mostly_garbage(text, threshold=0.6):
    """Check if text is mostly garbage/unreadable characters."""
    if not text: 
        return True
    printable = sum(32 <= ord(c) <= 126 for c in text)
    ratio = printable / max(1, len(text))
    return ratio < threshold

def is_encoded_pdf(text):
    """Check if PDF text contains encoded characters."""
    cid_count = text.count('(cid:')
    total_words = len(text.split())
    return cid_count > 0 and (cid_count / max(1, total_words)) > 0.01

def clean_order_number(val):
    """Clean and validate order number to be alphanumeric only."""
    if not val:
        return None
    
    # Remove all non-alphanumeric characters
    cleaned = re.sub(r'[^A-Za-z0-9]', '', str(val))
    
    if not cleaned or len(cleaned) < 3:
        return None
    
    return cleaned

def clean_mrn(val):
    """Enhanced MRN cleaning with stricter validation."""
    if not val:
        return None
    
    # Remove all non-alphanumeric characters
    cleaned = re.sub(r'[^A-Za-z0-9]', '', str(val))
    
    # Must be more than 3 characters and contain at least one digit
    if not cleaned or len(cleaned) <= 3:
        return None
    
    # Should contain at least one number for medical record validation
    if not any(c.isdigit() for c in cleaned):
        return None
    
    return cleaned

def validate_order_number(order_no: str) -> Tuple[bool, str]:
    """Validate order number format."""
    if not order_no:
        return False, "Order number is empty"
    
    # Clean order number
    cleaned_order = clean_order_number(order_no)
    
    if not cleaned_order:
        return False, "Order number too short or invalid characters"
    
    if len(cleaned_order) > 20:  # Reasonable max length
        return False, "Order number too long"
    
    return True, "Valid order number format"

def standardize_patient_sex(value):
    """Standardize patient sex values."""
    if not value:
        return ""
    val = str(value).strip().lower()
    if val in ["male", "m"]:
        return "MALE"
    if val in ["female", "f"]:
        return "FEMALE"
    return ""

def validate_icd10(icd_code):
    """Validate ICD-10 codes using external API."""
    url = f"http://www.icd10api.com/?code={icd_code}&r=json&desc=long&type=cm"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200 and resp.json().get("Description"):
            return resp.json()["Description"]
    except Exception:
        pass
    return None

class TextQualityAnalyzer:
    """Advanced text quality analysis for medical documents."""
    
    @staticmethod
    def analyze_comprehensive(text: str) -> Dict[str, Any]:
        """Comprehensive text quality analysis."""
        if not text:
            return {
                "score": 0, "quality": ExtractionQuality.FAILED,
                "printable_ratio": 0, "length": 0, "word_count": 0,
                "medical_indicators": 0, "structure_score": 0, "completeness": 0
            }
        
        # Basic metrics
        length = len(text)
        printable_chars = sum(32 <= ord(c) <= 126 for c in text)
        printable_ratio = printable_chars / length if length > 0 else 0
        word_count = len(text.split())
        
        # Medical document indicators
        medical_keywords = [
            'patient', 'diagnosis', 'icd', 'medication', 'treatment', 'doctor',
            'physician', 'medical', 'hospital', 'clinic', 'order', 'prescription',
            'mrn', 'dob', 'address', 'insurance', 'provider', 'care', 'service',
            'therapeutic', 'clinical', 'assessment', 'evaluation', 'procedure'
        ]
        
        medical_patterns = [
            r'\b\d{2}/\d{2}/\d{4}\b',  # Dates MM/dd/yyyy
            r'\b[A-Z]\d{6,}\b',         # MRN patterns
            r'\b[A-Z]\d{2}\.\d{1,2}\b', # ICD-10 patterns
            r'\bDOB\b|\bMRN\b|\bSOC\b', # Common medical abbreviations
        ]
        
        text_lower = text.lower()
        medical_keywords_found = sum(1 for keyword in medical_keywords if keyword in text_lower)
        medical_patterns_found = sum(1 for pattern in medical_patterns if re.search(pattern, text))
        
        medical_indicator_score = min(100, (medical_keywords_found * 5) + (medical_patterns_found * 10))
        
        # Document structure analysis
        structure_indicators = {
            'has_headers': bool(re.search(r'^[A-Z][A-Z\s]{5,}$', text, re.MULTILINE)),
            'has_dates': bool(re.search(r'\d{1,2}/\d{1,2}/\d{4}', text)),
            'has_addresses': bool(re.search(r'\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd)', text, re.IGNORECASE)),
            'has_phone_numbers': bool(re.search(r'\(\d{3}\)\s?\d{3}-?\d{4}|\d{3}-\d{3}-\d{4}', text)),
            'has_proper_formatting': '\n' in text and len(text.split('\n')) > 3
        }
        
        structure_score = sum(structure_indicators.values()) * 20
        
        # Content completeness analysis
        completeness_indicators = {
            'has_patient_info': any(keyword in text_lower for keyword in ['patient', 'name', 'dob']),
            'has_medical_info': any(keyword in text_lower for keyword in ['diagnosis', 'icd', 'treatment']),
            'has_provider_info': any(keyword in text_lower for keyword in ['doctor', 'physician', 'provider']),
            'has_order_info': any(keyword in text_lower for keyword in ['order', 'prescription', 'service']),
            'sufficient_length': word_count > 50
        }
        
        completeness_score = sum(completeness_indicators.values()) * 20
        
        # Calculate overall quality score
        base_score = 0
        if printable_ratio > 0.9: base_score += 30
        elif printable_ratio > 0.8: base_score += 25
        elif printable_ratio > 0.7: base_score += 20
        elif printable_ratio > 0.6: base_score += 10
        
        if word_count > 200: base_score += 25
        elif word_count > 100: base_score += 20
        elif word_count > 50: base_score += 15
        elif word_count > 20: base_score += 10
        
        # Penalize garbage indicators
        garbage_indicators = text.count('(cid:') + text.count('\x00') * 10
        if garbage_indicators > 0:
            base_score -= min(30, garbage_indicators * 2)
        
        final_score = max(0, min(100, base_score + medical_indicator_score * 0.3 + structure_score * 0.2 + completeness_score * 0.2))
        
        # Determine quality level
        if final_score >= 85:
            quality = ExtractionQuality.EXCELLENT
        elif final_score >= 70:
            quality = ExtractionQuality.GOOD
        elif final_score >= 50:
            quality = ExtractionQuality.FAIR
        elif final_score >= 25:
            quality = ExtractionQuality.POOR
        else:
            quality = ExtractionQuality.FAILED
        
        return {
            "score": final_score,
            "quality": quality,
            "printable_ratio": printable_ratio,
            "length": length,
            "word_count": word_count,
            "medical_indicators": medical_keywords_found + medical_patterns_found,
            "structure_score": structure_score,
            "completeness": completeness_score,
            "garbage_indicators": garbage_indicators,
            "structure_analysis": structure_indicators,
            "completeness_analysis": completeness_indicators
        }

class MedicalFieldValidator:
    """Validates extracted medical fields for accuracy."""
    
    @staticmethod
    def validate_mrn(mrn: str) -> Tuple[bool, str]:
        """Enhanced MRN validation with stricter rules."""
        if not mrn:
            return False, "MRN is empty"
        
        # Clean MRN
        cleaned_mrn = clean_mrn(mrn)
        
        if not cleaned_mrn:
            return False, "MRN must be more than 3 characters and alphanumeric"
        
        if len(cleaned_mrn) > 15:
            return False, "MRN too long"
        
        # Should contain at least one number
        if not any(c.isdigit() for c in cleaned_mrn):
            return False, "MRN should contain at least one number"
        
        # Common invalid patterns
        invalid_patterns = ['0000', '1111', '2222', '3333', '4444', '5555', '6666', '7777', '8888', '9999']
        if any(pattern in cleaned_mrn for pattern in invalid_patterns):
            return False, "MRN contains invalid pattern"
        
        return True, "Valid MRN format"
    
    @staticmethod
    def validate_date(date_str: str, field_name: str = "date") -> Tuple[bool, str, Optional[datetime]]:
        """Validate date format and logic."""
        if not date_str:
            return False, f"{field_name} is empty", None
        
        # Try multiple date formats
        date_formats = ["%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                
                # Validate date logic
                current_year = datetime.now().year
                if dt.year < 1900 or dt.year > current_year + 5:
                    return False, f"{field_name} year out of reasonable range", None
                
                return True, f"Valid {field_name}", dt
            except ValueError:
                continue
        
        return False, f"Invalid {field_name} format", None
    
    @staticmethod
    def validate_icd_code(icd_code: str) -> Tuple[bool, str]:
        """Validate ICD-10 code format."""
        if not icd_code:
            return False, "ICD code is empty"
        
        # Basic ICD-10 format validation
        icd_pattern = r'^[A-TV-Z][0-9][0-9AB]\.?[0-9A-TV-Z]{0,4}$'
        
        cleaned_code = str(icd_code).strip().upper()
        
        if re.match(icd_pattern, cleaned_code):
            return True, "Valid ICD-10 format"
        
        # Check if it's a valid ICD-9 format (legacy)
        icd9_pattern = r'^\d{3}\.?\d{0,2}$'
        if re.match(icd9_pattern, cleaned_code):
            return True, "Valid ICD-9 format (legacy)"
        
        return False, "Invalid ICD code format"
    
    @staticmethod
    def validate_patient_name(name: str) -> Tuple[bool, str]:
        """Validate patient name format."""
        if not name:
            return False, "Patient name is empty"
        
        name_str = str(name).strip()
        
        if len(name_str) < 2:
            return False, "Patient name too short"
        
        if len(name_str) > 100:
            return False, "Patient name too long"
        
        # Should contain at least one letter
        if not any(c.isalpha() for c in name_str):
            return False, "Patient name should contain letters"
        
        # Check for reasonable name patterns
        name_pattern = r'^[A-Za-z\s\-\'\.]+$'
        if not re.match(name_pattern, name_str):
            return False, "Patient name contains invalid characters"
        
        return True, "Valid patient name format"
    
    @staticmethod
    def validate_fields_comprehensive(fields: Dict[str, Any]) -> Tuple[float, List[str]]:
        """Comprehensive field validation with confidence scoring."""
        errors = []
        valid_fields = 0
        total_critical_fields = 0
        
        # Validate MRN
        mrn = fields.get("mrn")
        if mrn:
            is_valid, error = MedicalFieldValidator.validate_mrn(mrn)
            if not is_valid:
                errors.append(f"MRN validation failed: {error}")
            else:
                valid_fields += 2  # MRN is critical
        total_critical_fields += 2
        
        # Validate dates
        date_fields = [
            ("orderdate", "Order Date"),
            ("soc", "Start of Care"),
            ("dob", "Date of Birth")
        ]
        
        for field_key, field_name in date_fields:
            date_value = fields.get(field_key)
            if date_value:
                is_valid, error, _ = MedicalFieldValidator.validate_date(date_value, field_name)
                if not is_valid:
                    errors.append(f"{field_name} validation failed: {error}")
                else:
                    valid_fields += 1
            total_critical_fields += 1
        
        # Validate certification period
        cert_period = fields.get("cert_period", {})
        if isinstance(cert_period, dict):
            soe = cert_period.get("soe")
            eoe = cert_period.get("eoe")
            
            if soe:
                is_valid, error, soe_dt = MedicalFieldValidator.validate_date(soe, "Start of Episode")
                if not is_valid:
                    errors.append(f"SOE validation failed: {error}")
                else:
                    valid_fields += 1
            total_critical_fields += 1
            
            if eoe:
                is_valid, error, eoe_dt = MedicalFieldValidator.validate_date(eoe, "End of Episode")
                if not is_valid:
                    errors.append(f"EOE validation failed: {error}")
                else:
                    valid_fields += 1
            total_critical_fields += 1
        
        # Validate ICD codes
        icd_codes = fields.get("icd_codes", [])
        if isinstance(icd_codes, list) and icd_codes:
            valid_icd_count = 0
            for icd in icd_codes:
                is_valid, error = MedicalFieldValidator.validate_icd_code(icd)
                if is_valid:
                    valid_icd_count += 1
                else:
                    errors.append(f"ICD code '{icd}' validation failed: {error}")
            
            if valid_icd_count > 0:
                valid_fields += min(2, valid_icd_count)  # Cap at 2 points for ICD codes
            total_critical_fields += 2
        
        # Validate patient name
        patient_name = fields.get("patient_name")
        if patient_name:
            is_valid, error = MedicalFieldValidator.validate_patient_name(patient_name)
            if not is_valid:
                errors.append(f"Patient name validation failed: {error}")
            else:
                valid_fields += 1
        total_critical_fields += 1
        
        # Calculate confidence score
        confidence = (valid_fields / total_critical_fields) if total_critical_fields > 0 else 0.0
        
        return confidence, errors 