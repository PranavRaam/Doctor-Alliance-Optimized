import json
import re
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from openai import AzureOpenAI
from langchain_community.llms import Ollama

from validation import FieldExtractionResult, ExtractionQuality, MedicalFieldValidator
from config import FIELD_EXTRACTION_CONFIG, api_key, azure_endpoint, deployment_name, OLLAMA_LLM_MODEL

logger = logging.getLogger(__name__)

class AccuracyFocusedFieldExtractor:
    """Field extractor optimized for maximum accuracy using multiple validation approaches."""
    
    def __init__(self, config: Dict = None):
        self.config = config or FIELD_EXTRACTION_CONFIG
        self.validator = MedicalFieldValidator()
        
        # Initialize Ollama client for fallback
        self.ollama_client = None
        try:
            self.ollama_client = Ollama(model=OLLAMA_LLM_MODEL)
            logger.info(f"Ollama client initialized with model: {OLLAMA_LLM_MODEL}")
        except Exception as e:
            logger.warning(f"Failed to initialize Ollama client: {e}")
    

    
    def _extract_with_ollama_fallback(self, text: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Extract fields using Ollama as fallback for sensitive content."""
        if not self.ollama_client:
            logger.error("Ollama client not available for fallback extraction")
            return None
        
        try:
            # Create prompt for Ollama
            ollama_prompt = f"""
You are a medical document expert. Extract ONLY valid JSON with these keys from the document:

Extract these fields:
- orderno (order number)
- orderdate (order date in MM/DD/YYYY)
- mrn (medical record number, alphanumeric)
- soc (start of care date in MM/DD/YYYY)
- cert_period: {{
    "soe": "start of episode date in MM/DD/YYYY", 
    "eoe": "end of episode date in MM/DD/YYYY"
}}
- icd_codes (list of ICD-10 codes)
- patient_name (full patient name)
- dob (date of birth in MM/DD/YYYY)
- address (complete address)
- patient_sex ("MALE" or "FEMALE")

RETURN ONLY JSON. Document text:
{text[:6000]}
"""
            
            # Call Ollama with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.ollama_client.invoke(ollama_prompt)
                    
                    if response:
                        # Extract JSON from response
                        json_match = re.search(r'\{[\s\S]*\}', response)
                        if json_match:
                            json_str = json_match.group()
                            try:
                                parsed_result = json.loads(json_str)
                                
                                # Validate the structure
                                if self._validate_extraction_structure(parsed_result):
                                    logger.info(f"Ollama fallback extraction successful for {doc_id} on attempt {attempt + 1}")
                                    return parsed_result
                                else:
                                    logger.warning(f"Ollama extraction structure invalid for {doc_id} on attempt {attempt + 1}")
                            
                            except json.JSONDecodeError as e:
                                logger.warning(f"Ollama JSON parsing failed for {doc_id} on attempt {attempt + 1}: {e}")
                    
                    # Wait before retry
                    if attempt < max_retries - 1:
                        time.sleep(1 * (attempt + 1))
                        
                except Exception as e:
                    logger.error(f"Ollama API error for {doc_id} on attempt {attempt + 1}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(2 * (attempt + 1))
            
            logger.error(f"Ollama fallback extraction failed after {max_retries} attempts for {doc_id}")
            return None
            
        except Exception as e:
            logger.error(f"Ollama fallback extraction error for {doc_id}: {e}")
            return None
    
    def extract_fields_multi_approach(self, text: str, doc_id: str) -> FieldExtractionResult:
        """Smart field extraction with optimized approach selection."""
        
        # Quick text analysis to determine best approach
        text_characteristics = self._analyze_text_characteristics(text)
        
        # Smart approach selection based on text characteristics
        if text_characteristics["has_structured_dates"] and text_characteristics["has_medical_terms"]:
            # High-quality medical document - use enhanced chunking
            logger.info(f"Using enhanced chunking for high-quality medical document {doc_id}")
            result = self._extract_with_enhanced_chunking(text, doc_id)
            if result:
                return FieldExtractionResult(
                    fields=result,
                    confidence=0.85,
                    method="enhanced_chunked",
                    quality=ExtractionQuality.EXCELLENT
                )
        
        # Fallback to pattern-based extraction for speed
        logger.info(f"Using fast pattern-based extraction for {doc_id}")
        pattern_result = self._extract_with_patterns(text, doc_id)
        if pattern_result and self._has_sufficient_fields(pattern_result):
            return FieldExtractionResult(
                fields=pattern_result,
                confidence=0.75,
                method="pattern_based",
                quality=ExtractionQuality.GOOD
            )
        
        # If pattern extraction didn't get enough fields, try Azure OpenAI
        logger.info(f"Pattern extraction insufficient, trying Azure OpenAI for {doc_id}")
        azure_result = self._extract_with_azure_openai_enhanced(text, doc_id)
        if azure_result:
            return FieldExtractionResult(
                fields=azure_result,
                confidence=0.80,
                method="azure_openai",
                quality=ExtractionQuality.GOOD
            )
        
        # If Azure OpenAI fails, try Ollama as final fallback
        logger.info(f"Azure OpenAI failed, trying Ollama fallback for {doc_id}")
        ollama_result = self._extract_with_ollama_fallback(text, doc_id)
        if ollama_result:
            return FieldExtractionResult(
                fields=ollama_result,
                confidence=0.65,
                method="ollama_fallback",
                quality=ExtractionQuality.FAIR
            )
        
        # Final fallback - return pattern result even if incomplete
        if pattern_result:
            return FieldExtractionResult(
                fields=pattern_result,
                confidence=0.60,
                method="pattern_based_fallback",
                quality=ExtractionQuality.FAIR
            )
        
        # Complete failure
        logger.warning(f"All extraction attempts failed for {doc_id}")
        return FieldExtractionResult(
            fields=self._get_empty_fields_structure(),
            confidence=0.0,
            method="failed",
            validation_errors=["All extraction methods failed"],
            quality=ExtractionQuality.FAILED
        )
    
    def _analyze_text_characteristics(self, text: str) -> Dict[str, Any]:
        """Quickly analyze text to determine extraction strategy."""
        text_lower = text.lower()
        
        # Check for structured dates
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{4}',
            r'\d{1,2}-\d{1,2}-\d{4}',
            r'start of care|soc|start of episode|soe|end of episode|eoe'
        ]
        has_structured_dates = any(re.search(pattern, text_lower) for pattern in date_patterns)
        
        # Check for medical terms
        medical_terms = [
            'patient', 'diagnosis', 'icd', 'medication', 'treatment', 'doctor',
            'physician', 'medical', 'hospital', 'clinic', 'order', 'prescription',
            'mrn', 'dob', 'address', 'insurance', 'provider', 'care', 'service'
        ]
        has_medical_terms = any(term in text_lower for term in medical_terms)
        
        # Check text quality
        printable_ratio = sum(32 <= ord(c) <= 126 for c in text) / max(1, len(text))
        is_high_quality = printable_ratio > 0.8 and len(text) > 200
        
        return {
            "has_structured_dates": has_structured_dates,
            "has_medical_terms": has_medical_terms,
            "is_high_quality": is_high_quality,
            "text_length": len(text),
            "printable_ratio": printable_ratio
        }
    
    def _has_sufficient_fields(self, fields: Dict[str, Any]) -> bool:
        """Check if pattern extraction got enough fields to be useful."""
        critical_fields = ['mrn', 'soc', 'patient_name']
        found_critical = sum(1 for field in critical_fields if fields.get(field))
        
        # Need at least 2 critical fields or 1 critical + 2 other fields
        other_fields = ['orderno', 'orderdate', 'dob', 'address', 'patient_sex']
        found_other = sum(1 for field in other_fields if fields.get(field))
        
        return found_critical >= 2 or (found_critical >= 1 and found_other >= 2)
    
    def _extract_with_enhanced_chunking(self, text: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Enhanced extraction using chunking for better date accuracy."""
        
        def extract_one_chunk_enhanced(chunk):
            # Enhanced prompt with specific focus on date accuracy
            prompt = f"""
You are a medical document expert. Extract ONLY valid JSON with these keys, paying SPECIAL ATTENTION to date accuracy:

CRITICAL DATE EXTRACTION RULES:
- SOC (Start of Care): Look for "Start of Care", "SOC", "Care Start Date", "Service Start"
- SOE (Start of Episode): Look for "Start of Episode", "SOE", "Episode Start", "From Date"  
- EOE (End of Episode): Look for "End of Episode", "EOE", "Episode End", "To Date", "Through Date"
- All dates MUST be in MM/DD/YYYY format
- If you see dates like "12/15/2024 - 02/12/2025", the first is SOE, second is EOE
- Episode periods are typically 60-90 days apart

Extract these fields:
- orderno (order number)
- orderdate (order date in MM/DD/YYYY)
- mrn (medical record number, alphanumeric)
- soc (start of care date in MM/DD/YYYY)
- cert_period: {{
    "soe": "start of episode date in MM/DD/YYYY", 
    "eoe": "end of episode date in MM/DD/YYYY"
}}
- icd_codes (list of ICD-10 codes)
- patient_name (full patient name)
- dob (date of birth in MM/DD/YYYY)
- address (complete address)
- patient_sex ("MALE" or "FEMALE")

RETURN ONLY JSON. Document text:
{chunk}
"""
            
            openai_client = AzureOpenAI(
                api_key=api_key,
                azure_endpoint=azure_endpoint,
                api_version="2024-02-15-preview"
            )
            
            max_retries = self.config.get("max_retries", 3)  # Reduced from 5 to 3
            
            for retry in range(max_retries):
                try:
                    response = openai_client.chat.completions.create(
                        model=deployment_name,
                        messages=[
                            {"role": "system", "content": "You are a medical records expert specializing in accurate date extraction from healthcare documents."},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.1,  # Lower temperature for consistency
                        max_tokens=600  # Reduced from 800 to 600
                    )
                    content = response.choices[0].message.content.strip()
                    
                    try:
                        json_str = re.search(r'\{[\s\S]+\}', content).group()
                        result = json.loads(json_str)
                        
                        # Post-process dates for accuracy
                        result = self._post_process_dates_enhanced(result)
                        
                        logger.info(f"[Enhanced Chunked] JSON extracted on try {retry+1}")
                        return result
                    except Exception:
                        if retry < max_retries - 1:  # Only log warning if not last attempt
                            logger.warning(f"[Enhanced Chunked] JSON parsing failed (try {retry+1})")
                        return None
                except Exception as e:
                    logger.error(f"[ERROR] Enhanced Chunked OpenAI error: {e}")
                    if retry < max_retries - 1:  # Only sleep if not last attempt
                        time.sleep(1 * (retry + 1))  # Reduced sleep time
            return None
        
        # Smart chunking - only chunk if text is very long
        if len(text) < 4000:
            # For shorter texts, process in one go
            result = extract_one_chunk_enhanced(text)
            return result if result else None
        else:
            # For longer texts, use smart chunking
            chunk_size = 3000  # Larger chunks to reduce API calls
            chunks = []
            if len(text) < chunk_size:
                chunks = [text]
            else:
                # Split by paragraphs to maintain context
                paragraphs = text.split('\n\n')
                current_chunk = ""
                for para in paragraphs:
                    if len(current_chunk) + len(para) > chunk_size:
                        if current_chunk:
                            chunks.append(current_chunk)
                        current_chunk = para
                    else:
                        current_chunk += "\n\n" + para if current_chunk else para
                if current_chunk:
                    chunks.append(current_chunk)
            
            # Limit to 3 chunks maximum for speed
            chunks = chunks[:3]
            
            results = []
            for chunk in chunks:
                result = extract_one_chunk_enhanced(chunk)
                if result:
                    results.append(result)
            
            # Merge results from chunks
            merged = {}
            for key in ['orderno', 'orderdate', 'mrn', 'soc', 'cert_period', 'icd_codes','patient_name', 'dob', 'address', 'patient_sex']:
                found = False
                for r in results:
                    if r is None:
                        continue
                    val = r.get(key)
                    if val and (not isinstance(val, list) or len(val) > 0):
                        merged[key] = val
                        found = True
                        break
                if not found:
                    merged[key] = None if key != 'icd_codes' else []
            
            return merged if results else None
    
    def _extract_with_azure_openai_enhanced(self, text: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Enhanced Azure OpenAI extraction with better prompting and error handling."""
        
        # Enhanced prompt with specific focus on date accuracy
        enhanced_prompt = f"""
You are a medical document expert. Extract ONLY valid JSON with these keys, paying SPECIAL ATTENTION to date accuracy:

CRITICAL DATE EXTRACTION RULES:
- SOC (Start of Care): Look for "Start of Care", "SOC", "Care Start Date", "Service Start"
- SOE (Start of Episode): Look for "Start of Episode", "SOE", "Episode Start", "From Date"  
- EOE (End of Episode): Look for "End of Episode", "EOE", "Episode End", "To Date", "Through Date"
- All dates MUST be in MM/DD/YYYY format
- If you see dates like "12/15/2024 - 02/12/2025", the first is SOE, second is EOE
- Episode periods are typically 60-90 days apart

Extract these fields:
- orderno (order number)
- orderdate (order date in MM/DD/YYYY)
- mrn (medical record number, alphanumeric)
- soc (start of care date in MM/DD/YYYY)
- cert_period: {{
    "soe": "start of episode date in MM/DD/YYYY", 
    "eoe": "end of episode date in MM/DD/YYYY"
}}
- icd_codes (list of ICD-10 codes)
- patient_name (full patient name)
- dob (date of birth in MM/DD/YYYY)
- address (complete address)
- patient_sex ("MALE" or "FEMALE")

RETURN ONLY JSON. Document text:
{text[:8000]}
"""

        openai_client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=azure_endpoint,
            api_version="2024-02-15-preview"
        )
        
        max_retries = self.config.get("max_retries", 4)  # Reduced from 8 to 4
        
        for attempt in range(max_retries):
            try:
                response = openai_client.chat.completions.create(
                    model=deployment_name,
                    messages=[
                        {
                            "role": "system", 
                            "content": "You are a medical records expert. Extract information accurately and return only valid JSON. If unsure about any field, use null."
                        },
                        {
                            "role": "user", 
                            "content": enhanced_prompt
                        }
                    ],
                    temperature=0.1,  # Low temperature for consistency
                    max_tokens=800,  # Reduced from 1000 to 800
                    top_p=0.9
                )
                
                content = response.choices[0].message.content.strip()
                
                # Extract JSON from response
                json_match = re.search(r'\{[\s\S]*\}', content)
                if json_match:
                    json_str = json_match.group()
                    try:
                        parsed_result = json.loads(json_str)
                        
                        # Validate the structure
                        if self._validate_extraction_structure(parsed_result):
                            logger.info(f"Azure OpenAI extraction successful for {doc_id} on attempt {attempt + 1}")
                            return parsed_result
                        else:
                            logger.warning(f"Azure OpenAI extraction structure invalid for {doc_id} on attempt {attempt + 1}")
                    
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON parsing failed for {doc_id} on attempt {attempt + 1}: {e}")
                
                # Wait before retry
                if attempt < max_retries - 1:
                    time.sleep(1 * (attempt + 1))  # Reduced from 2 to 1
                    
            except Exception as e:
                error_msg = str(e).lower()
                # Check for content policy violations or restrictions
                if any(keyword in error_msg for keyword in ['content', 'policy', 'restriction', 'violation', 'sensitive']):
                    logger.warning(f"Azure OpenAI content policy restriction detected for {doc_id}: {e}")
                    return None  # Don't retry for content policy violations
                
                logger.error(f"Azure OpenAI API error for {doc_id} on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))  # Reduced from 3 to 2
        
        logger.error(f"Azure OpenAI extraction failed after {max_retries} attempts for {doc_id}")
        return None
    
    def _extract_with_patterns(self, text: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Extract fields using regex patterns and NLP techniques."""
        
        extracted = self._get_empty_fields_structure()
        
        try:
            # Order number patterns
            order_patterns = [
                r'(?:order\s*(?:number|no|#)[\s:]*)([\w\-]+)',
                r'(?:^|\n)([A-Z]{2,}\-?\d{4,})',
                r'(?:reference|ref)[\s:]*([A-Z0-9\-]{6,})'
            ]
            
            for pattern in order_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match and not extracted.get("orderno"):
                    extracted["orderno"] = match.group(1).strip()
                    break
            
            # MRN patterns
            mrn_patterns = [
                r'(?:mrn|medical\s*record)[\s:#]*([A-Z]?\d{6,}[A-Z]?)',
                r'(?:patient\s*(?:id|number))[\s:#]*([A-Z]?\d{4,}[A-Z]?)',
                r'(?:^|\n)(?:MRN:?\s*)([A-Z]?\d{6,}[A-Z]?)'
            ]
            
            for pattern in mrn_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    mrn_candidate = self._clean_mrn(match.group(1))
                    if mrn_candidate and len(mrn_candidate) >= 4:
                        extracted["mrn"] = mrn_candidate
                        break
            
            # Date patterns with context
            date_pattern = r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})'
            
            # Order date
            order_date_context = r'(?:order\s*date|date\s*of\s*order)[\s:]*' + date_pattern
            match = re.search(order_date_context, text, re.IGNORECASE)
            if match:
                extracted["orderdate"] = self._normalize_date(match.group(1))
            
            # Start of care
            soc_context = r'(?:start\s*of\s*care|soc)[\s:]*' + date_pattern
            match = re.search(soc_context, text, re.IGNORECASE)
            if match:
                extracted["soc"] = self._normalize_date(match.group(1))
            
            # Date of birth
            dob_context = r'(?:date\s*of\s*birth|dob|born)[\s:]*' + date_pattern
            match = re.search(dob_context, text, re.IGNORECASE)
            if match:
                extracted["dob"] = self._normalize_date(match.group(1))
            
            # Certification period
            cert_period = {}
            
            soe_context = r'(?:start\s*of\s*episode|soe)[\s:]*' + date_pattern
            match = re.search(soe_context, text, re.IGNORECASE)
            if match:
                cert_period["soe"] = self._normalize_date(match.group(1))
            
            eoe_context = r'(?:end\s*of\s*episode|eoe)[\s:]*' + date_pattern
            match = re.search(eoe_context, text, re.IGNORECASE)
            if match:
                cert_period["eoe"] = self._normalize_date(match.group(1))
            
            extracted["cert_period"] = cert_period
            
            # ICD codes
            icd_pattern = r'\b([A-TV-Z]\d{2}\.?\d{0,2})\b'
            icd_matches = re.findall(icd_pattern, text)
            if icd_matches:
                # Validate and clean ICD codes
                valid_icds = []
                for icd in icd_matches[:6]:  # Limit to 6
                    is_valid, _ = self.validator.validate_icd_code(icd)
                    if is_valid:
                        valid_icds.append(icd.upper())
                extracted["icd_codes"] = valid_icds
            
            # Patient name
            name_patterns = [
                r'(?:patient\s*name)[\s:]*([A-Za-z\s\-\'\.]+?)(?:\n|$)',
                r'(?:^|\n)([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})(?:\s+(?:DOB|MRN))',
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, text, re.MULTILINE)
                if match:
                    name_candidate = match.group(1).strip()
                    is_valid, _ = self.validator.validate_patient_name(name_candidate)
                    if is_valid:
                        extracted["patient_name"] = name_candidate
                        break
            
            # Patient sex
            sex_pattern = r'(?:sex|gender)[\s:]*(\w+)'
            match = re.search(sex_pattern, text, re.IGNORECASE)
            if match:
                sex_value = match.group(1).strip().upper()
                if sex_value.startswith(('M', 'MALE')):
                    extracted["patient_sex"] = "MALE"
                elif sex_value.startswith(('F', 'FEMALE')):
                    extracted["patient_sex"] = "FEMALE"
            
            # Address
            address_patterns = [
                r'(?:address)[\s:]*([^\n]+(?:\n[^\n]+){0,2})',
                r'(\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd)[^\n]*)'
            ]
            
            for pattern in address_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    address_candidate = match.group(1).strip()
                    if len(address_candidate) > 10:  # Reasonable address length
                        extracted["address"] = address_candidate
                        break
            
            logger.info(f"Pattern-based extraction completed for {doc_id}")
            return extracted
            
        except Exception as e:
            logger.error(f"Pattern-based extraction failed for {doc_id}: {e}")
            return None
    
    def _extract_with_context_enhancement(self, text: str, previous_attempts: List, doc_id: str) -> Optional[Dict[str, Any]]:
        """Use context from previous attempts to enhance extraction."""
        
        # Combine insights from previous attempts
        combined_fields = self._get_empty_fields_structure()
        field_confidence = {}
        
        for method, fields in previous_attempts:
            for key, value in fields.items():
                if value and value != "null":
                    if key not in combined_fields or not combined_fields[key]:
                        combined_fields[key] = value
                        field_confidence[key] = 1
                    elif combined_fields[key] == value:
                        field_confidence[key] = field_confidence.get(key, 0) + 1
        
        # Use high-confidence fields to inform more targeted extraction
        high_confidence_context = {k: v for k, v in combined_fields.items() 
                                 if field_confidence.get(k, 0) >= 2}
        
        if high_confidence_context:
            logger.info(f"Using high-confidence context for enhanced extraction: {doc_id}")
            # Could implement more sophisticated context-aware extraction here
            return combined_fields
        
        return None
    
    def _cross_validate_and_merge(self, extraction_attempts: List, doc_id: str) -> FieldExtractionResult:
        """Cross-validate extraction attempts and merge into final result."""
        
        final_fields = self._get_empty_fields_structure()
        validation_errors = []
        field_sources = {}
        
        # For each field, find the most reliable value
        all_field_keys = set()
        for _, fields in extraction_attempts:
            all_field_keys.update(fields.keys())
        
        for field_key in all_field_keys:
            candidates = []
            
            for method, fields in extraction_attempts:
                value = fields.get(field_key)
                if value and str(value).strip() and str(value) != "null":
                    candidates.append((method, value))
            
            if candidates:
                # Choose the best candidate based on validation and consistency
                best_candidate = self._select_best_field_value(field_key, candidates)
                if best_candidate:
                    final_fields[field_key] = best_candidate[1]
                    field_sources[field_key] = best_candidate[0]
        
        # Validate the final result
        confidence_score, field_errors = self.validator.validate_fields_comprehensive(final_fields)
        validation_errors.extend(field_errors)
        
        # Apply business logic corrections
        final_fields = self._apply_business_logic_corrections(final_fields)
        
        # Determine overall quality
        if confidence_score >= 0.9:
            quality = ExtractionQuality.EXCELLENT
        elif confidence_score >= 0.7:
            quality = ExtractionQuality.GOOD
        elif confidence_score >= 0.5:
            quality = ExtractionQuality.FAIR
        elif confidence_score >= 0.3:
            quality = ExtractionQuality.POOR
        else:
            quality = ExtractionQuality.FAILED
        
        method_summary = ", ".join([method for method, _ in extraction_attempts])
        
        logger.info(f"Final extraction result for {doc_id}: Quality={quality.value}, "
                   f"Confidence={confidence_score:.2f}, Methods={method_summary}")
        
        return FieldExtractionResult(
            fields=final_fields,
            confidence=confidence_score,
            method=method_summary,
            validation_errors=validation_errors,
            quality=quality
        )
    
    def _select_best_field_value(self, field_key: str, candidates: List[Tuple[str, Any]]) -> Optional[Tuple[str, Any]]:
        """Select the best value for a specific field from multiple candidates."""
        
        if not candidates:
            return None
        
        if len(candidates) == 1:
            return candidates[0]
        
        # Field-specific validation and selection logic
        valid_candidates = []
        
        for method, value in candidates:
            is_valid = True
            
            if field_key == "mrn":
                is_valid, _ = self.validator.validate_mrn(value)
            elif field_key in ["orderdate", "soc", "dob"]:
                is_valid, _, _ = self.validator.validate_date(value, field_key)
            elif field_key == "patient_name":
                is_valid, _ = self.validator.validate_patient_name(value)
            elif field_key == "icd_codes" and isinstance(value, list):
                valid_count = sum(1 for icd in value if self.validator.validate_icd_code(icd)[0])
                is_valid = valid_count > 0
            
            if is_valid:
                valid_candidates.append((method, value))
        
        if not valid_candidates:
            # If no valid candidates, return the first one
            return candidates[0]
        
        # Prefer enhanced chunked results, then Azure OpenAI, then pattern-based, then others
        method_priority = {"enhanced_chunked": 4, "azure_openai": 3, "pattern_based": 2, "context_enhanced": 1}
        
        best_candidate = max(valid_candidates, 
                           key=lambda x: method_priority.get(x[0], 0))
        
        return best_candidate
    
    def _clean_and_validate_extracted_fields(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and validate extracted fields with modular approach."""
        from validation import clean_order_number, clean_mrn, validate_order_number
        
        # Clean and validate order number
        if fields.get("orderno"):
            cleaned_order = self._clean_order_number(fields["orderno"])
            if cleaned_order:
                is_valid, _ = validate_order_number(cleaned_order)
                if is_valid:
                    fields["orderno"] = cleaned_order
                    logger.info(f"Cleaned order number: {fields['orderno']}")
                else:
                    logger.warning(f"Invalid order number after cleaning: {cleaned_order}")
                    fields["orderno"] = None
            else:
                logger.warning(f"Order number cleaning failed: {fields['orderno']}")
                fields["orderno"] = None
        
        # Clean and validate MRN
        if fields.get("mrn"):
            cleaned_mrn = self._clean_mrn(fields["mrn"])
            if cleaned_mrn:
                is_valid, _ = self.validator.validate_mrn(cleaned_mrn)
                if is_valid:
                    fields["mrn"] = cleaned_mrn
                    logger.info(f"Cleaned MRN: {fields['mrn']}")
                else:
                    logger.warning(f"Invalid MRN after cleaning: {cleaned_mrn}")
                    fields["mrn"] = None
            else:
                logger.warning(f"MRN cleaning failed: {fields['mrn']}")
                fields["mrn"] = None
        
        return fields

    def _apply_business_logic_corrections(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """Apply medical document business logic corrections with enhanced validation."""
        
        # First, clean and validate critical fields
        fields = self._clean_and_validate_extracted_fields(fields)
        
        # Generate order number if missing or invalid
        if not fields.get("orderno"):
            doc_id = fields.get("docId", "UNKNOWN")
            # Clean the document ID too
            from validation import clean_order_number
            cleaned_doc_id = clean_order_number(doc_id) or "UNKNOWN"
            fields["orderno"] = f"NOF{cleaned_doc_id}"
            logger.info(f"Generated fallback order number: {fields['orderno']}")
        
        # Enhanced date processing
        fields = self._post_process_dates_enhanced(fields)
        
        # Standardize patient sex
        if fields.get("patient_sex"):
            sex_val = str(fields["patient_sex"]).strip().lower()
            if sex_val.startswith(('m', 'male')):
                fields["patient_sex"] = "MALE"
            elif sex_val.startswith(('f', 'female')):
                fields["patient_sex"] = "FEMALE"
            else:
                fields["patient_sex"] = None
        
        return fields
    
    def _post_process_dates_enhanced(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced date processing with medical document logic."""
        
        def parse_date_flexible(date_str):
            if not date_str or not isinstance(date_str, str):
                return None
            
            # Clean the date string
            date_str = date_str.strip()
            
            # Try multiple formats including common variations
            formats = [
                "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y",
                "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y",
                "%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    # Ensure 4-digit year
                    if dt.year < 50:
                        dt = dt.replace(year=dt.year + 2000)
                    elif dt.year < 100:
                        dt = dt.replace(year=dt.year + 1900)
                    return dt
                except ValueError:
                    continue
            return None
        
        def format_date(dt):
            return dt.strftime("%m/%d/%Y") if dt else None
        
        # Parse all dates
        soc_dt = parse_date_flexible(fields.get("soc"))
        orderdate_dt = parse_date_flexible(fields.get("orderdate"))
        dob_dt = parse_date_flexible(fields.get("dob"))
        
        cert_period = fields.get("cert_period", {}) or {}
        soe_dt = parse_date_flexible(cert_period.get("soe"))
        eoe_dt = parse_date_flexible(cert_period.get("eoe"))
        
        # Apply medical document business logic
        current_date = datetime.now()
        
        # Validate dates are reasonable
        if soc_dt and (soc_dt.year < 2020 or soc_dt > current_date + timedelta(days=365)):
            logger.warning(f"SOC date seems unreasonable: {soc_dt}")
        
        if soe_dt and (soe_dt.year < 2020 or soe_dt > current_date + timedelta(days=365)):
            logger.warning(f"SOE date seems unreasonable: {soe_dt}")
        
        # Logic: If SOC exists but SOE doesn't, use SOC as SOE
        if soc_dt and not soe_dt:
            soe_dt = soc_dt
            logger.info(f"Using SOC as SOE: {format_date(soe_dt)}")
        
        # Logic: If SOE exists but SOC doesn't, use SOE as SOC
        if soe_dt and not soc_dt:
            soc_dt = soe_dt
            logger.info(f"Using SOE as SOC: {format_date(soc_dt)}")
        
        # Logic: Calculate EOE based on SOE if missing
        if soe_dt and not eoe_dt:
            eoe_dt = soe_dt + timedelta(days=60)  # Standard 60-day episode
            logger.info(f"Calculated EOE from SOE: {format_date(eoe_dt)}")
        
        # Logic: Validate EOE is after SOE
        if soe_dt and eoe_dt:
            gap = (eoe_dt - soe_dt).days
            if gap < 30:
                eoe_dt = soe_dt + timedelta(days=60)
                logger.info(f"Adjusted EOE (gap too small): {format_date(eoe_dt)}")
            elif gap > 120:
                eoe_dt = soe_dt + timedelta(days=90)
                logger.info(f"Adjusted EOE (gap too large): {format_date(eoe_dt)}")
        
        # Update fields with processed dates
        fields["soc"] = format_date(soc_dt)
        fields["orderdate"] = format_date(orderdate_dt)
        fields["dob"] = format_date(dob_dt)
        
        cert_period["soe"] = format_date(soe_dt)
        cert_period["eoe"] = format_date(eoe_dt)
        fields["cert_period"] = cert_period
        
        return fields
    
    def _validate_extraction_structure(self, extracted: Dict) -> bool:
        """Validate that extraction has the required structure."""
        required_keys = [
            'orderno', 'orderdate', 'mrn', 'soc', 'cert_period', 
            'icd_codes', 'patient_name', 'dob', 'address', 'patient_sex'
        ]
        
        return all(key in extracted for key in required_keys)
    
    def _get_empty_fields_structure(self) -> Dict[str, Any]:
        """Get empty fields structure."""
        return {
            "orderno": None,
            "orderdate": None,
            "mrn": None,
            "soc": None,
            "cert_period": {"soe": None, "eoe": None},
            "icd_codes": [],
            "patient_name": None,
            "dob": None,
            "address": None,
            "patient_sex": None
        }
    
    def _normalize_date(self, date_str: str) -> str:
        """Normalize date to MM/DD/YYYY format."""
        is_valid, _, parsed_date = self.validator.validate_date(date_str)
        if is_valid and parsed_date:
            return parsed_date.strftime("%m/%d/%Y")
        return date_str
    
    def _parse_date_safe(self, date_str: str) -> Optional[datetime]:
        """Safely parse date string."""
        if not date_str:
            return None
        
        is_valid, _, parsed_date = self.validator.validate_date(date_str)
        return parsed_date if is_valid else None
    
    def _clean_mrn(self, val):
        """Clean and validate MRN value."""
        if not val:
            return None
        val = re.sub(r'[^A-Za-z0-9]', '', str(val))
        if not val or len(val) < 4 or (val.isalpha() and not any(c.isdigit() for c in val)):
            return None
        return val
    
    def _clean_order_number(self, val):
        """Clean and validate order number value."""
        if not val:
            return None
        val = re.sub(r'[^A-Za-z0-9]', '', str(val))
        if not val or len(val) < 3:
            return None
        return val 