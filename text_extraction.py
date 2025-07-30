import os
import time
import logging
import pytesseract
from PIL import Image
import io
import re
import pdfplumber
from pdfminer.high_level import extract_text as pdfminer_extract_tex
from typing import List, Dict, Any
import numpy as np

# Try to import PyMuPDF, with fallback handling
try:
    import fitz  # PyMuPDF
    FITZ_AVAILABLE = True
except ImportError as e:
    print(f"Warning: PyMuPDF (fitz) not available: {e}")
    print("PDFPlumber and PDFMiner will be used as alternatives")
    FITZ_AVAILABLE = False
    fitz = None

from validation import ExtractionResult, TextQualityAnalyzer
from config import EXTRACTION_CONFIG

logger = logging.getLogger(__name__)

class AccuracyFocusedTextExtractor:
    """Text extractor optimized for maximum accuracy."""
    
    def __init__(self, config: Dict = None):
        self.config = config or EXTRACTION_CONFIG
        self.quality_analyzer = TextQualityAnalyzer()
        self.extraction_cache = {}
    
    def extract_with_all_methods(self, pdf_path: str, doc_id: str) -> List[ExtractionResult]:
        """Extract text using all available methods and analyze quality."""
        
        results = []
        
        # Method 1: PyMuPDF (fitz) - Multiple configurations
        if FITZ_AVAILABLE:
            fitz_configs = [
                {"flags": 0, "name": "fitz_standard"},
                {"flags": fitz.TEXT_PRESERVE_LIGATURES, "name": "fitz_ligatures"},
                {"flags": fitz.TEXT_PRESERVE_WHITESPACE, "name": "fitz_whitespace"},
            ]
            
            for config in fitz_configs:
                try:
                    text = self._extract_with_fitz_enhanced(pdf_path, config)
                    quality = self.quality_analyzer.analyze_comprehensive(text)
                    
                    results.append(ExtractionResult(
                        text=text,
                        method=config["name"],
                        quality_score=quality["score"],
                        confidence=quality["score"] / 100.0,
                        metrics=quality
                    ))
                except Exception as e:
                    logger.error(f"Fitz extraction failed for {doc_id}: {e}")
                    results.append(ExtractionResult(
                        text="",
                        method=config["name"],
                        quality_score=0,
                        confidence=0,
                        error=str(e)
                    ))
        else:
            logger.warning(f"PyMuPDF not available, skipping Fitz extraction for {doc_id}")
        
        # Method 2: PDFPlumber - Enhanced configuration
        try:
            text = self._extract_with_pdfplumber_enhanced(pdf_path)
            quality = self.quality_analyzer.analyze_comprehensive(text)
            
            results.append(ExtractionResult(
                text=text,
                method="pdfplumber_enhanced",
                quality_score=quality["score"],
                confidence=quality["score"] / 100.0,
                metrics=quality
            ))
        except Exception as e:
            logger.error(f"PDFPlumber extraction failed for {doc_id}: {e}")
            results.append(ExtractionResult(
                text="",
                method="pdfplumber_enhanced",
                quality_score=0,
                confidence=0,
                error=str(e)
            ))
        
        # Method 3: PDFMiner
        try:
            text = self._extract_with_pdfminer_enhanced(pdf_path)
            quality = self.quality_analyzer.analyze_comprehensive(text)
            
            results.append(ExtractionResult(
                text=text,
                method="pdfminer_enhanced",
                quality_score=quality["score"],
                confidence=quality["score"] / 100.0,
                metrics=quality
            ))
        except Exception as e:
            logger.error(f"PDFMiner extraction failed for {doc_id}: {e}")
            results.append(ExtractionResult(
                text="",
                method="pdfminer_enhanced",
                quality_score=0,
                confidence=0,
                error=str(e)
            ))
        
        # Method 4: OCR (if needed based on quality threshold)
        best_non_ocr = max(results, key=lambda x: x.quality_score)
        
        if best_non_ocr.quality_score < self.config.get("ocr_fallback_threshold", 60):
            logger.info(f"Quality too low ({best_non_ocr.quality_score}), trying OCR for {doc_id}")
            try:
                ocr_text = self._extract_with_ocr_comprehensive(pdf_path, doc_id)
                ocr_quality = self.quality_analyzer.analyze_comprehensive(ocr_text)
                
                results.append(ExtractionResult(
                    text=ocr_text,
                    method="ocr_comprehensive",
                    quality_score=ocr_quality["score"],
                    confidence=ocr_quality["score"] / 100.0,
                    metrics=ocr_quality
                ))
            except Exception as e:
                logger.error(f"OCR extraction failed for {doc_id}: {e}")
                results.append(ExtractionResult(
                    text="",
                    method="ocr_comprehensive",
                    quality_score=0,
                    confidence=0,
                    error=str(e)
                ))
        
        return results
    
    def select_best_extraction(self, results: List[ExtractionResult], doc_id: str) -> ExtractionResult:
        """Select the best extraction based on comprehensive analysis."""
        
        if not results:
            return ExtractionResult("", "none", 0.0, 0.0, "No extraction results")
        
        # Filter out failed extractions
        valid_results = [r for r in results if r.quality_score > 0 and r.text.strip()]
        
        if not valid_results:
            logger.warning(f"No valid extractions for {doc_id}")
            return max(results, key=lambda x: x.quality_score)
        
        # Multi-criteria selection
        scored_results = []
        
        for result in valid_results:
            # Base score from quality
            score = result.quality_score
            
            # Bonus for medical content indicators
            medical_bonus = result.metrics.get("medical_indicators", 0) * 2
            
            # Bonus for document structure
            structure_bonus = result.metrics.get("structure_score", 0) * 0.5
            
            # Bonus for completeness
            completeness_bonus = result.metrics.get("completeness", 0) * 0.3
            
            # Length bonus (reasonable length documents)
            word_count = result.metrics.get("word_count", 0)
            if 100 <= word_count <= 2000:
                length_bonus = 10
            elif 50 <= word_count < 100 or 2000 < word_count <= 5000:
                length_bonus = 5
            else:
                length_bonus = 0
            
            final_score = score + medical_bonus + structure_bonus + completeness_bonus + length_bonus
            
            scored_results.append((result, final_score))
            
            logger.info(f"Method {result.method}: Quality={result.quality_score:.1f}, "
                       f"Medical={medical_bonus}, Structure={structure_bonus:.1f}, "
                       f"Final={final_score:.1f}")
        
        # Select the best result
        best_result, best_score = max(scored_results, key=lambda x: x[1])
        
        logger.info(f"Selected method '{best_result.method}' with score {best_score:.1f} for {doc_id}")
        
        return best_result
    
    def _extract_with_fitz_enhanced(self, pdf_path: str, config: Dict) -> str:
        """Enhanced PyMuPDF extraction with multiple configurations."""
        if not FITZ_AVAILABLE:
            raise ImportError("PyMuPDF is not available.")
            
        doc = fitz.open(pdf_path)
        text_parts = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            
            # Try different extraction approaches
            page_text = page.get_text(flags=config.get("flags", 0))
            
            if not page_text.strip():
                # Try textpage approach for stubborn pages
                try:
                    textpage = page.get_textpage()
                    page_text = textpage.extractText()
                except:
                    pass
            
            if page_text.strip():
                text_parts.append(f"\n--- Page {page_num + 1} ---\n{page_text}")
        
        doc.close()
        return "\n".join(text_parts)
    
    def _extract_with_pdfplumber_enhanced(self, pdf_path: str) -> str:
        """Enhanced PDFPlumber extraction with table and layout awareness."""
        text_parts = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                page_text = ""
                
                # Try to extract tables first
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        table_text = "\n".join(["\t".join([cell or "" for cell in row]) for row in table])
                        page_text += f"\n[TABLE]\n{table_text}\n[/TABLE]\n"
                
                # Extract regular text
                regular_text = page.extract_text(x_tolerance=3, y_tolerance=3)
                if regular_text:
                    page_text += regular_text
                
                # Try word extraction if text extraction fails
                if not page_text.strip():
                    words = page.extract_words()
                    if words:
                        page_text = " ".join([word["text"] for word in words])
                
                if page_text.strip():
                    text_parts.append(f"\n--- Page {page_num + 1} ---\n{page_text}")
        
        return "\n".join(text_parts)
    
    def _extract_with_pdfminer_enhanced(self, pdf_path: str) -> str:
        """Enhanced PDFMiner extraction."""
        try:
            text = pdfminer_extract_tex(pdf_path, 
                                      laparams={'word_margin': 0.1, 'char_margin': 2.0, 'line_margin': 0.5})
            return text if text else ""
        except Exception as e:
            # Fallback to basic extraction
            try:
                return pdfminer_extract_tex(pdf_path)
            except:
                return ""
    
    def _extract_with_ocr_comprehensive(self, pdf_path: str, doc_id: str) -> str:
        """Comprehensive OCR extraction with multiple engines and configurations."""
        if not FITZ_AVAILABLE:
            # Fallback to PDFPlumber + OCR for first few pages
            return self._extract_with_ocr_fallback(pdf_path, doc_id)
            
        doc = fitz.open(pdf_path)
        all_text_parts = []
        
        for page_num in range(min(len(doc), 20)):  # Limit to 20 pages for performance
            page = doc[page_num]
            
            try:
                # Convert page to high-quality image
                mat = fitz.Matrix(2.0, 2.0)  # 2x scaling for better OCR
                pix = page.get_pixmap(matrix=mat)
                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))
                
                # Convert to grayscale and enhance
                img = img.convert('L')
                
                # Try multiple Tesseract configurations
                ocr_configs = [
                    '--psm 6 --oem 3',  # Uniform block of text
                    '--psm 4 --oem 3',  # Single column of text
                    '--psm 3 --oem 3',  # Fully automatic page segmentation
                    '--psm 11 --oem 3', # Sparse text
                ]
                
                best_ocr_text = ""
                best_confidence = 0
                
                for config in ocr_configs:
                    try:
                        ocr_result = pytesseract.image_to_string(img, config=config)
                        
                        # Simple confidence scoring based on text quality
                        quality = self.quality_analyzer.analyze_comprehensive(ocr_result)
                        
                        if quality["score"] > best_confidence:
                            best_confidence = quality["score"]
                            best_ocr_text = ocr_result
                            
                        # If we get good quality, don't try other configs
                        if quality["score"] > 80:
                            break
                            
                    except Exception as e:
                        logger.warning(f"Tesseract config {config} failed for page {page_num}: {e}")
                        continue
                
                # Try EasyOCR if Tesseract results are poor
                if best_confidence < 60:
                    try:
                        import easyocr
                        reader = easyocr.Reader(['en'], gpu=False)
                        easy_results = reader.readtext(np.array(img), detail=0, paragraph=True)
                        easy_text = "\n".join(easy_results)
                        
                        easy_quality = self.quality_analyzer.analyze_comprehensive(easy_text)
                        if easy_quality["score"] > best_confidence:
                            best_ocr_text = easy_text
                            best_confidence = easy_quality["score"]
                            
                    except Exception as e:
                        logger.warning(f"EasyOCR failed for page {page_num}: {e}")
                
                if best_ocr_text.strip():
                    all_text_parts.append(f"\n--- OCR Page {page_num + 1} ---\n{best_ocr_text}")
                    
            except Exception as e:
                logger.error(f"OCR failed for page {page_num}: {e}")
                continue
        
        doc.close()
        return "\n".join(all_text_parts)
    
    def _extract_with_ocr_fallback(self, pdf_path: str, doc_id: str) -> str:
        """Fallback OCR method using PDFPlumber when PyMuPDF is not available."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                all_text_parts = []
                
                for page_num, page in enumerate(pdf.pages[:5]):  # Limit to 5 pages
                    try:
                        # Extract text first
                        text = page.extract_text()
                        if text and len(text.strip()) > 50:
                            all_text_parts.append(f"\n--- Page {page_num + 1} ---\n{text}")
                            continue
                        
                        # If text extraction fails, try OCR on page image
                        img = page.to_image()
                        if img:
                            # Convert to PIL Image
                            pil_img = Image.fromarray(img.original)
                            pil_img = pil_img.convert('L')
                            
                            # Try Tesseract OCR
                            ocr_text = pytesseract.image_to_string(pil_img, config='--psm 6 --oem 3')
                            
                            if ocr_text.strip():
                                all_text_parts.append(f"\n--- OCR Page {page_num + 1} ---\n{ocr_text}")
                                
                    except Exception as e:
                        logger.warning(f"Fallback OCR failed for page {page_num}: {e}")
                        continue
                
                return "\n".join(all_text_parts)
                
        except Exception as e:
            logger.error(f"Fallback OCR extraction failed for {doc_id}: {e}")
            return ""
    
    def extract_document(self, pdf_path: str, doc_id: str) -> ExtractionResult:
        """Main method to extract text from a document with maximum accuracy."""
        
        # Check cache first
        if self.config.get("cache_extraction_results", True):
            cache_key = f"{pdf_path}_{os.path.getmtime(pdf_path)}"
            if cache_key in self.extraction_cache:
                logger.info(f"Using cached extraction for {doc_id}")
                return self.extraction_cache[cache_key]
        
        logger.info(f"Starting comprehensive text extraction for {doc_id}")
        start_time = time.time()
        
        # Extract with all methods
        all_results = self.extract_with_all_methods(pdf_path, doc_id)
        
        # Select the best result
        best_result = self.select_best_extraction(all_results, doc_id)
        
        extraction_time = time.time() - start_time
        
        logger.info(f"Extraction completed for {doc_id} in {extraction_time:.2f}s - "
                   f"Method: {best_result.method}, Quality: {best_result.quality_score:.1f}")
        
        # Cache the result
        if self.config.get("cache_extraction_results", True):
            cache_key = f"{pdf_path}_{os.path.getmtime(pdf_path)}"
            self.extraction_cache[cache_key] = best_result
        
        return best_result 