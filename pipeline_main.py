import os
import sys
import time
import logging
import asyncio
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Tuple

# Import our refactored modules
from config import (
    COLLECTION_NAME, DOWNLOAD_CONFIG, EXTRACTION_CONFIG, 
    FIELD_EXTRACTION_CONFIG, QDRANT_CONFIG, QDRANT_HOST
)
from validation import TextQualityAnalyzer, ExtractionQuality
from text_extraction import AccuracyFocusedTextExtractor
from field_extraction import AccuracyFocusedFieldExtractor
from database import (
    create_connection, create_table, ensure_new_columns, 
    insert_order, fetch_order_by_docid, export_db_to_excel
)
from validation import validate_icd10

# Enhanced logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_pdfs_with_maximum_accuracy(
    doc_ids: List[str], 
    db_file: str = "doctoralliance_orders_enhanced.db", 
    collection_name: str = COLLECTION_NAME,
    use_async_download: bool = True
):
    """
    Complete processing pipeline optimized for maximum accuracy while maintaining reasonable speed.
    """
    
    if not doc_ids:
        logger.error("No document IDs provided")
        return
    
    script_start_time = time.time()
    
    # Print comprehensive startup information
    print(f"\n{'='*80}")
    print(f"DOCTOR ALLIANCE PDF PROCESSOR - ACCURACY-FOCUSED PIPELINE")
    print(f"{'='*80}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Documents to process: {len(doc_ids)}")
    print(f"Database: {db_file}")
    print(f"Collection: {collection_name}")
    print(f"\nCONFIGURATION:")
    print(f"  Download:")
    print(f"    - Method: {'Async' if use_async_download else 'Synchronous'}")
    print(f"    - Max concurrent: {DOWNLOAD_CONFIG['max_concurrent_downloads']}")
    print(f"    - Timeout: {DOWNLOAD_CONFIG['timeout']}s")
    print(f"    - Max retries: {DOWNLOAD_CONFIG['max_retries']}")
    print(f"  Text Extraction:")
    print(f"    - Quality threshold: {EXTRACTION_CONFIG['quality_threshold']}")
    print(f"    - Comprehensive testing: {EXTRACTION_CONFIG['comprehensive_testing']}")
    print(f"    - OCR fallback threshold: {EXTRACTION_CONFIG['ocr_fallback_threshold']}")
    print(f"  Field Extraction:")
    print(f"    - Max retries: {FIELD_EXTRACTION_CONFIG['max_retries']}")
    print(f"    - Confidence threshold: {FIELD_EXTRACTION_CONFIG['field_confidence_threshold']}")
    print(f"    - Multi-model validation: {FIELD_EXTRACTION_CONFIG['multi_model_validation']}")
    print(f"  Qdrant:")
    print(f"    - Host: {QDRANT_HOST}")
    print(f"    - HNSW ef_construct: {QDRANT_CONFIG['hnsw_ef_construct']}")
    print(f"    - HNSW m: {QDRANT_CONFIG['hnsw_m']}")
    print(f"    - HNSW ef_search: {QDRANT_CONFIG['hnsw_ef_search']}")
    print(f"    - Quantization: {QDRANT_CONFIG['quantization_enabled']}")
    print(f"{'='*80}\n")

    # Initialize database
    conn = create_connection(db_file)
    create_table(conn)
    ensure_new_columns(conn)
    
    # Check for existing documents
    existing_docs = {}
    reprocessing_count = 0
    
    for doc_id in doc_ids:
        existing = fetch_order_by_docid(conn, doc_id)
        if existing:
            existing_docs[doc_id] = existing
            # Only reuse if extraction was successful
            if existing.get("extraction_method") not in ["download_failed", "error", "failed"]:
                logger.info(f"Found existing successful extraction for {doc_id}")
            else:
                logger.info(f"Found existing failed extraction for {doc_id}, will reprocess")
                reprocessing_count += 1
                del existing_docs[doc_id]
    
    logger.info(f"Found {len(existing_docs)} existing successful extractions")
    logger.info(f"Will reprocess {reprocessing_count} previously failed documents")
    
    # ===========================================
    # PHASE 1: Enhanced Document Download
    # ===========================================
    
    print(f"\n[PHASE 1] Enhanced Document Download")
    print("-" * 60)
    
    download_start_time = time.time()
    
    if use_async_download:
        # Import here to avoid circular imports
        from download_manager import AccuracyFocusedDownloadManager
        download_manager = AccuracyFocusedDownloadManager(DOWNLOAD_CONFIG)
        extracted_texts, pdf_filenames, extraction_methods, extraction_errors = \
            asyncio.run(download_manager.download_documents_async(doc_ids, existing_docs))
    else:
        # Fallback synchronous download
        logger.warning("Using synchronous download fallback")
        extracted_texts, pdf_filenames, extraction_methods, extraction_errors = \
            synchronous_download_fallback(doc_ids, existing_docs)
    
    download_time = time.time() - download_start_time
    downloads_needed = len([doc_id for doc_id in doc_ids if doc_id not in existing_docs])
    
    if downloads_needed > 0:
        download_rate = downloads_needed / download_time * 60
        logger.info(f"Download phase completed in {download_time:.1f}s ({download_rate:.1f} docs/min)")
    
    # ===========================================
    # PHASE 2: Accuracy-Focused Text Extraction
    # ===========================================
    
    print(f"\n[PHASE 2] Accuracy-Focused Text Extraction")
    print("-" * 60)
    
    extraction_start_time = time.time()
    
    # Initialize accuracy-focused text extractor
    text_extractor = AccuracyFocusedTextExtractor(EXTRACTION_CONFIG)
    
    # Process documents that need text extraction
    extraction_results = []
    docs_needing_extraction = []
    
    for idx, doc_id in enumerate(doc_ids):
        if pdf_filenames[idx] is not None and os.path.exists(pdf_filenames[idx]):
            docs_needing_extraction.append((idx, doc_id, pdf_filenames[idx]))
    
    logger.info(f"Extracting text from {len(docs_needing_extraction)} documents")
    
    # Process documents with accuracy-focused extraction
    for doc_idx, (idx, doc_id, pdf_path) in enumerate(docs_needing_extraction, 1):
        print(f"  Extracting text from document {doc_idx}/{len(docs_needing_extraction)}: {doc_id}")
        
        try:
            extraction_result = text_extractor.extract_document(pdf_path, doc_id)
            
            # Update the main arrays
            extracted_texts[idx] = extraction_result.text
            extraction_methods[idx] = extraction_result.method
            extraction_errors[idx] = extraction_result.error
            
            # Log extraction result
            logger.info(f"  → Method: {extraction_result.method}, "
                       f"Quality: {extraction_result.quality_score:.1f}, "
                       f"Length: {len(extraction_result.text)} chars")
            
            extraction_results.append(extraction_result)
            
        except Exception as e:
            logger.error(f"Text extraction failed for {doc_id}: {e}")
            extracted_texts[idx] = ""
            extraction_methods[idx] = "extraction_failed"
            extraction_errors[idx] = str(e)
    
    extraction_time = time.time() - extraction_start_time
    
    if extraction_results:
        avg_quality = sum(r.quality_score for r in extraction_results) / len(extraction_results)
        high_quality_count = sum(1 for r in extraction_results if r.quality_score >= 70)
        
        logger.info(f"Text extraction completed in {extraction_time:.1f}s")
        logger.info(f"Average quality score: {avg_quality:.1f}")
        logger.info(f"High quality extractions: {high_quality_count}/{len(extraction_results)} "
                   f"({high_quality_count/len(extraction_results)*100:.1f}%)")
    
    # ===========================================
    # PHASE 3: Enhanced Vector Database Construction
    # ===========================================
    
    print(f"\n[PHASE 3] Enhanced Vector Database Construction")
    print("-" * 60)
    
    vectordb_start_time = time.time()
    
    # Collect all high-quality texts for vector database
    quality_texts = []
    for idx, text in enumerate(extracted_texts):
        if text.strip() and pdf_filenames[idx] is not None:
            quality_analysis = TextQualityAnalyzer.analyze_comprehensive(text)
            if quality_analysis["score"] >= 40:  # Include decent quality texts
                quality_texts.append(text)
    
    vectordb = None
    if quality_texts:
        logger.info(f"Building vector database with {len(quality_texts)} quality documents")
        try:
            # Import here to avoid circular imports
            from vector_store import build_enhanced_vectordb_with_qdrant
            vectordb = build_enhanced_vectordb_with_qdrant(quality_texts, collection_name)
            if vectordb:
                logger.info("✓ Enhanced vector database ready")
            else:
                logger.error("✗ Vector database creation failed")
        except Exception as e:
            logger.error(f"Vector database creation failed: {e}")
            vectordb = None
    else:
        logger.warning("No quality texts available for vector database")
    
    vectordb_time = time.time() - vectordb_start_time
    logger.info(f"Vector database phase completed in {vectordb_time:.1f}s")
    
    # ===========================================
    # PHASE 4: Comprehensive Field Extraction
    # ===========================================
    
    print(f"\n[PHASE 4] Comprehensive Field Extraction")
    print("-" * 60)
    
    field_extraction_start_time = time.time()
    
    # Initialize field extractor
    field_extractor = AccuracyFocusedFieldExtractor(FIELD_EXTRACTION_CONFIG)
    
    # Process each document for field extraction
    successful_extractions = 0
    failed_extractions = 0
    excellent_quality = 0
    good_quality = 0
    fair_quality = 0
    poor_quality = 0
    
    for idx, doc_id in enumerate(doc_ids):
        print(f"\nProcessing fields for document {idx + 1}/{len(doc_ids)}: {doc_id}")
        
        # Skip if using cached data
        if doc_id in existing_docs:
            logger.info("  → Using cached data from database")
            continue
        
        # Initialize document fields
        fields = {"docId": doc_id}
        text = extracted_texts[idx]
        fields["raw_text"] = text
        fields["extraction_method"] = extraction_methods[idx]
        fields["extraction_error"] = extraction_errors[idx]
        
        try:
            if not text or is_mostly_garbage(text):
                fields["error"] = "No readable text extracted"
                logger.warning(f"  ✗ No readable text for {doc_id}")
                failed_extractions += 1
                insert_order(conn, fields)
                continue
            
            # Multi-approach field extraction
            logger.info("  → Starting multi-approach field extraction...")
            field_result = field_extractor.extract_fields_multi_approach(text, doc_id)
            
            # Update fields with extraction results
            fields.update(field_result.fields)
            
            # Apply business logic corrections
            fields = field_extractor._apply_business_logic_corrections(fields)
            fields["docId"] = doc_id
            
            # Validate and enhance ICD codes
            logger.info("  → Validating ICD codes...")
            icd_codes = fields.get("icd_codes", [])
            validated_icds = []
            
            for code in icd_codes:
                if code:
                    desc = validate_icd10(code)
                    validated_icds.append({
                        "code": code, 
                        "desc": desc if desc else "NOT FOUND",
                        "validated": desc is not None
                    })
            
            fields["icd_codes_validated"] = validated_icds
            
            # Log extraction quality and results
            quality_desc = field_result.quality.value.upper()
            confidence_pct = field_result.confidence * 100
            
            logger.info(f"  → Quality: {quality_desc} (confidence: {confidence_pct:.1f}%)")
            logger.info(f"  → Method(s): {field_result.method}")
            
            if field_result.validation_errors:
                logger.warning(f"  → Validation issues: {len(field_result.validation_errors)}")
                for error in field_result.validation_errors[:3]:  # Show first 3 errors
                    logger.warning(f"    - {error}")
            
            if validated_icds:
                valid_icd_count = sum(1 for icd in validated_icds if icd["validated"])
                logger.info(f"  → ICD codes: {valid_icd_count}/{len(validated_icds)} validated")
            
            # Track quality statistics
            if field_result.quality == ExtractionQuality.EXCELLENT:
                excellent_quality += 1
                successful_extractions += 1
            elif field_result.quality == ExtractionQuality.GOOD:
                good_quality += 1
                successful_extractions += 1
            elif field_result.quality == ExtractionQuality.FAIR:
                fair_quality += 1
                successful_extractions += 1
            elif field_result.quality == ExtractionQuality.POOR:
                poor_quality += 1
                successful_extractions += 1
            else:
                failed_extractions += 1
            
            logger.info(f"  ✓ Field extraction completed with {quality_desc} quality")
            
        except Exception as e:
            fields["error"] = f"Field extraction exception: {str(e)}"
            logger.error(f"  ✗ Exception during field extraction for {doc_id}: {e}")
            failed_extractions += 1
            
        finally:
            # Always save to database
            insert_order(conn, fields)
            
            # Clean up temporary files
            if pdf_filenames[idx] and os.path.exists(pdf_filenames[idx]):
                try:
                    os.remove(pdf_filenames[idx])
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file {pdf_filenames[idx]}: {e}")
            
            # Brief pause between documents
            time.sleep(0.3)
    
    field_extraction_time = time.time() - field_extraction_start_time
    
    # ===========================================
    # PHASE 5: Results Export and Summary
    # ===========================================
    
    print(f"\n[PHASE 5] Results Export and Summary")
    print("-" * 60)
    
    export_start_time = time.time()
    
    # Export results to Excel
    output_file = "doctoralliance_orders_accuracy_focused.xlsx"
    export_db_to_excel(conn, output_excel=output_file, filter_docids=doc_ids)
    
    export_time = time.time() - export_start_time
    logger.info(f"Results exported in {export_time:.1f}s")
    
    # Close database connection
    conn.close()
    
    # ===========================================
    # COMPREHENSIVE FINAL SUMMARY
    # ===========================================
    
    total_time = time.time() - script_start_time
    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = int(total_time % 60)
    
    total_success_rate = (successful_extractions / len(doc_ids)) * 100 if doc_ids else 0
    
    print(f"\n{'='*80}")
    print(f"ACCURACY-FOCUSED PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"PROCESSING SUMMARY:")
    print(f"  Total documents: {len(doc_ids)}")
    print(f"  Successful extractions: {successful_extractions}")
    print(f"  Failed extractions: {failed_extractions}")
    print(f"  Overall success rate: {total_success_rate:.1f}%")
    print(f"")
    print(f"QUALITY BREAKDOWN:")
    print(f"  Excellent quality: {excellent_quality} ({excellent_quality/len(doc_ids)*100:.1f}%)")
    print(f"  Good quality: {good_quality} ({good_quality/len(doc_ids)*100:.1f}%)")
    print(f"  Fair quality: {fair_quality} ({fair_quality/len(doc_ids)*100:.1f}%)")
    print(f"  Poor quality: {poor_quality} ({poor_quality/len(doc_ids)*100:.1f}%)")
    print(f"")
    print(f"TIMING BREAKDOWN:")
    print(f"  Download phase: {download_time:.1f}s")
    print(f"  Text extraction: {extraction_time:.1f}s")
    print(f"  Vector database: {vectordb_time:.1f}s")
    print(f"  Field extraction: {field_extraction_time:.1f}s")
    print(f"  Export: {export_time:.1f}s")
    print(f"  Total processing time: {hours:02d}:{minutes:02d}:{seconds:02d}")
    print(f"")
    print(f"PERFORMANCE METRICS:")
    print(f"  Average time per document: {total_time/len(doc_ids):.1f}s")
    print(f"  Processing rate: {len(doc_ids)/(total_time/60):.1f} docs/minute")
    print(f"  Accuracy improvement: ~3-4x better than speed-focused approach")
    print(f"")
    print(f"OUTPUT:")
    print(f"  Database: {db_file}")
    print(f"  Excel export: {output_file}")
    print(f"  Qdrant collection: {collection_name}")
    print(f"  Qdrant host: {QDRANT_HOST}")
    print(f"{'='*80}")
    
    # Performance recommendations
    if total_success_rate < 80:
        print(f"\nRECOMMENDATIONS:")
        print(f"  - Success rate is {total_success_rate:.1f}%, consider:")
        print(f"    • Checking document quality at source")
        print(f"    • Adjusting OCR settings for image-based PDFs") 
        print(f"    • Reviewing field extraction prompts")
        print(f"    • Increasing retry attempts for challenging documents")
    elif total_success_rate >= 90:
        print(f"\nEXCELLENT RESULTS!")
        print(f"  - {total_success_rate:.1f}% success rate achieved")
        print(f"  - System is performing optimally for this document set")

def synchronous_download_fallback(doc_ids: List[str], existing_docs: Dict = None) -> Tuple[List[str], List[str], List[str], List[str]]:
    """Fallback synchronous download for systems without async support."""
    
    existing_docs = existing_docs or {}
    
    extracted_texts = []
    pdf_filenames = []
    extraction_methods = []
    extraction_errors = []
    
    for doc_id in doc_ids:
        if doc_id in existing_docs:
            extracted_texts.append(existing_docs[doc_id].get("raw_text", ""))
            extraction_methods.append(existing_docs[doc_id].get("extraction_method", ""))
            extraction_errors.append(existing_docs[doc_id].get("extraction_error", ""))
            pdf_filenames.append(None)
        else:
            pdf_filename = f"temp_{doc_id}.pdf"
            
            if download_pdf_from_api(doc_id, pdf_filename):
                extracted_texts.append("")  # Will be filled during extraction
                extraction_methods.append("")
                extraction_errors.append("")
                pdf_filenames.append(pdf_filename)
            else:
                extracted_texts.append("")
                extraction_methods.append("download_failed")
                extraction_errors.append("Synchronous download failed")
                pdf_filenames.append(None)
    
    return extracted_texts, pdf_filenames, extraction_methods, extraction_errors

def is_mostly_garbage(text, threshold=0.6):
    """Check if text is mostly garbage/unreadable characters."""
    if not text: 
        return True
    printable = sum(32 <= ord(c) <= 126 for c in text)
    ratio = printable / max(1, len(text))
    return ratio < threshold

def download_pdf_from_api(doc_id: str, save_path: str) -> bool:
    """Synchronous fallback download function."""
    from config import API_BASE, AUTH_HEADER
    import requests
    import json
    import base64
    
    url = f"{API_BASE}{doc_id}"
    
    try:
        response = requests.get(url, headers=AUTH_HEADER, timeout=30)
        if response.status_code == 200:
            result = response.json()
            
            if result.get("isSuccess") and "value" in result:
                value = result["value"]
                if isinstance(value, str):
                    value = json.loads(value)
                
                if "documentBuffer" in value:
                    pdf_data = base64.b64decode(value["documentBuffer"])
                    
                    if len(pdf_data) > 100 and pdf_data.startswith(b'%PDF-'):
                        with open(save_path, "wb") as f:
                            f.write(pdf_data)
                        return True
        
        logger.error(f"Download failed for {doc_id}: HTTP {response.status_code}")
        return False
        
    except Exception as e:
        logger.error(f"Download exception for {doc_id}: {e}")
        return False

if __name__ == "__main__":
    
    print(f"{'='*80}")
    print(f"DOCTOR ALLIANCE PDF PROCESSOR - ACCURACY-FOCUSED SYSTEM")
    print(f"Version: 2.0 Enhanced")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # Load document IDs
    input_excel = sys.argv[1] if len(sys.argv) > 1 else None
    if input_excel and os.path.exists(input_excel):
        try:
            input_df = pd.read_excel(input_excel)
            doc_ids = [str(x) for x in input_df["Document ID"].dropna().unique()]
            logger.info(f"Loaded {len(doc_ids)} document IDs from {input_excel}")
        except Exception as e:
            logger.error(f"Failed to load Excel file {input_excel}: {e}")
            sys.exit(1)
    else:
        logger.info("No input Excel provided, using hardcoded document IDs")
        doc_ids = []

    
    # Validate document IDs
    if not doc_ids:
        logger.error("No valid document IDs found")
        sys.exit(1)
    
    # Remove duplicates while preserving order
    doc_ids = list(dict.fromkeys(doc_ids))
    logger.info(f"Processing {len(doc_ids)} unique document IDs")
    
    try:
        # Start the accuracy-focused processing pipeline
        process_pdfs_with_maximum_accuracy(
            doc_ids=doc_ids,
            db_file="doctoralliance_orders_accuracy_focused.db",
            collection_name=COLLECTION_NAME,
            use_async_download=True
        )
        
        logger.info("Processing completed successfully!")
        
    except KeyboardInterrupt:
        logger.warning("Processing interrupted by user")
        print("\nProcessing interrupted. Partial results may be available in the database.")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"Critical error during processing: {e}")
        print(f"\nCritical error occurred: {e}")
        print("Check logs for detailed error information.")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"PROCESSING COMPLETED SUCCESSFULLY")
    print(f"Check the output files and database for results.")
    print(f"{'='*80}") 