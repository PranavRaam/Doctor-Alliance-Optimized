import os
import time
import json
import base64
import logging
import asyncio
import aiohttp
import aiofiles
import threading
from typing import List, Dict, Any, Tuple, Optional
import requests

from config import DOWNLOAD_CONFIG, API_BASE, get_auth_header

logger = logging.getLogger(__name__)

class AccuracyFocusedDownloadManager:
    """Download manager optimized for reliability and accuracy tracking."""
    
    def __init__(self, config: Dict = None):
        self.config = config or DOWNLOAD_CONFIG
        self.progress_tracker = None
        
    async def download_documents_async(self, doc_ids: List[str], existing_docs: Dict = None) -> Tuple[List[str], List[str], List[str], List[str]]:
        """Download documents with enhanced error handling and progress tracking."""
        
        existing_docs = existing_docs or {}
        docs_to_download = [doc_id for doc_id in doc_ids if doc_id not in existing_docs]
        
        if not docs_to_download:
            logger.info("All documents already exist in database")
            extracted_texts = [existing_docs[doc_id].get("raw_text", "") for doc_id in doc_ids]
            extraction_methods = [existing_docs[doc_id].get("extraction_method", "") for doc_id in doc_ids]
            extraction_errors = [existing_docs[doc_id].get("extraction_error", "") for doc_id in doc_ids]
            pdf_filenames = [None] * len(doc_ids)
            return extracted_texts, pdf_filenames, extraction_methods, extraction_errors
        
        logger.info(f"Starting download of {len(docs_to_download)} documents")
        self.progress_tracker = DownloadProgressTracker(len(docs_to_download))
        
        # Setup async HTTP session with robust configuration and SSL bypass
        connector = aiohttp.TCPConnector(
            limit=self.config["connection_pool_size"],
            limit_per_host=self.config["max_concurrent_downloads"],
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
            ssl=False  # Disable SSL verification to bypass certificate issues
        )
        
        timeout = aiohttp.ClientTimeout(
            total=self.config["timeout"],
            connect=10,
            sock_read=20
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=get_auth_header(),
            trust_env=True
        ) as session:
            
            # Create download tasks with semaphore for concurrency control
            semaphore = asyncio.Semaphore(self.config["max_concurrent_downloads"])
            tasks = []
            
            for doc_id in docs_to_download:
                task = self._download_single_document_enhanced(
                    session, semaphore, doc_id, f"temp_{doc_id}.pdf"
                )
                tasks.append(task)
            
            # Execute downloads with progress tracking
            start_time = time.time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            download_time = time.time() - start_time
            
            # Process results and organize by original doc_ids order
            extracted_texts = []
            pdf_filenames = []
            extraction_methods = []
            extraction_errors = []
            
            successful_downloads = 0
            failed_downloads = 0
            
            # Map results back to original order
            result_map = {}
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Download task exception: {result}")
                    continue
                elif isinstance(result, dict) and "doc_id" in result:
                    result_map[result["doc_id"]] = result
            
            # Process each doc_id in original order
            for doc_id in doc_ids:
                if doc_id in existing_docs:
                    # Use existing data
                    extracted_texts.append(existing_docs[doc_id].get("raw_text", ""))
                    extraction_methods.append(existing_docs[doc_id].get("extraction_method", ""))
                    extraction_errors.append(existing_docs[doc_id].get("extraction_error", ""))
                    pdf_filenames.append(None)
                    
                elif doc_id in result_map:
                    # Use download result
                    result = result_map[doc_id]
                    if result["success"]:
                        extracted_texts.append("")  # Will be filled during extraction
                        extraction_methods.append("")
                        extraction_errors.append("")
                        pdf_filenames.append(f"temp_{doc_id}.pdf")
                        successful_downloads += 1
                    else:
                        extracted_texts.append("")
                        extraction_methods.append("download_failed")
                        extraction_errors.append(result["error"])
                        pdf_filenames.append(None)
                        failed_downloads += 1
                else:
                    # Download failed completely
                    extracted_texts.append("")
                    extraction_methods.append("download_failed")
                    extraction_errors.append("Download task failed or timed out")
                    pdf_filenames.append(None)
                    failed_downloads += 1
            
            # Log summary
            total_rate = len(docs_to_download) / download_time * 60 if download_time > 0 else 0
            logger.info(f"Download completed in {download_time:.1f}s")
            logger.info(f"Success: {successful_downloads}, Failed: {failed_downloads}")
            logger.info(f"Average rate: {total_rate:.1f} docs/minute")
            
            return extracted_texts, pdf_filenames, extraction_methods, extraction_errors
    
    async def _download_single_document_enhanced(
        self, 
        session: aiohttp.ClientSession, 
        semaphore: asyncio.Semaphore, 
        doc_id: str, 
        save_path: str
    ) -> Dict[str, Any]:
        """Enhanced single document download with comprehensive error handling."""
        
        async with semaphore:
            url = f"{API_BASE}{doc_id}"
            
            for attempt in range(self.config["max_retries"]):
                try:
                    self.progress_tracker.update_progress(doc_id, "downloading", attempt + 1)
                    
                    async with session.get(url) as response:
                        if response.status == 200:
                            try:
                                result = await response.json()
                                
                                if not result.get("isSuccess"):
                                    error_msg = f"API returned failure: {result.get('message', 'Unknown error')}"
                                    logger.warning(f"API failure for {doc_id}: {error_msg}")
                                    if attempt == self.config["max_retries"] - 1:
                                        self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                        return {"doc_id": doc_id, "success": False, "error": error_msg}
                                    continue
                                
                                value = result.get("value", {})
                                if isinstance(value, str):
                                    try:
                                        value = json.loads(value)
                                    except json.JSONDecodeError as e:
                                        error_msg = f"Failed to parse value JSON: {e}"
                                        logger.error(f"JSON parse error for {doc_id}: {error_msg}")
                                        if attempt == self.config["max_retries"] - 1:
                                            self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                            return {"doc_id": doc_id, "success": False, "error": error_msg}
                                        continue
                                
                                if "documentBuffer" in value:
                                    try:
                                        pdf_data = base64.b64decode(value["documentBuffer"])
                                        
                                        # Validate PDF data
                                        if len(pdf_data) < 100:  # Minimum PDF size check
                                            error_msg = f"PDF data too small ({len(pdf_data)} bytes)"
                                            logger.warning(f"Invalid PDF size for {doc_id}: {error_msg}")
                                            if attempt == self.config["max_retries"] - 1:
                                                self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                                return {"doc_id": doc_id, "success": False, "error": error_msg}
                                            continue
                                        
                                        # Check PDF header
                                        if not pdf_data.startswith(b'%PDF-'):
                                            error_msg = "Downloaded data is not a valid PDF"
                                            logger.warning(f"Invalid PDF header for {doc_id}")
                                            if attempt == self.config["max_retries"] - 1:
                                                self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                                return {"doc_id": doc_id, "success": False, "error": error_msg}
                                            continue
                                        
                                        # Write file asynchronously
                                        async with aiofiles.open(save_path, "wb") as f:
                                            await f.write(pdf_data)
                                        
                                        # Verify file was written correctly
                                        if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                                            self.progress_tracker.update_progress(doc_id, "completed", 0)
                                            logger.info(f"Successfully downloaded {doc_id} ({len(pdf_data)} bytes)")
                                            return {
                                                "doc_id": doc_id, 
                                                "success": True, 
                                                "error": None,
                                                "file_size": len(pdf_data)
                                            }
                                        else:
                                            error_msg = "File write verification failed"
                                            logger.error(f"File write failed for {doc_id}")
                                            if attempt == self.config["max_retries"] - 1:
                                                self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                                return {"doc_id": doc_id, "success": False, "error": error_msg}
                                            continue
                                            
                                    except Exception as e:
                                        error_msg = f"PDF processing error: {str(e)}"
                                        logger.error(f"PDF processing failed for {doc_id}: {error_msg}")
                                        if attempt == self.config["max_retries"] - 1:
                                            self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                            return {"doc_id": doc_id, "success": False, "error": error_msg}
                                        continue
                                else:
                                    error_msg = "documentBuffer not found in API response"
                                    logger.warning(f"No document buffer for {doc_id}")
                                    if attempt == self.config["max_retries"] - 1:
                                        self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                        return {"doc_id": doc_id, "success": False, "error": error_msg}
                                    continue
                                    
                            except Exception as e:
                                error_msg = f"Response processing error: {str(e)}"
                                logger.error(f"Response processing failed for {doc_id}: {error_msg}")
                                if attempt == self.config["max_retries"] - 1:
                                    self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                    return {"doc_id": doc_id, "success": False, "error": error_msg}
                                
                        else:
                            error_msg = f"HTTP {response.status}: {response.reason}"
                            logger.warning(f"HTTP error for {doc_id}: {error_msg}")
                            if response.status == 404:
                                # Don't retry 404s
                                self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                return {"doc_id": doc_id, "success": False, "error": error_msg}
                            elif attempt == self.config["max_retries"] - 1:
                                self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                                return {"doc_id": doc_id, "success": False, "error": error_msg}
                
                except asyncio.TimeoutError:
                    error_msg = f"Timeout on attempt {attempt + 1}"
                    logger.warning(f"Timeout for {doc_id}: {error_msg}")
                    if attempt == self.config["max_retries"] - 1:
                        self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                        return {"doc_id": doc_id, "success": False, "error": "Download timeout"}
                
                except Exception as e:
                    error_msg = f"Download exception on attempt {attempt + 1}: {str(e)}"
                    logger.error(f"Exception for {doc_id}: {error_msg}")
                    if attempt == self.config["max_retries"] - 1:
                        self.progress_tracker.update_progress(doc_id, "failed", 0, error_msg)
                        return {"doc_id": doc_id, "success": False, "error": str(e)}
                
                # Wait before retry with exponential backoff
                if attempt < self.config["max_retries"] - 1:
                    wait_time = self.config["retry_backoff"] ** attempt
                    logger.info(f"Retrying {doc_id} in {wait_time}s...")
                    await asyncio.sleep(wait_time)
            
            # Should not reach here, but just in case
            final_error = f"All {self.config['max_retries']} attempts failed"
            self.progress_tracker.update_progress(doc_id, "failed", 0, final_error)
            return {"doc_id": doc_id, "success": False, "error": final_error}

class DownloadProgressTracker:
    """Enhanced progress tracker with detailed metrics."""
    
    def __init__(self, total_docs: int):
        self.total_docs = total_docs
        self.completed = 0
        self.failed = 0
        self.in_progress = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.status_history = []
        
    def update_progress(self, doc_id: str, status: str, attempt: int = 0, error: str = ""):
        with self.lock:
            timestamp = time.time()
            
            if status == "downloading":
                self.in_progress += 1
            elif status == "completed":
                self.completed += 1
                self.in_progress = max(0, self.in_progress - 1)
            elif status == "failed":
                self.failed += 1
                self.in_progress = max(0, self.in_progress - 1)
            
            # Record status change
            self.status_history.append({
                "timestamp": timestamp,
                "doc_id": doc_id,
                "status": status,
                "attempt": attempt,
                "error": error
            })
            
            # Print progress updates
            if status in ["completed", "failed"] or (self.completed + self.failed) % 3 == 0:
                self._print_progress_update(doc_id, status, error)
    
    def _print_progress_update(self, doc_id: str, status: str, error: str = ""):
        elapsed = time.time() - self.start_time
        completed_total = self.completed + self.failed
        
        if completed_total > 0:
            rate = completed_total / elapsed * 60  # docs per minute
            remaining = self.total_docs - completed_total
            eta = remaining / (completed_total / elapsed) if elapsed > 0 else 0
            
            status_char = "✓" if status == "completed" else "✗" if status == "failed" else "→"
            
            logger.info(f"{status_char} {doc_id} | Progress: {completed_total}/{self.total_docs} "
                       f"({completed_total/self.total_docs*100:.1f}%) | "
                       f"Success: {self.completed} Failed: {self.failed} | "
                       f"Rate: {rate:.1f}/min | ETA: {eta/60:.1f}m")
            
            if error and len(error) < 150:
                logger.warning(f"    Error: {error}")
    
    def get_summary(self) -> Dict[str, Any]:
        elapsed = time.time() - self.start_time
        return {
            "total_docs": self.total_docs,
            "completed": self.completed,
            "failed": self.failed,
            "success_rate": self.completed / self.total_docs if self.total_docs > 0 else 0,
            "elapsed_time": elapsed,
            "average_rate": (self.completed + self.failed) / elapsed * 60 if elapsed > 0 else 0
        }

def download_pdf_from_api(doc_id: str, save_path: str) -> bool:
    """Synchronous fallback download function."""
    url = f"{API_BASE}{doc_id}"
    
    try:
        response = requests.get(url, headers=get_auth_header(), timeout=30, verify=False)  # Disable SSL verification
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