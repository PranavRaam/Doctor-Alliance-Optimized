import os
import time
import re
import sys
from datetime import datetime, timedelta
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import concurrent.futures
import threading
from queue import Queue
import asyncio
import aiohttp

def log_console(msg):
    print(msg)

def wait_and_find_element(driver, by, value, timeout=10):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def login_to_da(da_url, da_login, da_password, driver, timeout=15):
    log_console(f"🔐 Logging into DA Backoffice...")
    driver.get(da_url)
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Username']"))).send_keys(da_login)
    driver.find_element(By.XPATH, "//input[@placeholder='Password']").send_keys(da_password)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()
    wait.until(EC.presence_of_element_located((By.XPATH, "//nav[contains(@class, 'navbar-static-side')]")))
    log_console(f"✅ Login successful")

def go_to_signed_list(driver):
    signed_url = "https://backoffice.doctoralliance.com/Documents/Signed"
    driver.get(signed_url)
    WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.3)

def extract_doc_ids_from_inbox(driver, start_date, end_date=None):
    doc_ids = []
    page = 1
    start_cutoff_date = datetime.strptime(start_date, "%m/%d/%Y")
    end_cutoff_date = datetime.strptime(end_date, "%m/%d/%Y") if end_date else None
    seen_ids = set()
    processed_urls = set()
    consecutive_no_new_docs = 0
    max_consecutive_no_new = 3
    
    while True:
        current_url = driver.current_url
        log_console(f"📄 Inbox page {page} (Found {len(doc_ids)} total docs) - URL: {current_url}")
        
        # For inbox, we don't use URL-based loop detection since the URL stays the same
        # We rely on document ID tracking and consecutive empty pages instead
        
        time.sleep(2)
        
        try:
            # Wait for table to load with better error handling
            max_retries = 3
            current_rows = []
            
            for retry in range(max_retries):
                try:
                    log_console(f"🔄 Loading inbox table (attempt {retry + 1}/{max_retries})")
                    WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
                    time.sleep(3)
                    current_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                    
                    if current_rows:
                        log_console(f"✅ Successfully loaded {len(current_rows)} rows")
                        # Debug: Show first row structure
                        if page == 1 and len(current_rows) > 0:
                            try:
                                first_row = current_rows[0]
                                cells = first_row.find_elements(By.TAG_NAME, "td")
                                log_console(f"🔍 Debug: Inbox table has {len(cells)} columns")
                                for i, cell in enumerate(cells):
                                    cell_text = cell.text.strip()
                                    if cell_text:
                                        log_console(f"   Column {i+1}: {cell_text[:50]}...")
                            except Exception as e:
                                log_console(f"⚠️ Debug: Could not analyze first row: {e}")
                        break
                    else:
                        log_console(f"⚠️ No rows found on attempt {retry + 1}, retrying...")
                        driver.refresh()
                        time.sleep(5)
                        
                except TimeoutException as e:
                    log_console(f"⚠️ Timeout on attempt {retry + 1}: {e}")
                    if retry < max_retries - 1:
                        driver.refresh()
                        time.sleep(5)
                    else:
                        log_console("❌ All attempts to load inbox table failed")
                        break
                        
        except Exception as e:
            log_console(f"❌ Unexpected error loading inbox table: {e}")
            break
            
        if not current_rows:
            log_console("❌ No rows found on current page")
            break
        
        log_console(f"📊 Processing {len(current_rows)} rows on page {page}")
        stop_flag = False
        new_docs_on_page = 0
        
        for row in current_rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 9:
                    log_console(f"⚠️ Row has only {len(cells)} cells, skipping")
                    continue
                    
                # Try to find received date in different columns
                received_on = ""
                for col_idx in [8, 7, 6]:  # Try different column positions (column 8 first based on debug output)
                    if col_idx < len(cells):
                        received_on = cells[col_idx].text.strip()
                        if received_on and "/" in received_on:
                            break
                
                if not received_on:
                    log_console(f"⚠️ No received date found in row")
                    continue
                    
                try:
                    received_date = datetime.strptime(received_on, "%m/%d/%Y")
                except ValueError:
                    log_console(f"⚠️ Invalid date format: {received_on}")
                    continue
                    
                if received_date < start_cutoff_date:
                    stop_flag = True
                    break
                    
                if end_cutoff_date and received_date > end_cutoff_date:
                    continue
                    
                # Try to find doc_id in different columns
                doc_id = ""
                for col_idx in [9, 8, 10]:  # Try different column positions (column 9 first based on debug output)
                    if col_idx < len(cells):
                        doc_id = cells[col_idx].text.strip()
                        if doc_id and len(doc_id) > 5:  # Basic validation
                            break
                
                if not doc_id:
                    log_console(f"⚠️ No doc_id found in row")
                    continue
                
                # Extract document type from the Doc Type column (usually column 3 based on debug output)
                doc_type = ""
                if len(cells) > 2:
                    doc_type = cells[2].text.strip()  # Doc Type column (column 3)
                
                # Skip documents with type "conversation"
                if doc_type and "conversation" in doc_type.lower():
                    log_console(f"📄 Inbox - Doc ID: {doc_id} | Type: {doc_type} ❌ (CONVERSATION - SKIPPED)")
                    continue
                
                if doc_id not in seen_ids:
                    # Get company key from function parameters or use default
                    company_key = getattr(extract_doc_ids_from_inbox, 'company_key', None)
                    if not company_key:
                        # Try to get from config or use prima_care as fallback
                        from config import get_active_company
                        try:
                            active_company = get_active_company()
                            company_key = active_company.get('key', 'prima_care')
                            log_console(f"🔍 Debug: Detected company key: {company_key}")
                        except Exception as e:
                            company_key = 'prima_care'
                            log_console(f"⚠️ Debug: Failed to get company key, using fallback: {e}")
                    
                    # Filter for documents based on company configuration
                    from config import should_filter_document_types, get_allowed_document_types, get_document_type_filter
                    should_filter = should_filter_document_types(company_key)
                    filter_config = get_document_type_filter(company_key)
                    allowed_types = filter_config.get("allowed_types", [])
                    excluded_types = filter_config.get("excluded_types", [])
                    
                    log_console(f"🔍 Debug: Company: {company_key}, Should filter: {should_filter}, Allowed types: {allowed_types}, Excluded types: {excluded_types}")
                    
                    is_allowed_document = True  # Default to allow
                    if should_filter and doc_type:
                        doc_type_upper = doc_type.upper()
                        
                        # Check if document type is explicitly excluded
                        if excluded_types:
                            is_excluded = any(excluded.lower() in doc_type_upper.lower() for excluded in excluded_types)
                            if is_excluded:
                                is_allowed_document = False
                                log_console(f"📄 Inbox - Doc ID: {doc_id} | Type: {doc_type} ❌ (EXCLUDED - SKIPPED)")
                            else:
                                is_allowed_document = True
                        # If no excluded types, check allowed types (backward compatibility)
                        elif allowed_types:
                            is_allowed_document = any(keyword in doc_type_upper for keyword in allowed_types)
                    
                    if is_allowed_document or not should_filter:
                        seen_ids.add(doc_id)
                        doc_ids.append(doc_id)
                        new_docs_on_page += 1
                        # Store document type with doc_id
                        if not hasattr(extract_doc_ids_from_inbox, 'doc_types'):
                            extract_doc_ids_from_inbox.doc_types = {}
                        extract_doc_ids_from_inbox.doc_types[doc_id] = doc_type
                        log_console(f"📄 Inbox - Doc ID: {doc_id} | Type: {doc_type} ✅ (ALLOWED - INCLUDED)")
                    else:
                        log_console(f"📄 Inbox - Doc ID: {doc_id} | Type: {doc_type} ❌ (NOT ALLOWED - SKIPPED)")
                    
            except Exception as e:
                log_console(f"⚠️ Error processing row on page {page}: {e}")
                continue
        
        # Check if we found any new documents on this page
        if new_docs_on_page == 0:
            consecutive_no_new_docs += 1
            log_console(f"⚠️ No new documents on page {page} (consecutive: {consecutive_no_new_docs})")
            
            if consecutive_no_new_docs >= max_consecutive_no_new:
                log_console(f"🛑 Breaking due to {max_consecutive_no_new} consecutive pages with no new documents")
                break
        else:
            consecutive_no_new_docs = 0
            log_console(f"✅ Found {new_docs_on_page} new documents on page {page}")
        
        if stop_flag:
            log_console(f"🛑 Reached start date cutoff. Total inbox docs: {len(doc_ids)}")
            break
            
        # Improved pagination with better verification
        try:
            # Try multiple selectors for the next button
            next_button_selectors = [
                "//li[contains(@class,'page-next') and not(contains(@class, 'disabled'))]/a",
                "//a[contains(@class, 'page-next') and not(contains(@class, 'disabled'))]",
                "//li[@class='page-next']/a",
                "//a[text()='>']",
                "//a[contains(text(), 'Next')]"
            ]
            
            next_btn = None
            for selector in next_button_selectors:
                try:
                    next_btn = driver.find_element(By.XPATH, selector)
                    log_console(f"✅ Found next button with selector: {selector}")
                    break
                except:
                    continue
            
            if not next_btn:
                log_console("🛑 Next button not found or disabled - end of pages")
                break
            
            # Store current page state for comparison
            current_page_content = driver.find_element(By.CSS_SELECTOR, "#inbox-all-grid tbody").get_attribute("innerHTML")
            current_row_count = len(current_rows)
            
            # Click next button
            driver.execute_script("arguments[0].scrollIntoView();", next_btn)
            time.sleep(1)
            
            try:
                driver.execute_script("arguments[0].click();", next_btn)
                log_console(f"✅ Clicked next button for page {page + 1}")
            except Exception as e:
                log_console(f"⚠️ JavaScript click failed, trying regular click: {e}")
                next_btn.click()
            
            # Wait for page to change with better verification
            page_changed = False
            max_wait_attempts = 20  # Increased wait attempts
            
            for wait_attempt in range(max_wait_attempts):
                time.sleep(1.5)  # Increased wait time
                
                try:
                    # Check if content changed (most reliable for inbox)
                    new_page_content = driver.find_element(By.CSS_SELECTOR, "#inbox-all-grid tbody").get_attribute("innerHTML")
                    if new_page_content != current_page_content:
                        log_console(f"✅ Page content changed on attempt {wait_attempt + 1}")
                        page_changed = True
                        break
                    
                    # Check if table rows changed
                    WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
                    new_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                    if len(new_rows) != current_row_count:
                        log_console(f"✅ Row count changed: {current_row_count} -> {len(new_rows)}")
                        page_changed = True
                        break
                    
                    # Check if URL changed (less reliable for inbox)
                    new_url = driver.current_url
                    if new_url != current_url:
                        log_console(f"✅ URL changed: {new_url}")
                        page_changed = True
                        break
                        
                except Exception as e:
                    log_console(f"⚠️ Wait attempt {wait_attempt + 1} failed: {e}")
                    continue
            
            if not page_changed:
                log_console(f"⚠️ Page didn't change after {max_wait_attempts} attempts")
                # For inbox, try refreshing and clicking again
                try:
                    log_console(f"🔄 Trying page refresh and next button click again...")
                    driver.refresh()
                    time.sleep(3)
                    
                    # Find and click next button again
                    for selector in next_button_selectors:
                        try:
                            next_btn = driver.find_element(By.XPATH, selector)
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(3)
                            
                            # Check if it worked
                            new_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                            if len(new_rows) != current_row_count:
                                log_console(f"✅ Refresh and click successful")
                                page_changed = True
                                break
                        except:
                            continue
                    
                    if not page_changed:
                        log_console(f"🛑 Refresh and click also failed, ending pagination")
                        break
                        
                except Exception as e:
                    log_console(f"🛑 Refresh and click failed: {e}")
                    break
            
            if page_changed:
                page += 1
                log_console(f"✅ Successfully moved to page {page}")
            else:
                log_console(f"🛑 Could not navigate to next page. Ending pagination.")
                break
            
        except Exception as e:
            log_console(f"🛑 Pagination ended: {e}")
            break
    
    log_console(f"✅ Inbox extraction complete: {len(doc_ids)} documents found")
    
    # Show document type summary
    doc_types = getattr(extract_doc_ids_from_inbox, 'doc_types', {})
    if doc_types:
        type_counts = {}
        for doc_type in doc_types.values():
            if doc_type:
                type_counts[doc_type] = type_counts.get(doc_type, 0) + 1
        if type_counts:
            log_console(f"📊 Inbox Document Types found:")
            for doc_type, count in type_counts.items():
                log_console(f"   • {doc_type}: {count} documents")
    
    # Retry if no documents found (only if main extraction completely failed)
    if len(doc_ids) == 0:  # Only retry if we found zero documents
        log_console(f"⚠️ No documents found in main extraction, retrying with improved logic...")
        time.sleep(5)
        driver.refresh()
        time.sleep(3)
        
        retry_doc_ids = []
        retry_doc_types = {}
        page = 1
        consecutive_empty_pages = 0
        max_consecutive_empty = 3
        
        while True:
            log_console(f"📄 Retry - Inbox page {page}")
            time.sleep(3)
            
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
                time.sleep(2)
                current_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                
                if current_rows:
                    new_docs_on_page = 0
                    for row in current_rows:
                        try:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 9:
                                # Try to find doc_id in different columns
                                doc_id = ""
                                for col_idx in [8, 9, 10]:
                                    if col_idx < len(cells):
                                        doc_id = cells[col_idx].text.strip()
                                        if doc_id and len(doc_id) > 5:
                                            break
                                
                                if doc_id and doc_id not in retry_doc_ids:
                                    # Extract document type (column 3 based on debug output)
                                    doc_type = ""
                                    if len(cells) > 2:
                                        doc_type = cells[2].text.strip()
                                    
                                    # Skip documents with type "conversation"
                                    if doc_type and "conversation" in doc_type.lower():
                                        log_console(f"📄 Retry - Doc ID: {doc_id} | Type: {doc_type} ❌ (CONVERSATION - SKIPPED)")
                                        continue
                                    
                                    retry_doc_ids.append(doc_id)
                                    retry_doc_types[doc_id] = doc_type
                                    new_docs_on_page += 1
                                    log_console(f"📄 Retry - Found Doc ID: {doc_id} | Type: {doc_type}")
                        except Exception as e:
                            continue
                    
                    if new_docs_on_page == 0:
                        consecutive_empty_pages += 1
                        log_console(f"⚠️ No new documents on retry page {page} (consecutive: {consecutive_empty_pages})")
                        if consecutive_empty_pages >= max_consecutive_empty:
                            log_console(f"🛑 Breaking retry due to {max_consecutive_empty} consecutive empty pages")
                            break
                    else:
                        consecutive_empty_pages = 0
                        log_console(f"✅ Found {new_docs_on_page} new documents on retry page {page}")
                
                # Try to go to next page
                try:
                    next_btn = driver.find_element(By.XPATH, "//li[contains(@class,'page-next') and not(contains(@class, 'disabled'))]/a")
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(3)
                    
                    # Wait for page to load
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
                except Exception as e:
                    log_console(f"🛑 No more pages in retry: {e}")
                    break
                    
                page += 1
                
                # Safety limit to prevent infinite loops
                if page > 50:
                    log_console(f"🛑 Reached safety limit of 50 pages in retry")
                    break
                    
            except TimeoutException:
                log_console(f"🛑 Timeout on retry page {page}")
                break
            except Exception as e:
                log_console(f"🛑 Error on retry page {page}: {e}")
                break
        
        if retry_doc_ids:
            log_console(f"✅ Retry successful: Found {len(retry_doc_ids)} documents across {page-1} pages")
            # Store the doc types for the retry documents
            if not hasattr(extract_doc_ids_from_inbox, 'doc_types'):
                extract_doc_ids_from_inbox.doc_types = {}
            extract_doc_ids_from_inbox.doc_types.update(retry_doc_types)
            return retry_doc_ids, extract_doc_ids_from_inbox.doc_types
    
    return doc_ids, getattr(extract_doc_ids_from_inbox, 'doc_types', {})

def extract_doc_ids_from_signed(driver, start_date, end_date=None):
    doc_ids = []
    seen_ids = set()
    
    try:
        log_console("Navigating to Signed tab...")
        signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]")
        signed_link.click()
        time.sleep(2)
        
        # Click the "All" button to show all signed documents (not just "Signed & Unfiled")
        log_console("🔘 Clicking 'All' button to show all signed documents...")
        try:
            # Try multiple selectors for the "All" button
            all_button_selectors = [
                "//button[@data-doc-status='All']",
                "//button[contains(@class, 'btn-doc-status-filter') and contains(text(), 'All')]",
                "//button[contains(@class, 'btn-doc-status-filter') and @data-doc-status='All']",
                "//button[text()='All' and contains(@class, 'btn-doc-status-filter')]"
            ]
            
            all_button = None
            for selector in all_button_selectors:
                try:
                    all_button = driver.find_element(By.XPATH, selector)
                    log_console(f"✅ Found 'All' button with selector: {selector}")
                    break
                except:
                    continue
            
            if all_button:
                # Check if it's already active
                button_class = all_button.get_attribute("class")
                if "active" not in button_class:
                    log_console("🔘 'All' button found but not active, clicking...")
                    driver.execute_script("arguments[0].click();", all_button)
                    time.sleep(2)
                    log_console("✅ Clicked 'All' button")
                else:
                    log_console("✅ 'All' button is already active")
            else:
                log_console("⚠️ Could not find 'All' button, continuing with current view")
                
        except Exception as e:
            log_console(f"⚠️ Error with 'All' button: {e}")
            # Continue anyway, might already be on "All" view
        
        # Apply date filters
        log_console("📅 Applying date filters...")
        start_date_input = wait_and_find_element(driver, By.ID, "StartDatePicker")
        start_date_input.clear()
        start_date_input.send_keys(start_date)
        
        end_date_input = wait_and_find_element(driver, By.ID, "EndDatePicker")
        end_date_input.clear()
        if not end_date:
            end_date = datetime.now().strftime("%m/%d/%Y")
        end_date_input.send_keys(end_date)
        
        # Click Go button to apply filters
        go_button = wait_and_find_element(driver, By.ID, "btnRefreshGrid")
        log_console("🔘 Clicking 'Go' button to apply date filters...")
        
        try:
            driver.execute_script("arguments[0].click();", go_button)
            log_console("✅ 'Go' button clicked successfully")
        except Exception as e:
            log_console(f"⚠️ JavaScript click failed, trying regular click: {e}")
            go_button.click()
            log_console("✅ 'Go' button clicked with regular click")
        
        time.sleep(8)  # Increased wait time for page to load
        
        # Wait a bit more and check for "No matching records found"
        time.sleep(3)
        try:
            driver.find_element(By.XPATH, "//td[@colspan='11' and contains(text(), 'No matching records found')]")
            log_console("No Signed Orders found")
            return [], {}  # Return empty tuple instead of empty list
        except Exception:
            pass
            
        page = 1
        while True:
            log_console(f"📄 Signed docs page {page}")
            
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#signed-docs-grid tbody tr")))
                time.sleep(2)
                table_rows = driver.find_elements(By.CSS_SELECTOR, "#signed-docs-grid tbody tr")
            except TimeoutException:
                break
                
            if not table_rows:
                break
                
            # Debug: Show table structure for first row
            if page == 1 and len(table_rows) > 0:
                try:
                    first_row = table_rows[0]
                    cells = first_row.find_elements(By.TAG_NAME, "td")
                    log_console(f"🔍 Debug: Signed table has {len(cells)} columns")
                    for i, cell in enumerate(cells):
                        cell_text = cell.text.strip()
                        if cell_text:
                            log_console(f"   Column {i+1}: {cell_text[:30]}...")
                except Exception as e:
                    log_console(f"⚠️ Debug: Could not analyze table structure: {e}")
                
            for row in table_rows:
                try:
                    doc_id = row.find_element(By.CSS_SELECTOR, "td:nth-child(10) span.text-muted").text.strip()
                    if doc_id and doc_id not in seen_ids:
                        # Try to extract document type from signed table
                        doc_type = ""
                        try:
                            # First try to find the document type column by looking for specific text
                            all_cells = row.find_elements(By.TAG_NAME, "td")
                            for i, cell in enumerate(all_cells):
                                cell_text = cell.text.strip()
                                # Look for document type patterns
                                if any(keyword in cell_text.upper() for keyword in ["485", "CERT", "RECERT", "ORDER", "OTHER"]):
                                    doc_type = cell_text
                                    log_console(f"🔍 Found doc type in column {i+1}: {cell_text}")
                                    break
                            
                            # If not found by pattern, try common column positions
                            if not doc_type:
                                for col_num in [2, 3, 4, 5]:  # Try different columns
                                    try:
                                        cell = row.find_element(By.CSS_SELECTOR, f"td:nth-child({col_num})")
                                        cell_text = cell.text.strip()
                                        if cell_text and len(cell_text) < 50 and not cell_text.startswith("Dr."):
                                            doc_type = cell_text
                                            log_console(f"🔍 Found potential doc type in column {col_num}: {cell_text}")
                                            break
                                    except:
                                        continue
                        except Exception as e:
                            log_console(f"⚠️ Error extracting doc type: {e}")
                            pass
                        
                        # Get company key from function parameters or use default
                        company_key = getattr(extract_doc_ids_from_signed, 'company_key', None)
                        if not company_key:
                            # Try to get from config or use prima_care as fallback
                            from config import get_active_company
                            try:
                                active_company = get_active_company()
                                company_key = active_company.get('key', 'prima_care')
                                log_console(f"🔍 Debug: Detected company key: {company_key}")
                            except Exception as e:
                                company_key = 'prima_care'
                                log_console(f"⚠️ Debug: Failed to get company key, using fallback: {e}")
                        
                        # Filter for documents based on company configuration
                        from config import should_filter_document_types, get_allowed_document_types, get_document_type_filter
                        should_filter = should_filter_document_types(company_key)
                        filter_config = get_document_type_filter(company_key)
                        allowed_types = filter_config.get("allowed_types", [])
                        excluded_types = filter_config.get("excluded_types", [])
                        
                        log_console(f"🔍 Debug: Company: {company_key}, Should filter: {should_filter}, Allowed types: {allowed_types}, Excluded types: {excluded_types}")
                        
                        is_allowed_document = True  # Default to allow
                        if should_filter and doc_type:
                            doc_type_upper = doc_type.upper()
                            
                            # Check if document type is explicitly excluded
                            if excluded_types:
                                is_excluded = any(excluded.lower() in doc_type_upper.lower() for excluded in excluded_types)
                                if is_excluded:
                                    is_allowed_document = False
                                    log_console(f"📄 Signed - Doc ID: {doc_id} | Type: {doc_type} ❌ (EXCLUDED - SKIPPED)")
                                else:
                                    is_allowed_document = True
                            # If no excluded types, check allowed types (backward compatibility)
                            elif allowed_types:
                                is_allowed_document = any(keyword in doc_type_upper for keyword in allowed_types)
                        
                        if is_allowed_document or not should_filter:
                            seen_ids.add(doc_id)
                            doc_ids.append(doc_id)
                            
                            # Store document type with doc_id
                            if not hasattr(extract_doc_ids_from_signed, 'doc_types'):
                                extract_doc_ids_from_signed.doc_types = {}
                            extract_doc_ids_from_signed.doc_types[doc_id] = doc_type
                            log_console(f"📄 Signed - Doc ID: {doc_id} | Type: {doc_type} ✅ (ALLOWED - INCLUDED)")
                        else:
                            log_console(f"📄 Signed - Doc ID: {doc_id} | Type: {doc_type} ❌ (NOT ALLOWED - SKIPPED)")
                except Exception as e:
                    log_console(f"⚠️ Error extracting doc_id in signed tab: {e}")
                    continue
                    
            if len(table_rows) < 10:
                break
                
            try:
                next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
                next_button.click()
                time.sleep(2)
            except Exception as e:
                log_console(f"⚠️ Next button not found in signed tab, breaking. {e}")
                break
                
            page += 1
            
    except Exception as e:
        log_console(f"❌ Error scraping Signed tab: {e}")
        
    # Retry if no documents found
    if len(doc_ids) == 0:
        log_console("⚠️ No documents found in signed tab, retrying with longer wait...")
        time.sleep(5)
        driver.refresh()
        time.sleep(3)
        
        try:
            signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]")
            signed_link.click()
            time.sleep(3)
            
            start_date_input = wait_and_find_element(driver, By.ID, "StartDatePicker")
            start_date_input.clear()
            start_date_input.send_keys(start_date)
            
            end_date_input = wait_and_find_element(driver, By.ID, "EndDatePicker")
            end_date_input.clear()
            if not end_date:
                end_date = datetime.now().strftime("%m/%d/%Y")
            end_date_input.send_keys(end_date)
            
            go_button = wait_and_find_element(driver, By.ID, "btnRefreshGrid")
            go_button.click()
            time.sleep(8)
            
            try:
                driver.find_element(By.XPATH, "//td[@colspan='11' and contains(text(), 'No matching records found')]")
                log_console("No Signed Orders found on retry")
            except Exception:
                retry_doc_ids = []
                page = 1
                while page <= 3:
                    try:
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#signed-docs-grid tbody tr")))
                        time.sleep(2)
                        table_rows = driver.find_elements(By.CSS_SELECTOR, "#signed-docs-grid tbody tr")
                        
                        if table_rows:
                            for row in table_rows:
                                try:
                                    doc_id = row.find_element(By.CSS_SELECTOR, "td:nth-child(10) span.text-muted").text.strip()
                                    if doc_id and doc_id not in retry_doc_ids:
                                        retry_doc_ids.append(doc_id)
                                except Exception as e:
                                    continue
                        
                        if len(table_rows) < 10:
                            break
                            
                        try:
                            next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
                            next_button.click()
                            time.sleep(3)
                        except Exception as e:
                            break
                            
                        page += 1
                    except TimeoutException:
                        break
                
                if retry_doc_ids:
                    log_console(f"✅ Signed retry successful: Found {len(retry_doc_ids)} documents")
                    return retry_doc_ids, getattr(extract_doc_ids_from_signed, 'doc_types', {})
        except Exception as e:
            log_console(f"❌ Error on signed retry: {e}")
        
    # Show document type summary for signed documents
    doc_types = getattr(extract_doc_ids_from_signed, 'doc_types', {})
    if doc_types:
        type_counts = {}
        for doc_type in doc_types.values():
            if doc_type:
                type_counts[doc_type] = type_counts.get(doc_type, 0) + 1
        if type_counts:
            log_console(f"📊 Signed Document Types found:")
            for doc_type, count in type_counts.items():
                log_console(f"   • {doc_type}: {count} documents")
    
    return doc_ids, getattr(extract_doc_ids_from_signed, 'doc_types', {})

def extract_npi_only(doc_id, driver):
    """Optimized NPI extraction with better error handling and debugging."""
    # Thread-safe counter
    if not hasattr(extract_npi_only, 'counter'):
        extract_npi_only.counter = 0
        extract_npi_only.lock = threading.Lock()
        
    with extract_npi_only.lock:
        extract_npi_only.counter += 1
        current_counter = extract_npi_only.counter
    
    # Reduced frequency of page refreshes
    if current_counter % 100 == 1:
        go_to_signed_list(driver)
        time.sleep(0.1)  # Slightly increased for stability
    
    detail_url = f"https://backoffice.doctoralliance.com/Documents2/Show/{doc_id}"
    
    try:
        driver.get(detail_url)
        # Increased wait time for better reliability
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(0.5)  # Small delay for page to fully load
    except Exception as e:
        log_console(f"⚠️ Failed to load page for {doc_id}: {e}")
        return ""
        
    actual_url = driver.current_url
    if actual_url != detail_url:
        log_console(f"⚠️ Navigation failed for {doc_id}: {actual_url}")
        return ""
    
    # Enhanced NPI extraction with more XPath patterns
    xpaths_to_try = [
        "//span[contains(text(), 'NPI')]/following-sibling::span",
        "//p[contains(text(), 'NPI')]/span",
        "//div[contains(@class, 'physician')]//span[contains(text(), '1')]",
        "//*[contains(text(), '1') and string-length(normalize-space(.)) = 10]",
        "//span[contains(text(), 'NPI')]/parent::*/span[2]",
        "//div[contains(text(), 'NPI')]/following-sibling::div//span",
        "//td[contains(text(), 'NPI')]/following-sibling::td//span",
        "//label[contains(text(), 'NPI')]/following-sibling::span"
    ]
    
    npi = ""
    for xpath in xpaths_to_try:
        try:
            element = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, xpath)))
            text = element.text.strip()
            match = re.search(r'\b\d{10}\b', text)
            if match:
                npi = match.group(0)
                break
        except Exception:
            continue
            
    if not npi:
        # Enhanced page source extraction
        text = driver.page_source
        
        # Try multiple regex patterns
        patterns = [
            r"\[(\d{10})\]",
            r'\b(\d{10})\b',
            r'NPI[:\s]*(\d{10})',
            r'National Provider Identifier[:\s]*(\d{10})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                npi = match.group(1)
                break
    
    # Debug logging for empty NPIs
    if not npi:
        log_console(f"⚠️ No NPI found for {doc_id} - checking page content...")
        try:
            # Try to find any 10-digit number on the page
            text = driver.page_source
            all_numbers = re.findall(r'\b\d{10}\b', text)
            if all_numbers:
                log_console(f"   Found numbers: {all_numbers[:3]}...")  # Show first 3
        except:
            pass
    
    return npi

def run_id_and_npi_extraction(da_url, da_login, da_password, helper_id, start_date, end_date=None, company_key=None):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    if company_key:
        output_path = os.path.join("Combined", f"DocumentID_NPI_{company_key}_{timestamp}.xlsx")
    else:
        output_path = os.path.join("Combined", f"DocumentID_NPI_{timestamp}.xlsx")
    
    os.makedirs("Combined", exist_ok=True)
    
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")
    options.add_argument("--disable-css")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-features=TranslateUI")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--page-load-strategy=eager")
    options.add_argument("--aggressive-cache-discard")
    options.add_argument("--memory-pressure-off")
    # Removed --disable-javascript to prevent timeout issues
    options.add_argument("--disable-animations")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    # Add stability options
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-features=VizDisplayCompositor")
    
    # Additional performance optimizations
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-prompt-on-repost")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--no-first-run")
    options.add_argument("--safebrowsing-disable-auto-update")
    options.add_argument("--enable-automation")
    options.add_argument("--password-store=basic")
    options.add_argument("--use-mock-keychain")
    options.add_argument("--force-device-scale-factor=1")
    options.add_argument("--high-dpi-support=1")
    options.add_argument("--force-color-profile=srgb")
    options.add_argument("--disable-low-res-tiling")
    options.add_argument("--disable-partial-raster")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-threaded-animation")
    options.add_argument("--disable-threaded-scrolling")
    options.add_argument("--disable-checker-imaging")
    options.add_argument("--disable-new-content-rendering-timeout")
    options.add_argument("--disable-image-animation-resync")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-features=TranslateUI,BlinkGenPropertyTrees")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--enable-features=NetworkService,NetworkServiceLogging")
    options.add_argument("--force-fieldtrials=*BackgroundTracing/default/")
    options.add_argument("--memory-pressure-off")
    options.add_argument("--max_old_space_size=4096")
    
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.set_page_load_timeout(15)  # Reduced for faster failure detection
    driver.implicitly_wait(2)  # Reduced for faster processing
    driver.set_script_timeout(10)  # Set script timeout

    try:
        login_to_da(da_url, da_login, da_password, driver)
        driver.get("https://backoffice.doctoralliance.com/Search")
        wait_and_find_element(driver, By.ID, "Query").send_keys(helper_id)
        wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
        wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field").send_keys("Users")
        WebDriverWait(driver, 8).until(EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))).click()
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()
        time.sleep(0.5)
        wait_and_find_element(driver, By.CLASS_NAME, "linkedRow").click()
        time.sleep(0.5)
        wait_and_find_element(driver, By.LINK_TEXT, "Impersonate").click()
        time.sleep(1)
        driver.switch_to.window(driver.window_handles[1])
        
        # Inbox
        log_console("🔍 Inbox extraction")
        try:
            driver.get("https://live.doctoralliance.com/all/Inbox")
            time.sleep(5)  # Wait for page to load completely
            log_console("✅ Successfully navigated to inbox page")
        except Exception as e:
            log_console(f"❌ Failed to navigate to inbox page: {e}")
            raise e
            
        # Set company key for extraction functions
        extract_doc_ids_from_inbox.company_key = company_key
        extract_doc_ids_from_signed.company_key = company_key
        
        inbox_doc_ids, inbox_doc_types = extract_doc_ids_from_inbox(driver, start_date, end_date)
        
        # Signed
        log_console("🔍 Signed docs extraction")
        signed_doc_ids, signed_doc_types = extract_doc_ids_from_signed(driver, start_date, end_date)
        
        # Combine document types
        all_doc_types = {**inbox_doc_types, **signed_doc_types}
        all_doc_ids = list(dict.fromkeys(inbox_doc_ids + signed_doc_ids))
        log_console(f"📝 Extracting NPI from {len(all_doc_ids)} documents...")
        
        records = []
        filtered_records = []
        
        # Optimized batch processing
        batch_size = 20  # Process documents in batches
        total_batches = (len(all_doc_ids) + batch_size - 1) // batch_size
        
        log_console(f"🚀 Processing {len(all_doc_ids)} documents in {total_batches} batches of {batch_size}")
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(all_doc_ids))
            batch_doc_ids = all_doc_ids[start_idx:end_idx]
            
            log_console(f"📦 Processing batch {batch_idx + 1}/{total_batches} ({len(batch_doc_ids)} documents)")
            
                    # Process batch sequentially to avoid Selenium driver conflicts
        for doc_id in batch_doc_ids:
            npi = ""
            document_type = all_doc_types.get(doc_id, "")
            
            # Try up to 3 times for each document
            for attempt in range(3):
                try:
                    npi = extract_npi_only(doc_id, driver)
                    if npi:  # If we got an NPI, break
                        break
                    elif attempt < 2:  # If no NPI and not last attempt, retry
                        log_console(f"🔄 Retrying {doc_id} (attempt {attempt + 2}/3)")
                        time.sleep(1)  # Brief pause before retry
                except Exception as e:
                    if attempt < 2:
                        log_console(f"⚠️ Error processing {doc_id} (attempt {attempt + 1}/3): {e}")
                        time.sleep(1)
                    else:
                        log_console(f"❌ Failed to process {doc_id} after 3 attempts: {e}")
            
            record = {"Document ID": doc_id, "NPI": npi, "Document Type": document_type}
            records.append(record)
            filtered_records.append(record)
            
            log_console(f"✅ {doc_id}  NPI: {npi}  Type: {document_type} (PROCESSED)")
            
            # Progress update
            processed = len(records)
            log_console(f"📊 Progress: {processed}/{len(all_doc_ids)} ({processed/len(all_doc_ids)*100:.1f}%)")
        
        final_records = filtered_records if filtered_records else records
        combined_df = pd.DataFrame(final_records)
        combined_df.to_excel(output_path, index=False)
        log_console(f"✅ Combined Excel created at: {output_path}\nRows: {len(combined_df)}")
        
        if len(final_records) > 0:
            npi_found = len([r for r in final_records if r['NPI']])
            success_rate = (npi_found/len(final_records)*100) if len(final_records) > 0 else 0
            log_console(f"📊 Success rate: {npi_found}/{len(final_records)} ({success_rate:.1f}%)")
            
            # Check if filtering was applied
            from config import should_filter_document_types, get_document_type_filter
            should_filter = should_filter_document_types(company_key)
            filter_config = get_document_type_filter(company_key)
            allowed_types = filter_config.get("allowed_types", [])
            excluded_types = filter_config.get("excluded_types", [])
            
            if should_filter and (allowed_types or excluded_types):
                if excluded_types:
                    log_console(f"📊 Filtered: {len(filtered_records)}/{len(records)} documents (excluded types: {', '.join(excluded_types)})")
                else:
                    log_console(f"📊 Filtered: {len(filtered_records)}/{len(records)} documents matched allowed types: {', '.join(allowed_types)}")
            else:
                log_console(f"📊 No filtering applied: {len(final_records)} documents processed")
        else:
            log_console("📊 No documents found in the specified date range")
        
    except Exception as e:
        log_console(f"❌ Extraction failed: {e}")
    finally:
        driver.quit()
        log_console("👋 WebDriver closed")

if __name__ == "__main__":
    print("🚀 Doctor Alliance - Document ID & NPI Extractor (Ultra-Optimized Mode)")
    print("=" * 40)
    
    if len(sys.argv) > 1:
        user_start_date = sys.argv[1]
        print(f" Using start date from argument: {user_start_date}")
    else:
        user_start_date = input(" Enter start date (MM/DD/YYYY) or press Enter for default (30 days ago): ").strip()
        if not user_start_date:
            default_date = (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")
            user_start_date = default_date
            print(f" Using default start date: {user_start_date}")
    
    user_end_date = None
    if len(sys.argv) > 2:
        user_end_date = sys.argv[2]
        print(f" Using end date from argument: {user_end_date}")
    
    company_key = None
    if len(sys.argv) > 3:
        company_key = sys.argv[3]
        print(f" Using company key from argument: {company_key}")
    
    from config import get_active_company, get_company_config
    try:
        if company_key:
            company_info = get_company_config(company_key)
        else:
            company_info = get_active_company()
        helper_id = company_info['helper_id']
        print(f" Using helper ID for {company_info['name']}: {helper_id}")
    except Exception as e:
        print(f" Warning: Could not get helper ID from config, using default: {e}")
        helper_id = ""
    
    da_url = "https://backoffice.doctoralliance.com"
    da_login = "sannidhay"
    da_password = "DA@2025"
    
    run_id_and_npi_extraction(
        da_url=da_url,
        da_login=da_login,
        da_password=da_password,
        helper_id=helper_id,
        start_date=user_start_date,
        end_date=user_end_date,
        company_key=company_key
    )
