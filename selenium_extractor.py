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
from config import EXTRACTOR_LIMITS
import signal

# Global stop flag for graceful Ctrl+C handling
STOP_REQUESTED = False

def _sigint_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log_console("\n[CTRL-C] Stop requested. Finishing current doc and exiting...")

try:
    signal.signal(signal.SIGINT, _sigint_handler)
except Exception:
    pass

def log_console(msg):
    print(msg)

def wait_and_find_element(driver, by, value, timeout=5):  # Reduced default from 10 to 5
    return WebDriverWait(driver, timeout, poll_frequency=0.2).until(EC.presence_of_element_located((by, value)))  # Added faster polling

def login_to_da(da_url, da_login, da_password, driver, timeout=10):  # Reduced timeout from 15 to 10
    log_console(f"🔐 Logging into DA Backoffice...")
    driver.get(da_url)
    wait = WebDriverWait(driver, timeout, poll_frequency=0.2)  # Added faster polling
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
    # Use shared seen_ids to prevent duplicates across inbox and signed
    if not hasattr(extract_doc_ids_from_inbox, 'shared_seen_ids'):
        extract_doc_ids_from_inbox.shared_seen_ids = set()
    seen_ids = extract_doc_ids_from_inbox.shared_seen_ids
    processed_urls = set()
    consecutive_no_new_docs = 0
    max_consecutive_no_new = EXTRACTOR_LIMITS.get("max_consecutive_no_new", 3)
    max_pages = EXTRACTOR_LIMITS.get("inbox_max_pages", 200)
    
    while page <= max_pages and not STOP_REQUESTED:
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
            if STOP_REQUESTED:
                break
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
    # Use shared seen_ids to prevent duplicates across inbox and signed
    # Use the same shared_seen_ids as inbox function
    if not hasattr(extract_doc_ids_from_inbox, 'shared_seen_ids'):
        extract_doc_ids_from_inbox.shared_seen_ids = set()
    seen_ids = extract_doc_ids_from_inbox.shared_seen_ids
    
    try:
        log_console("Navigating to Signed tab...")
        
        # Try direct navigation to signed page first with retry mechanism
        navigation_successful = False
        for attempt in range(3):  # Try up to 3 times
            try:
                log_console(f"📄 Navigation attempt {attempt + 1}/3...")
                driver.get("https://live.doctoralliance.com/all/Documents/Signed")
                # Wait for critical elements with increased timeout
                WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                # Wait for the page to be interactive
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "StartDatePicker")))
                time.sleep(1)  # Allow page to fully load
                log_console("✅ Direct navigation to signed page successful")
                navigation_successful = True
                break
            except Exception as e:
                log_console(f"⚠️ Direct navigation attempt {attempt + 1} failed: {e}")
                if attempt < 2:  # Not the last attempt
                    time.sleep(2)  # Wait before retry
                    continue
                else:
                    log_console("⚠️ Direct navigation failed all attempts, trying click method...")
                    try:
                        signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]", timeout=10)
                        signed_link.click()
                        time.sleep(2)  # Wait for navigation
                        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.ID, "StartDatePicker")))
                        navigation_successful = True
                        log_console("✅ Click navigation to signed page successful")
                    except Exception as click_error:
                        log_console(f"❌ Click navigation also failed: {click_error}")
        
        if not navigation_successful:
            log_console("❌ All navigation methods failed for signed page")
            return [], {}
        
        # Additional wait for page stability
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "StartDatePicker")))
            log_console("✅ Date picker confirmed available")
        except:
            log_console("⚠️ Date picker not found, page may not be fully loaded")
            time.sleep(2)  # Extended fallback wait
        
        # Click the "All" button to show all signed documents (not just "Signed & Unfiled")
        log_console("🔘 Clicking 'All' button to show all signed documents...")
        try:
            # Extended list of selectors for the "All" button
            all_button_selectors = [
                "//button[@data-doc-status='All']",
                "//button[contains(@class, 'btn-doc-status-filter') and contains(text(), 'All')]",
                "//button[contains(@class, 'btn-doc-status-filter') and @data-doc-status='All']",
                "//button[text()='All' and contains(@class, 'btn-doc-status-filter')]",
                "//button[normalize-space(text())='All']",
                "//input[@value='All']/../button",
                "//a[contains(text(), 'All')]",
                "//span[contains(text(), 'All')]/.."
            ]
            
            all_button = None
            # Try to find All button with retry mechanism
            for attempt in range(3):
                for selector in all_button_selectors:
                    try:
                        all_button = WebDriverWait(driver, 2).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                        log_console(f"✅ Found 'All' button with selector: {selector}")
                        break
                    except:
                        continue
                
                if all_button:
                    break
                elif attempt < 2:
                    log_console(f"⚠️ All button not found on attempt {attempt + 1}, retrying...")
                    time.sleep(1)
                    # Try refreshing the current view
                    try:
                        driver.refresh()
                        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "StartDatePicker")))
                        time.sleep(1)
                    except:
                        pass
            
            if all_button:
                # Check if it's already active with retry
                button_clicked = False
                for click_attempt in range(3):
                    try:
                        button_class = all_button.get_attribute("class") or ""
                        if "active" not in button_class.lower():
                            log_console(f"🔘 'All' button found but not active, clicking... (attempt {click_attempt + 1})")
                            
                            # Try multiple click methods
                            if click_attempt == 0:
                                # JavaScript click first
                                driver.execute_script("arguments[0].scrollIntoView(true);", all_button)
                                time.sleep(0.2)
                                driver.execute_script("arguments[0].click();", all_button)
                                log_console("✅ Clicked 'All' button with JavaScript")
                            elif click_attempt == 1:
                                # Regular click with scroll
                                driver.execute_script("arguments[0].scrollIntoView(true);", all_button)
                                time.sleep(0.2)
                                all_button.click()
                                log_console("✅ Clicked 'All' button with regular click")
                            else:
                                # Force click with focus
                                driver.execute_script("arguments[0].focus(); arguments[0].click();", all_button)
                                log_console("✅ Clicked 'All' button with focus+click")
                            
                            time.sleep(1)  # Wait for click to register
                            
                            # Verify the click worked
                            try:
                                updated_class = all_button.get_attribute("class") or ""
                                if "active" in updated_class.lower():
                                    log_console("✅ 'All' button successfully activated")
                                    button_clicked = True
                                    break
                                else:
                                    log_console("⚠️ 'All' button click may not have registered, retrying...")
                            except:
                                # Button might have changed after click, try to find it again
                                time.sleep(1)
                                break
                        else:
                            log_console("✅ 'All' button is already active")
                            button_clicked = True
                            break
                    except Exception as click_error:
                        log_console(f"⚠️ Click attempt {click_attempt + 1} failed: {click_error}")
                        time.sleep(0.5)
                        # Try to find the button again
                        for selector in all_button_selectors:
                            try:
                                all_button = driver.find_element(By.XPATH, selector)
                                break
                            except:
                                continue
                
                if not button_clicked:
                    log_console("⚠️ Failed to activate 'All' button after multiple attempts")
            else:
                log_console("⚠️ Could not find 'All' button after multiple attempts, continuing with current view")
                
        except Exception as e:
            log_console(f"⚠️ Error with 'All' button: {e}")
            # Continue anyway, might already be on "All" view
        
        # Apply date filters with better error handling and retry
        log_console("📅 Applying date filters...")
        date_filters_applied = False
        for date_attempt in range(3):
            try:
                log_console(f"📅 Date filter attempt {date_attempt + 1}/3...")
                
                # Wait for and clear start date
                start_date_input = wait_and_find_element(driver, By.ID, "StartDatePicker", timeout=8)
                driver.execute_script("arguments[0].scrollIntoView(true);", start_date_input)
                time.sleep(0.3)
                start_date_input.clear()
                time.sleep(0.2)
                start_date_input.send_keys(start_date)
                log_console(f"✅ Start date set to: {start_date}")
                
                # Wait for and clear end date
                end_date_input = wait_and_find_element(driver, By.ID, "EndDatePicker", timeout=5)
                driver.execute_script("arguments[0].scrollIntoView(true);", end_date_input)
                time.sleep(0.3)
                end_date_input.clear()
                time.sleep(0.2)
                if not end_date:
                    end_date = datetime.now().strftime("%m/%d/%Y")
                end_date_input.send_keys(end_date)
                log_console(f"✅ End date set to: {end_date}")
                
                # Verify the dates were set correctly
                start_value = start_date_input.get_attribute("value")
                end_value = end_date_input.get_attribute("value")
                if start_value and end_value:
                    log_console(f"✅ Date filters verified - Start: {start_value}, End: {end_value}")
                    date_filters_applied = True
                    break
                else:
                    log_console(f"⚠️ Date filter verification failed - Start: {start_value}, End: {end_value}")
                    
            except Exception as e:
                log_console(f"⚠️ Date filter attempt {date_attempt + 1} failed: {e}")
                if date_attempt < 2:
                    time.sleep(1)
                    continue
                else:
                    log_console(f"❌ All date filter attempts failed: {e}")
                    return [], {}
        
        if not date_filters_applied:
            log_console("❌ Failed to apply date filters after multiple attempts")
            return [], {}
        
        # Click Go button to apply filters with enhanced retry logic
        log_console("🔘 Clicking 'Go' button to apply date filters...")
        go_button_clicked = False
        for go_attempt in range(3):
            try:
                log_console(f"🔘 Go button attempt {go_attempt + 1}/3...")
                go_button = wait_and_find_element(driver, By.ID, "btnRefreshGrid", timeout=8)
                
                # Scroll to button and ensure it's visible
                driver.execute_script("arguments[0].scrollIntoView(true);", go_button)
                time.sleep(0.3)
                
                # Try multiple click methods
                click_successful = False
                for click_method in range(3):
                    try:
                        if click_method == 0:
                            # JavaScript click with scroll
                            driver.execute_script("arguments[0].scrollIntoView(true); arguments[0].click();", go_button)
                            log_console("✅ 'Go' button clicked with JavaScript+scroll")
                        elif click_method == 1:
                            # Regular click after ensuring visibility
                            WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.ID, "btnRefreshGrid")))
                            go_button.click()
                            log_console("✅ 'Go' button clicked with regular click")
                        else:
                            # Force click with focus
                            driver.execute_script("arguments[0].focus(); arguments[0].click();", go_button)
                            log_console("✅ 'Go' button clicked with focus+click")
                        
                        # Wait for page to start loading/refreshing
                        time.sleep(1)
                        click_successful = True
                        break
                    except Exception as e:
                        log_console(f"⚠️ Click method {click_method+1} failed: {e}")
                        time.sleep(0.3)
                
                if click_successful:
                    go_button_clicked = True
                    break
                else:
                    log_console(f"⚠️ All click methods failed on attempt {go_attempt + 1}")
                    
            except Exception as e:
                log_console(f"⚠️ Go button attempt {go_attempt + 1} failed: {e}")
                if go_attempt < 2:
                    time.sleep(1)
                    continue
        
        if not go_button_clicked:
            log_console("❌ Failed to click 'Go' button after all attempts")
            return [], {}
        
        # Wait for filtered results with better timing
        log_console("⏳ Waiting for filtered results to load...")
        
        # Wait for page to start refreshing/loading
        time.sleep(2)  # Give time for the filter to trigger page refresh
        
        # Wait for table content to load with better detection
        table_loaded = False
        results_ready = False
        
        for wait_attempt in range(10):  # Increased attempts for better reliability
            try:
                log_console(f"🔄 Checking for filtered results... (attempt {wait_attempt + 1}/10)")
                
                # Wait for any loading indicators to disappear
                try:
                    loading_indicators = driver.find_elements(By.XPATH, "//div[contains(@class, 'loading') or contains(@class, 'spinner')]")
                    if loading_indicators:
                        for indicator in loading_indicators:
                            if indicator.is_displayed():
                                log_console("⏳ Page still loading...")
                                time.sleep(1)
                                continue
                except:
                    pass
                
                # Look for table content
                table = driver.find_element(By.XPATH, "//table[contains(@class, 'table')]//tbody")
                if table:
                    table_loaded = True
                    
                    # Check if we have actual data rows or just "no records" message
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    if len(rows) > 0:
                        # Check if it's the "no records" row
                        first_row = rows[0]
                        row_text = first_row.text.lower()
                        if "no matching records" in row_text or "no records found" in row_text:
                            log_console("❌ No Signed Orders found in the specified date range")
                            return [], {}
                        else:
                            log_console(f"✅ Found {len(rows)} rows in results table")
                            results_ready = True
                            break
                    else:
                        log_console("⏳ Table found but no rows yet...")
                        
            except Exception as e:
                log_console(f"⏳ Still waiting for results... ({e})")
                time.sleep(2)  # Longer wait between attempts
                continue
                
            if not results_ready:
                time.sleep(1)  # Wait before next check
        
        if not table_loaded:
            log_console("⚠️ Could not detect table loading, proceeding anyway...")
            time.sleep(3)  # Extended fallback wait
        
        if not results_ready and table_loaded:
            log_console("⚠️ Table loaded but results status unclear, proceeding with extraction...")
        
        # Additional debugging - let's see what's actually on the page
        try:
            log_console("🔍 Debug: Checking page content...")
            
            # Check current URL
            current_url = driver.current_url
            log_console(f"🔍 Debug: Current URL: {current_url}")
            
            # Check for any table content
            tables = driver.find_elements(By.TAG_NAME, "table")
            log_console(f"🔍 Debug: Found {len(tables)} tables on page")
            
            if tables:
                for i, table in enumerate(tables):
                    try:
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        log_console(f"🔍 Debug: Table {i+1} has {len(rows)} rows")
                        if len(rows) > 0:
                            # Check first few rows for content
                            for j, row in enumerate(rows[:3]):
                                cells = row.find_elements(By.TAG_NAME, "td")
                                if cells:
                                    log_console(f"🔍 Debug: Row {j+1} has {len(cells)} cells")
                                    if j == 0:  # Log first row content
                                        row_text = row.text.strip()[:100]  # First 100 chars
                                        log_console(f"🔍 Debug: First row text: {row_text}")
                    except Exception as e:
                        log_console(f"🔍 Debug: Error examining table {i+1}: {e}")
            
        except Exception as e:
            log_console(f"🔍 Debug: Error during page content check: {e}")
        
        log_console("✅ Date filters applied successfully, proceeding with extraction...")
            
        page = 1
        max_pages = EXTRACTOR_LIMITS.get("signed_max_pages", 200)  # Safety limit configurable
        consecutive_no_new_docs = 0
        max_consecutive_no_new = EXTRACTOR_LIMITS.get("max_consecutive_no_new", 3)
        
        while page <= max_pages and not STOP_REQUESTED:
            log_console(f"📄 Signed docs page {page}")
            new_docs_on_page = 0  # Count new documents found on this page
            
            try:
                # Try multiple selectors for the signed docs table
                table_selectors = [
                    "#signed-docs-grid tbody tr",
                    "table tbody tr",
                    "//table//tbody//tr",
                    "//table[contains(@class, 'table')]//tbody//tr"
                ]
                
                table_rows = []
                for selector in table_selectors:
                    try:
                        if selector.startswith("//"):
                            # XPath selector
                            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.XPATH, selector)))
                            table_rows = driver.find_elements(By.XPATH, selector)
                        else:
                            # CSS selector
                            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                            table_rows = driver.find_elements(By.CSS_SELECTOR, selector)
                        
                        if table_rows:
                            log_console(f"✅ Found table using selector: {selector}")
                            break
                    except:
                        continue
                
                if not table_rows:
                    log_console("⚠️ Could not find table with any selector, trying generic approach...")
                    time.sleep(3)
                    table_rows = driver.find_elements(By.XPATH, "//tr[td]")  # Any row with td elements
                
                # Add null check for table_rows
                if table_rows is None:
                    log_console("⚠️ table_rows is None, breaking out of loop")
                    break
                    
            except TimeoutException:
                log_console("⚠️ Timeout waiting for table rows")
                break
            except Exception as e:
                log_console(f"⚠️ Error finding table rows: {e}")
                break
                
            if not table_rows:
                log_console("⚠️ No table rows found, breaking out of loop")
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
                            new_docs_on_page += 1
                            
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
            
            # Check if we found any new documents on this page
            if new_docs_on_page == 0:
                consecutive_no_new_docs += 1
                log_console(f"⚠️ No new documents on signed page {page} (consecutive: {consecutive_no_new_docs})")
                
                if consecutive_no_new_docs >= max_consecutive_no_new:
                    log_console(f"🛑 Breaking signed extraction due to {max_consecutive_no_new} consecutive pages with no new documents")
                    break
            else:
                consecutive_no_new_docs = 0
                log_console(f"✅ Found {new_docs_on_page} new documents on signed page {page}")
                    
            # Check if there's a next page available
            try:
                next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
                # Check if the next button is disabled or not clickable
                button_class = next_button.get_attribute("class") or ""
                aria_disabled = next_button.get_attribute("aria-disabled") or ""
                if "disabled" in button_class or "disabled" in aria_disabled:
                    log_console("✅ Reached last page (next button disabled)")
                    break
                next_button.click()
                time.sleep(2)
            except Exception as e:
                log_console(f"✅ No more pages available in signed tab: {e}")
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
                while page <= EXTRACTOR_LIMITS.get("signed_max_pages", 200):
                    try:
                        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#signed-docs-grid tbody tr")))
                        time.sleep(2)
                        table_rows = driver.find_elements(By.CSS_SELECTOR, "#signed-docs-grid tbody tr")
                        
                        # Add null check for table_rows in retry
                        if table_rows is None:
                            log_console("⚠️ table_rows is None in retry, breaking out of loop")
                            break
                        
                        if table_rows:
                            for row in table_rows:
                                try:
                                    doc_id = row.find_element(By.CSS_SELECTOR, "td:nth-child(10) span.text-muted").text.strip()
                                    if doc_id and doc_id not in retry_doc_ids:
                                        retry_doc_ids.append(doc_id)
                                except Exception as e:
                                    continue
                        
                        # Check if there's a next page available
                        try:
                            next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
                            # Check if the next button is disabled or not clickable
                            button_class = next_button.get_attribute("class") or ""
                            aria_disabled = next_button.get_attribute("aria-disabled") or ""
                            if "disabled" in button_class or "disabled" in aria_disabled:
                                log_console("✅ Reached last page on retry (next button disabled)")
                                break
                            next_button.click()
                            time.sleep(3)
                        except Exception as e:
                            log_console(f"✅ No more pages available on retry: {e}")
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

def reset_extraction_state():
    """Reset global state to prevent duplicate processing across multiple company runs."""
    # Reset doc_types for both extraction functions
    if hasattr(extract_doc_ids_from_inbox, 'doc_types'):
        delattr(extract_doc_ids_from_inbox, 'doc_types')
    if hasattr(extract_doc_ids_from_signed, 'doc_types'):
        delattr(extract_doc_ids_from_signed, 'doc_types')
    
    # Reset company_key for both extraction functions
    if hasattr(extract_doc_ids_from_inbox, 'company_key'):
        delattr(extract_doc_ids_from_inbox, 'company_key')
    if hasattr(extract_doc_ids_from_signed, 'company_key'):
        delattr(extract_doc_ids_from_signed, 'company_key')
    
    # Reset shared seen_ids to prevent duplicates across companies
    if hasattr(extract_doc_ids_from_inbox, 'shared_seen_ids'):
        delattr(extract_doc_ids_from_inbox, 'shared_seen_ids')
    if hasattr(extract_doc_ids_from_signed, 'shared_seen_ids'):
        delattr(extract_doc_ids_from_signed, 'shared_seen_ids')
    
    log_console("🔄 Reset extraction state for new company processing")

def run_id_and_npi_extraction(da_url, da_login, da_password, helper_id, start_date, end_date=None, company_key=None):
    # Reset global state to prevent duplicate processing
    reset_extraction_state()
    
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
    options.add_argument("--page-load-strategy=none")  # Fastest loading - don't wait for everything
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
    
    # Ultra-aggressive network and rendering optimizations for speed
    options.add_argument("--disable-features=MediaRouter")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-domain-reliability")
    options.add_argument("--disable-features=AudioServiceOutOfProcess")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-background-sync")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-component-extensions-with-background-pages")
    options.add_argument("--disable-features=TransferSizeUpdatedOnPrefetch")
    options.add_argument("--disable-features=VizServiceDisplay")
    options.add_argument("--disable-web-resources")
    options.add_argument("--disable-datasaver-prompt")
    options.add_argument("--disable-save-password-bubble")
    options.add_argument("--disable-session-crashed-bubble")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-tools")
    
    # Network-level speed optimizations
    prefs = {
        "profile.default_content_setting_values": {
            "images": 2,  # Block images for speed
            "plugins": 2,  # Block plugins
            "popups": 2,   # Block popups
            "geolocation": 2,  # Block location
            "notifications": 2,  # Block notifications
            "media_stream": 2,  # Block media
        },
        "profile.managed_default_content_settings": {
            "images": 2
        },
        "profile.content_settings.exceptions.images": {},
        "webkit.webprefs.loads_images_automatically": False
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.maximize_window()
        driver.set_page_load_timeout(15)  # Aggressive timeout for speed
        driver.implicitly_wait(2)  # Minimal wait for speed  
        driver.set_script_timeout(10)  # Reduced script timeout for speed
        
        # Add stealth mode
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        login_to_da(da_url, da_login, da_password, driver)
        driver.get("https://backoffice.doctoralliance.com/Search")
        wait_and_find_element(driver, By.ID, "Query").send_keys(helper_id)
        wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
        wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field").send_keys("Users")
        WebDriverWait(driver, 8).until(EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))).click()
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()
        time.sleep(0.2)  # Minimal wait
        wait_and_find_element(driver, By.CLASS_NAME, "linkedRow").click()
        time.sleep(0.2)  # Minimal wait
        wait_and_find_element(driver, By.LINK_TEXT, "Impersonate").click()
        time.sleep(0.5)  # Reduced wait
        driver.switch_to.window(driver.window_handles[1])
        
        # Inbox
        log_console("🔍 Inbox extraction")
        try:
            # Check if driver is still alive
            driver.current_url  # This will throw an exception if session is dead
            
            driver.get("https://live.doctoralliance.com/all/Inbox")
            # Wait only for essential inbox content
            try:
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                time.sleep(1)  # Minimal fallback
            log_console("✅ Successfully navigated to inbox page")
        except Exception as e:
            log_console(f"❌ Failed to navigate to inbox page (may be session issue): {e}")
            # Try to recover by creating new driver
            try:
                log_console("🔄 Attempting to create new driver session...")
                driver.quit()
                driver = webdriver.Chrome(options=options)
                driver.maximize_window()
                driver.set_page_load_timeout(30)
                driver.implicitly_wait(5)
                driver.set_script_timeout(30)
                login_to_da(da_url, da_login, da_password, driver)
                log_console("✅ Successfully recovered driver session")
            except Exception as recovery_error:
                log_console(f"❌ Failed to recover driver session: {recovery_error}")
                raise e
            
        # Set company key for extraction functions
        extract_doc_ids_from_inbox.company_key = company_key
        extract_doc_ids_from_signed.company_key = company_key
        
        # Import extraction source configuration functions
        from config import should_extract_from_inbox, should_extract_from_signed, get_extraction_sources
        
        # Check which sources to extract from
        extract_inbox = should_extract_from_inbox(company_key)
        extract_signed = should_extract_from_signed(company_key)
        extraction_sources = get_extraction_sources(company_key)
        
        log_console(f"📋 Extraction configuration for {company_key or 'default'}: {extraction_sources}")
        
        inbox_doc_ids, inbox_doc_types = [], {}
        signed_doc_ids, signed_doc_types = [], {}
        
        # Conditional extraction based on configuration
        if extract_inbox:
            log_console("🔍 Inbox extraction - ENABLED")
            inbox_doc_ids, inbox_doc_types = extract_doc_ids_from_inbox(driver, start_date, end_date)
        else:
            log_console("🔍 Inbox extraction - SKIPPED (disabled in config)")
        
        if extract_signed:
            log_console("🔍 Signed docs extraction - ENABLED")
            signed_doc_ids, signed_doc_types = extract_doc_ids_from_signed(driver, start_date, end_date)
        else:
            log_console("🔍 Signed docs extraction - SKIPPED (disabled in config)")
        
        # Check if any extraction was performed
        if not extract_inbox and not extract_signed:
            log_console("❌ ERROR: No extraction sources enabled! Please check your company configuration.")
            log_console("💡 Available extraction sources: ['inbox'], ['signed'], or ['inbox', 'signed']")
            return
        
        # Combine document types
        all_doc_types = {**inbox_doc_types, **signed_doc_types}
        all_doc_ids = list(dict.fromkeys(inbox_doc_ids + signed_doc_ids))
        
        if len(all_doc_ids) == 0:
            log_console("⚠️ No documents found for extraction. Check date range and extraction sources.")
            log_console(f"📊 Summary: Inbox docs: {len(inbox_doc_ids)}, Signed docs: {len(signed_doc_ids)}")
            return
            
        log_console(f"📝 Extracting NPI from {len(all_doc_ids)} documents...")
        log_console(f"📊 Sources: Inbox: {len(inbox_doc_ids)} docs, Signed: {len(signed_doc_ids)} docs")
        
        records = []
        filtered_records = []
        
        # Optimized batch processing with concurrent execution
        batch_size = EXTRACTOR_LIMITS.get("npi_batch_size", 50)
        total_batches = (len(all_doc_ids) + batch_size - 1) // batch_size
        
        log_console(f"🚀 Processing {len(all_doc_ids)} documents in {total_batches} batches of {batch_size}")
        
        # Optimized sequential processing with better batching
        processed_count = 0
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(all_doc_ids))
            batch_doc_ids = all_doc_ids[start_idx:end_idx]
            
            log_console(f"📦 Processing batch {batch_idx + 1}/{total_batches} ({len(batch_doc_ids)} documents)")
            
            # Process batch sequentially but with optimized settings
            for doc_id in batch_doc_ids:
                if STOP_REQUESTED:
                    break
                npi = ""
                document_type = all_doc_types.get(doc_id, "")
                
                # Try up to 2 times for each document (reduced retries for speed)
                for attempt in range(2):
                    try:
                        npi = extract_npi_only(doc_id, driver)
                        if npi:  # If we got an NPI, break
                            break
                        elif attempt < 1:  # If no NPI and not last attempt, retry
                            time.sleep(0.3)  # Reduced wait time
                    except Exception as e:
                        if attempt < 1:
                            time.sleep(0.3)
                        else:
                            log_console(f"❌ Failed to process {doc_id} after 2 attempts: {e}")
                
                record = {"Document ID": doc_id, "NPI": npi, "Document Type": document_type}
                records.append(record)
                filtered_records.append(record)
                
                # Progress update (less frequent logging for speed)
                processed_count += 1
                if processed_count % 10 == 0:  # Reduced from 25 to 10 for better progress visibility
                    log_console(f"📊 Progress: {processed_count}/{len(all_doc_ids)} ({processed_count/len(all_doc_ids)*100:.1f}%)")
                
                # Reduced logging frequency for speed - only log every 5th document
                if processed_count % 5 == 0 or processed_count <= 5:
                    log_console(f"✅ {doc_id}  NPI: {npi}  Type: {document_type} (PROCESSED)")
        
        final_records = filtered_records if filtered_records else records
        
        # Create failsafe DataFrame structure when no documents are found
        if not final_records:
            log_console("⚠️ No documents found, creating empty Excel with proper structure...")
            final_records = [{
                "Document ID": "",
                "NPI": "",
                "Document Type": ""
            }]
        
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
    da_login = "rpabot"
    da_password = "Dallas@1234"
    
    run_id_and_npi_extraction(
        da_url=da_url,
        da_login=da_login,
        da_password=da_password,
        helper_id=helper_id,
        start_date=user_start_date,
        end_date=user_end_date,
        company_key=company_key
    )
