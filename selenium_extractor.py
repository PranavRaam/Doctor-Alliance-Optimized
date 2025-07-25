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
    previous_page_doc_ids = set()
    consecutive_same_pages = 0
    
    while True:
        log_console(f"📄 Inbox page {page} (Found {len(doc_ids)} total docs)")
        time.sleep(2)
        
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
            time.sleep(2)
            current_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
        except TimeoutException:
            log_console("❌ Timeout waiting for inbox table")
            break
            
        if not current_rows:
            log_console("❌ No rows found on current page")
            break
        
        log_console(f"📊 Processing {len(current_rows)} rows on page {page}")
        stop_flag = False
        current_page_doc_ids = set()
        
        for row in current_rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 9:
                    continue
                    
                received_on = cells[7].text.strip()
                if not received_on:
                    continue
                    
                received_date = datetime.strptime(received_on, "%m/%d/%Y")
                if received_date < start_cutoff_date:
                    stop_flag = True
                    break
                    
                if end_cutoff_date and received_date > end_cutoff_date:
                    continue
                    
                doc_id = cells[8].text.strip()
                if not doc_id:
                    continue
                
                # Extract document type from the Doc Type column (usually column 2)
                doc_type = ""
                if len(cells) > 2:
                    doc_type = cells[1].text.strip()  # Doc Type column
                
                current_page_doc_ids.add(doc_id)
                
                if doc_id not in seen_ids:
                    # Filter for 485 documents only
                    from config import should_filter_document_types, get_allowed_document_types
                    should_filter = should_filter_document_types("prima_care")  # Hardcode for now
                    allowed_types = get_allowed_document_types("prima_care")
                    
                    is_485_document = False
                    if should_filter and allowed_types and doc_type:
                        doc_type_upper = doc_type.upper()
                        is_485_document = any(keyword in doc_type_upper for keyword in allowed_types)
                    
                    if is_485_document or not should_filter:
                        seen_ids.add(doc_id)
                        doc_ids.append(doc_id)
                        # Store document type with doc_id
                        if not hasattr(extract_doc_ids_from_inbox, 'doc_types'):
                            extract_doc_ids_from_inbox.doc_types = {}
                        extract_doc_ids_from_inbox.doc_types[doc_id] = doc_type
                        log_console(f"📄 Inbox - Doc ID: {doc_id} | Type: {doc_type} ✅ (485 - INCLUDED)")
                    else:
                        log_console(f"📄 Inbox - Doc ID: {doc_id} | Type: {doc_type} ❌ (NOT 485 - SKIPPED)")
                    
            except Exception as e:
                log_console(f"⚠️ Error processing row on page {page}: {e}")
                continue
        
        # Infinite loop detection
        if current_page_doc_ids == previous_page_doc_ids:
            consecutive_same_pages += 1
            log_console(f"⚠️ Same page detected {consecutive_same_pages} times")
            
            if consecutive_same_pages >= 3:
                log_console(f"🛑 Breaking due to pagination loop. Found {len(doc_ids)} total docs")
                break
        else:
            consecutive_same_pages = 0
            previous_page_doc_ids = current_page_doc_ids.copy()
        
        if stop_flag:
            log_console(f"🛑 Reached start date cutoff. Total inbox docs: {len(doc_ids)}")
            break
            
        # Pagination
        try:
            next_btn = driver.find_element(By.XPATH, "//li[contains(@class,'page-next') and not(contains(@class, 'disabled'))]/a")
            if not next_btn:
                log_console("🛑 Next button not found - end of pages")
                break
                
            current_url = driver.current_url
            driver.execute_script("arguments[0].scrollIntoView();", next_btn)
            time.sleep(0.3)
            
            try:
                driver.execute_script("arguments[0].click();", next_btn)
            except:
                next_btn.click()
            
            page_changed = False
            for wait_attempt in range(10):
                time.sleep(0.5)
                new_url = driver.current_url
                
                try:
                    WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
                    new_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                    if len(new_rows) != len(current_rows):
                        page_changed = True
                        break
                except:
                    continue
            
            if not page_changed:
                log_console(f"⚠️ Page didn't change after next button click")
                try:
                    if "page=" in current_url:
                        new_url = re.sub(r'page=\d+', f'page={page+1}', current_url)
                        driver.get(new_url)
                        time.sleep(1)
                except:
                    log_console(f"🛑 Could not navigate to next page. Ending pagination.")
                    break
            
            page += 1
            
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
    
    # Retry if no documents found
    if len(doc_ids) == 0:
        log_console("⚠️ No documents found in inbox, retrying with longer wait...")
        time.sleep(5)
        driver.refresh()
        time.sleep(3)
        
        retry_doc_ids = []
        page = 1
        while page <= 3:
            log_console(f"📄 Retry - Inbox page {page}")
            time.sleep(3)
            
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
                time.sleep(2)
                current_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                
                if current_rows:
                    for row in current_rows:
                        try:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 9:
                                doc_id = cells[8].text.strip()
                                if doc_id and doc_id not in retry_doc_ids:
                                    retry_doc_ids.append(doc_id)
                        except Exception as e:
                            continue
                
                try:
                    next_btn = driver.find_element(By.XPATH, "//li[contains(@class,'page-next') and not(contains(@class, 'disabled'))]/a")
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(2)
                except:
                    break
                    
                page += 1
            except TimeoutException:
                break
        
        if retry_doc_ids:
            log_console(f"✅ Retry successful: Found {len(retry_doc_ids)} documents")
            return retry_doc_ids, getattr(extract_doc_ids_from_inbox, 'doc_types', {})
    
    return doc_ids, getattr(extract_doc_ids_from_inbox, 'doc_types', {})

def extract_doc_ids_from_signed(driver, start_date, end_date=None):
    doc_ids = []
    seen_ids = set()
    
    try:
        log_console("Navigating to Signed tab...")
        signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]")
        signed_link.click()
        time.sleep(1)
        
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
        time.sleep(5)
        
        try:
            driver.find_element(By.XPATH, "//td[@colspan='11' and contains(text(), 'No matching records found')]")
            log_console("No Signed Orders found")
            return []
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
                        
                        # Filter for 485 documents only
                        from config import should_filter_document_types, get_allowed_document_types
                        should_filter = should_filter_document_types("prima_care")  # Hardcode for now
                        allowed_types = get_allowed_document_types("prima_care")
                        
                        is_485_document = False
                        if should_filter and allowed_types and doc_type:
                            doc_type_upper = doc_type.upper()
                            is_485_document = any(keyword in doc_type_upper for keyword in allowed_types)
                        
                        if is_485_document or not should_filter:
                            seen_ids.add(doc_id)
                            doc_ids.append(doc_id)
                            
                            # Store document type with doc_id
                            if not hasattr(extract_doc_ids_from_signed, 'doc_types'):
                                extract_doc_ids_from_signed.doc_types = {}
                            extract_doc_ids_from_signed.doc_types[doc_id] = doc_type
                            log_console(f"📄 Signed - Doc ID: {doc_id} | Type: {doc_type} ✅ (485 - INCLUDED)")
                        else:
                            log_console(f"📄 Signed - Doc ID: {doc_id} | Type: {doc_type} ❌ (NOT 485 - SKIPPED)")
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

def extract_npi_and_document_type_with_session_refresh(doc_id, driver):
    if not hasattr(extract_npi_and_document_type_with_session_refresh, 'counter'):
        extract_npi_and_document_type_with_session_refresh.counter = 0
        
    extract_npi_and_document_type_with_session_refresh.counter += 1
    
    if extract_npi_and_document_type_with_session_refresh.counter % 25 == 1:
        go_to_signed_list(driver)
        time.sleep(0.3)
    
    # Enable network logging to capture API calls
    driver.execute_script("""
        if (!window.apiCalls) {
            window.apiCalls = [];
            const originalFetch = window.fetch;
            const originalXHR = window.XMLHttpRequest;
            
            // Intercept fetch calls
            window.fetch = function(...args) {
                const url = args[0];
                const options = args[1] || {};
                const callInfo = {
                    method: options.method || 'GET',
                    url: url,
                    timestamp: new Date().toISOString()
                };
                window.apiCalls.push(callInfo);
                
                return originalFetch.apply(this, args).then(response => {
                    // Try to capture response data
                    response.clone().text().then(text => {
                        try {
                            const data = JSON.parse(text);
                            callInfo.response = data;
                        } catch (e) {
                            callInfo.response = text.substring(0, 200); // First 200 chars
                        }
                    });
                    return response;
                });
            };
            
            // Intercept XHR calls
            const originalOpen = XMLHttpRequest.prototype.open;
            const originalSend = XMLHttpRequest.prototype.send;
            
            XMLHttpRequest.prototype.open = function(method, url) {
                this._apiCall = {
                    method: method,
                    url: url,
                    timestamp: new Date().toISOString()
                };
                window.apiCalls.push(this._apiCall);
                return originalOpen.apply(this, arguments);
            };
            
            XMLHttpRequest.prototype.send = function(data) {
                const xhr = this;
                const originalOnReadyStateChange = xhr.onreadystatechange;
                
                xhr.onreadystatechange = function() {
                    if (xhr.readyState === 4) {
                        try {
                            const response = xhr.responseText;
                            const data = JSON.parse(response);
                            xhr._apiCall.response = data;
                        } catch (e) {
                            xhr._apiCall.response = xhr.responseText.substring(0, 200);
                        }
                    }
                    if (originalOnReadyStateChange) {
                        originalOnReadyStateChange.apply(xhr, arguments);
                    }
                };
                
                return originalSend.apply(this, arguments);
            };
        }
    """)
    
    detail_url = f"https://backoffice.doctoralliance.com/Documents2/Show/{doc_id}"
    log_console(f"🔗 Navigating to: {detail_url}")
    
    try:
        driver.get(detail_url)
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except:
        log_console("❌ Timeout loading doc detail page (possibly session lost).")
        return "", ""
        
    actual_url = driver.current_url
    if actual_url != detail_url:
        log_console(f"❌ Navigation failed! Landed on: {actual_url}")
        return "", ""
    
    # Extract NPI
    xpaths_to_try = [
        "/html/body/div/div/div[2]/div[3]/div/div[3]/p",
        "//span[contains(text(), 'NPI')]/following-sibling::span",
        "//p[contains(text(), 'NPI')]/span",
        "/html/body/div/div/div[2]/div[2]/div[5]/div/div[4]/p[1]/span[2]",
        "/html/body/div/div/div[2]/div[3]/div/div[4]/p[1]/span[2]",
        "//div[contains(@class, 'physician')]//span[contains(text(), '1')]",
        "//*[contains(text(), '1') and string-length(normalize-space(.)) = 10]"
    ]
    
    npi = ""
    for xpath in xpaths_to_try:
        try:
            element = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, xpath)))
            text = element.text.strip()
            match = re.search(r'\b\d{10}\b', text)
            if match:
                npi = match.group(0)
                log_console(f"✅ Found NPI at {xpath}: {npi}")
                break
        except Exception:
            continue
            
    if not npi:
        text = driver.page_source
        match = re.search(r"\[(\d{10})\]", text)
        if match:
            npi = match.group(1)
        else:
            match = re.search(r'\b\d{10}\b', text)
            if match:
                npi = match.group(0)
            else:
                log_console(f"❌ No NPI found via Selenium for doc {doc_id}")
            
    # Extract Document Type - Look for actual document type in the page
    document_type = ""
    
    # First try to find document type in the main content area
    doc_type_xpaths = [
        "//div[contains(@class, 'document-details')]//span[contains(text(), 'Type')]/following-sibling::span",
        "//div[contains(@class, 'document-info')]//span[contains(text(), 'Type')]/following-sibling::span",
        "//div[contains(@class, 'doc-details')]//span[contains(text(), 'Type')]/following-sibling::span",
        "//span[contains(text(), 'Document Type')]/following-sibling::span",
        "//label[contains(text(), 'Document Type')]/following-sibling::*",
        "//div[contains(text(), 'Document Type')]/following-sibling::*",
        "//*[contains(text(), 'Document Type')]/following-sibling::*",
        "//span[contains(text(), 'Type')]/following-sibling::span",
        "//div[contains(@class, 'document-type')]//span",
        "//div[contains(@class, 'doc-type')]//span"
    ]
    
    for xpath in doc_type_xpaths:
        try:
            element = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, xpath)))
            text = element.text.strip()
            if text and text.lower() not in ['document type', 'type', ''] and len(text) < 50:  # Avoid very long text
                document_type = text
                log_console(f"✅ Found Document Type via XPath: {document_type}")
                break
        except Exception:
            continue
    
    # If not found via XPath, try to extract from the page source more carefully
    if not document_type:
        page_source = driver.page_source
        
        # Debug: Log what we're finding in the page source
        log_console(f"🔍 Debug: Searching page source for document type patterns...")
        
        # Look for document type in JSON-like structures
        json_patterns = [
            r'"documentType"\s*:\s*"([^"]+)"',
            r'"docType"\s*:\s*"([^"]+)"',
            r'"type"\s*:\s*"([^"]+)"',
            r'documentType["\']?\s*:\s*["\']([^"\']+)["\']',
            r'docType["\']?\s*:\s*["\']([^"\']+)["\']',
            r'type["\']?\s*:\s*["\']([^"\']+)["\']'
        ]
        
        for pattern in json_patterns:
            match = re.search(pattern, page_source, re.IGNORECASE)
            if match:
                potential_type = match.group(1).strip()
                log_console(f"🔍 Debug: Found potential document type: '{potential_type}'")
                # Filter out HTTP methods and other unwanted values
                if potential_type and potential_type.upper() not in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'] and len(potential_type) < 50:
                    document_type = potential_type
                    log_console(f"✅ Found Document Type via JSON: {document_type}")
                    break
                else:
                    log_console(f"❌ Debug: Rejected '{potential_type}' (HTTP method or too long)")
        
        # If still not found, look for specific document type keywords
        if not document_type:
            specific_patterns = [
                r'485[^"\s]*',
                r'CERT[^"\s]*',
                r'RECERT[^"\s]*',
                r'ORDER[^"\s]*',
                r'OTHER[^"\s]*'
            ]
            
            for pattern in specific_patterns:
                match = re.search(pattern, page_source, re.IGNORECASE)
                if match:
                    potential_type = match.group(0).strip()
                    if potential_type and len(potential_type) < 50:
                        document_type = potential_type
                        log_console(f"✅ Found Document Type via keyword: {document_type}")
                        break
            
    # Log captured API calls
    try:
        api_calls = driver.execute_script("return window.apiCalls || [];")
        if api_calls:
            log_console(f"🔍 Debug: Captured {len(api_calls)} API calls:")
            for i, call in enumerate(api_calls[-5:]):  # Show last 5 calls
                log_console(f"   {i+1}. {call['method']} {call['url']}")
                if 'response' in call and call['response']:
                    if isinstance(call['response'], dict):
                        # Look for document type in response
                        if 'documentType' in call['response']:
                            log_console(f"      📄 Found documentType in response: {call['response']['documentType']}")
                        if 'docType' in call['response']:
                            log_console(f"      📄 Found docType in response: {call['response']['docType']}")
                        if 'type' in call['response']:
                            log_console(f"      📄 Found type in response: {call['response']['type']}")
                    else:
                        log_console(f"      📄 Response preview: {str(call['response'])[:100]}...")
    except Exception as e:
        log_console(f"⚠️ Debug: Could not retrieve API calls: {e}")
    
    return npi, document_type

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
    options.add_argument("--page-load-strategy=none")
    options.add_argument("--aggressive-cache-discard")
    options.add_argument("--memory-pressure-off")
    
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.set_page_load_timeout(10)
    driver.implicitly_wait(2)

    try:
        login_to_da(da_url, da_login, da_password, driver)
        driver.get("https://backoffice.doctoralliance.com/Search")
        wait_and_find_element(driver, By.ID, "Query").send_keys(helper_id)
        wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
        wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field").send_keys("Users")
        WebDriverWait(driver, 8).until(EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))).click()
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()
        time.sleep(1)
        wait_and_find_element(driver, By.CLASS_NAME, "linkedRow").click()
        time.sleep(1)
        wait_and_find_element(driver, By.LINK_TEXT, "Impersonate").click()
        time.sleep(2)
        driver.switch_to.window(driver.window_handles[1])
        
        # Inbox
        log_console("🔍 Inbox extraction")
        driver.get("https://live.doctoralliance.com/all/Inbox")
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
        for idx, doc_id in enumerate(all_doc_ids):
            log_console(f"[{idx+1}/{len(all_doc_ids)}] Doc ID: {doc_id}")
            
            # Get document type from frontend (already extracted)
            document_type = all_doc_types.get(doc_id, "")
            log_console(f"📄 Document Type from frontend: {document_type}")
            
            # Still need to get NPI from individual document pages
            npi, page_document_type = extract_npi_and_document_type_with_session_refresh(doc_id, driver)
            
            # Use page document type if frontend extraction failed
            if not document_type and page_document_type:
                document_type = page_document_type
                log_console(f"📄 Using document type from page: {document_type}")
            
            record = {"Document ID": doc_id, "NPI": npi, "Document Type": document_type}
            records.append(record)
            filtered_records.append(record)  # All documents at this point are already filtered
            
            log_console(f"✅ {doc_id}  NPI: {npi}  Type: {document_type} (PROCESSED)")
        
        final_records = filtered_records if filtered_records else records
        combined_df = pd.DataFrame(final_records)
        combined_df.to_excel(output_path, index=False)
        log_console(f"✅ Combined Excel created at: {output_path}\nRows: {len(combined_df)}")
        
        if len(final_records) > 0:
            npi_found = len([r for r in final_records if r['NPI']])
            success_rate = (npi_found/len(final_records)*100) if len(final_records) > 0 else 0
            log_console(f"📊 Success rate: {npi_found}/{len(final_records)} ({success_rate:.1f}%)")
            
            if should_filter and allowed_types:
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
