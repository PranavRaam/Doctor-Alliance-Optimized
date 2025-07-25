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


def wait_and_find_element(driver, by, value, timeout=10):  # Reduced from 15 to 10
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def login_to_da(da_url, da_login, da_password, driver, timeout=15):  # Reduced from 20 to 15
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
    WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))  # Reduced from 10 to 8
    time.sleep(0.3)  # Reduced from 1.2 to 0.3


def extract_doc_ids_from_inbox(driver, start_date, end_date=None):
    doc_ids = []
    page = 1
    start_cutoff_date = datetime.strptime(start_date, "%m/%d/%Y")
    end_cutoff_date = datetime.strptime(end_date, "%m/%d/%Y") if end_date else None
    seen_ids = set()
    
    # Track previous page content to detect infinite loops
    previous_page_doc_ids = set()
    consecutive_same_pages = 0
    
    while True:
        log_console(f"📄 Inbox page {page} (Found {len(doc_ids)} total docs)")
        time.sleep(0.5)  # Slightly longer wait for page load
        
        try:
            # Wait for table to load and be stable
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr"))
            )
            # Additional wait for dynamic content to stabilize
            time.sleep(0.5)
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
                
                # Track current page doc IDs for loop detection
                current_page_doc_ids.add(doc_id)
                
                if doc_id not in seen_ids:
                    seen_ids.add(doc_id)
                    doc_ids.append(doc_id)
                    
            except Exception as e:
                log_console(f"⚠️ Error processing row on page {page}: {e}")
                continue
        
        # **INFINITE LOOP DETECTION**
        if current_page_doc_ids == previous_page_doc_ids:
            consecutive_same_pages += 1
            log_console(f"⚠️ Same page detected {consecutive_same_pages} times - possible pagination issue")
            
            if consecutive_same_pages >= 3:
                log_console(f"🛑 Breaking due to pagination loop. Found {len(doc_ids)} total docs from inbox")
                break
        else:
            consecutive_same_pages = 0
            previous_page_doc_ids = current_page_doc_ids.copy()
        
        if stop_flag:
            log_console(f"🛑 Reached start date cutoff. Total inbox docs: {len(doc_ids)}")
            break
            
        # **IMPROVED PAGINATION HANDLING**
        try:
            # First check if next button exists and is enabled
            next_btn = driver.find_element(By.XPATH, "//li[contains(@class,'page-next') and not(contains(@class, 'disabled'))]/a")
            
            if not next_btn:
                log_console("🛑 Next button not found - end of pages")
                break
                
            # Get current URL before clicking
            current_url = driver.current_url
            
            # Scroll to next button and click
            driver.execute_script("arguments[0].scrollIntoView();", next_btn)
            time.sleep(0.3)
            
            # Try JavaScript click first
            try:
                driver.execute_script("arguments[0].click();", next_btn)
            except:
                # Fallback to regular click
                next_btn.click()
            
            # Wait for URL to change or content to load
            page_changed = False
            for wait_attempt in range(10):  # Wait up to 5 seconds
                time.sleep(0.5)
                new_url = driver.current_url
                
                # Check if page content is loading
                try:
                    WebDriverWait(driver, 1).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr"))
                    )
                    # Check if we have different content
                    new_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                    if len(new_rows) != len(current_rows):
                        page_changed = True
                        break
                except:
                    continue
            
            if not page_changed:
                log_console(f"⚠️ Page didn't change after next button click - trying alternative method")
                
                # Alternative: Try direct URL manipulation if possible
                try:
                    # Look for page number in URL and increment it
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
    return doc_ids


def extract_doc_ids_from_signed(driver, start_date, end_date=None):
    doc_ids = []
    seen_ids = set()
    
    try:
        log_console("Navigating to Signed tab...")
        signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]")
        signed_link.click()
        time.sleep(1)  # Reduced from 2 to 1
        
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
        time.sleep(2)  # Reduced from 5 to 2
        
        try:
            driver.find_element(By.XPATH, "//td[@colspan='11' and contains(text(), 'No matching records found')]")
            log_console("No Signed Orders found")
            return []
        except Exception:
            pass
            
        page = 1
        while True:
            log_console(f"📄 Signed docs page {page}")
            
            # Wait for table to load
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#signed-docs-grid tbody tr"))
                )
                table_rows = driver.find_elements(By.CSS_SELECTOR, "#signed-docs-grid tbody tr")
            except TimeoutException:
                break
                
            if not table_rows:
                break
                
            for row in table_rows:
                try:
                    doc_id = row.find_element(By.CSS_SELECTOR, "td:nth-child(10) span.text-muted").text.strip()
                    if doc_id and doc_id not in seen_ids:
                        seen_ids.add(doc_id)
                        doc_ids.append(doc_id)
                except Exception as e:
                    log_console(f"⚠️ Error extracting doc_id in signed tab: {e}")
                    continue
                    
            if len(table_rows) < 10:
                break
                
            try:
                next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
                next_button.click()
                time.sleep(2)  # Reduced from 5 to 2
            except Exception as e:
                log_console(f"⚠️ Next button not found in signed tab, breaking. {e}")
                break
                
            page += 1
            
    except Exception as e:
        log_console(f"❌ Error scraping Signed tab: {e}")
        
    return doc_ids


def extract_npi_with_session_refresh(doc_id, driver):
    # Optimized session refresh - only do it every 25 documents
    if not hasattr(extract_npi_with_session_refresh, 'counter'):
        extract_npi_with_session_refresh.counter = 0
        
    extract_npi_with_session_refresh.counter += 1
    
    # Only refresh session every 25 documents instead of every document
    if extract_npi_with_session_refresh.counter % 25 == 1:
        go_to_signed_list(driver)
        time.sleep(0.3)  # Reduced from 1.1 to 0.3
    
    detail_url = f"https://backoffice.doctoralliance.com/Documents2/Show/{doc_id}"
    log_console(f"🔗 Navigating to: {detail_url}")
    
    try:
        # Navigate with minimal timeout
        driver.get(detail_url)
        
        # Very short wait - just enough for essential content
        WebDriverWait(driver, 3).until(  # Reduced from 10 to 3
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # Skip additional sleep - rely on WebDriverWait only
        
    except:
        log_console("❌ Timeout loading doc detail page (possibly session lost).")
        return ""
        
    actual_url = driver.current_url
    if actual_url != detail_url:
        log_console(f"❌ Navigation failed! Landed on: {actual_url}")
        return ""
    
    # Optimized XPath order - put most successful matches first based on your output
    xpaths_to_try = [
        "/html/body/div/div/div[2]/div[3]/div/div[3]/p",  # This one is working for your docs
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
            element = WebDriverWait(driver, 1).until(  # Very short wait
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            text = element.text.strip()
            match = re.search(r'\b\d{10}\b', text)
            if match:
                npi = match.group(0)
                log_console(f"✅ Found NPI at {xpath}: {npi}")
                break
        except Exception:
            continue
            
    if not npi:
        # Quick regex fallback on page source (only if needed)
        text = driver.page_source
        match = re.search(r"\[(\d{10})\]", text)
        if match:
            npi = match.group(1)
        else:
            # One more quick search for any 10-digit number
            match = re.search(r'\b\d{10}\b', text)
            if match:
                npi = match.group(0)
            else:
                log_console(f"❌ No NPI found via Selenium for doc {doc_id}")
            
    return npi


def run_id_and_npi_extraction(da_url, da_login, da_password, helper_id, start_date, end_date=None, company_key=None):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Include company key in filename if provided
    if company_key:
        output_path = os.path.join("Combined", f"DocumentID_NPI_{company_key}_{timestamp}.xlsx")
    else:
        output_path = os.path.join("Combined", f"DocumentID_NPI_{timestamp}.xlsx")
    
    os.makedirs("Combined", exist_ok=True)
    
    # Ultra-fast Chrome options
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
    options.add_argument("--page-load-strategy=none")  # Changed to 'none' for fastest loading
    options.add_argument("--aggressive-cache-discard")
    options.add_argument("--memory-pressure-off")
    
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    
    # Set very fast timeouts
    driver.set_page_load_timeout(10)  # Reduced from 30
    driver.implicitly_wait(2)  # Reduced from 5

    try:
        login_to_da(da_url, da_login, da_password, driver)
        driver.get("https://backoffice.doctoralliance.com/Search")
        wait_and_find_element(driver, By.ID, "Query").send_keys(helper_id)
        wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
        wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field").send_keys("Users")
        WebDriverWait(driver, 8).until(  # Reduced from 10 to 8
            EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))
        ).click()
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()
        time.sleep(1)  # Reduced from 4 to 1
        wait_and_find_element(driver, By.CLASS_NAME, "linkedRow").click()
        time.sleep(1)  # Reduced from 3 to 1
        wait_and_find_element(driver, By.LINK_TEXT, "Impersonate").click()
        time.sleep(2)  # Reduced from 7 to 2
        driver.switch_to.window(driver.window_handles[1])
        
        # Inbox
        log_console("🔍 Inbox extraction")
        driver.get("https://live.doctoralliance.com/all/Inbox")
        inbox_doc_ids = extract_doc_ids_from_inbox(driver, start_date, end_date)
        
        # Signed
        log_console("🔍 Signed docs extraction")
        signed_doc_ids = extract_doc_ids_from_signed(driver, start_date, end_date)
        
        all_doc_ids = list(dict.fromkeys(inbox_doc_ids + signed_doc_ids))
        log_console(f"📝 Extracting NPI from {len(all_doc_ids)} documents...")
        
        records = []
        for idx, doc_id in enumerate(all_doc_ids):
            log_console(f"[{idx+1}/{len(all_doc_ids)}] Doc ID: {doc_id}")
            npi = extract_npi_with_session_refresh(doc_id, driver)
            records.append({"Document ID": doc_id, "NPI": npi})
            log_console(f"➡️  {doc_id}  NPI: {npi}")
            
        combined_df = pd.DataFrame(records)
        combined_df.to_excel(output_path, index=False)
        log_console(f"✅ Combined Excel created at: {output_path}\nRows: {len(combined_df)}")
        
        # Show some statistics
        npi_found = len([r for r in records if r['NPI']])
        log_console(f"📊 Success rate: {npi_found}/{len(records)} ({npi_found/len(records)*100:.1f}%)")
        
    except Exception as e:
        log_console(f"❌ Extraction failed: {e}")
    finally:
        driver.quit()
        log_console("👋 WebDriver closed")


if __name__ == "__main__":
    print("🚀 Doctor Alliance - Document ID & NPI Extractor (Ultra-Optimized Mode)")
    print("=" * 40)
    
    # Get start date from command line arguments
    if len(sys.argv) > 1:
        user_start_date = sys.argv[1]
        print(f" Using start date from argument: {user_start_date}")
    else:
        user_start_date = input(" Enter start date (MM/DD/YYYY) or press Enter for default (30 days ago): ").strip()
        if not user_start_date:
            default_date = (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")
            user_start_date = default_date
            print(f" Using default start date: {user_start_date}")
    
    # Get end date from command line arguments
    user_end_date = None
    if len(sys.argv) > 2:
        user_end_date = sys.argv[2]
        print(f" Using end date from argument: {user_end_date}")
    
    # Get company key from command line arguments
    company_key = None
    if len(sys.argv) > 3:
        company_key = sys.argv[3]
        print(f" Using company key from argument: {company_key}")
    
    # Get helper_id from config based on active company or specified company
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
        helper_id = ""  # Default fallback
    
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
