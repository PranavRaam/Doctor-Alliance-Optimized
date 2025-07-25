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

def wait_and_find_element(driver, by, value, timeout=15):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )

def wait_for_page_load(driver, timeout=10):
    """Wait for page to load completely"""
    WebDriverWait(driver, timeout).until(
        lambda driver: driver.execute_script("return document.readyState") == "complete"
    )

def login_to_da(da_url, da_login, da_password, driver, timeout=20):
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
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    # Removed unnecessary 1.2 second delay - page load wait is sufficient

def extract_doc_ids_from_inbox(driver, start_date, end_date=None):
    doc_ids = []
    page = 1
    start_cutoff_date = datetime.strptime(start_date, "%m/%d/%Y")
    end_cutoff_date = datetime.strptime(end_date, "%m/%d/%Y") if end_date else None
    seen_ids = set()
    while True:
        log_console(f"📄 Inbox page {page}")
        # Removed unnecessary 1 second delay - WebDriverWait handles timing
        try:
            row_count = len(driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr"))
        except TimeoutException:
            break
        stop_flag = False
        for i in range(row_count):
            try:
                current_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
                row = current_rows[i]
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
                if doc_id in seen_ids or not doc_id:
                    continue
                seen_ids.add(doc_id)
                doc_ids.append(doc_id)
            except Exception as e:
                log_console(f"⚠️ Error processing row {i} on page {page}: {e}")
                continue
        if stop_flag:
            break
        try:
            next_btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, "//li[contains(@class,'page-next') and not(contains(@class, 'disabled'))]/a"))
            )
            next_btn.click()
            page += 1
            # Removed unnecessary 1 second delay - next page load is handled by WebDriverWait
        except TimeoutException:
            break
    return doc_ids

def extract_doc_ids_from_signed(driver, start_date, end_date=None):
    doc_ids = []
    seen_ids = set()
    try:
        log_console("Navigating to Signed tab...")
        signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]")
        signed_link.click()
        # Reduced from 2 to 0.5 seconds - just enough for UI to respond
        time.sleep(0.5)
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
        # Reduced from 5 to 2 seconds - WebDriverWait will handle the rest
        time.sleep(2)
        try:
            driver.find_element(By.XPATH, "//td[@colspan='11' and contains(text(), 'No matching records found')]")
            log_console("No Signed Orders found")
            return []
        except Exception:
            pass
        page = 1
        while True:
            log_console(f"📄 Signed docs page {page}")
            table_rows = driver.find_elements(By.CSS_SELECTOR, "#signed-docs-grid tbody tr")
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
                # Reduced from 5 to 1 second - just enough for page transition
                time.sleep(1)
            except Exception as e:
                log_console(f"⚠️ Next button not found in signed tab, breaking. {e}")
                break
            page += 1
    except Exception as e:
        log_console(f"❌ Error scraping Signed tab: {e}")
    return doc_ids

def extract_npi_with_session_refresh(doc_id, driver):
    # Always visit Signed list first to keep session valid
    go_to_signed_list(driver)
    # Reduced from 1.1 to 0.3 seconds - just enough for navigation
    time.sleep(0.3)
    detail_url = f"https://backoffice.doctoralliance.com/Documents2/Show/{doc_id}"
    log_console(f"🔗 Navigating to: {detail_url}")
    driver.get(detail_url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # Reduced from 1.3 to 0.2 seconds - page load wait is sufficient
        time.sleep(0.2)
    except:
        log_console("❌ Timeout loading doc detail page (possibly session lost).")
        return ""
    actual_url = driver.current_url
    if actual_url != detail_url:
        log_console(f"❌ Navigation failed! Landed on: {actual_url}")
        log_console(driver.page_source[:400])
        return ""
    xpaths_to_try = [
        "/html/body/div/div/div[2]/div[3]/div/div[3]/p",
        "/html/body/div/div/div[2]/div[2]/div[5]/div/div[4]/p[1]/span[2]",
        "/html/body/div/div/div[2]/div[3]/div/div[4]/p[1]/span[2]",
        "//span[contains(text(), 'NPI')]/following-sibling::span",
        "//p[contains(text(), 'NPI')]/span",
        "//div[contains(@class, 'physician')]//span[contains(text(), '1')]",
        "//*[contains(text(), '1') and string-length(normalize-space(.)) = 10]"
    ]
    npi = ""
    for xpath in xpaths_to_try:
        try:
            element = driver.find_element(By.XPATH, xpath)
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
            log_console(f"❌ No NPI found via Selenium for doc {doc_id}")
    return npi

def run_id_and_npi_extraction(da_url, da_login, da_password, helper_id, start_date, end_date=None):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = os.path.join("Combined", f"DocumentID_NPI_{timestamp}.xlsx")
    os.makedirs("Combined", exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")
    options.add_argument("--page-load-strategy=eager")
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()

    try:
        login_to_da(da_url, da_login, da_password, driver)
        driver.get("https://backoffice.doctoralliance.com/Search")
        wait_and_find_element(driver, By.ID, "Query").send_keys(helper_id)
        wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
        wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field").send_keys("Users")
        WebDriverWait(driver,10).until(
            EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))
        ).click()
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()
        # Reduced from 4 to 1 second - WebDriverWait handles the rest
        time.sleep(1)
        wait_and_find_element(driver, By.CLASS_NAME, "linkedRow").click()
        # Reduced from 3 to 0.5 seconds - just enough for UI response
        time.sleep(0.5)
        wait_and_find_element(driver, By.LINK_TEXT,"Impersonate").click()
        # Reduced from 7 to 2 seconds - impersonation takes some time but not 7 seconds
        time.sleep(2)
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
    except Exception as e:
        log_console(f"❌ Extraction failed: {e}")
    finally:
        driver.quit()
        log_console("👋 WebDriver closed")

if __name__ == "__main__":
    print("🚀 Doctor Alliance - Document ID & NPI Extractor (Detail Page Robust Mode)")
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
    
    # Get helper_id from config based on active company
    from config import get_active_company
    try:
        company_info = get_active_company()
        helper_id = company_info['helper_id']
        print(f" Using helper ID for {company_info['name']}: {helper_id}")
    except Exception as e:
        print(f" Warning: Could not get helper ID from config, using default: {e}")
        helper_id = "dhelperph621"  # Default fallback
    
    da_url = "https://backoffice.doctoralliance.com"
    da_login = "sannidhay"
    da_password = "DA@2025"
    
    run_id_and_npi_extraction(
        da_url=da_url,
        da_login=da_login,
        da_password=da_password,
        helper_id=helper_id,
        start_date=user_start_date,
        end_date=user_end_date
    )
