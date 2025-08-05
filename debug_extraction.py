#!/usr/bin/env python3
"""
Debug script to identify document extraction issues
"""

import os
import sys
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

def log_console(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")

def wait_and_find_element(driver, by, value, timeout=10):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))

def login_to_da(da_url, da_login, da_password, driver, timeout=15):
    try:
        driver.get(da_url)
        log_console("🔐 Logging in to Doctor Alliance...")
        
        username_field = wait_and_find_element(driver, By.ID, "Username", timeout)
        username_field.clear()
        username_field.send_keys(da_login)
        
        password_field = wait_and_find_element(driver, By.ID, "Password", timeout)
        password_field.clear()
        password_field.send_keys(da_password)
        
        login_button = wait_and_find_element(driver, By.CLASS_NAME, "btn-primary", timeout)
        login_button.click()
        
        # Wait for login to complete
        time.sleep(3)
        log_console("✅ Login successful")
        return True
    except Exception as e:
        log_console(f"❌ Login failed: {e}")
        return False

def debug_inbox_extraction(driver, start_date, end_date=None):
    """Debug inbox extraction to identify issues"""
    log_console("🔍 DEBUGGING INBOX EXTRACTION")
    log_console("=" * 50)
    
    try:
        driver.get("https://live.doctoralliance.com/all/Inbox")
        time.sleep(5)
        log_console("✅ Navigated to inbox page")
        
        # Wait for table to load
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#inbox-all-grid tbody tr")))
        time.sleep(3)
        
        # Get all rows
        table_rows = driver.find_elements(By.CSS_SELECTOR, "#inbox-all-grid tbody tr")
        log_console(f"📊 Found {len(table_rows)} total rows in inbox")
        
        if len(table_rows) == 0:
            log_console("❌ No rows found in inbox table")
            return
        
        # Analyze first few rows
        for i, row in enumerate(table_rows[:5]):  # Analyze first 5 rows
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                log_console(f"\n📋 Row {i+1} Analysis:")
                log_console(f"   Total cells: {len(cells)}")
                
                for j, cell in enumerate(cells):
                    cell_text = cell.text.strip()
                    if cell_text:
                        log_console(f"   Cell {j+1}: {cell_text[:50]}...")
                
                # Check for doc_id in different columns
                doc_id_found = False
                for col_idx in [9, 8, 10]:
                    if col_idx < len(cells):
                        doc_id = cells[col_idx].text.strip()
                        if doc_id and len(doc_id) > 5:
                            log_console(f"   ✅ Doc ID found in column {col_idx+1}: {doc_id}")
                            doc_id_found = True
                            break
                
                if not doc_id_found:
                    log_console(f"   ❌ No Doc ID found in any expected column")
                
                # Check for received date
                date_found = False
                for col_idx in [8, 7, 6]:
                    if col_idx < len(cells):
                        date_text = cells[col_idx].text.strip()
                        if date_text and "/" in date_text:
                            log_console(f"   📅 Date found in column {col_idx+1}: {date_text}")
                            date_found = True
                            break
                
                if not date_found:
                    log_console(f"   ❌ No date found in any expected column")
                
            except Exception as e:
                log_console(f"   ❌ Error analyzing row {i+1}: {e}")
        
        # Check pagination
        try:
            next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
            log_console(f"\n📄 Pagination: Next button found")
            if "disabled" in next_button.get_attribute("class"):
                log_console(f"   ⚠️ Next button is disabled")
            else:
                log_console(f"   ✅ Next button is clickable")
        except Exception as e:
            log_console(f"\n📄 Pagination: Next button not found - {e}")
            
    except Exception as e:
        log_console(f"❌ Error debugging inbox: {e}")

def debug_signed_extraction(driver, start_date, end_date=None):
    """Debug signed extraction to identify issues"""
    log_console("\n🔍 DEBUGGING SIGNED EXTRACTION")
    log_console("=" * 50)
    
    try:
        # Navigate to signed tab
        signed_link = wait_and_find_element(driver, By.XPATH, "//a[contains(@href, '/Documents/Signed')]")
        signed_link.click()
        time.sleep(2)
        log_console("✅ Navigated to signed tab")
        
        # Click "All" button if available
        try:
            all_button = driver.find_element(By.XPATH, "//button[@data-doc-status='All']")
            button_class = all_button.get_attribute("class")
            if "active" not in button_class:
                driver.execute_script("arguments[0].click();", all_button)
                time.sleep(2)
                log_console("✅ Clicked 'All' button")
            else:
                log_console("✅ 'All' button already active")
        except Exception as e:
            log_console(f"⚠️ Could not find 'All' button: {e}")
        
        # Apply date filters
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
        log_console("✅ Applied date filters")
        
        # Check for "No matching records found"
        try:
            driver.find_element(By.XPATH, "//td[@colspan='11' and contains(text(), 'No matching records found')]")
            log_console("❌ No Signed Orders found in date range")
            return
        except Exception:
            pass
        
        # Wait for table to load
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#signed-docs-grid tbody tr")))
        time.sleep(2)
        
        # Get all rows
        table_rows = driver.find_elements(By.CSS_SELECTOR, "#signed-docs-grid tbody tr")
        log_console(f"📊 Found {len(table_rows)} total rows in signed tab")
        
        if len(table_rows) == 0:
            log_console("❌ No rows found in signed table")
            return
        
        # Analyze first few rows
        for i, row in enumerate(table_rows[:5]):  # Analyze first 5 rows
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                log_console(f"\n📋 Signed Row {i+1} Analysis:")
                log_console(f"   Total cells: {len(cells)}")
                
                for j, cell in enumerate(cells):
                    cell_text = cell.text.strip()
                    if cell_text:
                        log_console(f"   Cell {j+1}: {cell_text[:50]}...")
                
                # Check for doc_id in column 10
                try:
                    doc_id_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(10) span.text-muted")
                    doc_id = doc_id_element.text.strip()
                    if doc_id:
                        log_console(f"   ✅ Doc ID found: {doc_id}")
                    else:
                        log_console(f"   ❌ Doc ID element found but empty")
                except Exception as e:
                    log_console(f"   ❌ Doc ID not found in column 10: {e}")
                
            except Exception as e:
                log_console(f"   ❌ Error analyzing signed row {i+1}: {e}")
        
        # Check pagination
        try:
            next_button = driver.find_element(By.XPATH, "//li[@class='page-next']/a")
            log_console(f"\n📄 Signed Pagination: Next button found")
            if "disabled" in next_button.get_attribute("class"):
                log_console(f"   ⚠️ Next button is disabled")
            else:
                log_console(f"   ✅ Next button is clickable")
        except Exception as e:
            log_console(f"\n📄 Signed Pagination: Next button not found - {e}")
            
    except Exception as e:
        log_console(f"❌ Error debugging signed extraction: {e}")

def main():
    log_console("🚀 Doctor Alliance - Document Extraction Debugger")
    log_console("=" * 60)
    
    # Get date range
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
    else:
        start_date = input("Enter start date (MM/DD/YYYY) or press Enter for default (30 days ago): ").strip()
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%m/%d/%Y")
    
    if len(sys.argv) > 2:
        end_date = sys.argv[2]
    else:
        end_date = input("Enter end date (MM/DD/YYYY) or press Enter for today: ").strip()
        if not end_date:
            end_date = datetime.now().strftime("%m/%d/%Y")
    
    log_console(f"📅 Date Range: {start_date} to {end_date}")
    
    # Setup Chrome driver
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
    options.add_argument("--disable-animations")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--disable-logging")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    options.add_argument("--disable-features=VizDisplayCompositor")
    
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.set_page_load_timeout(15)
    driver.implicitly_wait(2)
    driver.set_script_timeout(10)
    
    try:
        # Login
        if not login_to_da("https://backoffice.doctoralliance.com", "rpabot", "Dallas@1234", driver):
            log_console("❌ Login failed, exiting")
            return
        
        # Navigate to user
        driver.get("https://backoffice.doctoralliance.com/Search")
        wait_and_find_element(driver, By.ID, "Query").send_keys("ihelperph3232")  # Default helper ID
        wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
        wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field").send_keys("Users")
        WebDriverWait(driver, 8).until(EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))).click()
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()
        time.sleep(0.5)
        wait_and_find_element(driver, By.CLASS_NAME, "linkedRow").click()
        time.sleep(0.5)
        driver.switch_to.window(driver.window_handles[1])
        
        # Debug both extractions
        debug_inbox_extraction(driver, start_date, end_date)
        debug_signed_extraction(driver, start_date, end_date)
        
        log_console("\n✅ Debugging complete!")
        
    except Exception as e:
        log_console(f"❌ Error during debugging: {e}")
    finally:
        driver.quit()
        log_console("👋 WebDriver closed")

if __name__ == "__main__":
    main() 