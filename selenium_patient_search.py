import sys
import time
from typing import Optional, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Reuse existing helpers for DA login and resilient waits
from selenium_extractor import login_to_da, wait_and_find_element
from download_manager import download_pdf_from_api


def build_chrome_driver(headless: bool = False) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--log-level=3")
    # Block images for speed
    prefs = {
        "profile.default_content_setting_values": {"images": 2},
        "webkit.webprefs.loads_images_automatically": False,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.set_page_load_timeout(20)
    driver.implicitly_wait(2)
    driver.set_script_timeout(15)
    return driver


def _select_search_type(driver: webdriver.Chrome, label_candidates: Tuple[str, ...]) -> None:
    # Open select2 dropdown
    wait_and_find_element(driver, By.ID, "select2-SearchType-container").click()
    search_box = wait_and_find_element(driver, By.CLASS_NAME, "select2-search__field")
    # Try candidates until one matches a visible option
    for label in label_candidates:
        try:
            search_box.clear()
            search_box.send_keys(label)
            WebDriverWait(driver, 6).until(
                EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))
            ).click()
            return
        except Exception:
            continue
    # Fallback: pick the first available option
    WebDriverWait(driver, 6).until(
        EC.visibility_of_element_located((By.XPATH, "//li[contains(@id, 'select2-SearchType-result')][1]"))
    ).click()


def _normalize_name(text: str) -> str:
    return " ".join(text.replace(",", " ").split()).strip().lower()


def _wait_for_documents_rows(driver: webdriver.Chrome, max_wait_seconds: int = 20):
    """Wait until the #documents-grid tbody rows are populated and stable.
    Returns a list of row elements (fallbacks included)."""
    start_ts = time.time()
    last_count = -1
    stable_ticks = 0

    while time.time() - start_ts < max_wait_seconds:
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "#documents-grid tbody tr")
            count = len(rows)
        except Exception:
            rows = []
            count = 0

        try:
            info = driver.find_element(By.CSS_SELECTOR, ".pagination-info").text
        except Exception:
            info = ""

        print(f"[DOCS] Poll rows={count} info='{info}' stable={stable_ticks}")

        if count == last_count:
            stable_ticks += 1
        else:
            stable_ticks = 0

        # Heuristics to stop waiting
        if count >= 5 and stable_ticks >= 1:
            break
        if count >= 1 and "Showing" in info and stable_ticks >= 1:
            break

        last_count = count
        time.sleep(1)

    # Final collection with fallbacks
    rows = driver.find_elements(By.CSS_SELECTOR, "#documents-grid tbody tr")
    if not rows:
        try:
            rows = driver.find_elements(By.XPATH, "//*[@id='Documents']//table//tbody//tr")
        except Exception:
            rows = []
    return rows


def search_and_select_patient(
    da_url: str,
    da_login: str,
    da_password: str,
    last_name: str,
    first_name: str,
    *,
    headless: bool = False,
    exact_match: bool = True,
) -> Tuple[bool, Optional[str]]:
    driver = build_chrome_driver(headless=headless)
    try:
        # Login
        login_to_da(da_url, da_login, da_password, driver)

        # Navigate to global search
        driver.get("https://backoffice.doctoralliance.com/Search")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "Query")))

        # Enter query as "Last First" to bias by last name
        query = f"{last_name} {first_name}".strip()
        wait_and_find_element(driver, By.ID, "Query").clear()
        wait_and_find_element(driver, By.ID, "Query").send_keys(query)

        # Select Patients search scope
        _select_search_type(driver, ("Patients", "Patient"))

        # Submit search
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()

        # Wait for results
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
        except TimeoutException:
            return False, None

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if not rows:
            return False, None

        # Prefer the row with the most recent "Last Activity" date when duplicates exist
        def _parse_date_flex(s: str) -> float:
            try:
                from datetime import datetime as _dt
                return _dt.strptime(s.strip(), "%m/%d/%Y").timestamp()
            except Exception:
                try:
                    from datetime import datetime as _dt
                    return _dt.strptime(s.strip(), "%m/%d/%y").timestamp()
                except Exception:
                    return 0.0

        target_norm = _normalize_name(f"{first_name} {last_name}")
        scored_rows = []
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_text = _normalize_name(" ".join([c.text for c in cells]))
                if exact_match:
                    if not all(part in row_text for part in target_norm.split()):
                        continue
                else:
                    if first_name.lower() not in row_text or last_name.lower() not in row_text:
                        continue
                # Last Activity typically in column 6
                last_activity = cells[5].text if len(cells) >= 6 else ""
                score = _parse_date_flex(last_activity)
                scored_rows.append((score, row))
            except Exception:
                continue

        candidate = max(scored_rows, key=lambda x: x[0])[1] if scored_rows else rows[0]

        # Navigate using hidden input.rowLink if present (most reliable), else click
        try:
            try:
                hidden = candidate.find_element(By.CSS_SELECTOR, "input.rowLink")
                href = hidden.get_attribute("value") or ""
                if href:
                    if href.startswith("/"):
                        href = f"{da_url}{href}"
                    driver.get(href)
                else:
                    raise Exception("empty rowLink")
            except Exception:
                # Fallback to JS click on the row
                driver.execute_script("arguments[0].click();", candidate)
        except Exception:
            return False, None

        time.sleep(0.5)

        # Verify navigation to patient profile/details
        try:
            WebDriverWait(driver, 10).until(EC.url_contains("Patient"))
        except TimeoutException:
            pass

        # Always navigate to Documents tab
        try:
            # Prefer anchor with href="#Documents"
            try:
                elem = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href='#Documents']"))
                )
                driver.execute_script("arguments[0].click();", elem)
            except Exception:
                # Fallbacks: link text and generic XPath
                try:
                    elem = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.LINK_TEXT, "Documents"))
                    )
                    elem.click()
                except Exception:
                    elem = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[@data-toggle='tab' and @href='#Documents']"))
                    )
                    driver.execute_script("arguments[0].click();", elem)
            # Wait for documents table in the Documents pane
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[@id='Documents']//table//tbody//tr"))
                )
            except TimeoutException:
                # Final fallback: any table rows
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
                )
        except Exception:
            pass

        current_url = driver.current_url
        return True, current_url

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def download_latest_485_for_patient(
    da_url: str,
    da_login: str,
    da_password: str,
    last_name: str,
    first_name: str,
    *,
    headless: bool = False,
    save_dir: str = "Downloads_485",
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Open patient by name, go to Documents tab, pick the most recent 485-type document, and download via API.
    Returns (success, doc_id, saved_path).
    """
    driver = build_chrome_driver(headless=headless)
    try:
        print(f"[FLOW] Login → search '{last_name}, {first_name}' → open patient → Documents → find latest 485 → download via API")
        # 1) Login and open search
        login_to_da(da_url, da_login, da_password, driver)
        driver.get("https://backoffice.doctoralliance.com/Search")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "Query")))

        # 2) Search patients
        query = f"{last_name} {first_name}".strip()
        wait_and_find_element(driver, By.ID, "Query").clear()
        wait_and_find_element(driver, By.ID, "Query").send_keys(query)
        _select_search_type(driver, ("Patients", "Patient"))
        wait_and_find_element(driver, By.CLASS_NAME, "btn-success").click()

        # 3) Wait results and click first/best match
        print("[SEARCH] Waiting for patient results...")
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        print(f"[SEARCH] Found {len(rows)} result rows")

        # Choose best match by newest Last Activity
        def _parse_date_flex(s: str) -> float:
            try:
                from datetime import datetime as _dt
                return _dt.strptime(s.strip(), "%m/%d/%Y").timestamp()
            except Exception:
                try:
                    from datetime import datetime as _dt
                    return _dt.strptime(s.strip(), "%m/%d/%y").timestamp()
                except Exception:
                    return 0.0

        scored_rows = []
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                last_activity = cells[5].text if len(cells) >= 6 else ""
                score = _parse_date_flex(last_activity)
                scored_rows.append((score, row))
            except Exception:
                continue

        candidate = max(scored_rows, key=lambda x: x[0])[1] if scored_rows else rows[0]
        try:
            sample_cells = candidate.find_elements(By.TAG_NAME, "td")
            preview = " | ".join([c.text for c in sample_cells[:6]])
            print(f"[SEARCH] Selecting row: {preview}")
        except Exception:
            pass

        # Navigate using hidden input.rowLink if available
        try:
            try:
                hidden = candidate.find_element(By.CSS_SELECTOR, "input.rowLink")
                href = hidden.get_attribute("value") or ""
                if href:
                    if href.startswith("/"):
                        href = f"{da_url}{href}"
                    print(f"[NAV] Opening patient URL: {href}")
                    driver.get(href)
                else:
                    raise Exception("empty rowLink")
            except Exception:
                print("[NAV] Clicking row via JS (no rowLink)")
                driver.execute_script("arguments[0].click();", candidate)
        except Exception:
            return False, None, None

        # 4) Navigate to Documents tab
        try:
            print("[DOCS] Switching to Documents tab...")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, "Documents"))).click()
        except Exception:
            try:
                elem = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Documents')]"))
                )
                print("[DOCS] Clicking Documents tab via JS fallback")
                driver.execute_script("arguments[0].click();", elem)
            except Exception:
                return False, None, None

        # 5) Parse documents table and find latest 485 (scoped to #Documents)
        print("[DOCS] Waiting for Documents table to populate...")
        rows = _wait_for_documents_rows(driver, max_wait_seconds=25)
        print(f"[DOCS] Rows ready: {len(rows)}")
        candidates = []
        for r in rows:
            try:
                cells = r.find_elements(By.TAG_NAME, "td")
                if len(cells) < 5:
                    continue
                doc_type = cells[1].text.strip().upper()
                # Robust match for 485 family
                if not any(k in doc_type for k in ("485", "485CERT", "485 CERT", "RECERT")):
                    continue
                # Extract doc ID from first cell (supports nested link/span)
                doc_id_cell = cells[0]
                doc_id = doc_id_cell.text.strip()
                if not doc_id:
                    try:
                        doc_id = doc_id_cell.find_element(By.TAG_NAME, "a").text.strip()
                    except Exception:
                        try:
                            doc_id = doc_id_cell.find_element(By.TAG_NAME, "span").text.strip()
                        except Exception:
                            doc_id = ""
                order_on = cells[4].text.strip() if len(cells) >= 5 else ""
                sent_on = cells[3].text.strip() if len(cells) >= 4 else ""

                def parse_date(s: str) -> float:
                    try:
                        from datetime import datetime as _dt
                        return _dt.strptime(s, "%m/%d/%Y").timestamp()
                    except Exception:
                        return 0.0

                score = max(parse_date(order_on), parse_date(sent_on))
                if doc_id:
                    candidates.append((score, doc_id))
                    print(f"[DOCS] -> 485 candidate id={doc_id} score={score}")
            except Exception:
                continue

        if not candidates:
            print("[DOCS] No 485 records found in Documents table")
            return False, None, None

        candidates.sort(reverse=True)
        _, best_doc_id = candidates[0]
        print(f"[DOCS] Selected latest 485 id={best_doc_id}")

        # 6) Download via API
        import os
        os.makedirs(save_dir, exist_ok=True)
        safe_name = f"{last_name}_{first_name}".replace(" ", "_")
        save_path = os.path.join(save_dir, f"{safe_name}_485_{best_doc_id}.pdf")

        print(f"[DOWNLOAD] Downloading via API for doc_id={best_doc_id} -> {save_path}")
        ok = download_pdf_from_api(best_doc_id, save_path)
        if not ok:
            print("[DOWNLOAD] API download failed")
            return False, best_doc_id, None
        print("[DOWNLOAD] PDF saved successfully")
        return True, best_doc_id, save_path

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    # Usage: python selenium_patient_search.py LAST_NAME FIRST_NAME [DA_LOGIN] [DA_PASSWORD] [HEADLESS]
    if len(sys.argv) < 3:
        print("Usage: python selenium_patient_search.py LAST_NAME FIRST_NAME [DA_LOGIN] [DA_PASSWORD] [HEADLESS]")
        sys.exit(1)

    last = sys.argv[1]
    first = sys.argv[2]
    da_login = sys.argv[3] if len(sys.argv) >= 4 else "rpabot"
    da_password = sys.argv[4] if len(sys.argv) >= 5 else "Dallas@1234"
    headless = (sys.argv[5].lower() == "true") if len(sys.argv) >= 6 else False

    ok, url = search_and_select_patient(
        da_url="https://backoffice.doctoralliance.com",
        da_login=da_login,
        da_password=da_password,
        last_name=last,
        first_name=first,
        headless=headless,
        exact_match=True,
    )
    print(f"success={ok} url={url}")


