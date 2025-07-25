from datetime import datetime,timedelta
import time
from dateutil import parser
import config as rc
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
import chromedriver_autoinstaller
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import requests
import pyautogui
import shutil

def log_console(message):
    """Log message to console with timestamp (Windows Unicode safe)"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        print(f"[{timestamp}] {message}")
    except UnicodeEncodeError:
        # Fallback for Windows terminal encoding issues
        safe_message = message.encode('ascii', 'replace').decode('ascii')
        print(f"[{timestamp}] {safe_message}")

def is_valid_datetime(s):
    format = "%m/%d/%Y"
    try:
        datetime.strptime(s, format)
        return True
    except:
        return False

def clean_null_data(val):
    if pd.isna(val):
        return ""
    if str(val).isnumeric():
        val = str(val)
    if isinstance(val, datetime):
        format = "%m/%d/%Y"
        val = val.strftime(format)
        return val
    if isinstance(val, float):
        return str(val)
    if val==None or val=="None":
        return ""
    else:
        return str(val).strip()
    
def getFormattedName(fullname):
    if fullname:
        lastname= fullname.split(',')[0].strip()
        firstmiddlename=fullname.split(',')[1].strip()
        if " " in firstmiddlename:
            firstname=firstmiddlename.split(' ')[0].strip()
        else:
            firstname=firstmiddlename
        formattedName= lastname+", "+firstname
        return formattedName
    else:
        return fullname

def getFolderPath(mode,cred):
    configuration = rc.readConfig()
    workingFolder=""
    currDateStr=datetime.now().strftime("%Y-%m-%d") 
    if mode=='P':
        workingFolder= configuration["PatientListPath"]
    else:
        workingFolder= configuration["OrderFolderPath"]
    date_folder_path = workingFolder+"/"+ currDateStr +"/"+cred.replace(' ','_')
    if not os.path.exists(date_folder_path):
        os.makedirs(date_folder_path)
    return date_folder_path

def getPrevDayWorkingFolder(mode,cred):
    configuration = rc.readConfig()
    workingFolder=""
    previous_date = datetime.now() - timedelta(days=1)
    previous_date_str = previous_date.strftime("%Y-%m-%d")      
    if mode=='P':
        workingFolder= configuration["PatientListPath"]
    else:
        workingFolder= configuration["OrderFolderPath"]
    date_folder_path = workingFolder+"/"+ previous_date_str +"/"+cred.replace(' ','_')
    return date_folder_path
    

def get_payor_type(payor_val):
    payor_src = ""
    if payor_val:
        payor_src=payor_val
        if "(" in payor_val:
            index = payor_val.index("(")
            payor_src = payor_val[:index].strip()
    return payor_src

def get_clean_status(marital_status):
    status = ""
    if marital_status:
        if marital_status.isdigit():
            status = ""
        else:
            status = marital_status
    return status

def get_age(dob_date_string):
    age = 0
    age_str = ""
    if dob_date_string:
        dob = datetime.strptime(dob_date_string, "%m/%d/%Y")
        current_date = datetime.now()
        age = current_date.year - dob.year
        if current_date.month < dob.month or (current_date.month == dob.month and current_date.day < dob.day):
            age -= 1
        age_str = str(age)
    return age_str

def get_date_string(date_val):
    formatted_date = ""
    if date_val:
        if " " in date_val:
            date_val = date_val.split()[0].strip()
        if "/" not in date_val:
            excel_date_value = float(date_val)
            excel_base_date = datetime(1899, 12, 31)
            converted_date = excel_base_date + timedelta(days=excel_date_value - 1)
            formatted_date = converted_date.strftime("%m/%d/%Y")
        else:
            res = datetime.strptime(date_val, "%m/%d/%Y")
            formatted_date = res.strftime("%m/%d/%Y")
    return formatted_date

def isValidData(firstName,lastName,dob,certFrom,certTo,physicianNPI):
    if not firstName or not lastName or not dob or not certFrom or not certTo or not physicianNPI:
        blank_args = [arg_name for arg_name, arg in locals().items() if arg_name != 'blank_args' and not arg]
        return False, f"Invalid data! Argument(s) {', '.join(blank_args)} is/are blank."
    else:
        return True, ""
    
def logFile(logFilePath, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} - {message}\n"
    with open(logFilePath, "a") as log_file:
        log_file.write(log_entry)
        
    
    
def get_episode_end_date(date_val, start_of_episode, service_line):
    try:
        formatted_date = ''

        if not date_val:
            if start_of_episode:
                start_date = datetime.strptime(start_of_episode, "%m/%d/%Y")
                if not service_line or 'home' in service_line.lower():
                    end_date = start_date + timedelta(days=59)
                else:
                    end_date = start_date + timedelta(days=79)

                formatted_date = end_date.strftime("%m/%d/%Y")
        else:
            if '/' not in date_val:
                excel_date_value = float(date_val)
                excel_base_date = datetime(1899, 12, 31)
                converted_date = excel_base_date + timedelta(days=excel_date_value - 1)
                formatted_date = converted_date.strftime("%m/%d/%Y")
            else:
                parsed_date = parser.parse(date_val)
                formatted_date = parsed_date.strftime("%m/%d/%Y")

        return formatted_date
    except Exception as ex:
        raise Exception(f"Date is not Valid: {date_val}") from ex
    
def wait_and_find_element(driver, by, value, timeout=10):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )
    
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def login_to_da(da_url, da_login, da_password, driver, timeout=20):
    try:
        driver.get(da_url)
        
        # Wait for email input and fill it
        email_input = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Username']"))
        )
        email_input.click()
        email_input.clear()
        email_input.send_keys(da_login)
        
        # Wait for password input
        password_input = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder='Password']"))
        )
        password_input.click()
        password_input.clear()
        password_input.send_keys(da_password)
        
        # Wait for login button and click it
        login_button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn.btn-primary.btn-block"))
        )
        login_button.click()
        
        # Optional: wait for login to complete, e.g., wait for URL change or dashboard element
        WebDriverWait(driver, timeout).until(
            EC.url_contains("backoffice.doctoralliance.com")
        )
        # Removed unnecessary 1 second delay - URL wait is sufficient

    except Exception as e:
        raise Exception(f"Could not login to DA Backoffice: {e}")
    
def click_element(element):
    try:
        element.click()
    except Exception as ex:
        print(f"Error clicking element: {str(ex)}")
        exit()

def login_to_EHR(url,email,password):
    try:
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(url)
        # Reduced from 10 to 3 seconds - page load wait handles the rest
        time.sleep(3)
        try:
            pyautogui.press('enter')
            # Removed unnecessary 1 second delay
        except Exception as e:
            pass
        username_input = driver.find_element(By.ID, "USERNAME")
        # Removed unnecessary 1 second delay
        username_input.send_keys(email)
        # Removed unnecessary 1 second delay

        password_input = driver.find_element(By.ID, "PASSWORD")
        # Removed unnecessary 1 second delay
        password_input.send_keys(password)
        # Removed unnecessary 1 second delay

        # Find the login button by id and click it
        login_button = driver.find_element(By.ID, "loginbutton")
        # Removed unnecessary 1 second delay
        login_button.click()
        # Reduced from 5 to 2 seconds - login processing takes some time
        time.sleep(2)
        login_button = driver.find_element(By.ID, "loginbutton")
        # Removed unnecessary 1 second delay
        login_button.click()
        # Reduced from 10 to 3 seconds - login completion
        time.sleep(3)
        driver.switch_to.frame("GlobalNav")
        # Removed unnecessary 1 second delay

        patients_menu = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='patientsmenucomponent']")))
        patients_menu.click()
        # Removed unnecessary 1 second delay
        driver.switch_to.default_content()
        # Removed unnecessary 1 second delay
        add_document_submenu = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//div[@class="categoryitem" and text()="Add Document"]')))
        add_document_submenu.click()
        # Reduced from 10 to 3 seconds - menu navigation
        time.sleep(3)
    except Exception as e:
        raise str(e)+" Could not login to PG"
    
def get_access_token(daAPITokenUrl,daAPITokenUserName,daAPITokenPswd):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'password',
        'username': daAPITokenUserName,
        'password': daAPITokenPswd
    }
    response = requests.post(daAPITokenUrl, headers=headers, data=data)
    if response.status_code == 200:
        json_response = response.json()
        access_token = json_response.get('access_token')
        return access_token
    else:
        raise Exception(f"Error: {response.status_code} - {response.text}")
    
def date_in_standard_format(dateVal):
    if dateVal:
        dateobj=dateVal[:10]
        dateobj = datetime.strptime(dateobj, "%Y-%m-%d")
        formatted_date = dateobj.strftime("%m/%d/%Y")
        # print("Formatted Date:", formatted_date)
        return formatted_date
    else:
        return dateVal
        

def DeleteOldFolders():
    configuration = rc.readConfig()
    workingFolder=""
    workingFolder= configuration["OrderFolderPath"]
    archiveDays= int(configuration["ArchivalDays"])
    currDate = datetime.now()
    cutoffDate = currDate - timedelta(days=archiveDays)
    for folder in os.listdir(workingFolder):
        folderDate = datetime.strptime(folder, "%Y-%m-%d")
        if folderDate < cutoffDate:
            folderPath = os.path.join(workingFolder, folder)
            shutil.rmtree(folderPath)