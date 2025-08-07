import os
import glob
import pandas as pd
import requests
import base64
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime

# Gmail API
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# ========== CONFIG ==========

EXCEL_INPUT_PATH = "supreme_excel_with_patient_and_order_upload.xlsx"   # Your output Excel file
TO_EMAILS = ["sujay@doctoralliance.com","shubhash@doctoralliance.com","pranavraam@doctoralliance.com","lisa@doctoralliance.com"]
CC_EMAILS = []

# Google OAuth2 credentials
CLIENT_ID = "592800963579-4gl24i96kfju80tgus3dh0aubjdgipi7.apps.googleusercontent.com"
CLIENT_SECRET = "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"
REFRESH_TOKEN = "1//057XTCROZiPw4CgYIARAAGAUSNwF-L9IrlbgVKpMlJlHLNrXNe3BLPrH-pxI42fa0g1wu5osMkFmgxO-JkGf8xI8qwxrOWQw_4Ks"
FROM_EMAIL = "admin_mydaplatform@doctoralliance.com"

# ========== HELPERS ==========

def fetch_id_name_map(entity_type):
    url = f"https://dawaventity-g5a6apetdkambpcu.eastus-01.azurewebsites.net/api/Entity?EntityType={entity_type}"
    resp = requests.get(url, timeout=30)
    entities = resp.json()
    return {ent['id']: ent.get('name', '') for ent in entities if 'id' in ent}

def replace_ids_with_names(df):
    print("Fetching ANCILLIARY names...")
    ancilliary_map = fetch_id_name_map("ANCILLIARY")
    print("Fetching PRACTICE names...")
    practice_map = fetch_id_name_map("PRACTICE")
    
    # Check available columns and handle missing ones gracefully
    print(f"Available columns in Excel: {list(df.columns)}")
    
    # Handle companyId column (try different possible names)
    company_id_col = None
    for col_name in ['companyId', 'company_id', 'agency name']:
        if col_name in df.columns:
            company_id_col = col_name
            break
    
    if company_id_col:
        df['ANCILLIARYName'] = df[company_id_col].map(ancilliary_map).fillna(df[company_id_col])
        print(f"✅ Added ANCILLIARYName column using {company_id_col}")
    else:
        df['ANCILLIARYName'] = "N/A"
        print("⚠️  No company ID column found, setting ANCILLIARYName to N/A")
    
    # Handle Pgcompanyid column (try different possible names)
    pg_company_id_col = None
    for col_name in ['Pgcompanyid', 'pg_company_id', 'pg name']:
        if col_name in df.columns:
            pg_company_id_col = col_name
            break
    
    if pg_company_id_col:
        df['PRACTICEName'] = df[pg_company_id_col].map(practice_map).fillna(df[pg_company_id_col])
        print(f"✅ Added PRACTICEName column using {pg_company_id_col}")
    else:
        df['PRACTICEName'] = "N/A"
        print("⚠️  No PG company ID column found, setting PRACTICEName to N/A")
    
    return df

def send_patient_script_mail(to_emails, cc_emails, excel_path, subject="PATIENT SCRIPT"):
    creds = Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token"
    )
    creds.refresh(Request())
    service = build('gmail', 'v1', credentials=creds)
    message = MIMEMultipart()
    message['From'] = FROM_EMAIL
    message['To'] = ', '.join(to_emails)
    message['Cc'] = ', '.join(cc_emails)
    message['Subject'] = subject
    body_html = "<p>Please find attached the output Excel.</p>"
    message.attach(MIMEText(body_html, 'html'))
    if excel_path and os.path.isfile(excel_path):
        with open(excel_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(excel_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(excel_path)}"'
        message.attach(part)
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    body = {'raw': raw_message}
    try:
        sent = service.users().messages().send(userId="me", body=body).execute()
        print(f"Message sent! Message Id: {sent['id']}")
        return True
    except Exception as ex:
        print(f"Failed to send email: {ex}")
        return False

def cleanup_files(xlsx_dir):
    # Delete all Excel files in Combined/
    for file in glob.glob("Combined/*.xlsx"):
        try:
            os.remove(file)
            print(f"Deleted {file}")
        except Exception as e:
            print(f"Could not delete {file}: {e}")
    # Delete all files in ocr_debug/
    for file in glob.glob("ocr_debug/*"):
        try:
            os.remove(file)
            print(f"Deleted {file}")
        except Exception as e:
            print(f"Could not delete {file}: {e}")
    # Delete all specified Excel files in xlsx_dir
    files_to_delete = [
        "supreme_excel_with_patient_upload.xlsx",
        "supreme_excel_with_patient_and_order_upload.xlsx",
        "doctoralliance_orders_final.xlsx",
        "doctoralliance_combined_output.xlsx",
        "PATIENT_SCRIPT_output.xlsx",
        "*_PROCESSED.xlsx"  # New naming convention
    ]
    for fname in files_to_delete:
        fpath = os.path.join(xlsx_dir, fname)
        if os.path.exists(fpath):
            try:
                os.remove(fpath)
                print(f"Deleted {fpath}")
            except Exception as e:
                print(f"Could not delete {fpath}: {e}")

# ========== MAIN ==========
if __name__ == "__main__":
    # Get input file name from command line arguments
    input_file = EXCEL_INPUT_PATH  # default
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # 1. Prepare dated output folder
    today_str = datetime.now().strftime("%Y-%m-%d")
    xlsx_dir = os.path.join(today_str, "xlsx")
    os.makedirs(xlsx_dir, exist_ok=True)

    # 2. Read file (Excel or text)
    print(f"Reading file: {input_file}")
    
    # Check if it's a ZIP file first
    if input_file.lower().endswith('.zip'):
        # Handle ZIP files - just send them directly without reading
        print(f"✅ Detected ZIP file: {input_file}")
        # For ZIP files, we'll skip the DataFrame processing and send directly
        excel_out_path = input_file  # Use the original ZIP file
        email_subject = "FAILED PDFS ZIP FILE"
        
        # Send the ZIP file directly
        print(f"Sending ZIP file with subject: {email_subject}...")
        send_patient_script_mail(TO_EMAILS, CC_EMAILS, excel_out_path, email_subject)
        print("ZIP file sent successfully.")
        sys.exit(0)  # Exit early for ZIP files
    
    # Check if it's a text file
    elif input_file.lower().endswith('.txt'):
        try:
            # For text files, read as text and create a simple DataFrame
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Create a simple DataFrame with the content
            df = pd.DataFrame({'Content': [content]})
            print(f"✅ Successfully read text file with content length: {len(content)} characters")
        except Exception as e:
            print(f"❌ Error reading text file: {e}")
            sys.exit(1)
    else:
        try:
            df = pd.read_excel(input_file)
            print(f"✅ Successfully read Excel with {len(df)} rows and {len(df.columns)} columns")
        except Exception as e:
            print(f"❌ Error reading Excel file: {e}")
            sys.exit(1)

    # 3. Replace companyid and Pgcompanyid (only for Excel files)
    if not input_file.lower().endswith('.txt'):
        try:
            df = replace_ids_with_names(df)
        except Exception as e:
            print(f"❌ Error in replace_ids_with_names: {e}")
            print("Continuing without name replacement...")
            # Add default columns if they don't exist
            if 'ANCILLIARYName' not in df.columns:
                df['ANCILLIARYName'] = "N/A"
            if 'PRACTICEName' not in df.columns:
                df['PRACTICEName'] = "N/A"
    else:
        print("Skipping name replacement for text file")

    # 4. Save as new Excel for emailing with better naming
    base_filename = os.path.basename(input_file)
    name_without_ext = os.path.splitext(base_filename)[0]
    
    # Get current date for naming
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # Get PG name from company key in filename
    def get_pg_name_from_filename(filename):
        # Extract company key from filename
        if "trucare" in filename.lower():
            return "Trucare"
        elif "acohealth" in filename.lower():
            return "AcoHealth"
        elif "health_quality_primary_care" in filename.lower():
            return "Health_Quality_Primary_Care"
        elif "caring" in filename.lower():
            return "Caring"
        else:
            return "Unknown_PG"
    
    pg_name = get_pg_name_from_filename(input_file)
    
    # Determine the type of file and create appropriate name and subject
    if "failed_records_by_pg" in input_file.lower():
        # This is a failed records report
        excel_out_path = os.path.join(xlsx_dir, f"{pg_name}_{current_date}_FAILED.xlsx")
        email_subject = "FAILED RECORDS REPORT"
    elif "with_patient_and_order_upload" in input_file.lower():
        # This is the final upload file
        excel_out_path = os.path.join(xlsx_dir, f"{pg_name}_{current_date}_MAIN.xlsx")
        email_subject = "PATIENT SCRIPT - FINAL UPLOAD RESULTS"
    elif "processing_summary" in input_file.lower():
        # This is a summary report
        if input_file.lower().endswith('.txt'):
            # For text files, just copy the file
            excel_out_path = os.path.join(xlsx_dir, f"Processing_Summary_{current_date}.txt")
            # Copy the original file content
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
            with open(excel_out_path, 'w', encoding='utf-8') as f:
                f.write(content)
        else:
            excel_out_path = os.path.join(xlsx_dir, f"Processing_Summary_{current_date}.xlsx")
            df.to_excel(excel_out_path, index=False)
        email_subject = "PATIENT SCRIPT - PROCESSING SUMMARY"
    else:
        # This is a main supreme Excel file
        excel_out_path = os.path.join(xlsx_dir, f"{pg_name}_{current_date}_MAIN.xlsx")
        email_subject = "PATIENT SCRIPT - MAIN RESULTS"
    
    # Save file based on type
    if excel_out_path.lower().endswith('.txt'):
        # For text files, content is already saved above
        print(f"Text file saved as {excel_out_path}")
    else:
        df.to_excel(excel_out_path, index=False)
        print(f"Excel saved as {excel_out_path}")

    # 5. Send mail with appropriate subject
    print(f"Sending email with subject: {email_subject}...")
    send_patient_script_mail(TO_EMAILS, CC_EMAILS, excel_out_path, email_subject)

    # 6. Cleanup files
    print("Cleaning up old files...")
    cleanup_files(xlsx_dir)
    print("All done.")
