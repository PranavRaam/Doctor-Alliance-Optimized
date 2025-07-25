import os
import glob
import pandas as pd
import requests
import base64
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
TO_EMAILS = ["sujay@doctoralliance.com"]
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
    # Replace IDs with names; fallback to original value if not found
    df['ANCILLIARYName'] = df['companyId'].map(ancilliary_map).fillna(df['companyId'])
    df['PRACTICEName'] = df['Pgcompanyid'].map(practice_map).fillna(df['Pgcompanyid'])
    return df

def send_patient_script_mail(to_emails, cc_emails, excel_path):
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
    message['Subject'] = "PATIENT SCRIPT"
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
        "PATIENT_SCRIPT_output.xlsx"
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
    # 1. Prepare dated output folder
    today_str = datetime.now().strftime("%Y-%m-%d")
    xlsx_dir = os.path.join(today_str, "xlsx")
    os.makedirs(xlsx_dir, exist_ok=True)

    # 2. Read Excel
    print("Reading Excel...")
    df = pd.read_excel(EXCEL_INPUT_PATH)

    # 3. Replace companyid and Pgcompanyid
    df = replace_ids_with_names(df)

    # 4. Save as new Excel for emailing
    excel_out_path = os.path.join(xlsx_dir, "PATIENT_SCRIPT_output.xlsx")
    df.to_excel(excel_out_path, index=False)
    print(f"Excel saved as {excel_out_path}")

    # 5. Send mail
    print("Sending email...")
    send_patient_script_mail(TO_EMAILS, CC_EMAILS, excel_out_path)

    # 6. Cleanup files
    print("Cleaning up old files...")
    cleanup_files(xlsx_dir)
    print("All done.")
