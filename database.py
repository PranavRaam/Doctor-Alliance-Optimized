import sqlite3
import json
import pandas as pd
from typing import List, Dict, Any
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

def create_connection(db_file):
    """Create a database connection to the SQLite database."""
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn):
    """Create the orders table if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        docId INTEGER PRIMARY KEY,
        orderno TEXT,
        orderdate TEXT,
        mrn TEXT,
        soc TEXT,
        cert_period_soe TEXT,
        cert_period_eoe TEXT,
        icd_codes TEXT,
        icd_codes_validated TEXT,
        patient_name TEXT,
        dob TEXT,
        address TEXT,
        patient_sex TEXT,
        raw_text TEXT,
        extraction_method TEXT,
        extraction_error TEXT,
        error TEXT
    );
""")
    conn.commit()

def ensure_new_columns(conn):
    """Ensure all required columns exist in the database."""
    cur = conn.cursor()
    try: 
        cur.execute("ALTER TABLE orders ADD COLUMN patient_name TEXT")
    except sqlite3.OperationalError: 
        pass
    try: 
        cur.execute("ALTER TABLE orders ADD COLUMN dob TEXT")
    except sqlite3.OperationalError: 
        pass
    try: 
        cur.execute("ALTER TABLE orders ADD COLUMN address TEXT")
    except sqlite3.OperationalError: 
        pass
    try: 
        cur.execute("ALTER TABLE orders ADD COLUMN patient_sex TEXT")
    except sqlite3.OperationalError: 
        pass
    conn.commit()

def insert_order(conn, fields):
    """Insert or update an order record in the database."""
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO orders (
        docId, orderno, orderdate, mrn, soc, cert_period_soe, cert_period_eoe,
        icd_codes, icd_codes_validated, patient_name, dob, address, patient_sex,
        raw_text, extraction_method, extraction_error, error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (
    fields.get("docId"),
    fields.get("orderno"),
    fields.get("orderdate"),
    fields.get("mrn"),
    fields.get("soc"),
    fields.get("cert_period", {}).get("soe"),
    fields.get("cert_period", {}).get("eoe"),
    json.dumps(fields.get("icd_codes", [])),
    json.dumps(fields.get("icd_codes_validated", [])),
    fields.get("patient_name"),
    fields.get("dob"),
    fields.get("address"),
    fields.get("patient_sex"),
    fields.get("raw_text", ""),
    fields.get("extraction_method", ""),
    fields.get("extraction_error", ""),
    fields.get("error")
))
    conn.commit()

def fetch_all_orders(conn):
    """Fetch all orders from the database."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM orders")
    rows = cur.fetchall()
    columns = [col[0] for col in cur.description]
    return [dict(zip(columns, row)) for row in rows]

def fetch_order_by_docid(conn, docid):
    """Fetch a specific order by document ID."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM orders WHERE docId = ?", (docid,))
    row = cur.fetchone()
    if row:
        columns = [col[0] for col in cur.description]
        return dict(zip(columns, row))
    return None

def clean_illegal_excel_chars(obj):
    """Clean illegal characters for Excel export."""
    if isinstance(obj, str):
        return ILLEGAL_CHARACTERS_RE.sub("", obj)
    elif isinstance(obj, dict):
        return {k: clean_illegal_excel_chars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_illegal_excel_chars(v) for v in obj]
    else:
        return obj

def export_db_to_excel(conn, output_excel="doctoralliance_orders_final.xlsx", filter_docids=None):
    """Export database records to Excel file."""
    all_orders = fetch_all_orders(conn)
    if filter_docids:
        orders = [r for r in all_orders if str(r.get("docId")) in [str(d) for d in filter_docids]]
    else:
        orders = all_orders
    results_clean = [clean_illegal_excel_chars(r) for r in orders]
    df = pd.json_normalize(results_clean)
    df.to_excel(output_excel, index=False)
    print(f"Done! Output written to {output_excel}")
    print(f"[INFO] Exported {len(orders)} documents to Excel.") 