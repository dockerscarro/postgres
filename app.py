import os
import requests
import psycopg2
import logging

# ---------------- CONFIG ----------------
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "hubspot_db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "hubspot")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

HUBSPOT_URL = "https://api.hubapi.com/crm/v3/objects/contacts"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# ---------- DB CONNECTION ----------
def get_connection(db_name=None):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=db_name if db_name else POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    return conn


# ---------- CREATE DATABASE IF NOT EXISTS ----------
def create_database():
    try:
        conn = get_connection("postgres")  # connect to default postgres
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{POSTGRES_DB}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {POSTGRES_DB};")
            logging.info(f"Database '{POSTGRES_DB}' created.")
        cur.close()
        conn.close()
    except Exception as e:
        logging.info(f"Database '{POSTGRES_DB}' already exists or cannot create: {e}")


# ---------- CREATE TABLE IF NOT EXISTS ----------
def create_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hubspot_contacts (
            hubspot_id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            business_name TEXT,
            vat_number TEXT,
            country_ TEXT,
            number_of_users INT,
            vendor TEXT,
            lead_status TEXT,
            created_date TEXT,
            last_activity_date TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


# ---------- FETCH HUBSPOT CONTACTS ----------
def fetch_contacts():
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
    params = {
        "limit": 100,
        "properties": [
            "firstname", "lastname", "email", "business_name",
            "vat_number", "country_", "number_of_users",
            "vendor", "lead_status", "createdate", "last_activity_date"
        ]
    }
    all_contacts = []
    after = None
    while True:
        if after:
            params["after"] = after
        r = requests.get(HUBSPOT_URL, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        all_contacts.extend(data.get("results", []))
        paging = data.get("paging")
        if paging and "next" in paging:
            after = paging["next"]["after"]
        else:
            break
    return all_contacts


# ---------- UPSERT CONTACTS ----------
def upsert_contacts(records):
    conn = get_connection()
    cur = conn.cursor()
    for c in records:
        props = c.get("properties", {})
        cur.execute("""
            INSERT INTO hubspot_contacts (
                hubspot_id, first_name, last_name, email, business_name, vat_number,
                country_, number_of_users, vendor, lead_status, created_date, last_activity_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (hubspot_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                business_name = EXCLUDED.business_name,
                vat_number = EXCLUDED.vat_number,
                country_ = EXCLUDED.country_,
                number_of_users = EXCLUDED.number_of_users,
                vendor = EXCLUDED.vendor,
                lead_status = EXCLUDED.lead_status,
                created_date = EXCLUDED.created_date,
                last_activity_date = EXCLUDED.last_activity_date,
                updated_at = NOW();
        """, (
            c["id"], props.get("firstname"), props.get("lastname"), props.get("email"),
            props.get("business_name"), props.get("vat_number"), props.get("country_"),
            int(props.get("number_of_users") or 0), props.get("vendor"),
            props.get("lead_status"), props.get("createdate"), props.get("last_activity_date")
        ))
    conn.commit()
    cur.close()
    conn.close()


# ---------- MAIN ----------
def run():
    logging.info("🚀 Running sync job...")
    create_database()
    create_table()
    contacts = fetch_contacts()
    logging.info(f"Fetched {len(contacts)} contacts from HubSpot")
    upsert_contacts(contacts)
    logging.info("✅ Contacts upserted successfully")


if __name__ == "__main__":
    run()
