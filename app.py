import os
import requests
import psycopg2
import logging

# ================= ENV =================

HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

HUBSPOT_URL = "https://api.hubapi.com/crm/v3/objects/contacts"

# ================= LOGGING =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ================= DB =================

def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )

def create_table(conn):
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
            number_of_users INTEGER,
            vendor TEXT,
            lead_status TEXT,
            created_date TIMESTAMP,
            last_activity_date TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now()
        );
    """)
    conn.commit()
    cur.close()

# ================= FETCH =================

def fetch_contacts():
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}"
    }

    params = {
        "limit": 100,
        "properties": [
            "firstname",
            "lastname",
            "email",
            "business_name",
            "vat_number",
            "country_",
            "number_of_users",
            "vendor",
            "lead_status",
            "createdate",
            "last_activity_date"
        ]
    }

    contacts = []
    after = None

    while True:
        if after:
            params["after"] = after

        response = requests.get(HUBSPOT_URL, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        contacts.extend(data.get("results", []))

        paging = data.get("paging")
        if paging and "next" in paging:
            after = paging["next"]["after"]
        else:
            break

    return contacts

# ================= LOAD =================

def load_contacts(conn, records):
    cur = conn.cursor()

    for contact in records:
        props = contact.get("properties", {})

        cur.execute("""
            INSERT INTO hubspot_contacts (
                hubspot_id,
                first_name,
                last_name,
                email,
                business_name,
                vat_number,
                country_,
                number_of_users,
                vendor,
                lead_status,
                created_date,
                last_activity_date,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (hubspot_id)
            DO UPDATE SET
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
                updated_at = now();
        """, (
            contact["id"],
            props.get("firstname"),
            props.get("lastname"),
            props.get("email"),
            props.get("business_name"),
            props.get("vat_number"),
            props.get("country_"),
            int(props.get("number_of_users") or 0),
            props.get("vendor"),
            props.get("lead_status"),
            props.get("createdate"),
            props.get("last_activity_date")
        ))

    conn.commit()
    cur.close()

# ================= MAIN =================

def run():
    logging.info("🚀 HubSpot Sync Started")

    conn = get_connection()
    create_table(conn)

    contacts = fetch_contacts()
    logging.info(f"Fetched {len(contacts)} contacts")

    load_contacts(conn, contacts)

    conn.close()
    logging.info("✅ Sync Completed Successfully")

if __name__ == "__main__":
    run()
