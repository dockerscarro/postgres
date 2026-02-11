import requests
import psycopg2
import logging
import os

# ---------------- CONFIG FROM ENV ----------------

HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

CONTACTS_URL = "https://api.hubapi.com/crm/v3/objects/contacts"

# -------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}"
}


# ---------- DB HELPERS ----------

def get_last_contact_id(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(MAX(hubspot_id::BIGINT), 0)
        FROM hubspot_contacts;
    """)
    last_id = cur.fetchone()[0]
    cur.close()
    return last_id


# ---------- HUBSPOT FETCH ----------

def fetch_contacts():
    results = []
    after = None

    params = {
        "limit": 100,
        "archived": False,
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

    while True:
        if after:
            params["after"] = after

        r = requests.get(CONTACTS_URL, headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()

        results.extend(data.get("results", []))

        if "paging" not in data:
            break

        after = data["paging"]["next"]["after"]

    return results


# ---------- LOAD TO POSTGRES ----------

def load_contacts(conn, records, last_id):
    cur = conn.cursor()
    inserted = 0

    for c in records:
        cid = int(c["id"])

        if cid <= last_id:
            continue

        p = c.get("properties", {})

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
            ON CONFLICT (hubspot_id) DO NOTHING;
        """, (
            str(cid),
            p.get("firstname"),
            p.get("lastname"),
            p.get("email"),
            p.get("business_name"),
            p.get("vat_number"),
            p.get("country_"),
            int(p.get("number_of_users") or 0),
            p.get("vendor"),
            p.get("lead_status"),
            p.get("createdate"),
            p.get("last_activity_date")
        ))

        inserted += 1

    conn.commit()
    cur.close()
    return inserted


# ---------- MAIN PIPELINE ----------

def run_pipeline():
    logging.info("HubSpot ID-based sync started")

    conn = psycopg2.connect(**PG_CONFIG)

    last_id = get_last_contact_id(conn)
    logging.info(f"Last HubSpot ID in DB: {last_id}")

    contacts = fetch_contacts()
    logging.info(f"Fetched {len(contacts)} contacts from HubSpot")

    inserted = load_contacts(conn, contacts, last_id)
    logging.info(f"Inserted {inserted} new contacts")

    conn.close()
    logging.info("Sync completed successfully")


if __name__ == "__main__":
    run_pipeline()

