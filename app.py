import os
import requests
import psycopg2
import logging
from psycopg2.extras import execute_batch
 
# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
 
# ---------------- ENV VARIABLES ----------------
HUBSPOT_TOKEN = os.getenv("HUBSPOT_TOKEN")
 
PG_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}
 
HUBSPOT_URL = "https://api.hubapi.com/crm/v3/objects/contacts"
 
 
# ---------------- FETCH CONTACTS ----------------
def fetch_contacts():
    if not HUBSPOT_TOKEN:
        raise ValueError("HUBSPOT_TOKEN is not set")
 
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
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
 
    all_contacts = []
    after = None
 
    logging.info("Fetching contacts from HubSpot...")
 
    while True:
        if after:
            params["after"] = after
 
        response = requests.get(HUBSPOT_URL, headers=headers, params=params)
        response.raise_for_status()
 
        data = response.json()
        results = data.get("results", [])
        all_contacts.extend(results)
 
        paging = data.get("paging")
        if paging and "next" in paging:
            after = paging["next"]["after"]
        else:
            break
 
    logging.info(f"Total contacts fetched: {len(all_contacts)}")
    return all_contacts
 
 
# ---------------- LOAD TO POSTGRES ----------------
def load_contacts(records):
    if not records:
        logging.info("No records to insert.")
        return
 
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
 
    insert_query = """
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
            last_activity_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
    """
 
    data_to_insert = []
 
    for contact in records:
        props = contact.get("properties", {})
 
        data_to_insert.append((
            contact.get("id"),
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
 
    execute_batch(cur, insert_query, data_to_insert)
 
    conn.commit()
    cur.close()
    conn.close()
 
    logging.info(f"{len(records)} records inserted/updated successfully.")
 
 
# ---------------- MAIN PIPELINE ----------------
def run_pipeline():
    logging.info("HubSpot ETL started")
 
    contacts = fetch_contacts()
    load_contacts(contacts)
 
    logging.info("HubSpot ETL completed successfully")
 
 
if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
