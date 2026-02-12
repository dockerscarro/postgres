import os
import logging
import requests
import psycopg2
from contextlib import closing
from datetime import datetime

# ---------------- ENV CONFIG ----------------
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

HUBSPOT_URL = "https://api.hubapi.com/crm/v3/objects/contacts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ---------- UTILS ----------
def parse_timestamp(ts: str):
    """Convert HubSpot ISO timestamp string to Python datetime."""
    if ts:
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            logging.warning(f"Failed to parse timestamp: {ts}")
            return None
    return None


# ---------- CREATE TABLE ----------
def create_table_if_not_exists():
    logging.info("Ensuring hubspot_contacts table exists...")
    with closing(psycopg2.connect(**PG_CONFIG)) as conn:
        with conn.cursor() as cur:
            # Create table if it doesn't exist (minimal)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hubspot_contacts (
                    hubspot_id TEXT PRIMARY KEY
                );
                """
            )

            # Add missing columns safely
            columns = {
                "first_name": "TEXT",
                "last_name": "TEXT",
                "email": "TEXT",
                "business_name": "TEXT",
                "vat_number": "TEXT",
                "country_": "TEXT",
                "number_of_users": "INTEGER",
                "vendor": "TEXT",
                "lead_status": "TEXT",
                "created_date": "TIMESTAMP",
                "last_activity_date": "TIMESTAMP",
                "updated_at": "TIMESTAMP DEFAULT now()",
            }

            for col, col_type in columns.items():
                cur.execute(
                    f"ALTER TABLE hubspot_contacts "
                    f"ADD COLUMN IF NOT EXISTS {col} {col_type};"
                )

        conn.commit()
    logging.info("Table is ready.")


# ---------- FETCH HUBSPOT CONTACTS ----------
def fetch_contacts():
    logging.info("Fetching contacts from HubSpot...")

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json",
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
            "last_activity_date",
        ],
    }

    all_contacts = []
    after = None

    while True:
        if after:
            params["after"] = after

        response = requests.get(HUBSPOT_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_contacts.extend(data.get("results", []))

        paging = data.get("paging")
        if paging and "next" in paging:
            after = paging["next"]["after"]
        else:
            break

    logging.info(f"Fetched {len(all_contacts)} contacts.")
    return all_contacts


# ---------- UPSERT INTO POSTGRES ----------
def load_contacts(records):
    logging.info("Upserting contacts into PostgreSQL...")

    sql = (
        "INSERT INTO hubspot_contacts (hubspot_id, first_name, last_name, "
        "email, business_name, vat_number, country_, number_of_users, "
        "vendor, lead_status, created_date, last_activity_date, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (hubspot_id) DO UPDATE SET "
        "first_name = EXCLUDED.first_name, "
        "last_name = EXCLUDED.last_name, "
        "email = EXCLUDED.email, "
        "business_name = EXCLUDED.business_name, "
        "vat_number = EXCLUDED.vat_number, "
        "country_ = EXCLUDED.country_, "
        "number_of_users = EXCLUDED.number_of_users, "
        "vendor = EXCLUDED.vendor, "
        "lead_status = EXCLUDED.lead_status, "
        "created_date = EXCLUDED.created_date, "
        "last_activity_date = EXCLUDED.last_activity_date, "
        "updated_at = now()"
    )

    with closing(psycopg2.connect(**PG_CONFIG)) as conn:
        with conn.cursor() as cur:
            for contact in records:
                props = contact.get("properties", {})

                cur.execute(
                    sql,
                    (
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
                        parse_timestamp(props.get("createdate")),
                        parse_timestamp(props.get("last_activity_date")),
                    ),
                )
        conn.commit()

    logging.info(f"{len(records)} contacts synced successfully.")


# ---------- MAIN ----------
def run_pipeline():
    logging.info("🚀 HubSpot ETL started")

    if not HUBSPOT_API_KEY:
        raise ValueError("HUBSPOT_API_KEY is not set")

    create_table_if_not_exists()
    contacts = fetch_contacts()
    load_contacts(contacts)

    logging.info("✅ HubSpot ETL completed successfully.")


if __name__ == "__main__":
    run_pipeline()
