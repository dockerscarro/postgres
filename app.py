import os
import psycopg2
import requests
import logging

logging.basicConfig(level=logging.INFO)

HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)

# Since DB is inside same container
POSTGRES_HOST = "localhost"


def create_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{POSTGRES_DB}'")
    exists = cur.fetchone()

    if not exists:
        logging.info(f"Creating database {POSTGRES_DB}")
        cur.execute(f"CREATE DATABASE {POSTGRES_DB}")

    cur.close()
    conn.close()


def get_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )


def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hubspot_contacts (
            id TEXT PRIMARY KEY,
            email TEXT,
            firstname TEXT,
            lastname TEXT,
            phone TEXT,
            lastmodifieddate TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()


def fetch_contacts():
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}
    params = {"limit": 100}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def upsert_contacts(conn, contacts):
    cur = conn.cursor()

    for contact in contacts:
        props = contact.get("properties", {})
        cur.execute("""
            INSERT INTO hubspot_contacts (id, email, firstname, lastname, phone, lastmodifieddate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET
                email = EXCLUDED.email,
                firstname = EXCLUDED.firstname,
                lastname = EXCLUDED.lastname,
                phone = EXCLUDED.phone,
                lastmodifieddate = EXCLUDED.lastmodifieddate
        """, (
            contact.get("id"),
            props.get("email"),
            props.get("firstname"),
            props.get("lastname"),
            props.get("phone"),
            props.get("lastmodifieddate")
        ))

    conn.commit()
    cur.close()


def run():
    logging.info("Starting HubSpot Sync")

    create_database()

    conn = get_connection()
    create_table(conn)

    contacts = fetch_contacts()
    upsert_contacts(conn, contacts)

    conn.close()
    logging.info("Sync Completed Successfully")


if __name__ == "__main__":
    run()
