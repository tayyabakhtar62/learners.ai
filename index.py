import psycopg2
import requests
from datetime import datetime, timedelta
import time
import random
import string
from psycopg2.errors import UniqueViolation
from apscheduler.schedulers.background import BackgroundScheduler


class ContactSync:
    def __init__(self):
        # PostgreSQL credentials
        self.postgres_host = "jelani.db.elephantsql.com"
        self.postgres_username = "sqqczmzw"
        self.postgres_database = "sqqczmzw"
        self.postgres_password = "tu3jEaooFIUGYb8A5bqwHRnFJ2oc0xNa"

        # HubSpot credentials
        self.hubspot_client_id = "eb890419-5660-4b46-9cf7-6457a8ad28b6"
        self.hubspot_client_secret = "7f5db265-c6fb-45d2-9cbb-8776470950b2"
        self.hubspot_redirect_url = "https://google.com"

        # Table schema
        self.table_schema = {
            "table_name": "Tayyab - Contact",
            "columns": ["First Name", "Last Name", "Email", "HubSpot ID", "Create Date"]
        }

    def create_table(self):
        conn = psycopg2.connect(
            host=self.postgres_host,
            database=self.postgres_database,
            user=self.postgres_username,
            password=self.postgres_password
        )
        cursor = conn.cursor()

        # Check if table already exists
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s)",
            (self.table_schema["table_name"],)
        )
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Create table if it doesn't exist
            create_table_query = (
                f"CREATE TABLE {self.table_schema['table_name']} ({', '.join(self.table_schema['columns'])})"
            )
            cursor.execute(create_table_query)
            print(f"Table '{self.table_schema['table_name']}' created successfully.")
        else:
            print(f"Table '{self.table_schema['table_name']}' already exists.")

        conn.commit()
        cursor.close()
        conn.close()

    def insert_records(self):
        conn = psycopg2.connect(
            host=self.postgres_host,
            database=self.postgres_database,
            user=self.postgres_username,
            password=self.postgres_password
        )
        cursor = conn.cursor()

        for _ in range(2):
            # Generate random data for the record
            first_name = self.generate_random_string(5)
            last_name = self.generate_random_string(5)
            email = self.generate_random_email()
            hubspot_id = ""
            create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Insert record into the table
            insert_record_query = f"INSERT INTO {self.table_schema['table_name']} " \
                                  f"({', '.join(self.table_schema['columns'])}) " \
                                  f"VALUES (%s, %s, %s, %s, %s)"
            record_data = (first_name, last_name, email, hubspot_id, create_date)

            try:
                cursor.execute(insert_record_query, record_data)
            except UniqueViolation:
                print("Record already exists, skipping insertion.")

        conn.commit()
        cursor.close()
        conn.close()

    def generate_random_string(self, length):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for _ in range(length))

    def generate_random_email(self):
        domain = "example.com"
        letters = string.ascii_lowercase
        username = ''.join(random.choice(letters) for _ in range(5))
        return f"{username}@{domain}"

    def get_access_token(self):
        token_url = "https://api.hubapi.com/oauth/v1/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.hubspot_client_id,
            "client_secret": self.hubspot_client_secret
        }
        response = requests.post(token_url, data=payload)
        access_token = response.json().get("access_token")
        return access_token

    def search_contact(self, email, access_token):
        search_url = f"https://api.hubapi.com/crm/v3/objects/contacts/search"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"q": f'email:{email}'}
        response = requests.get(search_url, headers=headers, params=params)
        return response.json()

    def create_or_update_contact(self, data, access_token):
        contact_url = f"https://api.hubapi.com/crm/v3/objects/contacts"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        response = requests.post(contact_url, headers=headers, json=data)
        return response.json()

    def update_hubspot_id(self, contact_id, hubspot_id):
        conn = psycopg2.connect(
            host=self.postgres_host,
            database=self.postgres_database,
            user=self.postgres_username,
            password=self.postgres_password
        )
        cursor = conn.cursor()

        # Update the HubSpot ID in the table
        update_query = f"UPDATE {self.table_schema['table_name']} " \
                       f"SET \"HubSpot ID\" = %s " \
                       f"WHERE \"Contact ID\" = %s"
        cursor.execute(update_query, (hubspot_id, contact_id))

        conn.commit()
        cursor.close()
        conn.close()

    def sync_contacts(self):
        self.create_table()
        self.insert_records()
        access_token = self.get_access_token()

        conn = psycopg2.connect(
            host=self.postgres_host,
            database=self.postgres_database,
            user=self.postgres_username,
            password=self.postgres_password
        )
        cursor = conn.cursor()

        select_query = f"SELECT * FROM {self.table_schema['table_name']} WHERE \"HubSpot ID\" = ''"
        cursor.execute(select_query)
        records = cursor.fetchall()

        for record in records:
            first_name, last_name, email, hubspot_id, create_date = record

            search_response = self.search_contact(email, access_token)
            search_results = search_response.get("results", [])

            if search_results:
                contact_id = search_results[0].get("id")
                update_data = {
                    "properties": [
                        {"property": "firstname", "value": first_name},
                        {"property": "lastname", "value": last_name},
                        {"property": "email", "value": email}
                    ]
                }
                update_response = self.create_or_update_contact(update_data, access_token)
                updated_hubspot_id = update_response.get("id")
                self.update_hubspot_id(contact_id, updated_hubspot_id)
            else:
                create_data = {
                    "properties": [
                        {"property": "firstname", "value": first_name},
                        {"property": "lastname", "value": last_name},
                        {"property": "email", "value": email}
                    ]
                }
                create_response = self.create_or_update_contact(create_data, access_token)
                new_hubspot_id = create_response.get("id")
                self.update_hubspot_id(email, new_hubspot_id)

        cursor.close()
        conn.close()

    def schedule_sync(self):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.sync_contacts, 'interval', minutes=15)
        scheduler.start()

        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()


if __name__ == "__main__":
    contact_sync = ContactSync()
    contact_sync.schedule_sync()
