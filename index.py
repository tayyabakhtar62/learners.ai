import psycopg2
import requests
import json
from datetime import datetime
import time
import random
import string
from psycopg2.errors import UniqueViolation
from apscheduler.schedulers.background import BackgroundScheduler
import atexit


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
            "table_name": "Tayyab_Contact",
            "columns": [
                "First Name",
                "Last Name",
                "Email",
                "HubSpot ID",
                "Create Date",
            ],
        }

    def create_connection(self):
        try:
            conn = psycopg2.connect(
                host=self.postgres_host,
                database=self.postgres_database,
                user=self.postgres_username,
                password=self.postgres_password,
            )
            print("Successfully connected to the PostgreSQL database.")
            return conn
        except Exception as e:
            print(f"Failed to connect to the PostgreSQL database: {e}")
            return None

    def create_table(self):
        conn = self.create_connection()
        if conn:
            cursor = conn.cursor()
            try:
                # Create table if it doesn't exist
                create_table_query = (
                    'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})'.format(
                        table_name=self.table_schema["table_name"],
                        columns=", ".join(
                            [
                                '"{}" TEXT'.format(column)
                                for column in self.table_schema["columns"]
                            ]
                        ),
                    )
                )

                cursor.execute(create_table_query)
                print(
                    f"Table '{self.table_schema['table_name']}' created or already exists."
                )

                conn.commit()
            except Exception as e:
                print(f"Failed to create table: {e}")
            finally:
                cursor.close()
                conn.close()
        else:
            print("Cannot create table without database connection.")

    def insert_records(self):
        conn = self.create_connection()
        if conn:
            cursor = conn.cursor()
            try:
                for _ in range(2):
                    # Generate random data for the record
                    first_name = self.generate_random_string(5)
                    last_name = self.generate_random_string(5)
                    email = self.generate_random_email()
                    hubspot_id = ""
                    create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Insert record into the table
                    insert_record_query = 'INSERT INTO "{table_name}" ("First Name", "Last Name", "Email", "HubSpot ID", "Create Date") VALUES (%s, %s, %s, %s, %s)'.format(
                        table_name=self.table_schema["table_name"]
                    )

                    record_data = (
                        first_name,
                        last_name,
                        email,
                        hubspot_id,
                        create_date,
                    )

                    try:
                        cursor.execute(insert_record_query, record_data)
                    except UniqueViolation:
                        print("Record already exists, skipping insertion.")

                conn.commit()
                print("Records inserted successfully.")
            except Exception as e:
                print(f"Failed to insert records: {e}")
            finally:
                cursor.close()
                conn.close()
        else:
            print("Cannot insert records without database connection.")

    def generate_random_string(self, length):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for _ in range(length))

    def generate_random_email(self):
        domain = "example.com"
        letters = string.ascii_lowercase
        username = "".join(random.choice(letters) for _ in range(5))
        return f"{username}@{domain}"

    def get_access_token(self):
        try:
            token_url = "https://api.hubapi.com/oauth/v1/token"
            payload = {
                "grant_type": "authorization_code",
                "client_id": self.hubspot_client_id,
                "client_secret": self.hubspot_client_secret,
                "redirect_uri": "https://google.com"
            }
            headers = {'Content-Type:  application/x-www-form-urlencoded;charset=utf-8'}
            
            response = requests.post(token_url, data=payload, headers=headers)
            if response.status_code == 200:
                print("Successfully retrieved the access token.")
                access_token = response.json().get("access_token")
                return access_token
            else:
                print(
                    f"Failed to retrieve the access token. Status code: {response.status_code}"
                )
                print(response.text)
        except Exception as e:
            print(f"Failed to get access token: {e}")

    def search_contact(self, email, access_token):
        if access_token:
            try:
                search_url = f"https://api.hubapi.com/crm/v3/objects/contacts/search"
                headers = {"Authorization": f"Bearer {access_token}"}
                params = {"q": f"email:{email}"}
                response = requests.get(search_url, headers=headers, params=params)
                if response.status_code == 200:
                    print("Successfully searched the contact.")
                    return response.json()
                else:
                    print(
                        f"Failed to search the contact. Status code: {response.status_code}"
                    )
                    return None
            except Exception as e:
                print(f"Failed to search contact: {e}")
                return None
        else:
            print("Cannot search contact without access token.")

    def create_or_update_contact(self, data, access_token):
        if access_token:
            try:
                contact_url = f"https://api.hubapi.com/crm/v3/objects/contacts"
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                }
                response = requests.post(contact_url, headers=headers, json=data)
                if response.status_code == 200:
                    print("Successfully created or updated the contact.")
                    return response.json()
                else:
                    print(
                        f"Failed to create or update the contact. Status code: {response.status_code}"
                    )
            except Exception as e:
                print(f"Failed to create or update contact: {e}")
        else:
            print("Cannot create or update contact without access token.")

    def update_hubspot_id(self, contact_id, hubspot_id):
        conn = self.create_connection()
        if conn:
            cursor = conn.cursor()
            try:
                # Update the HubSpot ID in the table
                update_query = 'UPDATE "{table_name}" SET "HubSpot ID" = %s WHERE "Contact ID" = %s'.format(
                    table_name=self.table_schema["table_name"]
                )

                cursor.execute(update_query, (hubspot_id, contact_id))
                conn.commit()
                print("HubSpot ID updated successfully.")
            except Exception as e:
                print(f"Failed to update HubSpot ID: {e}")
            finally:
                cursor.close()
                conn.close()
        else:
            print("Cannot update HubSpot ID without database connection.")

    def sync_contacts(self):
        access_token = self.get_access_token()
        if access_token:
            conn = self.create_connection()
            if conn:
                cursor = conn.cursor()
                try:
                    # Get all records from the table
                    select_query = 'SELECT * FROM "{table_name}"'.format(
                        table_name=self.table_schema["table_name"]
                    )
                    cursor.execute(select_query)
                    records = cursor.fetchall()

                    # For each record, create or update the contact in HubSpot
                    for record in records:
                        data = {
                            "properties": {
                                "firstname": record[0],
                                "lastname": record[1],
                                "email": record[2],
                            }
                        }
                        response = self.create_or_update_contact(data, access_token)
                        if response:
                            hubspot_id = response["id"]
                            self.update_hubspot_id(record[3], hubspot_id)

                except Exception as e:
                    print(f"Failed to sync contacts: {e}")
                finally:
                    cursor.close()
                    conn.close()
            else:
                print("Cannot sync contacts without database connection.")
        else:
            print("Cannot sync contacts without access token.")

    def run(self):
        self.create_table()
        self.insert_records()
        self.sync_contacts()


if __name__ == "__main__":
    contact_sync = ContactSync()
    contact_sync.run()
