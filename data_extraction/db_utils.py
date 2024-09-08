import psycopg2
import logging

DB_CONNECTION = "dbname=postgres user=kishan password=ds2023 host=localhost"

# Function to check if a table exists
def table_exists(table_name):
    try:
        conn = psycopg2.connect(DB_CONNECTION)
        cur = conn.cursor()
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');")
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except Exception as e:
        logging.error(f"Error checking table existence for {table_name}: {str(e)}", exc_info=True)
        return False

# Function to create tables if they do not exist
def create_table_if_not_exists(table_name, create_query):
    if not table_exists(table_name):
        try:
            conn = psycopg2.connect(DB_CONNECTION)
            cur = conn.cursor()
            cur.execute(create_query)
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"Created table {table_name}.")
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {str(e)}", exc_info=True)
    else:
        logging.info(f"Table {table_name} already exists.")

# Function to clear tables before inserting data
def clear_table(table_name):
    if table_exists(table_name):
        try:
            conn = psycopg2.connect(DB_CONNECTION)
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {table_name};")
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"Cleared table {table_name}")
        except Exception as e:
            logging.error(f"Error clearing table {table_name}: {str(e)}", exc_info=True)
    else:
        logging.error(f"Table {table_name} does not exist, skipping truncation.")

# Function to insert data into the database
def insert_into_table(query, data, table_name):
    if table_exists(table_name):
        try:
            conn = psycopg2.connect(DB_CONNECTION)
            cur = conn.cursor()
            cur.executemany(query, data)
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"Inserted {len(data)} records into {table_name}.")
        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {str(e)}", exc_info=True)
    else:
        logging.error(f"Table {table_name} does not exist, skipping insertion.")
