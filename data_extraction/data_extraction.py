import requests
import time
import logging
from db_utils import clear_table, insert_into_table, create_table_if_not_exists

API_BASE_URL = "http://ec2-44-202-253-230.compute-1.amazonaws.com"
TOKEN = "entrupy_Kishan"

# Function to fetch data from the API
def fetch_data(endpoint, params=None):
    url = f"{API_BASE_URL}/{endpoint}"
    params = params or {}
    params['token'] = TOKEN
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Function to log metadata about the extraction process
def log_metadata(table_name, rows_added, execution_time):
    try:
        query = """
            INSERT INTO table_metadata (table_name, rows_added, execution_time)
            VALUES (%s, %s, %s)
        """
        insert_into_table(query, [(table_name, rows_added, execution_time)], 'table_metadata')
        logging.info(f"Logged metadata for {table_name}: {rows_added} rows, {execution_time:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error logging metadata for {table_name}: {str(e)}", exc_info=True)

# Extract and load companies data
def extract_companies():
    logging.info("Starting extraction for companies data...")
    clear_table('raw_companies')
    logging.info("Cleared table raw_companies")
    
    start_time = time.time()
    companies = fetch_data("companies")
    
    query = """
        INSERT INTO raw_companies (company_name, id, num_staff, region)
        VALUES (%s, %s, %s, %s)
    """
    data = [(company_name, company_data['id'], company_data['num_staff'], company_data['region'])
            for company in companies for company_name, company_data in company.items()]
    
    insert_into_table(query, data, 'raw_companies')
    logging.info(f"Inserted {len(data)} records into raw_companies")

    execution_time = time.time() - start_time
    log_metadata('raw_companies', len(data), execution_time)

# Extract and load items data
def extract_items():
    logging.info("Starting extraction for items data...")
    clear_table('raw_items')
    logging.info("Cleared table raw_items")
    
    start_time = time.time()
    items = fetch_data("items")
    
    query = """
        INSERT INTO raw_items (brand, item_name, msrp)
        VALUES (%s, %s, %s)
    """
    data = [(item[0], item[1], float(item[2])) for item in items]
    
    insert_into_table(query, data, 'raw_items')
    logging.info(f"Inserted {len(data)} records into raw_items")

    execution_time = time.time() - start_time
    log_metadata('raw_items', len(data), execution_time)

# Extract and load auth events data with pagination
def extract_auth_events():
    logging.info("Starting extraction for auth events data...")
    clear_table('raw_auth_events')
    logging.info("Cleared table raw_auth_events")
    
    start_time = time.time()
    offset = 0
    limit = 1000
    all_auth_events = []
    
    while True:
        params = {'limit': limit, 'offset': offset}
        auth_events = fetch_data("auth_events", params)
        if not auth_events:
            break
        all_auth_events.extend(auth_events)
        offset += limit
        logging.info(f"Extracted {len(auth_events)} auth events at offset {offset}")

    query = """
        INSERT INTO raw_auth_events (company_id, auth_date, item_name, result)
        VALUES (%s, %s, %s, %s)
    """
    data = [(event['company_id'], event['date'], event['item_name'], event['result']) for event in all_auth_events]
    
    insert_into_table(query, data, 'raw_auth_events')
    logging.info(f"Inserted {len(data)} records into raw_auth_events")

    execution_time = time.time() - start_time
    log_metadata('raw_auth_events', len(data), execution_time)
