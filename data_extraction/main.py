import logging
from db_utils import create_table_if_not_exists
from data_extraction import extract_companies, extract_items, extract_auth_events

# Configure logging only once in main.py
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CREATE_COMPANIES_TABLE = """
CREATE TABLE IF NOT EXISTS raw_companies (
    id INT PRIMARY KEY,
    company_name VARCHAR(255),
    num_staff INT,
    region VARCHAR(50)
);
"""

CREATE_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_items (
    brand VARCHAR(255),
    item_name VARCHAR(255),
    msrp FLOAT
);
"""

CREATE_AUTH_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS raw_auth_events (
    company_id INT,
    auth_date TIMESTAMP,
    item_name VARCHAR(255),
    result VARCHAR(50)
);
"""

CREATE_METADATA_TABLE = """
CREATE TABLE IF NOT EXISTS table_metadata (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),
    rows_added INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    execution_time FLOAT
);
"""

# Ensure tables are set up before processing
def setup_tables():
    create_table_if_not_exists('raw_companies', CREATE_COMPANIES_TABLE)
    create_table_if_not_exists('raw_items', CREATE_ITEMS_TABLE)
    create_table_if_not_exists('raw_auth_events', CREATE_AUTH_EVENTS_TABLE)
    create_table_if_not_exists('table_metadata', CREATE_METADATA_TABLE)

if __name__ == "__main__":
    setup_tables()

    # Extract and load data
    extract_companies()
    extract_items()
    extract_auth_events()
