import time
from datetime import datetime, timedelta
start_time = time.time()
import os
import logging
import json
from dotenv import load_dotenv
from db_operations import (
    connect_odbc,
    connect_mysql,
    fetch_odbc_metadata,
    create_mysql_table_from_odbc_metadata,
    fetch_and_insert_rows,
    fetch_and_update_rows,
    close_connections
)

# Load environment variables
load_dotenv()

# Fetch configuration from environment variables
odbc_dsn = os.getenv("ODBC_DSN")
db_host = os.getenv("DB_HOST", "localhost")
db_user = os.getenv("DB_USER", "root")
db_password = os.getenv("DB_PASSWORD", "")
db_name = os.getenv("DB_NAME", "tracker")
log_file_path = os.getenv("LOG_FILE_PATH", "script_log.log")
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 5000))

# Set up logging
logging.basicConfig(
    filename=log_file_path,
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filemode='a'
)
logging.getLogger().addHandler(logging.StreamHandler())  # Enable console logging

logging.info("Script started")

# Initialize connections to None
odbc_conn = None
mysql_conn = None

# Load table mappings from external JSON file
try:
    with open('table_mappings.json', 'r') as f:
        table_mappings = json.load(f)["table_mappings"]
except FileNotFoundError as e:
    logging.error(f"FileNotFoundError: {e}")
    table_mappings = []
except json.JSONDecodeError as e:
    logging.error(f"JSONDecodeError: Failed to parse 'table_mappings.json' - {e}")
    table_mappings = []
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
    table_mappings = []

# Connect to ODBC and MySQL
try:
    odbc_conn = connect_odbc(odbc_dsn)
    mysql_conn = connect_mysql(db_host, db_user, db_password, db_name)

    if not table_mappings:
        logging.error("No valid table mappings found. Exiting the script.")
        exit(1)

    # Iterate over each table mapping dynamically
    for mapping in table_mappings:
        if not mapping.get("active", True):
            logging.info(f"Skipping inactive table mapping for source: {mapping.get('source')} -> destination: {mapping.get('destination')}")
            continue

        source_table = mapping.get("source")
        destination_table = mapping.get("destination")
        primary_key = mapping.get("primary_key", [])
        unique_keys = mapping.get("unique_keys", [])
        update_columns = mapping.get("update_columns", [])
        sort_column = mapping.get("sort_column", None)
        exceptions = mapping.get("exceptions", {})
        trim_trailing_spaces = mapping.get("trim_trailing_spaces", False)
        insert_columns = mapping.get("insert_columns", None)
        since = mapping.get("since", None)

        logging.info(f"Processing migration for source: {source_table} -> destination: {destination_table}")

        # Fetch ODBC metadata
        columns_metadata = fetch_odbc_metadata(odbc_conn, source_table, exceptions)

        # Check if table exists; create if missing
        cursor = mysql_conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{destination_table}'")
        result = cursor.fetchone()

        if not result:
            logging.info(f"MySQL table `{destination_table}` does not exist. Creating table.")
            create_mysql_table_from_odbc_metadata(
                mysql_conn,
                destination_table,
                columns_metadata,
                primary_key,
                unique_keys,
                exceptions
            )

        # Determine operation: update or fresh insert
        if update_columns:
            logging.info(f"Table `{destination_table}` will be updated with columns: {update_columns}.")
            fetch_and_update_rows(
                odbc_conn=odbc_conn,
                mysql_conn=mysql_conn,
                source_table=source_table,
                destination_table=destination_table,
                columns=columns_metadata,
                primary_key=primary_key,
                unique_keys=unique_keys,
                sort_column=sort_column,
                update_columns=update_columns,
                chunk_size=BATCH_SIZE,
                exceptions=exceptions,
                trim_trailing_spaces=trim_trailing_spaces, 
                since=since  
            )
        else:
            logging.info(f"Table `{destination_table}` will be freshly inserted.")
            fetch_and_insert_rows(
                chunk_size=BATCH_SIZE,
                odbc_conn=odbc_conn,
                mysql_conn=mysql_conn,
                source_table=source_table,
                destination_table=destination_table,
                columns=columns_metadata,
                primary_key=primary_key,
                unique_keys=unique_keys,
                sort_column=sort_column,
                exceptions=exceptions,
                since=since,
                trim_trailing_spaces=trim_trailing_spaces,
                insert_columns=insert_columns
            )

except Exception as e:
    logging.error(f"An error occurred: {str(e)}", exc_info=True)
finally:
    # Close connections if they were successfully created
    if odbc_conn is not None:
        close_connections(odbc_conn)
    if mysql_conn is not None:
        close_connections(mysql_conn)
    logging.info("Connections closed")
    logging.info("Script finished")
    logging.info(f"Script finished. Total runtime: {str(timedelta(seconds=round(time.time() - start_time)))}")