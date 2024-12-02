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
    migrate_table_with_difference,
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
log_format = '%(asctime)s - %(levelname)s - %(message)s'
date_format = '%Y-%m-%d %H:%M:%S'
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE"))  # Set CHUNK_SIZE as a global variable

# Set up logging
logging.basicConfig(
    filename=log_file_path,
    level=getattr(logging, log_level, logging.INFO),
    format=log_format,
    datefmt=date_format,
    filemode='a'
)
# Add console logging with time and date
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Adjust as needed (e.g., DEBUG, WARNING)
console_formatter = logging.Formatter(log_format, datefmt=date_format)
console_handler.setFormatter(console_formatter)
logging.getLogger().addHandler(console_handler)
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
        source_table = mapping.get("source")
        destination_table = mapping.get("destination")
        primary_key = mapping.get("primary_key", [])
        unique_keys = mapping.get("unique_keys", [])
        update_columns = mapping.get("update_columns", [])
        sort_column = mapping.get("sort_column", None)
        exceptions = mapping.get("exceptions", {})
        trim_trailing_spaces = mapping.get("trim_trailing_spaces", False)
        insert_columns = mapping.get("insert_columns", None)

        logging.info(f"Processing migration for source: {source_table} -> destination: {destination_table}")

        try:
            migrate_table_with_difference(
                CHUNK_SIZE,
                mysql_conn=mysql_conn,
                odbc_conn=odbc_conn,
                source_table=source_table,
                destination_table=destination_table,
                primary_key=primary_key,
                unique_keys=unique_keys,
                update_columns=update_columns,
                sort_column=sort_column,
                exceptions=exceptions,
                trim_trailing_spaces=trim_trailing_spaces,
                insert_columns=insert_columns
            )
        except Exception as e:
            logging.error(f"Failed to migrate table {source_table} to {destination_table}: {str(e)}", exc_info=True)

except Exception as e:
    logging.error(f"An error occurred: {str(e)}", exc_info=True)
finally:
    # Close connections if they were successfully created
    if odbc_conn is not None:
        close_connections(odbc_conn)
    if mysql_conn is not None:
        close_connections(mysql_conn)
    logging.info("Connections closed")    
    logging.info(f"Script finished. Total runtime: {str(timedelta(seconds=round(time.time() - start_time)))}")