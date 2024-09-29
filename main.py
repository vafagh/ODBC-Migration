import logging
import os
import json
from dotenv import load_dotenv
from db_operations import connect_odbc, connect_mysql, fetch_odbc_metadata, create_mysql_table_from_python_metadata, fetch_odbc_data_in_chunks, insert_data_to_mysql, close_connections, prepare_row_for_mysql

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

# Set up logging to both file and console
logger = logging.getLogger()
logger.setLevel(getattr(logging, log_level, logging.INFO))

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(getattr(logging, log_level, logging.INFO))
file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_format)

console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, log_level, logging.INFO))
console_format = logging.Formatter('%(levelname)s - %(message)s')
console_handler.setFormatter(console_format)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logging.info("Script started")

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

# Proceed only if table_mappings are valid
if not table_mappings:
    logging.error("No valid table mappings found. Exiting the script.")
else:
    try:
        # Connect to ODBC and MySQL
        odbc_conn = connect_odbc(odbc_dsn)
        mysql_conn = connect_mysql(db_host, db_user, db_password, db_name)

        # Migrate each table
        for mapping in table_mappings:
            source_table = mapping["source"]
            destination_table = mapping["destination"]
            insert_columns = mapping.get("insert_columns", None)
            update_columns = mapping.get("update_columns", None)
            primary_key = mapping.get("primary_key", [])
            unique_keys = mapping.get("unique_keys", [])
            exceptions = mapping.get("exceptions", {})

            # Fetch column metadata from ODBC
            columns_metadata = fetch_odbc_metadata(odbc_conn, source_table)

            # Use columns from metadata if no insert_columns are specified
            if not insert_columns:
                insert_columns = [col[0] for col in columns_metadata]

            # If update_columns are not provided, default to insert_columns
            if not update_columns:
                update_columns = insert_columns

            # Create the MySQL table using the fetched metadata, handling exceptions and keys
            create_mysql_table_from_python_metadata(mysql_conn, destination_table, columns_metadata, exceptions, primary_key, unique_keys)

            # Migrate data in chunks
            for chunk in fetch_odbc_data_in_chunks(odbc_conn, source_table):
                prepared_rows = [prepare_row_for_mysql(row, exceptions, insert_columns) for row in chunk]
                insert_data_to_mysql(mysql_conn, destination_table, insert_columns, prepared_rows, update_columns)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

    finally:
        # Close connections
        close_connections(odbc_conn, mysql_conn)
        logging.info("Script finished")
