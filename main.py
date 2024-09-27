import logging
import os
import json
from dotenv import load_dotenv
from db_operations import connect_odbc, connect_mysql, migrate_table, close_connections

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

# Set up logging
logging.basicConfig(
    filename=log_file_path,
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filemode='a'
)

logging.info("Script started")

# Load table mappings from external JSON file
with open('table_mappings.json', 'r') as f:
    table_mappings = json.load(f)["table_mappings"]

try:
    # Connect to ODBC and MySQL
    odbc_conn = connect_odbc(odbc_dsn)
    mysql_conn = connect_mysql(db_host, db_user, db_password, db_name)

    # Migrate each table
    for mapping in table_mappings:
        migrate_table(odbc_conn, mysql_conn, mapping["source"], mapping["destination"])

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")

finally:
    # Close connections
    close_connections(odbc_conn, mysql_conn)
    logging.info("Script finished")
