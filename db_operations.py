import pyodbc
import mysql.connector
import logging


def connect_odbc(dsn):
    """Establish a readonly connection to the ODBC source."""  # Connect to ODBC with readonly access
    try:
        logging.info(f"Connecting to ODBC source using DSN: {dsn} (readonly)")
        return pyodbc.connect(f"DSN={dsn};READONLY=YES")
    except pyodbc.Error as e:
        logging.error(f"Error connecting to ODBC: {str(e)}")
        raise


def connect_mysql(host, user, password, database):
    """Establish a connection to the MySQL database."""  # Connect to MySQL
    try:
        logging.info(f"Connecting to MySQL database at {host}")
        return mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
    except mysql.connector.Error as e:
        logging.error(f"Error connecting to MySQL: {str(e)}")
        raise


def fetch_odbc_columns(odbc_conn, table_name):
    """Fetch column names and types from ODBC table."""  # Retrieve ODBC table column names
    try:
        logging.info(f"Fetching columns from ODBC table {table_name}")
        cursor = odbc_conn.cursor()
        cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
        return [column[0] for column in cursor.description]
    except pyodbc.Error as e:
        logging.error(f"Error fetching columns from ODBC table {table_name}: {str(e)}")
        raise


def create_mysql_table(mysql_conn, table_name, columns):
    """Create a MySQL table dynamically if it doesn't exist based on ODBC columns."""  # Create MySQL table if not exists
    mysql_cursor = mysql_conn.cursor()
    column_definitions = ', '.join([f"`{col}` TEXT" for col in columns])  # Use TEXT as default; adjust if needed
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {column_definitions}
    )"""
    try:
        logging.info(f"Creating MySQL table {table_name} if it does not exist")
        mysql_cursor.execute(create_table_query)
    except mysql.connector.Error as e:
        logging.error(f"Error creating MySQL table {table_name}: {str(e)}")
        raise


def fetch_odbc_data_in_chunks(odbc_conn, table_name, chunk_size=1000):
    """Fetch ODBC data in chunks to handle large datasets efficiently."""  # Fetch ODBC data in chunks
    cursor = odbc_conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    
    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break
        yield chunk


def prepare_row_for_mysql(row):
    """Prepare a row for MySQL insertion."""  # Prepare ODBC row for MySQL insertion
    return tuple(None if v is None else str(v) for v in row)


def insert_data_to_mysql(mysql_conn, table_name, columns, rows, batch_size=1000):
    """Insert data into MySQL table in batches."""  # Insert data into MySQL
    mysql_cursor = mysql_conn.cursor()
    column_names = ', '.join([f"`{col}`" for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
    
    try:
        logging.info(f"Inserting data into MySQL table {table_name} in batches")
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            mysql_cursor.executemany(insert_query, [prepare_row_for_mysql(row) for row in batch])
            mysql_conn.commit()
            logging.info(f"Batch {i//batch_size + 1} committed for table {table_name}")
    except mysql.connector.Error as e:
        logging.error(f"Error inserting data into MySQL table {table_name}: {str(e)}")
        raise


def migrate_table(odbc_conn, mysql_conn, source_table, destination_table, chunk_size=1000):
    """Migrate a table from ODBC to MySQL."""  # Migrate data from ODBC to MySQL
    logging.info(f"Starting migration for {source_table} -> {destination_table}")
    columns = fetch_odbc_columns(odbc_conn, source_table)
    create_mysql_table(mysql_conn, destination_table, columns)
    
    for chunk in fetch_odbc_data_in_chunks(odbc_conn, source_table, chunk_size=chunk_size):
        insert_data_to_mysql(mysql_conn, destination_table, columns, chunk, batch_size=chunk_size)
    
    logging.info(f"Completed migration for {source_table} -> {destination_table}")


def close_connections(*connections):
    """Safely close multiple database connections."""  # Close ODBC and MySQL connections
    for conn in connections:
        if conn:
            conn.close()
    logging.info("Connections closed")
