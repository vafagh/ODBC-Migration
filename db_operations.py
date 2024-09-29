import pyodbc
import mysql.connector
import logging
from datetime import date, datetime

def connect_odbc(dsn):
    """Establish a readonly connection to the ODBC source."""
    try:
        logging.info(f"Connecting to ODBC source using DSN: {dsn} (readonly)")
        return pyodbc.connect(f"DSN={dsn};READONLY=YES")
    except pyodbc.Error as e:
        logging.error(f"Error connecting to ODBC: {str(e)}")
        raise

def connect_mysql(host, user, password, database):
    """Establish a connection to the MySQL database."""
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

def fetch_odbc_metadata(odbc_conn, table_name):
    """Fetch metadata (column names, types, lengths) from ODBC table."""
    try:
        cursor = odbc_conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")  # No data, just schema

        columns_metadata = []
        for column in cursor.description:
            col_name = column[0]
            col_type = column[1]  # Get actual type from ODBC metadata
            col_length = column[3]  # Column length

            # logging.info(f"Column: {col_name}, Type: {col_type}, Length: {col_length}")

            columns_metadata.append((col_name, col_type, col_length))

        return columns_metadata
    except pyodbc.Error as e:
        logging.error(f"Error fetching metadata from ODBC table {table_name}: {str(e)}")
        raise

def map_python_to_mysql_type(python_type, length=None):
    """Map Python data types (from ODBC) to MySQL data types, considering length."""
    python_to_mysql = {
        int: 'INT',
        float: 'FLOAT',
        str: f'VARCHAR({length or 255})',  # Use length if available, else default to 255
        bool: 'TINYINT(1)',
        date: 'DATE',
        datetime: 'DATETIME',
    }
    return python_to_mysql.get(python_type, f'VARCHAR({length or 255})')

def handle_exceptions(col_name, exceptions):
    """Check if the column is in the exceptions list and handle its type."""
    if col_name in exceptions:
        if exceptions[col_name] == "TIME":
            return "TIME"
    return None

def clean_column_name(column_name):
    """Clean column name to remove invalid characters."""
    return column_name.strip().replace('#', '')

def create_mysql_table_from_python_metadata(mysql_conn, table_name, columns_metadata, exceptions, primary_key, unique_keys):
    """Create a MySQL table based on Python metadata, handling exceptions and keys."""
    mysql_cursor = mysql_conn.cursor()

    column_definitions = []
    for col_name, col_type, col_length in columns_metadata:
        cleaned_col_name = clean_column_name(col_name)  # Clean the column name
        exception_type = handle_exceptions(cleaned_col_name, exceptions)
        if exception_type:
            column_definitions.append(f"`{cleaned_col_name}` {exception_type}")
        else:
            column_definitions.append(f"`{cleaned_col_name}` {map_python_to_mysql_type(col_type, col_length)}")

    # Add primary key if provided
    if primary_key:
        primary_key_clause = f"PRIMARY KEY ({', '.join([f'`{clean_column_name(col)}`' for col in primary_key])})"
        column_definitions.append(primary_key_clause)

    # Add unique keys if provided
    if unique_keys:
        unique_key_clause = f"UNIQUE KEY ({', '.join([f'`{clean_column_name(col)}`' for col in unique_keys])})"
        column_definitions.append(unique_key_clause)

    column_definitions.append("`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    column_definitions.append("`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(column_definitions)}
    )
    """

    try:
        logging.info(f"Creating MySQL table `{table_name}` with exception handling and primary/unique keys")
        mysql_cursor.execute(create_table_query)
    except mysql.connector.Error as e:
        logging.error(f"Error creating MySQL table {table_name}: {str(e)}")
        raise

def fetch_odbc_data_in_chunks(odbc_conn, table_name, chunk_size=1000):
    """Fetch ODBC data in chunks to handle large datasets efficiently."""
    cursor = odbc_conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    
    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break
        yield chunk

def prepare_row_for_mysql(row, exceptions, column_names):
    """Prepare a row for MySQL insertion, handling exceptions like time formatting."""
    prepared_row = []
    for idx, value in enumerate(row):
        col_name = column_names[idx]

        if col_name in exceptions and exceptions[col_name] == "TIME" and isinstance(value, str):
            try:
                time_value = datetime.strptime(value.strip(), '%I:%M %p').time()
                prepared_row.append(time_value.strftime('%H:%M:%S'))
            except ValueError:
                prepared_row.append('00:00:00')
        else:
            prepared_row.append(None if value is None else str(value).strip())

    return tuple(prepared_row)

def insert_data_to_mysql(mysql_conn, table_name, insert_columns, rows, update_columns=None, batch_size=1000):
    """Insert data into MySQL table, using insert_columns and update_columns from JSON."""
    mysql_cursor = mysql_conn.cursor()

    # Clean the column names
    clean_columns = [clean_column_name(col) for col in insert_columns]

    column_names = ', '.join([f"`{col}`" for col in clean_columns])
    placeholders = ', '.join(['%s'] * len(clean_columns))

    # If no specific update columns are provided, update all insert columns
    if not update_columns:
        update_columns = clean_columns

    update_clauses = ', '.join([f"`{clean_column_name(col)}`=VALUES(`{clean_column_name(col)}`)" for col in update_columns])
    
    insert_query = f"""
    INSERT INTO `{table_name}` ({column_names}, `created_at`, `updated_at`)
    VALUES ({placeholders}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    ON DUPLICATE KEY UPDATE {update_clauses}, `updated_at`=CURRENT_TIMESTAMP
    """

    try:
        logging.info(f"Inserting data into MySQL table {table_name} in batches")
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            mysql_cursor.executemany(insert_query, batch)
            mysql_conn.commit()
            logging.info(f"Batch {i//batch_size + 1} committed for table {table_name}")
    except mysql.connector.Error as e:
        logging.error(f"Error inserting data into MySQL table {table_name}: {str(e)}")
        raise

def close_connections(*connections):
    """Safely close multiple database connections."""
    for conn in connections:
        if conn:
            conn.close()
    logging.info("Connections closed")
