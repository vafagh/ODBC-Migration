
# ODBC to MySQL Migration Tool

## Overview

This project provides a Python-based tool for migrating data from an ODBC source to a MySQL database. The tool dynamically fetches data from ODBC tables, creates corresponding tables in MySQL (if they don’t exist), and migrates the data efficiently, handling large datasets in chunks.

Key features include:
- **Automatic Table Creation**: Dynamically creates MySQL tables based on the ODBC schema.
- **Avoid Duplicate Records**: Updates existing rows in MySQL if they already exist.
- **Timestamps**: Adds `created_at` and `updated_at` fields to every MySQL table.
- **Batch Processing**: Handles large datasets efficiently by migrating data in batches.
- **Type Mapping**: Automatically maps ODBC data types to MySQL data types.

## Prerequisites

- Python 3.x
- ODBC Driver installed for your data source (e.g., Pervasive, SQL Server)
- MySQL Database

### Python Dependencies

Install the required Python packages by running:

```bash
pip install -r requirements.txt
```

The main dependencies are:
- `pyodbc`
- `mysql-connector-python`
- `python-dotenv`

## Project Structure

- `main_script.py`: The entry point for the script that handles the migration process.
- `db_operations.py`: Contains helper functions for connecting to databases, creating MySQL tables, and migrating data.
- `.env`: Configuration file containing database connection credentials and other settings.
- `requirements.txt`: Lists all Python dependencies.

## Setup

1. Clone this repository to your local machine:

```bash
git clone https://github.com/vafagh/ODBC-Migration.git
cd ODBC-Migration
```

2. Install the required Python dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with the following structure:

```bash
# .env file

# ODBC configuration
ODBC_DSN=your_dsn_name

# MySQL configuration
DB_HOST=your_mysql_host
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=your_mysql_database

# Logging configuration
LOG_FILE_PATH=path_to_your_log_file.log
LOG_LEVEL=INFO
```

4. Modify `table_mappings` in `main_script.py` to reflect your ODBC source tables and their corresponding MySQL destination tables. Example:

```python
table_mappings = [
    {"source": "BKARINV", "destination": "invoices"},
    {"source": "BKICMSTR", "destination": "products"},
    # Add more table mappings as needed
]
```

## Usage

Run the migration script using Python:

```bash
python main_script.py
```

The script will:
1. Connect to the ODBC source and MySQL database.
2. Fetch data from the specified ODBC tables.
3. Dynamically create MySQL tables (if they don’t exist) based on the ODBC schema.
4. Insert or update rows in MySQL, avoiding duplicates.
5. Add `created_at` and `updated_at` timestamps to each row.

### Logging

The tool will log the migration process, including any errors or batch processing information, to the log file specified in your `.env` file (`LOG_FILE_PATH`).

## Configuration

- **ODBC**: Make sure the DSN you are using in `.env` is configured correctly in your ODBC driver. You can verify ODBC connectivity using tools like Excel or an ODBC client before running the script.
- **MySQL**: Ensure your MySQL database is set up and accessible from the machine where the script is running.
- **Logging Level**: Adjust the log level in the `.env` file as needed (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).

## Contributing

Feel free to submit pull requests or report issues to improve the functionality of the migration tool.

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE Version 3 License. See the `LICENSE` file for details.
