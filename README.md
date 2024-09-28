
# ODBC to MySQL Migration Tool

## Overview

This tool allows you to migrate data from an ODBC-connected source to a MySQL database. It is designed to handle multiple tables and columns dynamically, with the flexibility to specify table mappings, exceptions, and column configurations via a `table_mappings.json` file.

## Features

- **Dynamic Table Creation**: Tables in MySQL are created based on the metadata from the ODBC source, including column names, types, and lengths.
- **JSON Configuration**: The tool uses a `table_mappings.json` file to configure which tables and columns to migrate. This keeps the logic separate from the configuration.
- **Exception Handling**: You can specify exceptions for certain columns (e.g., time formatting) in the JSON configuration.
- **Chunked Data Migration**: Data is fetched in chunks to handle large datasets without consuming too much memory.
- **Upsert Logic**: If a row already exists in the MySQL table (based on a unique key), it will be updated instead of inserted, avoiding duplicates.

## Prerequisites

- Python 3.x
- ODBC source (e.g., SQL Server, Actian Zen, etc.)
- MySQL database

### Python Libraries

You need to install the following Python libraries, which are listed in the `requirements.txt` file:
- `pyodbc`
- `mysql-connector-python`
- `python-dotenv`

## Setup

1. Clone this repository to your local machine:

```bash
git clone https://github.com/vafagh/ODBC-Migration.git
cd ODBC-Migration
```

2. Install the required Python dependencies:

Install them using pip:
```bash
pip install -r requirements.txt
```

## Configuration

### `.env` File

The `.env` file contains the configuration details for the ODBC and MySQL connections. Example:

```bash
ODBC_DSN=your_odbc_dsn
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=acsys
LOG_FILE_PATH=script_log.log
LOG_LEVEL=INFO
```

### `table_mappings.json`

The `table_mappings.json` file defines which tables to migrate. You can also specify exceptions for alter columns data type. Example:

```json
{
  "table_mappings": [
    {
      "source": "INVOICES",
      "destination": "INVOICES",
      "exceptions": {
        "INVOICES_TIME": "TIME"
      }
    },
    {
      "source": "INVOICESL",
      "destination": "INVOICESL",
      "exceptions": {}
    }
  ]
}
```

- **`source`**: The name of the table in the ODBC source.
- **`destination`**: The name of the table in the MySQL database.
- **`exceptions`**: Columns that require special handling, such as time formatting.

## Running the Tool

1. Ensure your `.env` and `table_mappings.json` files are correctly configured.
2. Run the migration script:
   ```bash
   python main.py
   ```

## Logging

The tool logs both to the terminal and to a log file. The log file path and log level can be set in the `.env` file.

Example:
```bash
LOG_FILE_PATH=script_log.log
LOG_LEVEL=INFO
```

## License

This project is licensed under the GNU GENERAL PUBLIC LICENSE Version 3.
