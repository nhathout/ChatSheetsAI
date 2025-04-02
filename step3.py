import sqlite3
import pandas as pd
import argparse
import sys
import os

ERROR_LOG_FILE = "error_log.txt"

def log_error(message: str):
    """
    Logs an error or warning message to error_log.txt.
    """
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(message + "\n")

def get_existing_table_schema(conn, table_name: str):
    """
    Retrieves the schema (column names & types) of an existing table
    using PRAGMA table_info. Returns a dict: {column_name: column_type}
    If the table does not exist, returns an empty dict.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    result = cursor.fetchone()
    
    if not result:
        return {}
    
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    rows = cursor.fetchall()
    # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
    schema = {}
    for row in rows:
        col_name = row[1]
        col_type = row[2]
        schema[col_name] = col_type
    return schema

def create_table_with_prompt(csv_file: str, db_name: str, table_name: str):
    """
    Reads CSV and attempts to create/import into the specified table.
    If table already exists, uses PRAGMA table_info() to compare schemas
    and prompts user to overwrite, rename, or skip.
    """
    # 1) Read CSV into DataFrame
    try:
        df = pd.read_csv(csv_file)
        print(f"CSV '{csv_file}' loaded successfully. Columns: {list(df.columns)}")
    except Exception as e:
        error_msg = f"Error reading CSV file '{csv_file}': {str(e)}"
        print(error_msg)
        log_error(error_msg)
        sys.exit(1)

    # 2) Connect to (or create) SQLite DB
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 3) Check if the table already exists and gather its schema
    existing_schema = get_existing_table_schema(conn, table_name)
    if existing_schema:
        # Compare columns in CSV vs existing table
        df_columns = list(df.columns)
        existing_columns = list(existing_schema.keys())
        
        # Only do something if there’s a difference. (Otherwise, we can just append.)
        if set(df_columns) != set(existing_columns):
            print(f"Table '{table_name}' already exists with a different schema.")
            print(f"Existing columns: {existing_columns}")
            print(f"CSV columns: {df_columns}")
            
            print("\nChoose an option:")
            print("(O)verwrite existing table (will drop and recreate)")
            print("(R)ename new table before import")
            print("(S)kip importing this CSV")
            
            choice = input("Enter O/R/S: ").strip().upper()

            if choice == "O":
                # Overwrite
                print(f"Overwriting table '{table_name}'...")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                conn.commit()
            elif choice == "R":
                # Prompt for new name
                new_table_name = input("Enter a new table name: ").strip()
                if new_table_name:
                    table_name = new_table_name
                    print(f"Proceeding with new table name '{table_name}'.")
                else:
                    print("Invalid new table name. Skipping import.")
                    log_error("User provided invalid table name for rename option.")
                    return
            else:
                # Skip or any other input
                skip_msg = f"User chose to skip importing '{csv_file}' into '{table_name}'."
                print(skip_msg)
                log_error(skip_msg)
                return
    
    # 4) Infer column types from DataFrame
    def infer_sqlite_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "REAL"
        else:
            # Default to TEXT
            return "TEXT"

    columns_with_types = []
    for col in df.columns:
        col_type = infer_sqlite_type(df[col].dtype)
        columns_with_types.append(f'"{col}" {col_type}')

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {', '.join(columns_with_types)}\n);"
    
    # 5) Create table if it doesn't already exist
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{table_name}' is ready.")
    except Exception as e:
        error_msg = f"Error creating table '{table_name}': {str(e)}"
        print(error_msg)
        log_error(error_msg)
        return

    # 6) Insert data into the table (append if table already had a matching schema)
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data from '{csv_file}' loaded into table '{table_name}' successfully.")
    except Exception as e:
        error_msg = f"Error inserting data into '{table_name}': {str(e)}"
        print(error_msg)
        log_error(error_msg)

    return conn

def run_example_query(conn, table_name: str):
    """
    Runs a basic SQL query to show the first 5 rows.
    """
    cursor = conn.cursor()
    query = f"SELECT * FROM {table_name} LIMIT 5;"
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"\nExample query: {query}")
        for row in rows:
            print(row)
    except Exception as e:
        error_msg = f"Error running query on table '{table_name}': {str(e)}"
        print(error_msg)
        log_error(error_msg)

def main():
    parser = argparse.ArgumentParser(description="Step 3: Validate schemas and handle conflicts with user prompts.")
    parser.add_argument("--csv_file", required=True, help="Path to the input CSV file.")
    parser.add_argument("--db_name", required=True, help="Name of the SQLite database file.")
    parser.add_argument("--table_name", default="my_table", help="Name of the table in SQLite.")
    args = parser.parse_args()

    # Create or update table with prompts on conflicts
    conn = create_table_with_prompt(args.csv_file, args.db_name, args.table_name)
    
    if conn:
        # Run an example query
        run_example_query(conn, args.table_name)
        conn.close()

if __name__ == "__main__":
    main()
