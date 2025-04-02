import sqlite3
import pandas as pd
import argparse
import os
import sys

def connect_to_db(db_name: str):
    """
    Connects to or creates a SQLite database.
    """
    conn = sqlite3.connect(db_name)
    print(f"Connected to database '{db_name}'.")
    return conn

def load_csv_into_table(conn, csv_file: str, table_name: str):
    """
    Loads a CSV into a specified table. By default, it infers a schema
    dynamically and appends the data. If the table doesn't exist, it will be created.
    """
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error: Could not read CSV file '{csv_file}' - {e}")
        return

    # Infer column types
    def infer_sqlite_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "REAL"
        else:
            return "TEXT"

    # Build CREATE TABLE statement if table doesn't exist
    cursor = conn.cursor()
    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    existing_table = cursor.fetchone()

    if not existing_table:
        # Table doesn't exist, create it
        columns_with_types = []
        for col in df.columns:
            col_type = infer_sqlite_type(df[col].dtype)
            columns_with_types.append(f'"{col}" {col_type}')

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {', '.join(columns_with_types)}\n);"
        try:
            cursor.execute(create_table_sql)
            conn.commit()
            print(f"Created table '{table_name}'.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {str(e)}")
            return
    else:
        # If table exists, we are appending
        print(f"Table '{table_name}' already exists, appending data...")

    # Insert data
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Loaded CSV '{csv_file}' into table '{table_name}'.")
    except Exception as e:
        print(f"Error inserting data into '{table_name}': {str(e)}")

def list_tables(conn):
    """
    Lists all tables in the connected SQLite database by querying sqlite_master.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = cursor.fetchall()
    if not tables:
        print("No tables found in the database.")
    else:
        print("Tables in this database:")
        for t in tables:
            print(f" - {t[0]}")

def run_sql_query(conn, query_str: str):
    """
    Runs the given SQL query and prints the results.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query_str)
        rows = cursor.fetchall()
        # Print up to some limit to avoid flooding console (or just print all).
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Error executing query: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Step 4: Simple interactive CLI to load CSVs, run queries, and list tables.")
    parser.add_argument("--db_name", required=True, help="Name of the SQLite database file.")
    args = parser.parse_args()

    # Connect or create the DB
    conn = connect_to_db(args.db_name)

    print("\nWelcome to the ChatSheetsAI CLI")
    print("Type 'help' for a list of commands.\n")

    while True:
        user_input = input("> ").strip()
        if not user_input:
            continue

        cmd_parts = user_input.split(" ", 1)
        command = cmd_parts[0].lower()

        if command == "help":
            print("Commands:")
            print("  load <csv_file> <table_name>  - Load a CSV into the specified table.")
            print("  list tables                   - List all tables in the current database.")
            print("  query <SQL statement>         - Run an SQL query (e.g., SELECT * FROM my_table).")
            print("  exit                          - Exit this application.")
        
        elif command == "exit":
            print("Exiting ChatSheetAI CLI.")
            break
        
        elif command == "list":
            # Possibly 'list tables' if they typed 'list' only
            if len(cmd_parts) > 1 and cmd_parts[1].lower() == "tables":
                list_tables(conn)
            else:
                print("Usage: list tables")
        
        elif command == "load":
            # e.g., load example_data.csv my_table
            if len(cmd_parts) < 2:
                print("Usage: load <csv_file> <table_name>")
                continue
            load_args = cmd_parts[1].split()
            if len(load_args) < 2:
                print("Usage: load <csv_file> <table_name>")
                continue
            csv_file, table_name = load_args[0], load_args[1]
            load_csv_into_table(conn, csv_file, table_name)

        elif command == "query":
            if len(cmd_parts) < 2:
                print("Usage: query <SQL statement>")
                continue
            sql_stmt = cmd_parts[1]
            run_sql_query(conn, sql_stmt)
        
        else:
            print(f"Unknown command: {command}")
            print("Type 'help' to see available commands.")

    conn.close()

if __name__ == "__main__":
    main()
