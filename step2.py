import sqlite3
import pandas as pd
import argparse

def infer_sqlite_type(dtype):
    """
    Map a pandas dtype to an appropriate SQLite type.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    else:
        # Default to TEXT for strings, objects, etc.
        return "TEXT"

def create_table_dynamically(csv_file: str, db_name: str, table_name: str):
    """
    Infers schema from a CSV file and creates a table dynamically in SQLite.
    """
    # Read CSV into a DataFrame
    df = pd.read_csv(csv_file)
    print(f"CSV '{csv_file}' loaded successfully. Columns: {list(df.columns)}")

    # Infer column types and build CREATE TABLE statement
    columns_with_types = []
    for col in df.columns:
        col_type = infer_sqlite_type(df[col].dtype)
        # Escape column names with quotes in case of special characters/spaces
        columns_with_types.append(f'"{col}" {col_type}')

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {', '.join(columns_with_types)}\n);"

    # Connect to (or create) the SQLite database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Execute the CREATE TABLE statement
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")  # start fresh
    cursor.execute(create_table_sql)
    print(f"Table '{table_name}' created with inferred schema in the database '{db_name}'.")

    # Load data into the new table using df.to_sql
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Data from '{csv_file}' loaded into '{table_name}' successfully.")

    # Return the connection for further operations
    return conn

def run_example_query(conn, table_name: str):
    """
    Runs a basic SQL query to demonstrate data retrieval.
    """
    cursor = conn.cursor()
    query = f"SELECT * FROM {table_name} LIMIT 5;"
    cursor.execute(query)
    rows = cursor.fetchall()
    print(f"\nExample query: {query}")
    for row in rows:
        print(row)

def main():
    parser = argparse.ArgumentParser(description="Step 2: Dynamically create tables in SQLite by inferring schema from CSV.")
    parser.add_argument("--csv_file", required=True, help="Path to the input CSV file.")
    parser.add_argument("--db_name", required=True, help="Name of the SQLite database file.")
    parser.add_argument("--table_name", default="my_inferred_table", help="Name of the table to create in SQLite.")
    args = parser.parse_args()

    # 1. Create table dynamically
    conn = create_table_dynamically(args.csv_file, args.db_name, args.table_name)

    # 2. Run a basic query for demonstration
    run_example_query(conn, args.table_name)

    conn.close()

if __name__ == "__main__":
    main()
