import sqlite3
import pandas as pd
import argparse

#loads a csv file into a SQLite db using pandas.
def load_csv_to_sqlite(csv_file: str, db_name: str, table_name: str):
    # Read CSV into a DataFrame
    df = pd.read_csv(csv_file)
    print(f"CSV '{csv_file}' loaded successfully. Columns: {list(df.columns)}")

    # Connect to (or create) the SQLite database
    conn = sqlite3.connect(db_name)

    # Write the DataFrame to a SQL table
    # if_exists='replace' will drop the table if it already exists
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Table '{table_name}' created/replaced in the database '{db_name}'.")
    
    # Return the connection so we can run queries later
    return conn

#runs basic sql query to retrieve data
def run_example_query(conn, table_name: str):
    cursor = conn.cursor()
    
    # For demonstration, let's just SELECT the first 5 rows
    query = f"SELECT * FROM {table_name} LIMIT 5;"
    cursor.execute(query)
    
    rows = cursor.fetchall()
    print(f"\nExample query: {query}")
    for row in rows:
        print(row)

def main():
    parser = argparse.ArgumentParser(description="Step 1: Load CSV into SQLite and run basic queries.")
    parser.add_argument("--csv_file", required=True, help="Path to the input CSV file.")
    parser.add_argument("--db_name", required=True, help="Name of the SQLite database file.")
    parser.add_argument("--table_name", default="my_table", help="Name of the table to be created in SQLite.")
    
    args = parser.parse_args()
    
    # 1. Load CSV into SQLite
    conn = load_csv_to_sqlite(args.csv_file, args.db_name, args.table_name)
    
    # 2. Run an example query
    run_example_query(conn, args.table_name)
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
