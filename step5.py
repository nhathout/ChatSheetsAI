import sqlite3
import pandas as pd
import argparse
import textwrap
import os

from openai import OpenAI

client = None

def set_openai_api_key():
    """
    Retrieves OpenAI API key from environment variable or prompts user to enter it.
    Then instantiates the OpenAI client.
    """
    global client
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        print("OpenAI API key not found in environment. Please set OPENAI_API_KEY or hardcode it for testing.")
        # If you want quick tests without environment variables:
        # key = "sk-..."
    
    if key:
        client = OpenAI(api_key=key)
    else:
        client = None

def connect_to_db(db_name: str):
    """
    Connects to (or creates) a SQLite database.
    """
    conn = sqlite3.connect(db_name)
    print(f"Connected to database '{db_name}'.")
    return conn

def list_tables(conn):
    """
    Lists all tables in the database.
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

def get_db_schema(conn):
    """
    Gathers table info (names & columns) from the database.
    Returns a structured string used for LLM context.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    table_names = [row[0] for row in cursor.fetchall()]

    schema_info = []
    for table in table_names:
        cursor.execute(f"PRAGMA table_info('{table}')")
        columns = cursor.fetchall()
        # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
        col_defs = [f"{col[1]} {col[2]}" for col in columns]
        schema_info.append(f"- {table} ({', '.join(col_defs)})")

    if schema_info:
        return "\n".join(schema_info)
    else:
        return "No tables available."

def ask_llm_for_sql(schema_str, user_query):
    """
    Calls the Chat Completions endpoint to generate SQL based on user query + schema.
    """

    if not client:
        print("Error: OpenAI client not initialized. Ensure your API key is set correctly.")
        return None

    developer_content = textwrap.dedent(f"""
    You are an AI assistant tasked with converting user queries into SQL statements.
    The database uses SQLite. Here is the current schema:
    {schema_str}

    Requirements:
    1. Generate a SQL query that accurately answers the user's question or instruction.
    2. Ensure the SQL is valid SQLite syntax.
    3. Provide a short comment explaining what the query does.
    4. Do NOT wrap the SQL in triple backticks or code fences.

    Respond EXACTLY in this format (include the headings verbatim):
    SQL Query
    <Your SQL statement here>

    Explanation
    <Short explanation here>
    """)

    user_content = user_query  # The natural language request

    try:
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",  # or "gpt-4o" if you have GPT-4 API access
            messages=[
                {"role": "developer", "content": developer_content},
                {"role": "user", "content": user_content},
            ],
            temperature=0.0,
            max_tokens=300
        )
        ai_response = completion.choices[0].message.content.strip()
        return ai_response
    except Exception as e:
        print(f"Error from OpenAI API: {e}")
        return None

def parse_ai_response(ai_response):
    """
    Attempts to parse the AI response into:
      - sql_query: The generated SQL
      - explanation: The short comment or explanation

    If no "SQL Query" or "Explanation" heading is found,
    we treat the entire response as a single SQL command.
    """

    if not ai_response:
        return None, None

    # 1) Strip any lines with triple backticks
    lines_stripped = []
    for line in ai_response.splitlines():
        if "```" in line:
            continue
        lines_stripped.append(line)
    cleaned = "\n".join(lines_stripped)

    # 2) Attempt to parse
    lines = cleaned.splitlines()
    sql_query = []
    explanation = []
    mode = None  # can be "sql" or "exp"

    for line in lines:
        lower_line = line.lower()
        if "sql query" in lower_line:
            mode = "sql"
            continue
        elif "explanation" in lower_line:
            mode = "exp"
            continue

        if mode == "sql":
            sql_query.append(line)
        elif mode == "exp":
            explanation.append(line)

    sql_query_str = "\n".join(sql_query).strip()
    explanation_str = "\n".join(explanation).strip()

    # 3) Fallback if we didn't find the headings at all
    if not sql_query_str and not explanation_str:
        # We'll treat the entire AI response as a single SQL query
        sql_query_str = cleaned
        explanation_str = ""

    return sql_query_str, explanation_str

def execute_sql_and_print(conn, sql_query):
    """
    Executes the given SQL query and prints up to 10 rows from the result.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        print(f"\nQuery Results (up to 10 rows):")
        for i, row in enumerate(rows[:10], start=1):
            print(f"{i}. {row}")
    except Exception as e:
        print(f"Error executing SQL: {e}")

def main():
    parser = argparse.ArgumentParser(description="Step 5 with new OpenAI Python library (Chat Completions).")
    parser.add_argument("--db_name", required=True, help="Name of the SQLite database file.")
    args = parser.parse_args()

    # 1) Configure OpenAI
    set_openai_api_key()

    # 2) Connect to DB
    conn = connect_to_db(args.db_name)

    print("\nWelcome to the ChatSheetAI CLI (latest OpenAI library)!")
    print("Commands:")
    print("  ask <natural language>  - Let AI generate SQL and execute it.")
    print("  query <SQL statement>   - Execute a direct SQL statement yourself.")
    print("  list tables             - List all tables in this DB.")
    print("  exit                    - Exit.\n")

    while True:
        user_input = input("> ").strip()
        if not user_input:
            continue
        
        parts = user_input.split(" ", 1)
        command = parts[0].lower()

        if command == "exit":
            print("Exiting CLI.")
            break

        elif command == "list":
            # e.g. "list tables"
            if len(parts) > 1 and parts[1].lower() == "tables":
                list_tables(conn)
            else:
                print("Usage: list tables")

        elif command == "ask":
            # Let LLM generate SQL
            if len(parts) < 2:
                print("Usage: ask <natural language prompt>")
                continue
            user_query = parts[1]

            # Gather schema (may be "No tables available.")
            schema_str = get_db_schema(conn)

            # Call the LLM
            ai_response = ask_llm_for_sql(schema_str, user_query)
            if not ai_response:
                print("No valid response from the LLM.")
                continue

            # Parse
            sql_query, explanation = parse_ai_response(ai_response)
            if not sql_query:
                print("Could not parse SQL from AI response:")
                print(ai_response)
                continue

            print("\n--- AI-Generated SQL ---")
            print(sql_query)
            print("\n--- Explanation ---")
            if explanation:
                print(explanation)
            else:
                print("No explanation provided.")

            # Execute
            execute_sql_and_print(conn, sql_query)

        elif command == "query":
            # Direct user-driven SQL
            if len(parts) < 2:
                print("Usage: query <SQL statement>")
                continue
            sql = parts[1]
            execute_sql_and_print(conn, sql)

        else:
            print(f"Unknown command: {command}")
            print("Type 'ask <prompt>' or 'query <SQL>' or 'list tables' or 'exit'.")

    conn.close()

if __name__ == "__main__":
    main()
