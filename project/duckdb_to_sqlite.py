import duckdb
import sqlite3
import os

# Define SQLite database name
sqlite_db_path = "/Users/luigilizzini/Downloads/project/exported_database.db"

# Connect to DuckDB
duckdb_conn = duckdb.connect("/Users/luigilizzini/Downloads/project/my_database.db")

# Connect to SQLite
sqlite_conn = sqlite3.connect(sqlite_db_path)
sqlite_cursor = sqlite_conn.cursor()

# Get all tables from DuckDB
tables = duckdb_conn.execute("SHOW TABLES").fetchall()

if not tables:
    print("No tables found in DuckDB!")
else:
    print(f"Found tables: {tables}")

for table in tables:
    table_name = table[0]
    print(f"Exporting table: {table_name}")

    # Fetch data from DuckDB
    df = duckdb_conn.execute(f"SELECT * FROM {table_name}").fetchdf()

    if df.empty:
        print(f"Skipping empty table: {table_name}")
        continue

    # Export to SQLite
    df.to_sql(table_name, sqlite_conn, if_exists="replace", index=False)

# Commit and close connections
sqlite_conn.commit()
sqlite_conn.close()
duckdb_conn.close()

# Verify if the database file exists
if os.path.exists(sqlite_db_path):
    print(f"Export successful! SQLite database saved at: {os.path.abspath(sqlite_db_path)}")
else:
    print("Error: SQLite database file was not created!")

