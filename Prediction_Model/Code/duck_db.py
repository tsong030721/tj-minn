# Safe helpers for using duckdb
import duckdb as ddb

# Use DuckDB to connect to database
def connect(file):
    conn = ddb.connect(file)
    return conn

# Kill connection
def kill(conn):
    conn.close()

# Execute a command
def execute(conn, cmd):
    try:
        return conn.execute(cmd)
    except:
        print("Invalid command or connection.")
        return None