import duckdb
from pathlib import Path

def get_db_path():
    """Return the path to the DuckDB analytics database."""
    return Path("data") / "analytics.duckdb"

def get_connection():
    """Create (or open) the DuckDB database."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))

def load_parquet_as_table(con, table_name, parquet_path):
    """
    Load a Parquet file into DuckDB as a table.
    If the table exists, it will be replaced.
    """
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_parquet('{parquet_path}')
    """)
    print(f"Loaded table: {table_name}")