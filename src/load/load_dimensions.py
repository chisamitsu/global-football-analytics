from pathlib import Path
from src.load.utils_db import get_connection, load_parquet_as_table

def load_dimensions():
    con = get_connection()

    clean_path = Path("data/clean")

    dimensions = {
        "dim_team": clean_path / "dim_team.parquet",
        "dim_player": clean_path / "dim_player.parquet",
        "dim_venue": clean_path / "dim_venue.parquet",
        "dim_league": clean_path / "dim_league.parquet",
    }

    for table_name, parquet_path in dimensions.items():
        load_parquet_as_table(con, table_name, parquet_path)

    con.close()
    print("Dimensions loaded successfully.")