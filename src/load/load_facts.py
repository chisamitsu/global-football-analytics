from pathlib import Path
from src.load.utils_db import get_connection, load_parquet_as_table

def load_facts():
    con = get_connection()

    clean_path = Path("data/clean")

    facts = {
        "fact_match": clean_path / "fact_match.parquet",
        "fact_team_season": clean_path / "fact_team_season.parquet",
        "fact_player_season": clean_path / "fact_player_season.parquet",
    }

    for table_name, parquet_path in facts.items():
        load_parquet_as_table(con, table_name, parquet_path)

    con.close()
    print("Facts loaded successfully.")