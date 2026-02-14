import json
import os
import pandas as pd

RAW_PATH = "data/raw/leagues"
CLEAN_PATH = "data/clean"

def transform_leagues():
    """
    Build dim_league from raw league JSON files.

    Output: data/clean/dim_league.parquet
    Columns:
      - league_id
      - league_name
      - league_type
      - league_logo
      - country_name
      - country_code
      - country_flag
    """

    rows = []

    for filename in os.listdir(RAW_PATH):
        if not filename.endswith(".json"):
            continue

        path = os.path.join(RAW_PATH, filename)

        with open(path, "r") as f:
            data = json.load(f)

        for item in data.get("response", []):
            league = item.get("league", {})
            country = item.get("country", {})

            rows.append({
                "league_id": league.get("id"),
                "league_name": league.get("name"),
                "league_type": league.get("type"),
                "league_logo": league.get("logo"),
                "country_name": country.get("name"),
                "country_code": country.get("code"),
                "country_flag": country.get("flag"),
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["league_id"])

    os.makedirs(CLEAN_PATH, exist_ok=True)
    output_path = os.path.join(CLEAN_PATH, "dim_league.parquet")

    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} leagues to {output_path}")

    return df


if __name__ == "__main__":
    transform_leagues()