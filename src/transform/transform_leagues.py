import json
import os
import pandas as pd
import yaml

RAW_PATH = "data/raw/leagues"
CLEAN_PATH = "data/clean"
YAML_PATH = "config/leagues.yaml"

REGION_PLACEHOLDERS = {
    "Europe": "https://upload.wikimedia.org/wikipedia/commons/b/b5/UEFA_logo.svg",
    "South America": "https://upload.wikimedia.org/wikipedia/commons/9/9c/Official_Image_of_CONMEBOL.svg",
    "North America": "https://upload.wikimedia.org/wikipedia/commons/8/87/Concacaf_logo.svg",
    "Asia": "https://upload.wikimedia.org/wikipedia/en/5/5c/AFC_logo.svg",
    "Africa": "https://upload.wikimedia.org/wikipedia/en/0/03/CAF_logo.svg",
    "Global": "https://upload.wikimedia.org/wikipedia/en/3/36/FIFA_logo.svg",
}

def load_yaml_metadata():
    with open(YAML_PATH, "r") as f:
        data = yaml.safe_load(f)

    meta = {}
    for key, league in data.items():  # sem "leagues:"
        league_id = league["league_id"]
        meta[league_id] = {
            "scope": league.get("scope"),
            "region": league.get("region"),
        }
    return meta


def enrich_league(row, yaml_meta):
    meta = yaml_meta.get(row["league_id"], {})

    row["scope"] = meta.get("scope")
    row["region"] = meta.get("region")

    # 1. Se a API já trouxe country_flag → manter
    if row.get("country_flag"):
        return row

    # 2. Se domestic e sem flag → usar placeholder da região
    if row["scope"] == "domestic":
        row["country_flag"] = REGION_PLACEHOLDERS.get(row["region"])
        return row

    # 3. Se continental/global → sempre placeholder
    row["country_flag"] = REGION_PLACEHOLDERS.get(row["region"])
    return row


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
    yaml_meta = load_yaml_metadata()
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

    df = df.apply(lambda row: enrich_league(row, yaml_meta), axis=1)

    os.makedirs(CLEAN_PATH, exist_ok=True)
    output_path = os.path.join(CLEAN_PATH, "dim_league.parquet")

    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} leagues to {output_path}")

    return df


if __name__ == "__main__":
    transform_leagues()