import json
import os
import pandas as pd
from src.transform.utils_filename import parse_generic_filename

RAW_PATH = "data/raw/teams"
CLEAN_PATH = "data/clean"

def transform_teams():
    """
    Build:
      - dim_team
      - dim_venue
      - fact_team_season

    from raw team JSON files.
    """

    dim_team_rows = []
    dim_venue_rows = []
    fact_team_season_rows = []

    for filename in os.listdir(RAW_PATH):
        if not filename.endswith(".json"):
            continue

        league_key, season_year = parse_generic_filename(filename)

        path = os.path.join(RAW_PATH, filename)

        with open(path, "r") as f:
            data = json.load(f)

        for item in data.get("response", []):
            team = item.get("team", {})
            venue = item.get("venue", {})

            team_id = team.get("id")
            venue_id = venue.get("id")

            # -------------------------
            # DIM TEAM
            # -------------------------
            dim_team_rows.append({
                "team_id": team_id,
                "team_name": team.get("name"),
                "team_country": team.get("country"),
                "team_founded": team.get("founded"),
                "team_logo": team.get("logo"),
                "venue_id": venue_id,
            })

            # -------------------------
            # DIM VENUE
            # -------------------------
            dim_venue_rows.append({
                "venue_id": venue_id,
                "venue_name": venue.get("name"),
                "venue_city": venue.get("city"),
                "venue_capacity": venue.get("capacity"),
                "venue_surface": venue.get("surface"),
                "venue_address": venue.get("address"),
                "venue_image": venue.get("image"),
            })

            # -------------------------
            # FACT TEAM SEASON
            # -------------------------
            fact_team_season_rows.append({
                "team_id": team_id,
                "league_key": league_key,
                "season_year": season_year,
            })

    # Convert to DataFrames
    dim_team = pd.DataFrame(dim_team_rows).drop_duplicates(subset=["team_id"])
    dim_venue = pd.DataFrame(dim_venue_rows).drop_duplicates(subset=["venue_id"])
    fact_team_season = pd.DataFrame(fact_team_season_rows).drop_duplicates()

    # Save outputs
    os.makedirs(CLEAN_PATH, exist_ok=True)

    dim_team.to_parquet(os.path.join(CLEAN_PATH, "dim_team.parquet"), index=False)
    dim_venue.to_parquet(os.path.join(CLEAN_PATH, "dim_venue.parquet"), index=False)
    fact_team_season.to_parquet(os.path.join(CLEAN_PATH, "fact_team_season.parquet"), index=False)

    print(f"Saved {len(dim_team)} teams, {len(dim_venue)} venues, {len(fact_team_season)} team-season rows.")

    return dim_team, dim_venue, fact_team_season


if __name__ == "__main__":
    transform_teams()