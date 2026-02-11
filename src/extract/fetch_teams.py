import json
import os
import yaml
from src.extract.api_client import APIClient

def load_league_config(path="config/leagues.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_teams(league_key="la_liga", season=None):
    client = APIClient()

    leagues = load_league_config()
    league_id = leagues[league_key]["league_id"]

    all_teams = []

    # If season is provided → use only that season
    seasons_to_fetch = [season] if season else client.seasons

    for s in seasons_to_fetch:
        print(f"Fetching teams for {league_key} - season {s}...")

        params = {
            "league": league_id,
            "season": s
        }

        data = client.get("teams", params=params)

        if data is None:
            print(f"Skipping season {s} due to API restrictions.")
            continue

        # Save raw JSON
        raw_path = "data/raw/teams"
        os.makedirs(raw_path, exist_ok=True)

        file_path = f"{raw_path}/{league_key}_{s}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        all_teams.extend(data)

    return all_teams

if __name__ == "__main__":
    fetch_teams()