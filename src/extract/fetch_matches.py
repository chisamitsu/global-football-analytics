import json
import os
import yaml
from src.extract.api_client import APIClient

def load_league_config(path="config/leagues.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_matches(league_key="la_liga"):
    client = APIClient()

    # Load league ID from YAML
    leagues = load_league_config()
    league_id = leagues[league_key]["league_id"]

    all_matches = []

    for season in client.seasons:
        print(f"Fetching matches for {league_key} - season {season}...")

        params = {
            "league": league_id,
            "season": season
        }

        data = client.get("fixtures", params=params)

        # Handle API errors (None means error)
        if data is None:
            print(f"Skipping season {season} due to API restrictions.")
            continue

        # Save raw JSON
        raw_path = "data/raw/matches"
        os.makedirs(raw_path, exist_ok=True)

        file_path = f"{raw_path}/{league_key}_{season}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        all_matches.extend(data)

    return all_matches

if __name__ == "__main__":
    fetch_matches()