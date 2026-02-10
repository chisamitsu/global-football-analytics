import json
import os
import yaml
from src.extract.api_client import APIClient

def load_league_config(path="config/leagues.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_league_data(league_key="la_liga"):
    client = APIClient()

    # Load league ID from YAML
    leagues = load_league_config()
    league_id = leagues[league_key]["league_id"]

    all_data = []

    # Loop through seasons
    for season in client.seasons:
        print(f"Fetching {league_key} for season {season}...")

        params = {"id": league_id, "season": season}
        data = client.get("leagues", params=params)

        if data is None:
            print(f"Skipping season {season} due to API restrictions.")
            continue

        # Save raw JSON
        raw_path = "data/raw/leagues"
        os.makedirs(raw_path, exist_ok=True)

        file_path = f"{raw_path}/{league_key}_{season}.json"
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        all_data.extend(data)

    return all_data

if __name__ == "__main__":
    fetch_league_data()