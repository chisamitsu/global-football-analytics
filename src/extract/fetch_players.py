import json
import os
import time
import yaml
from src.extract.api_client import APIClient
from src.extract.fetch_teams import fetch_teams

def load_league_config(path="config/leagues.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_players(league_key="la_liga"):
    client = APIClient()

    leagues = load_league_config()
    league_id = leagues[league_key]["league_id"]

    all_players = []

    for season in client.seasons:
        print(f"\n=== Fetching players for {league_key} - season {season} ===")

        teams = fetch_teams(league_key, season=season)
        if not teams:
            print(f"No teams found for season {season}. Skipping.")
            continue

        for team in teams:
            team_id = team["team"]["id"]
            team_name = team["team"]["name"]

            print(f"\n  Team: {team_name} ({team_id})")

            page = 1
            total_pages = None

            while True:
                print(f"    → Page {page}")

                raw_path = "data/raw/players"
                os.makedirs(raw_path, exist_ok=True)

                file_path = f"{raw_path}/{league_key}_{season}_team_{team_id}_page_{page}.json"

                # Skip if already downloaded
                if os.path.exists(file_path):
                    print(f"      Skipping page {page} — already downloaded.")
                    page += 1
                    continue

                params = {
                    "league": league_id,
                    "team": team_id,
                    "season": season,
                    "page": page
                }

                data = client.get("players", params=params)

                if data is None:
                    print("      API error. Skipping this page.")
                    break

                # Empty response → no more pages
                if len(data) == 0:
                    print("      No more players for this team.")
                    break

                # Save page
                with open(file_path, "w") as f:
                    json.dump(data, f, indent=2)

                all_players.extend(data)

                # Determine total pages from API
                if total_pages is None:
                    total_pages = data[0]["paging"]["total"]

                # Stop if we've reached the last page
                if page >= total_pages:
                    print(f"      Finished all {total_pages} pages.")
                    break

                # Respect rate limit
                time.sleep(6)

                page += 1

    return all_players

if __name__ == "__main__":
    fetch_players()