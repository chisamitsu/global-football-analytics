import json
import os
import time
import yaml
from src.extract.api_client import APIClient
from src.extract.fetch_teams import fetch_teams

def load_league_config(path: str = "config/leagues.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_settings(path: str = "config/settings.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_players(league_key: str = "la_liga", season: int | None = None, force_update: bool = False):
    """
    Fetch player statistics for a given league.

    - If season=None → fetch all seasons from APIClient.seasons
    - If season=YYYY → fetch only that season
    - Teams are always fetched incrementally (force_update applies only to players)
    - Stops immediately if daily request limit is reached
    - Saves full JSON per page
    - Returns merged list of all players (data["response"])
    """

    client = APIClient()
    leagues_cfg = load_league_config()
    settings = load_settings()

    delay_seconds = settings.get("rate_limit", {}).get("delay_seconds", 6)

    if league_key not in leagues_cfg:
        raise ValueError(f"League key '{league_key}' not found in leagues.yaml")

    league_id = leagues_cfg[league_key]["league_id"]

    raw_path = "data/raw/players"
    os.makedirs(raw_path, exist_ok=True)

    all_players = []

    # Determine which seasons to fetch
    seasons_to_fetch = [season] if season else client.seasons

    for s in seasons_to_fetch:
        print(f"\n=== Fetching players for {league_key} - season {s} ===")

        # Fetch teams WITHOUT forcing update
        teams = fetch_teams(league_key=league_key, season=s, force_update=False)

        if not teams:
            print(f"  No teams found for season {s}. Skipping.")
            continue

        for team in teams:
            team_id = team["team"]["id"]
            team_name = team["team"]["name"]

            print(f"\n  Team: {team_name} ({team_id})")

            page = 1
            total_pages = None

            while True:
                file_path = os.path.join(
                    raw_path,
                    f"{league_key}_{s}_team_{team_id}_page_{page}.json"
                )

                # Incremental extraction for players
                if not force_update and os.path.exists(file_path):
                    print(f"    Skipping page {page} — already exists.")

                    with open(file_path, "r") as f:
                        existing_data = json.load(f)

                    # Merge existing players
                    if "response" in existing_data:
                        all_players.extend(existing_data["response"])

                    # Read pagination info from existing file
                    paging = existing_data.get("paging", {})
                    current = paging.get("current", page)
                    total = paging.get("total", page)

                    # If this was the last page → stop
                    if current >= total:
                        print(f"    All {total} pages already fetched for team {team_id}.")
                        break

                    # Otherwise continue to next page
                    page += 1
                    continue

                print(f"    Fetching page {page}...")

                params = {
                    "league": league_id,
                    "team": team_id,
                    "season": s,
                    "page": page
                }

                data = client.get("players", params=params)

                if data is None:
                    print("    API error. Skipping this page.")
                    break

                # Detect daily limit
                errors = data.get("errors", {})
                if errors:
                    print(f"    Daily limit reached: {errors}")
                    print("    Stopping extraction early.")
                    return all_players

                response_items = data.get("response", [])

                # No more players
                if not response_items:
                    print("    No more players for this team.")
                    break

                # Save full JSON
                with open(file_path, "w") as f:
                    json.dump(data, f, indent=2)

                all_players.extend(response_items)

                # Determine total pages
                if total_pages is None:
                    total_pages = data.get("paging", {}).get("total", 1)

                if page >= total_pages:
                    print(f"    Completed all {total_pages} pages.")
                    break

                # Respect rate limit (configurable)
                time.sleep(delay_seconds)
                page += 1

    return all_players


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch player data from API-Football")

    parser.add_argument(
        "league_key",
        nargs="?",
        default="la_liga",
        help="League key from leagues.yaml (default: la_liga)"
    )

    parser.add_argument(
        "season",
        nargs="?",
        type=int,
        help="Specific season to fetch (optional)"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update even if files already exist (players only)"
    )

    args = parser.parse_args()

    fetch_players(league_key=args.league_key, season=args.season, force_update=args.force)