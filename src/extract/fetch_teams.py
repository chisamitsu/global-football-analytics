import json
import os
import yaml
from src.extract.api_client import APIClient

def load_league_config(path: str = "config/leagues.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_teams(league_key: str = "la_liga", season: int | None = None, force_update: bool = False):
    """
    Fetch teams for a given league.

    - If season=None → fetch all seasons from APIClient.seasons
    - If season=YYYY → fetch only that season
    - Skips existing files unless force_update=True
    - Saves full JSON (not only response)
    - Returns a merged list of all teams (data["response"])
    """

    client = APIClient()
    leagues_cfg = load_league_config()

    if league_key not in leagues_cfg:
        raise ValueError(f"League key '{league_key}' not found in leagues.yaml")

    league_id = leagues_cfg[league_key]["league_id"]

    raw_path = "data/raw/teams"
    os.makedirs(raw_path, exist_ok=True)

    all_teams = []

    # Determine which seasons to fetch
    seasons_to_fetch = [season] if season else client.seasons

    for s in seasons_to_fetch:
        print(f"\n=== Fetching teams for {league_key} - season {s} ===")

        file_path = os.path.join(raw_path, f"{league_key}_{s}.json")

        # Incremental extraction
        if not force_update and os.path.exists(file_path):
            print(f"  Skipping season {s} — file already exists.")
            with open(file_path, "r") as f:
                existing_data = json.load(f)
            if "response" in existing_data:
                all_teams.extend(existing_data["response"])
            continue

        params = {
            "league": league_id,
            "season": s,
        }

        data = client.get("teams", params=params)

        if data is None:
            print(f"  API error for season {s}. Skipping.")
            continue

        response_items = data.get("response", [])
        print(f"  Retrieved {len(response_items)} teams for season {s}.")

        # Save full JSON
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        all_teams.extend(response_items)

    return all_teams


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch team data from API-Football")

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
        help="Force update even if files already exist"
    )

    args = parser.parse_args()

    fetch_teams(league_key=args.league_key, season=args.season, force_update=args.force)