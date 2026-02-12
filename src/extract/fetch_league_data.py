import json
import os
import yaml
from src.extract.api_client import APIClient

def load_league_config(path: str = "config/leagues.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def fetch_league_data(league_key: str = "la_liga", season: int | None = None, force_update: bool = False):
    """
    Fetch league metadata for all configured seasons for a given league_key.

    - Uses API-Football v3 /leagues endpoint with league + season.
    - Saves one file per season under data/raw/leagues/.
    - If season=None → fetch all seasons from APIClient.seasons
    - If season=YYYY → fetch only that season
    - Skips seasons where the file already exists unless force_update=True.
    - Returns a list with all league responses (data["response"] merged).
    """
    client = APIClient()
    leagues_cfg = load_league_config()

    if league_key not in leagues_cfg:
        raise ValueError(f"League key '{league_key}' not found in leagues.yaml")

    league_id = leagues_cfg[league_key]["league_id"]

    raw_path = "data/raw/leagues"
    os.makedirs(raw_path, exist_ok=True)

    all_leagues = []

    # Determine which seasons to fetch
    seasons_to_fetch = [season] if season else client.seasons

    for season in seasons_to_fetch:
        print(f"\n=== Fetching league data for {league_key} - season {season} ===")

        file_path = os.path.join(raw_path, f"{league_key}_{season}.json")

        # Incremental: skip if already fetched and not forcing update
        if not force_update and os.path.exists(file_path):
            print(f"  Skipping season {season} — file already exists.")
            # Load existing file if you want it in the return object
            with open(file_path, "r") as f:
                existing_data = json.load(f)
            # existing_data is the full JSON, we want its "response"
            if "response" in existing_data:
                all_leagues.extend(existing_data["response"])
            continue

        params = {
            "id": league_id,
            "season": season,
        }

        data = client.get("leagues", params=params)

        # If APIClient returned None, skip this season
        if data is None:
            print(f"  API error for season {season}. Skipping.")
            continue

        # Defensive: ensure "response" exists
        response_items = data.get("response", [])
        if not response_items:
            print(f"  No league data returned for season {season}.")
        else:
            print(f"  Retrieved {len(response_items)} league record(s) for season {season}.")

        # Save full JSON (including paging, parameters, etc.)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        all_leagues.extend(response_items)

    return all_leagues


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch league metadata from API-Football")

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

    fetch_league_data(league_key=args.league_key, season=args.season, force_update=args.force)