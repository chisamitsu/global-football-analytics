import argparse
from src.extract.fetch_league_data import fetch_league_data
from src.extract.fetch_matches import fetch_matches
from src.extract.fetch_teams import fetch_teams
from src.extract.fetch_players import fetch_players

def run_pipeline(league_key: str, season: int | None, force_update: bool, only: str | None):
    """
    Orchestrates the full Extract pipeline.

    Steps:
    1. Fetch league metadata
    2. Fetch matches
    3. Fetch teams
    4. Fetch players

    The `only` parameter allows running a specific extractor.
    """

    print("\n==============================")
    print("      GLOBAL EXTRACTION")
    print("==============================\n")

    # 1) LEAGUE DATA
    if only is None or only == "league":
        print("\n>>> Extracting LEAGUE data")
        fetch_league_data(
            league_key=league_key,
            season=season,
            force_update=force_update
        )

    # 2) MATCHES
    if only is None or only == "matches":
        print("\n>>> Extracting MATCHES")
        fetch_matches(
            league_key=league_key,
            season=season,
            force_update=force_update
        )

    # 3) TEAMS
    if only is None or only == "teams":
        print("\n>>> Extracting TEAMS")
        fetch_teams(
            league_key=league_key,
            season=season,
            force_update=force_update
        )

    # 4) PLAYERS
    if only is None or only == "players":
        print("\n>>> Extracting PLAYERS")
        players = fetch_players(
            league_key=league_key,
            season=season,
            force_update=force_update
        )

        # If daily limit was hit, players extractor returns early
        print(f"\n>>> Total players extracted so far: {len(players)}")

    print("\n==============================")
    print("      EXTRACTION COMPLETE")
    print("==============================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full extraction pipeline")

    parser.add_argument(
        "--league",
        default="la_liga",
        help="League key from leagues.yaml (default: la_liga)"
    )

    parser.add_argument(
        "--season",
        type=int,
        help="Specific season to fetch (optional)"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update even if files already exist"
    )

    parser.add_argument(
        "--only",
        choices=["league", "matches", "teams", "players"],
        help="Run only a specific extractor"
    )

    args = parser.parse_args()

    run_pipeline(
        league_key=args.league,
        season=args.season,
        force_update=args.force,
        only=args.only
    )