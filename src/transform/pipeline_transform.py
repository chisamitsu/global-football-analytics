import argparse

from src.transform.transform_leagues import transform_leagues
from src.transform.transform_seasons import transform_seasons
from src.transform.transform_teams import transform_teams
from src.transform.transform_matches import transform_matches
from src.transform.transform_players import transform_players


def run_transform_pipeline(only: str | None = None):
    """
    Orchestrate all Transform steps (STAR schema).

    Dimensions:
      - dim_league
      - dim_season
      - dim_team
      - dim_venue
      - dim_player

    Facts:
      - fact_team_season
      - fact_match
      - fact_player_season
    """

    print("\n==============================")
    print("      TRANSFORM PIPELINE")
    print("==============================\n")

    # DIM LEAGUE
    if only is None or only == "leagues":
        print(">>> Transforming dim_league")
        transform_leagues()

    # DIM SEASON
    if only is None or only == "seasons":
        print(">>> Transforming dim_season")
        transform_seasons()

    # TEAMS / VENUES / FACT TEAM SEASON
    if only is None or only == "teams":
        print(">>> Transforming dim_team, dim_venue, fact_team_season")
        transform_teams()

    # FACT MATCH
    if only is None or only == "matches":
        print(">>> Transforming fact_match")
        transform_matches()

    # PLAYERS
    if only is None or only == "players":
        print(">>> Transforming dim_player, fact_player_season")
        transform_players()

    print("\n==============================")
    print("   TRANSFORM PIPELINE DONE")
    print("==============================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Transform (STAR schema) pipeline")

    parser.add_argument(
        "--only",
        choices=["leagues", "seasons", "teams", "matches", "players"],
        help="Run only a specific transform step",
    )

    args = parser.parse_args()

    run_transform_pipeline(only=args.only)