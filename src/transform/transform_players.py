import json
import os
import pandas as pd

RAW_PATH = "data/raw/players"
PROCESSED_PATH = "data/processed"


def transform_players():
    """
    Build:
      - dim_player
      - fact_player_season

    from raw player JSON files.
    """

    dim_player_rows = []
    fact_player_season_rows = []

    for filename in os.listdir(RAW_PATH):
        if not filename.endswith(".json"):
            continue

        # Example filename: la_liga_2023_team_529_page_1.json
        parts = filename.replace(".json", "").split("_")

        league_key = parts[0] + "_" + parts[1] if len(parts) > 3 else parts[0]
        season_year = int(parts[2])
        team_id = int(parts[4])
        # page = parts[-1]  # not needed for transforms

        path = os.path.join(RAW_PATH, filename)

        with open(path, "r") as f:
            data = json.load(f)

        for item in data.get("response", []):
            player = item.get("player", {})
            stats_list = item.get("statistics", [])

            # DIM PLAYER
            dim_player_rows.append({
                "player_id": player.get("id"),
                "player_name": player.get("name"),
                "firstname": player.get("firstname"),
                "lastname": player.get("lastname"),
                "nationality": player.get("nationality"),
                "birth_date": player.get("birth", {}).get("date"),
                "birth_place": player.get("birth", {}).get("place"),
                "birth_country": player.get("birth", {}).get("country"),
                "height": player.get("height"),
                "weight": player.get("weight"),
                "photo": player.get("photo"),
            })

            # FACT PLAYER SEASON
            for stats in stats_list:
                league = stats.get("league", {})
                games = stats.get("games", {})
                goals = stats.get("goals", {})
                cards = stats.get("cards", {})

                fact_player_season_rows.append({
                    "player_id": player.get("id"),
                    "team_id": team_id,
                    "league_id": league.get("id"),
                    "season_year": season_year,
                    "position": games.get("position"),
                    "appearances": games.get("appearences"),
                    "minutes": games.get("minutes"),
                    "rating": games.get("rating"),
                    "goals": goals.get("total"),
                    "assists": goals.get("assists"),
                    "yellow_cards": cards.get("yellow"),
                    "red_cards": cards.get("red"),
                })

    # Convert to DataFrames
    dim_player = pd.DataFrame(dim_player_rows).drop_duplicates(subset=["player_id"])
    fact_player_season = pd.DataFrame(fact_player_season_rows).drop_duplicates()

    # Save outputs
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    dim_player.to_parquet(os.path.join(PROCESSED_PATH, "dim_player.parquet"), index=False)
    fact_player_season.to_parquet(os.path.join(PROCESSED_PATH, "fact_player_season.parquet"), index=False)

    print(f"Saved {len(dim_player)} players and {len(fact_player_season)} player-season rows.")

    return dim_player, fact_player_season


if __name__ == "__main__":
    transform_players()