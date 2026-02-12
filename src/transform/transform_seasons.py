import json
import os
import pandas as pd

RAW_PATH = "data/raw/leagues"
PROCESSED_PATH = "data/processed"


def transform_seasons():
    """
    Build dim_season from raw league JSON files.

    Output: data/processed/dim_season.parquet
    Columns:
      - league_id
      - season_year
      - start_date
      - end_date
      - is_current
      - coverage_fixtures_events
      - coverage_fixtures_lineups
      - coverage_fixtures_statistics
      - coverage_fixtures_players
      - coverage_standings
      - coverage_players
      - coverage_top_scorers
      - coverage_top_assists
      - coverage_top_cards
    """

    rows = []

    for filename in os.listdir(RAW_PATH):
        if not filename.endswith(".json"):
            continue

        path = os.path.join(RAW_PATH, filename)

        with open(path, "r") as f:
            data = json.load(f)

        for item in data.get("response", []):
            league = item.get("league", {})
            league_id = league.get("id")

            for season in item.get("seasons", []):
                year = season.get("year")
                start = season.get("start")
                end = season.get("end")
                current = season.get("current")

                coverage = season.get("coverage", {})
                fixtures_cov = coverage.get("fixtures", {})

                rows.append({
                    "league_id": league_id,
                    "season_year": year,
                    "start_date": start,
                    "end_date": end,
                    "is_current": current,
                    "coverage_fixtures_events": fixtures_cov.get("events"),
                    "coverage_fixtures_lineups": fixtures_cov.get("lineups"),
                    "coverage_fixtures_statistics": fixtures_cov.get("statistics"),
                    "coverage_fixtures_players": fixtures_cov.get("players"),
                    "coverage_standings": coverage.get("standings"),
                    "coverage_players": coverage.get("players"),
                    "coverage_top_scorers": coverage.get("top_scorers"),
                    "coverage_top_assists": coverage.get("top_assists"),
                    "coverage_top_cards": coverage.get("top_cards"),
                })

    df = pd.DataFrame(rows).drop_duplicates(subset=["league_id", "season_year"])

    os.makedirs(PROCESSED_PATH, exist_ok=True)
    output_path = os.path.join(PROCESSED_PATH, "dim_season.parquet")

    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} league-season rows to {output_path}")

    return df


if __name__ == "__main__":
    transform_seasons()