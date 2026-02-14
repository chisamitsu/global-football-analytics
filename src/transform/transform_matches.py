import json
import os
import pandas as pd
from src.transform.utils_filename import parse_generic_filename

RAW_PATH = "data/raw/matches"
CLEAN_PATH = "data/clean"


def transform_matches():
    """
    Build fact_match from raw match JSON files.

    Output: data/clean/fact_match.parquet

    Columns include:
      - fixture_id
      - league_id
      - season_year
      - date
      - status
      - referee
      - venue_id
      - home_team_id
      - away_team_id
      - goals_home
      - goals_away
      - halftime_home
      - halftime_away
      - fulltime_home
      - fulltime_away
      - extratime_home
      - extratime_away
      - penalty_home
      - penalty_away
    """

    rows = []

    for filename in os.listdir(RAW_PATH):
        if not filename.endswith(".json"):
            continue

        league_key, season_year = parse_generic_filename(filename)

        path = os.path.join(RAW_PATH, filename)

        with open(path, "r") as f:
            data = json.load(f)

        for item in data.get("response", []):
            fixture = item.get("fixture", {})
            league = item.get("league", {})
            teams = item.get("teams", {})
            goals = item.get("goals", {})
            score = item.get("score", {})

            rows.append({
                # IDs
                "fixture_id": fixture.get("id"),
                "league_id": league.get("id"),
                "season_year": season_year,
                "venue_id": fixture.get("venue", {}).get("id"),

                # Teams
                "home_team_id": teams.get("home", {}).get("id"),
                "away_team_id": teams.get("away", {}).get("id"),

                # Match metadata
                "date": fixture.get("date"),
                "status": fixture.get("status", {}).get("short"),
                "referee": fixture.get("referee"),
                "timezone": fixture.get("timezone"),

                # Goals
                "goals_home": goals.get("home"),
                "goals_away": goals.get("away"),

                # Score breakdown
                "halftime_home": score.get("halftime", {}).get("home"),
                "halftime_away": score.get("halftime", {}).get("away"),
                "fulltime_home": score.get("fulltime", {}).get("home"),
                "fulltime_away": score.get("fulltime", {}).get("away"),
                "extratime_home": score.get("extratime", {}).get("home"),
                "extratime_away": score.get("extratime", {}).get("away"),
                "penalty_home": score.get("penalty", {}).get("home"),
                "penalty_away": score.get("penalty", {}).get("away"),
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["fixture_id"])

    os.makedirs(CLEAN_PATH, exist_ok=True)
    output_path = os.path.join(CLEAN_PATH, "fact_match.parquet")

    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} matches to {output_path}")

    return df


if __name__ == "__main__":
    transform_matches()