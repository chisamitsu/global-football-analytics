
def parse_generic_filename(filename: str):
    """
    Parse filenames like:
        {league_key}_{season}.json

    Works for league keys with any number of underscores.
    """

    name = filename.replace(".json", "")
    parts = name.split("_")

    # Season is always the last token
    try:
        season_year = int(parts[-1])
    except ValueError:
        raise ValueError(f"Invalid season in filename: {filename}")

    # League key is everything before the season
    league_key = "_".join(parts[:-1])

    return league_key, season_year


def parse_player_filename(filename: str):
    """
    Parse filenames like:
        {league_key}_{season}_team_{team_id}_page_{page}.json

    Works for league keys with any number of underscores.
    """

    name = filename.replace(".json", "")
    parts = name.split("_")

    # Find the index of "team"
    try:
        team_idx = parts.index("team")
    except ValueError:
        raise ValueError(f"Invalid filename format (missing 'team'): {filename}")

    # Season is the token before "team"
    try:
        season_year = int(parts[team_idx - 1])
    except ValueError:
        raise ValueError(f"Invalid season in filename: {filename}")

    # League key is everything before the season
    league_key = "_".join(parts[: team_idx - 1])

    # Team ID is the token after "team"
    try:
        team_id = int(parts[team_idx + 1])
    except ValueError:
        raise ValueError(f"Invalid team_id in filename: {filename}")

    # Page is the token after "page"
    try:
        page_idx = parts.index("page")
        page = int(parts[page_idx + 1])
    except Exception:
        page = None

    return league_key, season_year, team_id, page