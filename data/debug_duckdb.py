import duckdb

con = duckdb.connect("data/analytics.duckdb")

print(con.execute("DESCRIBE dim_league").fetchdf())
print()
print(con.execute("SELECT league_id, league_name, country_flag, flag_url, scope, region FROM dim_league").fetchdf())