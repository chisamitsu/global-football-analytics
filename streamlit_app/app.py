import streamlit as st
import duckdb
from pathlib import Path
import pandas as pd
import altair as alt

# -----------------------------------------
# Connect to DuckDB
# -----------------------------------------
db_path = Path("data") / "analytics.duckdb"
con = duckdb.connect(str(db_path))

st.title("⚽ Global Football Analytics Dashboard")

# -----------------------------------------
# Sidebar filters
# -----------------------------------------
st.sidebar.header("Filters")

# Load leagues
leagues_df = con.execute("""
    SELECT league_id, league_name, league_logo, country_flag
    FROM dim_league
    ORDER BY league_name
""").df()

selected_leagues = st.sidebar.multiselect(
    "Select Leagues",
    leagues_df["league_name"],
    default=leagues_df["league_name"].tolist()[:3]
)

selected_league_ids = leagues_df[
    leagues_df["league_name"].isin(selected_leagues)
]["league_id"].tolist()

# Load seasons for selected leagues
seasons_df = con.execute(f"""
    SELECT DISTINCT season_year
    FROM fact_match
    WHERE league_id IN ({','.join(map(str, selected_league_ids))})
    ORDER BY season_year
""").df()

selected_seasons = st.sidebar.multiselect(
    "Select Seasons",
    seasons_df["season_year"],
    default=seasons_df["season_year"].tolist()[-3:]
)

# -----------------------------------------
# Query aggregated data
# -----------------------------------------
query = f"""
SELECT
    m.season_year,
    l.league_name,
    l.league_logo,
    l.country_flag,
    SUM(m.goals_home) AS total_goals_home,
    SUM(m.goals_away) AS total_goals_away,
    SUM(m.goals_home + m.goals_away) AS total_goals,
    AVG(m.goals_home) AS avg_goals_home,
    AVG(m.goals_away) AS avg_goals_away,
    AVG(m.goals_home + m.goals_away) AS avg_goals_per_match,
    CASE 
        WHEN AVG(m.goals_away) = 0 THEN NULL
        ELSE AVG(m.goals_home) / AVG(m.goals_away)
    END AS home_advantage_index
FROM fact_match m
LEFT JOIN dim_league l ON m.league_id = l.league_id
WHERE m.league_id IN ({','.join(map(str, selected_league_ids))})
  AND m.season_year IN ({','.join(map(str, selected_seasons))})
GROUP BY m.season_year, l.league_name, l.league_logo, l.country_flag
ORDER BY season_year, league_name;
"""

df = con.execute(query).df()

# -----------------------------------------
# Header with logos
# -----------------------------------------
st.subheader("Selected Leagues")

cols = st.columns(len(selected_leagues))

for col, league in zip(cols, selected_leagues):
    row = leagues_df[leagues_df["league_name"] == league].iloc[0]
    with col:
        st.image(row["league_logo"], width=80)
        st.image(row["country_flag"], width=40)
        st.caption(league)

# -----------------------------------------
# KPIs
# -----------------------------------------
st.subheader("Key Metrics")

kpi1, kpi2, kpi3, kpi4 = st.columns(4)

kpi1.metric("Total Goals", int(df["total_goals"].sum()))
kpi2.metric("Avg Goals per Match", round(df["avg_goals_per_match"].mean(), 2))
kpi3.metric("Home Advantage Index", round(df["home_advantage_index"].mean(), 2))
kpi4.metric("Matches Count", int(df["total_goals"].count()))

# -----------------------------------------
# Charts
# -----------------------------------------
st.subheader("Home vs Away Goals (Multi‑Season)")

goals_melted = df.melt(
    id_vars=["season_year", "league_name"],
    value_vars=["total_goals_home", "total_goals_away"],
    var_name="goal_type",
    value_name="goals"
)

chart_goals = alt.Chart(goals_melted).mark_bar().encode(
    x=alt.X("season_year:O", title="Season"),
    y=alt.Y("goals:Q", title="Goals"),
    color="goal_type:N",
    column="league_name:N"
).properties(height=300)

st.altair_chart(chart_goals, use_container_width=True)

# Home Advantage Trend
st.subheader("Home Advantage Index Trend")

hai_chart = alt.Chart(df).mark_line(point=True).encode(
    x="season_year:O",
    y="home_advantage_index:Q",
    color="league_name:N",
    tooltip=["league_name", "season_year", "home_advantage_index"]
).properties(height=300)

st.altair_chart(hai_chart, use_container_width=True)

# -----------------------------------------
# Data Table
# -----------------------------------------
st.subheader("Aggregated Data")
st.dataframe(df)