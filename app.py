
import os
import json
import pandas as pd
import psycopg2
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh


st.set_page_config(
    page_title="Cricket Analytics",
    page_icon="🏏",
    layout="wide"
)

st.title("🏏 Real-Time Cricket Analytics")
st.caption("Cricsheet replay → Kafka → PySpark → PostgreSQL")

st_autorefresh(interval=5000, key="refresh")


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "cricket_analytics"),
        user=os.getenv("POSTGRES_USER", "cricket"),
        password=os.getenv("POSTGRES_PASSWORD", "cricketpass"),
    )


try:
    conn = connect()

    # ---------------- FETCH THE MATCH ----------------

    matches = pd.read_sql(
        "SELECT DISTINCT match_id FROM deliveries ORDER BY match_id",
        conn
    )

    if matches.empty:
        st.info(
            "No match data yet. Start the Spark service "
            "and run the Cricsheet replay producer."
        )
        conn.close()
        st.stop()

    # Automatically use the only available match.
    match_id = matches.iloc[0]["match_id"]

    # ---------------- MATCH INFORMATION ----------------

    with open("/data/match.json", "r", encoding="utf-8") as f:
        match_data = json.load(f)

    match_info = match_data.get("info", {})

    teams = match_info.get("teams", [])
    match_date = match_info.get("dates", ["Unknown"])[0]
    venue = match_info.get("venue", "Unknown")
    match_type = match_info.get("match_type", "Unknown")

    # ---------------- INNINGS SUMMARY ----------------

    innings_summary = pd.read_sql(
        """
        SELECT
            innings_number,
            MIN(batting_team) AS batting_team,
            MIN(bowling_team) AS bowling_team,
            COALESCE(SUM(total_runs), 0) AS runs,
            COUNT(*) FILTER (WHERE is_wicket) AS wickets,
            COUNT(*) FILTER (
                WHERE extras_type IS DISTINCT FROM 'wides'
                  AND extras_type IS DISTINCT FROM 'noballs'
            ) AS legal_balls
        FROM deliveries
        WHERE match_id = %s
        GROUP BY innings_number
        ORDER BY innings_number
        """,
        conn,
        params=(match_id,)
    )

    # ---------------- MATCH INFORMATION DISPLAY ----------------

    st.divider()
    st.header("🏟️ Match Information")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f"""
            <div style="font-size:20px; margin-bottom:14px;">
                <b>Teams:</b> {" vs ".join(teams) if teams else "Unknown"}
            </div>
            <div style="font-size:20px;">
                <b>Match Type:</b> {match_type}
            </div>
            """,
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"""
            <div style="font-size:20px; margin-bottom:14px;">
                <b>Date:</b> {match_date}
            </div>
            <div style="font-size:20px;">
                <b>Venue:</b> {venue}
            </div>
            """,
            unsafe_allow_html=True
        )

    # ---------------- MATCH RESULT ----------------

    st.divider()
    st.header("🏆 Match Result")

    if len(innings_summary) == 2:

        first = innings_summary.iloc[0]
        second = innings_summary.iloc[1]

        first_runs = int(first["runs"])
        second_runs = int(second["runs"])
        second_wickets = int(second["wickets"])
        second_balls = int(second["legal_balls"])

        second_innings_complete = (
            second_wickets >= 10
            or second_balls >= 120
            or second_runs > first_runs
        )

        if second_innings_complete:

            if first_runs > second_runs:
                margin = first_runs - second_runs

                st.success(
                    f"🏆 {first['batting_team']} won by {margin} runs!"
                )

            elif second_runs > first_runs:
                wickets_remaining = 10 - second_wickets

                st.success(
                    f"🏆 {second['batting_team']} won "
                    f"by {wickets_remaining} wickets!"
                )

            else:
                st.info("Match tied!")

        else:
            st.info("Second innings is still in progress.")

    else:
        st.info("Waiting for both innings to be recorded.")

    # ---------------- INNINGS SCORECARD ----------------

    st.divider()
    st.header("🏏 Match Scorecard")

    # ---------------- BATTING STATISTICS ----------------

    batting = pd.read_sql(
        """
        SELECT
            s.innings_number,
            t.batting_team AS team,
            s.batter,
            s.runs,
            s.balls_faced,
            s.strike_rate
        FROM batting_statistics s
        JOIN (
            SELECT DISTINCT match_id, innings_number, batting_team
            FROM deliveries
        ) t
          ON s.match_id = t.match_id
         AND s.innings_number = t.innings_number
        WHERE s.match_id = %s
        ORDER BY s.innings_number, s.runs DESC
        """,
        conn,
        params=(match_id,)
    )

    # ---------------- BOWLING STATISTICS ----------------

    bowling = pd.read_sql(
        """
        SELECT
            s.innings_number,
            t.bowling_team AS team,
            s.bowler,
            s.runs_conceded,
            s.legal_balls,
            s.wickets,
            s.economy
        FROM bowling_statistics s
        JOIN (
            SELECT DISTINCT match_id, innings_number, bowling_team
            FROM deliveries
        ) t
          ON s.match_id = t.match_id
         AND s.innings_number = t.innings_number
        WHERE s.match_id = %s
        ORDER BY s.innings_number, s.wickets DESC
        """,
        conn,
        params=(match_id,)
    )

    # ---------------- DISPLAY EACH INNINGS ----------------

    for _, innings in innings_summary.iterrows():

        innings_no = int(innings["innings_number"])
        batting_team = innings["batting_team"]
        bowling_team = innings["bowling_team"]

        runs = int(innings["runs"])
        wickets = int(innings["wickets"])
        legal_balls = int(innings["legal_balls"])

        overs = f"{legal_balls // 6}.{legal_balls % 6}"
        run_rate = runs * 6 / legal_balls if legal_balls else 0

        st.divider()

        st.subheader(
            f"Innings {innings_no}: {batting_team}"
        )

        score1, score2, score3 = st.columns(3)

        score1.metric("Score", f"{runs}/{wickets}")
        score2.metric("Overs", overs)
        score3.metric("Run Rate", f"{run_rate:.2f}")

        innings_batting = batting[
            batting["innings_number"] == innings_no
        ].copy()

        innings_bowling = bowling[
            bowling["innings_number"] == innings_no
        ].copy()

        innings_bowling["overs"] = innings_bowling[
            "legal_balls"
        ].apply(
            lambda balls: f"{int(balls) // 6}.{int(balls) % 6}"
        )

        left, right = st.columns(2)

        with left:
            st.markdown(f"#### 🏏 {batting_team} — Batting")

            batting_display = innings_batting[
                ["batter", "runs", "balls_faced", "strike_rate"]
            ].rename(
                columns={
                    "batter": "Batter",
                    "runs": "Runs",
                    "balls_faced": "Balls",
                    "strike_rate": "Strike Rate"
                }
            )

            st.dataframe(
                batting_display,
                use_container_width=True,
                hide_index=True
            )

        with right:
            st.markdown(f"#### 🎯 {bowling_team} — Bowling")

            bowling_display = innings_bowling[
                ["bowler", "overs", "runs_conceded", "wickets", "economy"]
            ].rename(
                columns={
                    "bowler": "Bowler",
                    "overs": "Overs",
                    "runs_conceded": "Runs",
                    "wickets": "Wickets",
                    "economy": "Economy"
                }
            )

            st.dataframe(
                bowling_display,
                use_container_width=True,
                hide_index=True
            )

    # ---------------- RUNS PER OVER ----------------

    overs_data = pd.read_sql(
        """
        SELECT
            innings_number,
            over_number,
            SUM(total_runs) AS runs
        FROM deliveries
        WHERE match_id = %s
        GROUP BY innings_number, over_number
        ORDER BY innings_number, over_number
        """,
        conn,
        params=(match_id,)
    )

    st.divider()
    st.header("📊 Runs Per Over")

    if not overs_data.empty:

        team_names = {
            int(row["innings_number"]): row["batting_team"]
            for _, row in innings_summary.iterrows()
        }

        overs_data["Team"] = overs_data[
            "innings_number"
        ].map(team_names)

        fig = px.bar(
            overs_data,
            x="over_number",
            y="runs",
            color="Team",
            barmode="group",
            labels={
                "over_number": "Over",
                "runs": "Runs",
                "Team": "Batting Team"
            }
        )

        st.plotly_chart(fig, use_container_width=True)

    # ---------------- LATEST DELIVERIES ----------------

    st.divider()
    st.header("🔴 Latest Deliveries")

    recent = pd.read_sql(
        """
        SELECT
            innings_number,
            over_number,
            ball_number,
            batter,
            bowler,
            total_runs,
            is_wicket
        FROM deliveries
        WHERE match_id = %s
        ORDER BY
            innings_number DESC,
            over_number DESC,
            ball_number DESC
        LIMIT 15
        """,
        conn,
        params=(match_id,)
    )

    st.dataframe(
        recent,
        use_container_width=True,
        hide_index=True
    )

    conn.close()

    st.caption("Dashboard refreshes automatically every five seconds.")

except Exception as exc:
    st.error(f"Database connection/data error: {exc}")
    st.info(
        "Check that the PostgreSQL, Kafka and Spark containers are running."
    )
