import json
import os
from contextlib import closing
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))

st.set_page_config(
    page_title="Cricket Analytics",
    page_icon="🏏",
    layout="wide",
)

st.title("🏏 Real-Time Cricket Analytics")
st.caption("Cricsheet replay → Kafka → PySpark → PostgreSQL")

st_autorefresh(interval=5000, key="autorefresh")


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def connect():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "cricket_analytics"),
        user=os.getenv("POSTGRES_USER", "cricket"),
        password=os.getenv("POSTGRES_PASSWORD", "cricketpass"),
        connect_timeout=5,
    )
    # Read-only dashboard: autocommit avoids "idle in transaction" sessions
    # and a single failed query poisoning every later query on the connection.
    conn.autocommit = True
    return conn


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        return pd.DataFrame.from_records(
            cur.fetchall(), columns=columns, coerce_float=True
        )


def load_match_info(match_id):
    """Read <match_id>.json from the data folder (the file the producer replayed)."""
    path = DATA_DIR / f"{Path(match_id).name}.json"
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f).get("info", {})
    except (OSError, ValueError):
        return {}


def overs_text(legal_balls, balls_per_over=6):
    return f"{legal_balls // balls_per_over}.{legal_balls % balls_per_over}"


def innings_result(innings, max_balls, max_wickets):
    """Work out the result of a two-innings contest from its summary rows."""
    if len(innings) < 2:
        return {"status": "waiting"}

    first, second = innings.iloc[0], innings.iloc[1]
    first_runs, second_runs = int(first["runs"]), int(second["runs"])
    second_wickets = int(second["wickets"])

    finished = (
        second_wickets >= max_wickets
        or int(second["legal_balls"]) >= max_balls
        or second_runs > first_runs
    )
    if not finished:
        return {"status": "in_progress"}
    if first_runs > second_runs:
        return {
            "status": "won",
            "text": f"{first['batting_team']} won by {first_runs - second_runs} runs!",
        }
    if second_runs > first_runs:
        return {
            "status": "won",
            "text": f"{second['batting_team']} won by "
                    f"{max_wickets - second_wickets} wickets!",
        }
    return {"status": "tied"}


def render_result(result):
    if result["status"] == "waiting":
        st.info("Waiting for both innings to be recorded.")
    elif result["status"] == "in_progress":
        st.info("Second innings is still in progress.")
    elif result["status"] == "won":
        st.success(f"🏆 {result['text']}")
    else:
        st.info("Match tied!")


# --------------------------------------------------------------------------
# Page
# --------------------------------------------------------------------------

try:
    # closing() guarantees the connection is released on every path (normal
    # end, st.stop(), or an exception) so refreshes never leak connections.
    with closing(connect()) as conn:

        # ---------------- MATCH SELECTION ----------------

        matches = query(
            conn,
            """
            SELECT match_id, MAX(processed_at) AS last_seen
            FROM deliveries
            GROUP BY match_id
            ORDER BY last_seen DESC
            """,
        )

        if matches.empty:
            st.info(
                "No match data yet. Start the Spark service "
                "and run the Cricsheet replay producer."
            )
            st.stop()

        match_ids = matches["match_id"].tolist()

        # The selection survives auto-refresh; reset it if that match vanished
        # (e.g. the database was cleared).
        if st.session_state.get("match_select") not in match_ids:
            st.session_state.pop("match_select", None)

        match_id = st.selectbox(
            "Match", match_ids, key="match_select"
        )

        # ---------------- MATCH INFORMATION ----------------

        match_info = load_match_info(match_id)

        teams = match_info.get("teams", [])
        match_date = (match_info.get("dates") or ["Unknown"])[0]
        venue = match_info.get("venue", "Unknown")
        match_type = match_info.get("match_type", "Unknown")
        balls_per_over = int(match_info.get("balls_per_over", 6))
        max_overs = int(match_info.get("overs", 20))

        innings_summary = query(
            conn,
            """
            SELECT *
            FROM innings_statistics
            WHERE match_id = %s
            ORDER BY innings_number
            """,
            (match_id,),
        )

        st.divider()
        st.header("🏟️ Match Information")

        info_left, info_right = st.columns(2)

        with info_left:
            st.markdown(f"**Teams:** {' vs '.join(teams) if teams else 'Unknown'}")
            st.markdown(f"**Match Type:** {match_type}")

        with info_right:
            st.markdown(f"**Date:** {match_date}")
            st.markdown(f"**Venue:** {venue}")

        # ---------------- MATCH RESULT ----------------

        st.divider()
        st.header("🏆 Match Result")

        main_innings = innings_summary[~innings_summary["is_super_over"]]
        super_innings = innings_summary[innings_summary["is_super_over"]]

        main_result = innings_result(
            main_innings, max_overs * balls_per_over, 10
        )
        render_result(main_result)

        if main_result["status"] == "tied" and not super_innings.empty:
            st.subheader("Super Over")
            # A super over is one over (6 balls) and two wickets per side.
            render_result(
                innings_result(super_innings, balls_per_over, 2)
            )

        # ---------------- STATISTICS ----------------

        batting = query(
            conn,
            """
            SELECT innings_number, batting_position, batter, how_out,
                   runs, balls_faced, fours, sixes, strike_rate
            FROM batting_statistics
            WHERE match_id = %s
            ORDER BY innings_number, batting_position
            """,
            (match_id,),
        )

        bowling = query(
            conn,
            """
            SELECT innings_number, bowler, legal_balls, maidens,
                   runs_conceded, wickets, wides, noballs, economy
            FROM bowling_statistics
            WHERE match_id = %s
            ORDER BY innings_number, wickets DESC, economy
            """,
            (match_id,),
        )

        fall = query(
            conn,
            """
            SELECT innings_number, wicket_number, player_out, score, over_text
            FROM fall_of_wickets
            WHERE match_id = %s
            ORDER BY innings_number, wicket_number
            """,
            (match_id,),
        )

        st.divider()
        st.header("🏏 Match Scorecard")

        for _, innings in innings_summary.iterrows():
            innings_no = int(innings["innings_number"])
            batting_team = innings["batting_team"]
            bowling_team = innings["bowling_team"]
            label = "Super Over" if innings["is_super_over"] else f"Innings {innings_no}"

            runs = int(innings["runs"])
            wickets = int(innings["wickets"])
            legal_balls = int(innings["legal_balls"])

            run_rate = runs * balls_per_over / legal_balls if legal_balls else 0

            st.divider()
            st.subheader(f"{label}: {batting_team}")

            score1, score2, score3 = st.columns(3)
            score1.metric("Score", f"{runs}/{wickets}")
            score2.metric("Overs", overs_text(legal_balls, balls_per_over))
            score3.metric("Run Rate", f"{run_rate:.2f}")

            innings_batting = batting[batting["innings_number"] == innings_no]
            innings_bowling = bowling[
                bowling["innings_number"] == innings_no
            ].copy()
            innings_fall = fall[fall["innings_number"] == innings_no]

            innings_bowling["overs"] = innings_bowling["legal_balls"].apply(
                lambda balls: overs_text(int(balls), balls_per_over)
            )

            left, right = st.columns(2)

            with left:
                st.markdown(f"#### 🏏 {batting_team} — Batting")
                st.dataframe(
                    innings_batting[
                        ["batter", "how_out", "runs", "balls_faced",
                         "fours", "sixes", "strike_rate"]
                    ].rename(
                        columns={
                            "batter": "Batter",
                            "how_out": "Dismissal",
                            "runs": "Runs",
                            "balls_faced": "Balls",
                            "fours": "4s",
                            "sixes": "6s",
                            "strike_rate": "SR",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                    key=f"batting_{match_id}_{innings_no}",
                )

                st.markdown(
                    f"**Extras:** {int(innings['extras'])} "
                    f"(w {int(innings['wides'])}, nb {int(innings['noballs'])}, "
                    f"b {int(innings['byes'])}, lb {int(innings['legbyes'])}, "
                    f"pen {int(innings['penalty'])})"
                )

                batted = set(innings_batting["batter"])
                squad = match_info.get("players", {}).get(batting_team, [])
                did_not_bat = [p for p in squad if p not in batted]
                if did_not_bat and not innings["is_super_over"]:
                    st.markdown(f"**Did not bat:** {', '.join(did_not_bat)}")

                if not innings_fall.empty:
                    fow = ", ".join(
                        f"{int(r.wicket_number)}-{int(r.score)} "
                        f"({r.player_out}, {r.over_text} ov)"
                        for r in innings_fall.itertuples()
                    )
                    st.markdown(f"**Fall of wickets:** {fow}")

            with right:
                st.markdown(f"#### 🎯 {bowling_team} — Bowling")
                st.dataframe(
                    innings_bowling[
                        ["bowler", "overs", "maidens", "runs_conceded",
                         "wickets", "economy", "wides", "noballs"]
                    ].rename(
                        columns={
                            "bowler": "Bowler",
                            "overs": "O",
                            "maidens": "M",
                            "runs_conceded": "R",
                            "wickets": "W",
                            "economy": "Econ",
                            "wides": "Wd",
                            "noballs": "Nb",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                    key=f"bowling_{match_id}_{innings_no}",
                )

        # ---------------- RUNS PER OVER ----------------

        overs_data = query(
            conn,
            """
            SELECT innings_number, over_number, SUM(total_runs) AS runs
            FROM deliveries
            WHERE match_id = %s
            GROUP BY innings_number, over_number
            ORDER BY innings_number, over_number
            """,
            (match_id,),
        )

        st.divider()
        st.header("📊 Runs Per Over")

        if not overs_data.empty:
            team_names = {
                int(row["innings_number"]): row["batting_team"]
                for _, row in innings_summary.iterrows()
            }
            overs_data["Team"] = overs_data["innings_number"].map(team_names)

            fig = px.bar(
                overs_data,
                x="over_number",
                y="runs",
                color="Team",
                barmode="group",
                labels={
                    "over_number": "Over",
                    "runs": "Runs",
                    "Team": "Batting Team",
                },
            )
            st.plotly_chart(
                fig, use_container_width=True, key=f"runs_per_over_{match_id}"
            )

        # ---------------- LATEST DELIVERIES ----------------

        st.divider()
        st.header("🔴 Latest Deliveries")

        recent = query(
            conn,
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
            ORDER BY innings_number DESC, over_number DESC, ball_number DESC
            LIMIT 15
            """,
            (match_id,),
        )

        st.dataframe(
            recent,
            use_container_width=True,
            hide_index=True,
            key=f"latest_{match_id}",
        )

        st.caption("Dashboard refreshes automatically every five seconds.")

except psycopg2.Error as exc:
    st.error(f"Database error: {exc}")
    st.info(
        "Check that the PostgreSQL, Kafka and Spark containers are running."
    )
