import streamlit as st
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import re
import requests
import logging
import threading
import asyncio
import time
from datetime import datetime
from urllib.parse import urlparse
from streamlit_autorefresh import st_autorefresh  # Import the autorefresh component
import plotly.express as px
# Configure logging
logging.basicConfig(level=logging.INFO)


# --- Helper Functions ---

def safe_get(d, keys, default=None):
    """Safely retrieve nested data from dictionaries or lists."""
    for key in keys:
        if isinstance(d, list):
            try:
                d = d[key]
            except (IndexError, TypeError):
                return default
        else:
            if isinstance(key, int) and isinstance(d, list):
                try:
                    d = d[key]
                except IndexError:
                    return default
            elif isinstance(key, str):
                d = d.get(key, default) if d else default
            else:
                return default
    return d if d is not None else default


def extract_match_id(url):
    """Extract the match ID from the provided URL."""
    pattern = r'/match/([a-f0-9\-]+)/'
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    else:
        logging.error(f"Could not extract match ID from URL: {url}")
        return None


def call_api(url):
    """Make a GET request to the specified URL and return JSON data."""
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.8",
        "cache-control": "no-cache",
        "origin": "https://play.cricket.com.au",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://play.cricket.com.au/",
        "sec-ch-ua": '"Not)A;Brand";v="99", "Brave";v="127", "Chromium";v="127"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Successfully fetched data from {url}")
        return data
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return {}


def create_tables(conn):
    """Create necessary tables in the SQLite database."""
    cursor = conn.cursor()
    cursor.executescript('''
    CREATE TABLE IF NOT EXISTS ball_by_ball (
        id TEXT PRIMARY KEY,
        innings_id TEXT,
        innings_number INTEGER,
        innings_order INTEGER,
        innings_name TEXT,
        batting_team_id TEXT,
        progress_runs INTEGER,
        progress_wickets INTEGER,
        progress_score TEXT,
        striker_participant_id TEXT,
        striker_short_name TEXT,
        striker_runs_scored INTEGER,
        striker_balls_faced INTEGER,
        non_striker_participant_id TEXT,
        non_striker_short_name TEXT,
        non_striker_runs_scored INTEGER,
        non_striker_balls_faced INTEGER,
        bowler_participant_id TEXT,
        bowler_short_name TEXT,
        over_number INTEGER,
        ball_display_number INTEGER,
        ball_time DATETIME,
        runs_bat INTEGER,
        wides INTEGER,
        no_balls INTEGER,
        leg_byes INTEGER,
        byes INTEGER,
        penalty_runs INTEGER,
        short_description TEXT,
        description TEXT,
        fetched_time DATETIME
    );

    CREATE TABLE IF NOT EXISTS players (
        id TEXT PRIMARY KEY,
        team_id TEXT,
        name TEXT,
        short_name TEXT,
        role TEXT
    );

    CREATE TABLE IF NOT EXISTS innings (
        id TEXT PRIMARY KEY,
        match_id TEXT,
        name TEXT,
        innings_close_type TEXT,
        innings_number INTEGER,
        innings_order INTEGER,
        batting_team_id TEXT,
        is_declared BOOLEAN,
        is_follow_on BOOLEAN,
        byes_runs INTEGER,
        leg_byes_runs INTEGER,
        no_balls INTEGER,
        wide_balls INTEGER,
        penalties INTEGER,
        total_extras INTEGER,
        overs_bowled REAL,
        runs_scored INTEGER,
        number_of_wickets_fallen INTEGER
    );

    CREATE TABLE IF NOT EXISTS teams (
        id TEXT PRIMARY KEY,
        match_id TEXT,
        display_name TEXT,
        result_type_id INTEGER,
        result_type TEXT,
        won_toss BOOLEAN,
        batted_first BOOLEAN,
        is_home BOOLEAN,
        score_text TEXT,
        is_winner BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS matches (
        id TEXT PRIMARY KEY,
        status TEXT,
        status_id INTEGER,
        team_a TEXT,
        team_b TEXT,
        season TEXT,
        match_type TEXT,
        match_type_id INTEGER,
        is_ball_by_ball BOOLEAN,
        result_text TEXT,
        round_id TEXT,
        grade_id TEXT,
        venue_id TEXT,
        start_datetime DATETIME
    );

    CREATE TABLE IF NOT EXISTS rounds (
        id TEXT PRIMARY KEY,
        name TEXT,
        short_name TEXT
    );

    CREATE TABLE IF NOT EXISTS grades (
        id TEXT PRIMARY KEY,
        name TEXT
    );

    CREATE TABLE IF NOT EXISTS venues (
        id TEXT PRIMARY KEY,
        name TEXT,
        line1 TEXT,
        suburb TEXT,
        post_code TEXT,
        state_name TEXT,
        country TEXT,
        playing_surface_id TEXT
    );

    CREATE TABLE IF NOT EXISTS playing_surfaces (
        id TEXT PRIMARY KEY,
        name TEXT,
        latitude REAL,
        longitude REAL
    );

    CREATE TABLE IF NOT EXISTS organisations (
        id TEXT PRIMARY KEY,
        name TEXT,
        short_name TEXT,
        logo_url TEXT
    );
    ''')
    conn.commit()


def insert_ball_by_ball(cursor, ball, innings_id, fetched_time):
    """Insert a single ball into the ball_by_ball table if it doesn't already exist."""
    ball_id = safe_get(ball, ['id'], default=None)
    if not ball_id:
        logging.warning("Ball without ID encountered. Skipping insertion.")
        return

    # Check if the ball already exists
    cursor.execute("SELECT id FROM ball_by_ball WHERE id = ?", (ball_id,))
    if cursor.fetchone():
        logging.info(f"Ball ID {ball_id} already exists. Skipping insertion.")
        return

    # Insert the ball
    try:
        cursor.execute('''
            INSERT INTO ball_by_ball (
                id, innings_id, innings_number, innings_order, innings_name,
                batting_team_id, progress_runs, progress_wickets, progress_score,
                striker_participant_id, striker_short_name, striker_runs_scored, striker_balls_faced,
                non_striker_participant_id, non_striker_short_name, non_striker_runs_scored, non_striker_balls_faced,
                bowler_participant_id, bowler_short_name, over_number, ball_display_number, ball_time,
                runs_bat, wides, no_balls, leg_byes, byes, penalty_runs, short_description, description,
                fetched_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            ball_id,
            innings_id,
            safe_get(ball, ['overNumber'], 0),
            safe_get(ball, ['overNumber'], 0),  # sometimes same as innings_number
            safe_get(ball, ['inningsName']),
            safe_get(ball, ['battingTeamId']),
            safe_get(ball, ['progressRuns'], 0),
            safe_get(ball, ['progressWickets'], 0),
            safe_get(ball, ['progressScore']),
            safe_get(ball, ['strikerParticipantId']),
            safe_get(ball, ['strikerShortName']),
            safe_get(ball, ['strikerRunsScored'], 0),
            safe_get(ball, ['strikerBallsFaced'], 0),
            safe_get(ball, ['nonStrikerParticipantId']),
            safe_get(ball, ['nonStrikerShortName']),
            safe_get(ball, ['nonStrikerRunsScored'], 0),
            safe_get(ball, ['nonStrikerBallsFaced'], 0),
            safe_get(ball, ['bowlerParticipantId']),
            safe_get(ball, ['bowlerShortName']),
            safe_get(ball, ['overNumber'], 0),
            safe_get(ball, ['ballDisplayNumber'], 0),
            safe_get(ball, ['ballTime']),
            safe_get(ball, ['runsBat'], 0),
            safe_get(ball, ['wides'], 0),
            safe_get(ball, ['noBalls'], 0),
            safe_get(ball, ['legByes'], 0),
            safe_get(ball, ['byes'], 0),
            safe_get(ball, ['penaltyRuns'], 0),
            safe_get(ball, ['shortDescription']),
            safe_get(ball, ['description']),
            fetched_time
        ))
        logging.info(f"Inserted new ball with ID {ball_id} at {safe_get(ball, ['ballTime'])}")
    except Exception as e:
        logging.error(f"Error inserting ball ID {ball_id}: {e}")


def update_related_tables(cursor, ball, match_id):
    """Update related tables (teams, players, etc.) if needed."""
    pass


def fetch_and_store_ball_data(conn, match_id):
    """Fetch the latest ball data for the match and store new balls into the database."""
    cursor = conn.cursor()

    # 1) Fetch teams data (only once per match).
    #    We may also store or update 'teams' table from the JSON.
    teams_url = f"https://grassrootsapiproxy.cricket.com.au/scores/matches/{match_id}?jsconfig=eccn%3Atrue"
    teams_data = call_api(teams_url)

    if teams_data:
        # Insert or update teams in the 'teams' table
        for t in safe_get(teams_data, ['teams'], []):
            team_id = safe_get(t, ['id'])
            display_name = safe_get(t, ['displayName'])
            # Insert or IGNORE to not duplicate
            cursor.execute('''
                INSERT OR IGNORE INTO teams (id, match_id, display_name)
                VALUES (?, ?, ?)
            ''', (team_id, match_id, display_name))

    # 2) Fetch the ball-by-ball data
    balls_url = f"https://grassrootsapiproxy.cricket.com.au/scores/matches/{match_id}/balls?jsconfig=eccn%3Atrue"
    data = call_api(balls_url)

    if not data:
        logging.warning(f"No data returned for match ID {match_id}")
        return

    fetched_time = datetime.utcnow()

    innings_list = safe_get(data, ['innings'], [])
    for innings in innings_list:
        innings_id = safe_get(innings, ['id'])
        if not innings_id:
            continue

        # Insert/update innings
        cursor.execute('''
            INSERT OR IGNORE INTO innings (
                id, match_id, name, innings_close_type, innings_number, innings_order,
                batting_team_id, is_declared, is_follow_on, byes_runs, leg_byes_runs,
                no_balls, wide_balls, penalties, total_extras, overs_bowled, runs_scored, 
                number_of_wickets_fallen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            innings_id,
            match_id,
            safe_get(innings, ['inningsName']),
            safe_get(innings, ['inningsCloseType'], ''),
            safe_get(innings, ['inningsNumber'], 0),
            safe_get(innings, ['inningsOrder'], 0),
            safe_get(innings, ['battingTeamId']),
            safe_get(innings, ['isDeclared'], False),
            safe_get(innings, ['isFollowOn'], False),
            safe_get(innings, ['byesRuns'], 0),
            safe_get(innings, ['legByesRuns'], 0),
            safe_get(innings, ['noBalls'], 0),
            safe_get(innings, ['wideBalls'], 0),
            safe_get(innings, ['penalties'], 0),
            safe_get(innings, ['totalExtras'], 0),
            safe_get(innings, ['oversBowled'], 0.0),
            safe_get(innings, ['runsScored'], 0),
            safe_get(innings, ['numberOfWicketsFallen'], 0)
        ))

        # Insert balls
        for ball in safe_get(innings, ['balls'], []):
            insert_ball_by_ball(cursor, ball, innings_id, fetched_time)
            update_related_tables(cursor, ball, match_id)

    conn.commit()
    logging.info(f"Fetched and stored latest data for match ID {match_id}")


def clear_database(conn):
    """Clear all data from the database tables."""
    cursor = conn.cursor()
    try:
        cursor.executescript('''
        DELETE FROM ball_by_ball;
        DELETE FROM innings;
        DELETE FROM teams;
        DELETE FROM matches;
        DELETE FROM rounds;
        DELETE FROM grades;
        DELETE FROM venues;
        DELETE FROM playing_surfaces;
        DELETE FROM organisations;
        DELETE FROM players;
        ''')
        conn.commit()
        logging.info("Database has been cleared.")
    except Exception as e:
        logging.error(f"Error clearing database: {e}")


# --- Async Functions ---

async def async_live_data_fetcher(conn, match_id, stop_event):
    """Asynchronous function to fetch/store data every 5 seconds."""
    while not stop_event.is_set():
        fetch_and_store_ball_data(conn, match_id)
        await asyncio.sleep(5)


def run_async_loop(loop):
    """Run the asyncio event loop in a background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


# --- Streamlit App ---

st.title("Live Match Data Viewer")

# Auto-refresh every 5s
st_autorefresh(interval=5000, limit=None, key="autorefresh_key")

# Session State
if 'async_loop' not in st.session_state:
    st.session_state.async_loop = asyncio.new_event_loop()

if 'loop_thread' not in st.session_state:
    loop = st.session_state.async_loop
    thread = threading.Thread(target=run_async_loop, args=(loop,), daemon=True)
    thread.start()
    st.session_state.loop_thread = thread

if 'stop_event' not in st.session_state:
    st.session_state.stop_event = threading.Event()

if 'live_task' not in st.session_state:
    st.session_state.live_task = None


# Database connection
@st.cache_resource
def get_db_connection():
    db_path = 'try.db'
    conn = sqlite3.connect(db_path, check_same_thread=False)
    return conn


conn = get_db_connection()
create_tables(conn)

# User input
match_url = st.text_input("Enter match URL:", "")
start_button = st.button("Start Live")
stop_button = st.button("Stop")

match_id = extract_match_id(match_url) if match_url else None

# Start live data
if start_button:
    if not match_id:
        st.error("Invalid match URL.")
    elif st.session_state.live_task and not st.session_state.live_task.done():
        st.warning("Live data collection is already running.")
    else:
        if st.session_state.live_task:
            st.session_state.stop_event.set()
            st.session_state.live_task = None
            st.session_state.stop_event = threading.Event()
        clear_database(conn)
        coro = async_live_data_fetcher(conn, match_id, st.session_state.stop_event)
        task = asyncio.run_coroutine_threadsafe(coro, st.session_state.async_loop)
        st.session_state.live_task = task
        st.success("Live data collection started and DB cleared.")

# Stop live data
if stop_button:
    if st.session_state.live_task and not st.session_state.live_task.done():
        st.session_state.stop_event.set()
        st.session_state.live_task = None
        st.success("Live data collection stopped.")
    else:
        st.warning("No live data collection is running.")

# -------------------------------
#   LIVE DATA VISUALIZATIONS
# -------------------------------

# -------------------------------
#   LIVE DATA VISUALIZATIONS
# -------------------------------

st.header("Live Match Insights")

if match_id:
    # --- Real-time Cumulative Runs and Wickets Chart ---
    st.subheader("Cumulative Runs and Wickets Over Time")
    runs_wickets_query = """
    SELECT 
        innings_id,
        innings_number,
        ball_time, 
        runs_bat,
        CASE WHEN short_description LIKE '%OUT%' THEN 1 ELSE 0 END AS wicket
    FROM ball_by_ball 
    WHERE ball_time IS NOT NULL
    ORDER BY innings_number, ball_time ASC
    """
    df_runs_wickets = pd.read_sql_query(runs_wickets_query, conn)

    if not df_runs_wickets.empty:
        df_runs_wickets['ball_time'] = pd.to_datetime(df_runs_wickets['ball_time'], utc=True)
        df_runs_wickets = df_runs_wickets.sort_values(['innings_number', 'ball_time'])
        df_runs_wickets['cumulative_runs'] = df_runs_wickets.groupby('innings_number')['runs_bat'].cumsum()
        df_runs_wickets['cumulative_wickets'] = df_runs_wickets.groupby('innings_number')['wicket'].cumsum()

        fig_cumulative = go.Figure()

        # Add traces for each innings
        for innings in df_runs_wickets['innings_id'].unique():
            innings_data = df_runs_wickets[df_runs_wickets['innings_id'] == innings]
            fig_cumulative.add_trace(go.Scatter(
                x=innings_data['ball_time'],
                y=innings_data['cumulative_runs'],
                mode='lines+markers',
                name=f'Runs - Innings {innings}',
                yaxis='y1'
            ))
            fig_cumulative.add_trace(go.Scatter(
                x=innings_data['ball_time'],
                y=innings_data['cumulative_wickets'],
                mode='lines+markers',
                name=f'Wickets - Innings {innings}',
                yaxis='y2'
            ))

        fig_cumulative.update_layout(
            xaxis_title="Time",
            yaxis_title="Cumulative Runs",
            yaxis2=dict(title="Cumulative Wickets", overlaying='y', side='right'),
            template="plotly_dark",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_cumulative, use_container_width=True)
    else:
        st.info("No data yet.")



    # --- Wicket Fall Analysis ---
    st.subheader("Wicket Fall Analysis")
    wicket_fall_query = """
    SELECT
        innings_id,
        over_number + (ball_display_number / 10.0) as over_ball,
        progress_runs,
        progress_wickets
    FROM ball_by_ball
    WHERE short_description LIKE '%OUT%'
    ORDER BY innings_id, over_number, ball_display_number
    """
    df_wicket_fall = pd.read_sql_query(wicket_fall_query, conn)

    if not df_wicket_fall.empty:
        fig_wicket_fall = go.Figure()
        for innings in df_wicket_fall['innings_id'].unique():
            innings_data = df_wicket_fall[df_wicket_fall['innings_id'] == innings]
            fig_wicket_fall.add_trace(go.Scatter(
                x=innings_data['over_ball'],
                y=innings_data['progress_runs'],
                mode='markers',
                name=f'Innings {innings}',
                text=innings_data['progress_wickets'].astype(str) + " Wickets",
                marker=dict(size=10)
            ))
        fig_wicket_fall.update_layout(
            title="Wicket Fall Over Time",
            xaxis_title="Over",
            yaxis_title="Runs at Wicket Fall",
            template="plotly_dark"
        )
        st.plotly_chart(fig_wicket_fall, use_container_width=True)
    else:
        st.info("No wickets have fallen yet.")

    # --- Powerplay Performance ---
    st.subheader("Powerplay Performance")

    # Typically, powerplay is the first 6 overs. Adjust if necessary.
    powerplay_overs = 6

    powerplay_query = """
    SELECT
        innings_id,
        SUM(runs_bat) as powerplay_runs,
        SUM(CASE WHEN short_description LIKE '%OUT%' THEN 1 ELSE 0 END) as powerplay_wickets
    FROM ball_by_ball
    WHERE over_number < ?
    GROUP BY innings_id
    """
    df_powerplay = pd.read_sql_query(powerplay_query, conn, params=(powerplay_overs,))

    if not df_powerplay.empty:
        fig_powerplay = go.Figure()
        for index, row in df_powerplay.iterrows():
            fig_powerplay.add_trace(go.Bar(
                name=f'Innings {row["innings_id"]}',
                x=['Runs', 'Wickets'],
                y=[row['powerplay_runs'], row['powerplay_wickets']],
                text=[row['powerplay_runs'], row['powerplay_wickets']],
                textposition='auto'
            ))
        fig_powerplay.update_layout(
            title="Powerplay Performance",
            yaxis_title="Score",
            template="plotly_dark"
        )
        st.plotly_chart(fig_powerplay, use_container_width=True)
    else:
        st.info("Powerplay data not available yet.")

    # --- Economy Rate of Bowlers ---
    st.subheader("Bowlers' Economy Rate")
    economy_rate_query = """
    SELECT 
        bowler_participant_id,
        bowler_short_name,
        SUM(runs_bat + wides + no_balls) as runs_conceded,
        COUNT(DISTINCT over_number) as overs_bowled
    FROM ball_by_ball
    WHERE bowler_participant_id IS NOT NULL
    GROUP BY bowler_participant_id, bowler_short_name
    """
    df_economy = pd.read_sql_query(economy_rate_query, conn)

    if not df_economy.empty:
        df_economy['economy_rate'] = df_economy['runs_conceded'] / df_economy['overs_bowled']
        fig_economy = px.bar(
            df_economy,
            x='bowler_short_name',
            y='economy_rate',
            title="Bowlers' Economy Rate",
            labels={'bowler_short_name': 'Bowler', 'economy_rate': 'Economy Rate'},
            template="plotly_dark"
        )
        st.plotly_chart(fig_economy, use_container_width=True)
    else:
        st.info("Bowling data not available yet.")

    # --- Boundary Distribution ---
    st.subheader("Boundary Distribution")
    boundary_distribution_query = """
    SELECT
        CASE
            WHEN runs_bat BETWEEN 4 AND 5 THEN '4s'
            WHEN runs_bat = 6 THEN '6s'
            ELSE 'Other'
        END as boundary_type,
        COUNT(*) as count
    FROM ball_by_ball
    WHERE runs_bat >= 4
    GROUP BY boundary_type
    """
    df_boundary = pd.read_sql_query(boundary_distribution_query, conn)

    if not df_boundary.empty:
        fig_boundary = px.pie(
            df_boundary,
            names='boundary_type',
            values='count',
            title="Boundary Distribution",
            template="plotly_dark"
        )
        st.plotly_chart(fig_boundary, use_container_width=True)
    else:
        st.info("No boundaries have been scored yet.")

# -------------------------------------------------
#   TEAM-BASED KPIs (Side-by-Side Columns)
# -------------------------------------------------
# ... [Rest of the code remains the same]

# -------------------------------
#   TEAM-BASED KPIs (Side-by-Side Columns)
# -------------------------------

st.header("Team-Based KPIs")

if match_id:
    # 1) Identify the two teams from the DB
    teams_df = pd.read_sql_query(
        "SELECT id, display_name FROM teams WHERE match_id = ? LIMIT 2",
        conn,
        params=(match_id,)
    )
    if len(teams_df) == 2:
        # Assign aliases for clarity
        team_a_id = teams_df.iloc[0]['id']
        team_b_id = teams_df.iloc[1]['id']
        team_a_name = teams_df.iloc[0]['display_name']
        team_b_name = teams_df.iloc[1]['display_name']

        # Create 2 columns
        col1, col2 = st.columns(2)

        # ---- Team A KPIs ----
        with col1:
            st.subheader(team_a_name)

            # Dot Ball % for Team A
            dot_query_a = """
            SELECT
                SUM(
                    CASE WHEN (bb.runs_bat = 0 AND bb.wides = 0 AND bb.no_balls = 0
                               AND bb.leg_byes = 0 AND bb.byes = 0)
                    THEN 1 ELSE 0 END
                ) AS dot_balls,
                COUNT(*) AS total_balls
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ? AND bb.wides = 0 AND bb.no_balls = 0
            """
            df_dot_a = pd.read_sql_query(dot_query_a, conn, params=(team_a_id,))
            if not df_dot_a.empty and df_dot_a['total_balls'][0] > 0:
                dot_pct_a = (df_dot_a['dot_balls'][0] / df_dot_a['total_balls'][0]) * 100
            else:
                dot_pct_a = 0
            st.metric(label="Dot Ball %", value=f"{dot_pct_a:.2f}%")

            # Boundary % for Team A
            boundary_query_a = """
            SELECT
                SUM(
                    CASE WHEN bb.runs_bat >= 4 THEN 1 ELSE 0 END
                ) AS boundaries,
                COUNT(*) AS total_balls
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ? AND bb.wides = 0 AND bb.no_balls = 0
            """
            df_boundary_a = pd.read_sql_query(boundary_query_a, conn, params=(team_a_id,))
            if not df_boundary_a.empty and df_boundary_a['total_balls'][0] > 0:
                boundary_pct_a = (df_boundary_a['boundaries'][0] / df_boundary_a['total_balls'][0]) * 100
            else:
                boundary_pct_a = 0
            st.metric(label="Boundary %", value=f"{boundary_pct_a:.2f}%")

            # Partnerships for Team A
            partnership_query_a = """
            SELECT
                striker_short_name || ' & ' || non_striker_short_name AS partnership,
                SUM(runs_bat) AS partnership_runs
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ?
            GROUP BY striker_short_name, non_striker_short_name
            ORDER BY partnership_runs DESC
            LIMIT 5
            """
            df_partnership_a = pd.read_sql_query(partnership_query_a, conn, params=(team_a_id,))
            st.write("Top Partnerships:")
            st.dataframe(df_partnership_a)

            # Average Runs and Wickets per 5 Overs for Team A
            five_over_query_a = """
            SELECT
                (over_number / 5) * 5 AS over_interval,
                AVG(runs_bat) AS avg_runs_per_over,
                AVG(CASE WHEN short_description LIKE '%OUT%' THEN 1 ELSE 0 END) AS avg_wickets_per_over
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ?
            GROUP BY over_interval
            ORDER BY over_interval
            """
            df_five_over_a = pd.read_sql_query(five_over_query_a, conn, params=(team_a_id,))
            st.write("Average Runs and Wickets per 5 Overs:")
            st.dataframe(df_five_over_a)

        # ---- Team B KPIs ----
        with col2:
            st.subheader(team_b_name)

            # Dot Ball % for Team B
            dot_query_b = """
            SELECT
                SUM(
                    CASE WHEN (bb.runs_bat = 0 AND bb.wides = 0 AND bb.no_balls = 0
                               AND bb.leg_byes = 0 AND bb.byes = 0)
                    THEN 1 ELSE 0 END
                ) AS dot_balls,
                COUNT(*) AS total_balls
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ? AND bb.wides = 0 AND bb.no_balls = 0
            """
            df_dot_b = pd.read_sql_query(dot_query_b, conn, params=(team_b_id,))
            if not df_dot_b.empty and df_dot_b['total_balls'][0] > 0:
                dot_pct_b = (df_dot_b['dot_balls'][0] / df_dot_b['total_balls'][0]) * 100
            else:
                dot_pct_b = 0
            st.metric(label="Dot Ball %", value=f"{dot_pct_b:.2f}%")

            # Boundary % for Team B
            boundary_query_b = """
            SELECT
                SUM(
                    CASE WHEN bb.runs_bat >= 4 THEN 1 ELSE 0 END
                ) AS boundaries,
                COUNT(*) AS total_balls
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ? AND bb.wides = 0 AND bb.no_balls = 0
            """
            df_boundary_b = pd.read_sql_query(boundary_query_b, conn, params=(team_b_id,))
            if not df_boundary_b.empty and df_boundary_b['total_balls'][0] > 0:
                boundary_pct_b = (df_boundary_b['boundaries'][0] / df_boundary_b['total_balls'][0]) * 100
            else:
                boundary_pct_b = 0
            st.metric(label="Boundary %", value=f"{boundary_pct_b:.2f}%")

            # Partnerships for Team B
            partnership_query_b = """
            SELECT
                striker_short_name || ' & ' || non_striker_short_name AS partnership,
                SUM(runs_bat) AS partnership_runs
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ?
            GROUP BY striker_short_name, non_striker_short_name
            ORDER BY partnership_runs DESC
            LIMIT 5
            """
            df_partnership_b = pd.read_sql_query(partnership_query_b, conn, params=(team_b_id,))
            st.write("Top Partnerships:")
            st.dataframe(df_partnership_b)

            # Average Runs and Wickets per 5 Overs for Team B
            five_over_query_b = """
            SELECT
                (over_number / 5) * 5 AS over_interval,
                AVG(runs_bat) AS avg_runs_per_over,
                AVG(CASE WHEN short_description LIKE '%OUT%' THEN 1 ELSE 0 END) AS avg_wickets_per_over
            FROM ball_by_ball AS bb
            JOIN innings AS inn ON bb.innings_id = inn.id
            WHERE inn.batting_team_id = ?
            GROUP BY over_interval
            ORDER BY over_interval
            """
            df_five_over_b = pd.read_sql_query(five_over_query_b, conn, params=(team_b_id,))
            st.write("Average Runs and Wickets per 5 Overs:")
            st.dataframe(df_five_over_b)

    else:
        st.info("Could not find two teams in the database yet. Wait for data to load.")
else:
    st.info("Enter a valid match URL to see team-based KPIs.")