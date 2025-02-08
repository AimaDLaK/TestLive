# rds_queries.py
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import sys
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """
    Establish a connection to the RDS PostgreSQL database via SQLAlchemy engine.
    Returns the engine (not a direct connection).
    """
    try:
        host = 'cricket-db.cl06e4yca5zc.ap-southeast-2.rds.amazonaws.com'
        port = 5432
        user = 'postgres'
        password = '34poh2aGaHybsKxPB07B'
        database = 'cricket_rds'

        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        logging.info("Successfully connected to the RDS PostgreSQL database.")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        sys.exit(1)


def get_historical_batting_averages(engine, grade_like_pattern):
    """
    Retrieves historical batting KPI averages for winning and losing teams per phase.
    Metrics computed:
      - Dot Balls (%)
      - Singles (%)
      - Boundaries (%)
      - Boundaries (First & Last Ball) (%)
      - Strike Rate (runs per 100 balls)
      - Run Rate (runs per over)
      - Wickets Lost (% of balls resulting in wicket)

    The query uses the ball_by_ball table (with a window function to detect wicket events via progress_wickets)
    and joins with innings, matches, teams, and grades.
    """
    query = text("""
    WITH phase_balls AS (
        SELECT
            hbb.innings_id,
            CASE WHEN t.is_winner = 1 THEN TRUE ELSE FALSE END AS is_winner,
            CASE
                WHEN hbb.over_number BETWEEN 0 AND 9 THEN 'Powerplay'
                WHEN hbb.over_number BETWEEN 10 AND 39 THEN 'Middle'
                ELSE 'Death'
            END AS phase,
            hbb.batting_team_id,
            COALESCE(hbb.runs_bat, 0) AS runs_bat,
            COALESCE(hbb.wides, 0) AS wides,
            COALESCE(hbb.no_balls, 0) AS no_balls,
            hbb.ball_display_number,
            hbb.progress_wickets,
            LAG(hbb.progress_wickets) OVER (PARTITION BY hbb.innings_id ORDER BY hbb.over_number, hbb.ball_display_number) AS prev_progress_wickets
        FROM public.ball_by_ball hbb
        JOIN public.innings i ON hbb.innings_id = i.id
        JOIN public.matches m ON i.match_id = m.id
        JOIN public.teams t ON hbb.batting_team_id = t.id AND m.id = t.match_id
        JOIN public.grades g ON m.grade_id = g.id
        WHERE LOWER(g.name) LIKE LOWER(:grade_like_pattern)
    ),
    phase_balls_wickets AS (
        SELECT *,
            CASE 
                WHEN prev_progress_wickets IS NOT NULL AND progress_wickets > prev_progress_wickets 
                     THEN progress_wickets - prev_progress_wickets
                ELSE 0
            END AS wicket_fell
        FROM phase_balls
    ),
    inner_group AS (
        SELECT
            phase,
            is_winner,
            batting_team_id,
            COUNT(*) AS total_balls,
            SUM(CASE WHEN runs_bat = 0 AND wides = 0 AND no_balls = 0 THEN 1 ELSE 0 END) AS dot_balls,
            SUM(CASE WHEN runs_bat = 1 THEN 1 ELSE 0 END) AS singles,
            SUM(CASE WHEN runs_bat IN (4,6) THEN 1 ELSE 0 END) AS boundaries,
            SUM(CASE WHEN runs_bat IN (4,6) AND (ball_display_number = 1 OR ball_display_number = 6) THEN 1 ELSE 0 END) AS boundaries_fl,
            SUM(runs_bat) AS total_runs,
            SUM(wicket_fell) AS wickets_lost
        FROM phase_balls_wickets
        GROUP BY phase, is_winner, batting_team_id
    )
    SELECT
        phase,
        is_winner,
        (SUM(dot_balls) / SUM(total_balls)) * 100 AS avg_dot_pct,
        (SUM(singles) / SUM(total_balls)) * 100 AS avg_singles_pct,
        (SUM(boundaries) / SUM(total_balls)) * 100 AS avg_boundaries_pct,
        (SUM(boundaries_fl) / SUM(total_balls)) * 100 AS avg_boundaries_fl_pct,
        (SUM(total_runs) / SUM(total_balls)) * 100 AS avg_strike_rate,
        (SUM(total_runs) * 6.0 / SUM(total_balls)) AS avg_run_rate,
        (SUM(wickets_lost) / SUM(total_balls)) * 100 AS avg_wickets_lost
    FROM inner_group
    GROUP BY phase, is_winner
    ORDER BY phase, is_winner;
    """)

    try:
        params = {'grade_like_pattern': grade_like_pattern}
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        logging.info(f"Successfully fetched historical batting averages. Rows: {len(df)}")
        logging.info(df)
        return df
    except Exception as e:
        logging.error(f"Error fetching historical batting averages: {e}")
        logging.error(traceback.format_exc())
        return pd.DataFrame()


def get_historical_bowling_averages(engine, grade_like_pattern):
    """
    Retrieves historical bowling KPI averages for winning and losing teams per phase.
    Metrics computed:
      - Dot Balls (%)
      - Singles (%)
      - Boundaries (%)
      - Boundaries (First & Last Ball) (%)
      - Economy Rate (runs conceded per over)

    In this query the batting team’s data is used, but we self-join the teams table to identify the bowling team.
    The total runs conceded are computed as (runs_bat + wides + no_balls).
    """
    query = text("""
    WITH phase_balls AS (
        SELECT
            hbb.innings_id,
            CASE WHEN t.is_winner = 1 THEN TRUE ELSE FALSE END AS is_winner,
            CASE
                WHEN hbb.over_number BETWEEN 0 AND 9 THEN 'Powerplay'
                WHEN hbb.over_number BETWEEN 10 AND 39 THEN 'Middle'
                ELSE 'Death'
            END AS phase,
            hbb.batting_team_id,
            ht2.id AS bowling_team_id,
            COALESCE(hbb.runs_bat, 0) AS runs_bat,
            COALESCE(hbb.wides, 0) AS wides,
            COALESCE(hbb.no_balls, 0) AS no_balls,
            hbb.ball_display_number,
            (COALESCE(hbb.runs_bat,0) + COALESCE(hbb.wides,0) + COALESCE(hbb.no_balls,0)) AS total_runs_conceded
        FROM public.ball_by_ball hbb
        JOIN public.innings i ON hbb.innings_id = i.id
        JOIN public.matches m ON i.match_id = m.id
        JOIN public.teams t ON hbb.batting_team_id = t.id AND m.id = t.match_id
        LEFT JOIN public.teams ht2 ON m.id = ht2.match_id AND ht2.id <> hbb.batting_team_id
        JOIN public.grades g ON m.grade_id = g.id
        WHERE LOWER(g.name) LIKE LOWER(:grade_like_pattern)
    ),
    inner_group AS (
        SELECT
            phase,
            is_winner,
            bowling_team_id,
            COUNT(*) AS total_balls,
            SUM(CASE WHEN runs_bat = 0 AND wides = 0 AND no_balls = 0 THEN 1 ELSE 0 END) AS dot_balls,
            SUM(CASE WHEN runs_bat = 1 THEN 1 ELSE 0 END) AS singles,
            SUM(CASE WHEN runs_bat IN (4,6) THEN 1 ELSE 0 END) AS boundaries,
            SUM(CASE WHEN runs_bat IN (4,6) AND (ball_display_number = 1 OR ball_display_number = 6) THEN 1 ELSE 0 END) AS boundaries_fl,
            SUM(total_runs_conceded) AS total_runs_conceded
        FROM phase_balls
        GROUP BY phase, is_winner, bowling_team_id
    )
    SELECT
        phase,
        is_winner,
        (SUM(dot_balls) / SUM(total_balls)) * 100 AS avg_dot_pct,
        (SUM(singles) / SUM(total_balls)) * 100 AS avg_singles_pct,
        (SUM(boundaries) / SUM(total_balls)) * 100 AS avg_boundaries_pct,
        (SUM(boundaries_fl) / SUM(total_balls)) * 100 AS avg_boundaries_fl_pct,
        (SUM(total_runs_conceded) * 6.0 / SUM(total_balls)) AS avg_economy_rate
    FROM inner_group
    GROUP BY phase, is_winner
    ORDER BY phase, is_winner;
    """)

    try:
        params = {'grade_like_pattern': grade_like_pattern}
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        logging.info(f"Successfully fetched historical bowling averages. Rows: {len(df)}")
        logging.info(df)
        return df
    except Exception as e:
        logging.error(f"Error fetching historical bowling averages: {e}")
        logging.error(traceback.format_exc())
        return pd.DataFrame()


def save_dataframe(df, filename):
    """Save DataFrame to a CSV file for reference."""
    try:
        df.to_csv(filename, index=False)
        logging.info(f"DataFrame saved to {filename}.")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {filename}: {e}")

def main():
    engine = get_db_connection()

    # Example: user can pass a custom pattern, or we can define one here


    # Fetch historical batting averages
    hist_batting_df = get_historical_batting_averages(engine, grade_like_pattern)
    if not hist_batting_df.empty:
        save_dataframe(hist_batting_df, 'historical_batting_averages.csv')
    else:
        logging.warning("Historical batting averages DataFrame is empty. No CSV saved.")

    # Fetch historical bowling averages
    hist_bowling_df = get_historical_bowling_averages(engine, grade_like_pattern)
    if not hist_bowling_df.empty:
        save_dataframe(hist_bowling_df, 'historical_bowling_averages.csv')
    else:
        logging.warning("Historical bowling averages DataFrame is empty. No CSV saved.")

    # Print results to console for debugging/verification
    print("Historical Batting Averages:")
    print(hist_batting_df)

    print("\nHistorical Bowling Averages:")
    print(hist_bowling_df)

if __name__ == "__main__":
    main()
