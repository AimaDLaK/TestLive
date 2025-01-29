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

def create_db():
    try:
        # Connect to SQLite3 database
        cursor = conn.cursor()

        # Create tables (same as original script)
        cursor.executescript('''
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
            start_datetime DATETIME,
            FOREIGN KEY (round_id) REFERENCES rounds(id),
            FOREIGN KEY (grade_id) REFERENCES grades(id),
            FOREIGN KEY (venue_id) REFERENCES venues(id)
        );

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
            FOREIGN KEY (innings_id) REFERENCES innings(id),
            FOREIGN KEY (batting_team_id) REFERENCES teams(id),
            FOREIGN KEY (striker_participant_id) REFERENCES players(id),
            FOREIGN KEY (non_striker_participant_id) REFERENCES players(id),
            FOREIGN KEY (bowler_participant_id) REFERENCES players(id)
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
            playing_surface_id TEXT,
            FOREIGN KEY (playing_surface_id) REFERENCES playing_surfaces(id)
        );

        CREATE TABLE IF NOT EXISTS playing_surfaces (
            id TEXT PRIMARY KEY,
            name TEXT,
            latitude REAL,
            longitude REAL
        );

        CREATE TABLE IF NOT EXISTS teams (
            id TEXT,
            match_id TEXT,
            display_name TEXT,
            result_type_id INTEGER,
            result_type TEXT,
            won_toss BOOLEAN,
            batted_first BOOLEAN,
            is_home BOOLEAN,
            score_text TEXT,
            is_winner BOOLEAN,
            PRIMARY KEY (id, match_id),
            FOREIGN KEY (match_id) REFERENCES matches(id)
        );

        CREATE TABLE IF NOT EXISTS organisations (
            id TEXT PRIMARY KEY,
            name TEXT,
            short_name TEXT,
            logo_url TEXT
        );

        CREATE TABLE IF NOT EXISTS players (
            id TEXT PRIMARY KEY,
            team_id TEXT,
            name TEXT,
            short_name TEXT,
            role TEXT,
            FOREIGN KEY (team_id) REFERENCES teams(id)
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
            number_of_wickets_fallen INTEGER,
            FOREIGN KEY (match_id) REFERENCES matches(id),
            FOREIGN KEY (batting_team_id) REFERENCES teams(id)
        );

        CREATE TABLE IF NOT EXISTS batting_stats (
            id TEXT PRIMARY KEY,
            innings_id TEXT,
            player_id TEXT,
            bat_order INTEGER,
            bat_instance INTEGER,
            balls_faced INTEGER,
            fours_scored INTEGER,
            sixes_scored INTEGER,
            runs_scored INTEGER,
            batting_minutes INTEGER,
            strike_rate REAL,
            dismissal_type_id INTEGER,
            dismissal_type TEXT,
            dismissal_text TEXT,
            FOREIGN KEY (innings_id) REFERENCES innings(id),
            FOREIGN KEY (player_id) REFERENCES players(id)
        );

        CREATE TABLE IF NOT EXISTS bowling_stats (
            id TEXT PRIMARY KEY,
            innings_id TEXT,
            player_id TEXT,
            bowl_order INTEGER,
            overs_bowled REAL,
            maidens_bowled INTEGER,
            runs_conceded INTEGER,
            wickets_taken INTEGER,
            wide_balls INTEGER,
            no_balls INTEGER,
            economy REAL,
            FOREIGN KEY (innings_id) REFERENCES innings(id),
            FOREIGN KEY (player_id) REFERENCES players(id)
        );

        CREATE TABLE IF NOT EXISTS fielding_stats (
            id TEXT PRIMARY KEY,
            innings_id TEXT,
            player_id TEXT,
            catches INTEGER,
            wicket_keeper_catches INTEGER,
            total_catches INTEGER,
            unassisted_run_outs INTEGER,
            assisted_run_outs INTEGER,
            run_outs INTEGER,
            stumpings INTEGER,
            FOREIGN KEY (innings_id) REFERENCES innings(id),
            FOREIGN KEY (player_id) REFERENCES players(id)
        );

        CREATE TABLE IF NOT EXISTS fall_of_wickets (
            id TEXT PRIMARY KEY,
            innings_id TEXT,
            player_id TEXT,
            "order" INTEGER,
            runs INTEGER,
            FOREIGN KEY (innings_id) REFERENCES innings(id),
            FOREIGN KEY (player_id) REFERENCES players(id)
        );

        CREATE TABLE IF NOT EXISTS match_schedule (
            id TEXT PRIMARY KEY,
            match_id TEXT,
            match_day INTEGER,
            start_datetime DATETIME,
            FOREIGN KEY (match_id) REFERENCES matches(id)
        );

        CREATE TABLE IF NOT EXISTS ladder (
            id TEXT PRIMARY KEY,
            grade_id TEXT,
            team_id TEXT,
            rank INTEGER,
            played INTEGER,
            points INTEGER,
            bonus_points INTEGER,
            quotient REAL,
            net_run_rate REAL,
            won INTEGER,
            lost INTEGER,
            ties INTEGER,
            no_results INTEGER,
            byes INTEGER,
            forfeits INTEGER,
            disqualifications INTEGER,
            adjustments INTEGER,
            runs_for INTEGER,
            overs_faced REAL,
            wickets_lost INTEGER,
            runs_against INTEGER,
            overs_bowled REAL,
            wickets_taken INTEGER,
            FOREIGN KEY (grade_id) REFERENCES grades(id),
            FOREIGN KEY (team_id) REFERENCES teams(id)
        );
        ''')

        conn.commit()
        return cursor, conn
    except Exception as e:
        logging.error(f"Error creating database: {e}")
        raise


def insert_match(cursor, match_data, season):
    try:
        match_id = safe_get(match_data, ['id'])
        status = safe_get(match_data, ['status'])
        status_id = safe_get(match_data, ['statusId'])
        team_a = safe_get(match_data, ['matchSummary', 'teams', 0, 'id'])
        team_b = safe_get(match_data, ['matchSummary', 'teams', 1, 'id'])
        match_type = safe_get(match_data, ['matchType'])
        match_type_id = safe_get(match_data, ['matchTypeId'])
        is_ball_by_ball = safe_get(match_data, ['isBallByBall'], False)
        result_text = safe_get(match_data, ['matchSummary', 'resultText'])
        round_id = safe_get(match_data, ['round', 'id'])
        grade_id = safe_get(match_data, ['grade', 'id'])
        venue_id = safe_get(match_data, ['venue', 'id'])
        start_datetime = safe_get(match_data, ['matchSchedule', 0, 'startDateTime'])

        cursor.execute('''
            INSERT OR IGNORE INTO matches (
                id, status, status_id, team_a, team_b, season, match_type, match_type_id, 
                is_ball_by_ball, result_text, round_id, grade_id, venue_id, start_datetime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            match_id,
            status,
            status_id,
            team_a,
            team_b,
            season,
            match_type,
            match_type_id,
            is_ball_by_ball,
            result_text,
            round_id,
            grade_id,
            venue_id,
            start_datetime
        ))
        return is_ball_by_ball
    except Exception as e:
        logging.error(f"Error inserting match data for match ID {match_data.get('id')}: {e}")
        return False


def insert_round(cursor, round_data):
    try:
        round_id = safe_get(round_data, ['id'])
        name = safe_get(round_data, ['name'])
        short_name = safe_get(round_data, ['shortName'])

        cursor.execute('''
            INSERT OR IGNORE INTO rounds (id, name, short_name)
            VALUES (?, ?, ?)
        ''', (
            round_id,
            name,
            short_name
        ))
    except Exception as e:
        logging.error(f"Error inserting round data: {e}")


def insert_grade(cursor, grade_data):
    try:
        grade_id = safe_get(grade_data, ['id'])
        name = safe_get(grade_data, ['name'])

        cursor.execute('''
            INSERT OR IGNORE INTO grades (id, name)
            VALUES (?, ?)
        ''', (
            grade_id,
            name
        ))
    except Exception as e:
        logging.error(f"Error inserting grade data: {e}")


def insert_venue(cursor, venue_data):
    try:
        venue_id = safe_get(venue_data, ['id'])
        if venue_id:
            name = safe_get(venue_data, ['name'])
            line1 = safe_get(venue_data, ['line1'])
            suburb = safe_get(venue_data, ['suburb'])
            post_code = safe_get(venue_data, ['postCode'])
            state_name = safe_get(venue_data, ['stateName'])
            country = safe_get(venue_data, ['country'])
            playing_surface_id = safe_get(venue_data, ['playingSurface', 'id'])

            cursor.execute('''
                INSERT OR IGNORE INTO venues (
                    id, name, line1, suburb, post_code, state_name, country, playing_surface_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                venue_id,
                name,
                line1,
                suburb,
                post_code,
                state_name,
                country,
                playing_surface_id
            ))
    except Exception as e:
        logging.error(f"Error inserting venue data for venue ID {venue_data.get('id')}: {e}")


def insert_playing_surface(cursor, surface_data):
    try:
        surface_id = safe_get(surface_data, ['id'])
        if surface_id:
            name = safe_get(surface_data, ['name'])
            latitude = safe_get(surface_data, ['latitude'])
            longitude = safe_get(surface_data, ['longitude'])

            cursor.execute('''
                INSERT OR IGNORE INTO playing_surfaces (
                    id, name, latitude, longitude
                ) VALUES (?, ?, ?, ?)
            ''', (
                surface_id,
                name,
                latitude,
                longitude
            ))
    except Exception as e:
        logging.error(f"Error inserting playing surface data for surface ID {surface_data.get('id')}: {e}")


def insert_teams(cursor, match_id, teams_data):
    try:
        for team in teams_data:
            team_id = safe_get(team, ['id'])
            display_name = safe_get(team, ['displayName'])
            result_type_id = safe_get(team, ['resultTypeId'])
            result_type = safe_get(team, ['resultType'])
            won_toss = safe_get(team, ['wonToss'], False)
            batted_first = safe_get(team, ['battedFirst'], False)
            is_home = safe_get(team, ['isHome'], False)
            score_text = safe_get(team, ['scoreText'])
            is_winner = safe_get(team, ['isWinner'], False)

            cursor.execute('''
                INSERT OR IGNORE INTO teams (
                    id, match_id, display_name, 
                    result_type_id, result_type, won_toss, batted_first, 
                    is_home, score_text, is_winner
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                team_id,
                match_id,
                display_name,
                result_type_id,
                result_type,
                won_toss,
                batted_first,
                is_home,
                score_text,
                is_winner
            ))
    except Exception as e:
        logging.error(f"Error inserting teams for match ID {match_id}: {e}")


def insert_organisations(cursor, organisation_data):
    try:
        org_id = safe_get(organisation_data, ['id'])
        if org_id:
            name = safe_get(organisation_data, ['name'])
            short_name = safe_get(organisation_data, ['shortName'])
            logo_url = safe_get(organisation_data, ['logoUrl'])

            cursor.execute('''
                INSERT OR IGNORE INTO organisations (id, name, short_name, logo_url)
                VALUES (?, ?, ?, ?)
            ''', (
                org_id,
                name,
                short_name,
                logo_url
            ))
    except Exception as e:
        logging.error(f"Error inserting organisation data for organisation ID {organisation_data.get('id')}: {e}")


def insert_players(cursor, team_id, players_data):
    try:
        for player in players_data:
            player_id = safe_get(player, ['participantId'])
            name = safe_get(player, ['name'])
            short_name = safe_get(player, ['shortName'])
            roles = safe_get(player, ['roles'], [])
            role = "--".join(roles) if roles else None

            cursor.execute('''
                INSERT OR IGNORE INTO players (id, team_id, name, short_name, role)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                player_id,
                team_id,
                name,
                short_name,
                role,
            ))
    except Exception as e:
        logging.error(f"Error inserting players for team ID {team_id}: {e}")


def insert_innings(cursor, match_id, innings_data):
    try:
        for innings in innings_data:
            innings_id = safe_get(innings, ['id'])
            name = safe_get(innings, ['name'])
            innings_close_type = safe_get(innings, ['inningsCloseType'])
            innings_number = safe_get(innings, ['inningsNumber'])
            innings_order = safe_get(innings, ['inningsOrder'])
            batting_team_id = safe_get(innings, ['battingTeamId'])
            is_declared = safe_get(innings, ['isDeclared'], False)
            is_follow_on = safe_get(innings, ['isFollowOn'], False)
            byes_runs = safe_get(innings, ['byesRuns'], 0)
            leg_byes_runs = safe_get(innings, ['legByesRuns'], 0)
            no_balls = safe_get(innings, ['noBalls'], 0)
            wide_balls = safe_get(innings, ['wideBalls'], 0)
            penalties = safe_get(innings, ['penalties'], 0)
            total_extras = safe_get(innings, ['totalExtras'], 0)
            overs_bowled = safe_get(innings, ['oversBowled'], 0.0)
            runs_scored = safe_get(innings, ['runsScored'], 0)
            number_of_wickets_fallen = safe_get(innings, ['numberOfWicketsFallen'], 0)

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
                name,
                innings_close_type,
                innings_number,
                innings_order,
                batting_team_id,
                is_declared,
                is_follow_on,
                byes_runs,
                leg_byes_runs,
                no_balls,
                wide_balls,
                penalties,
                total_extras,
                overs_bowled,
                runs_scored,
                number_of_wickets_fallen
            ))
    except Exception as e:
        logging.error(f"Error inserting innings data for match ID {match_id}: {e}")


def insert_batting_stats(cursor, innings_id, batting_data):
    try:
        for batting in batting_data:
            participant_id = safe_get(batting, ['participantId'])
            if not participant_id:
                continue  # Skip if participant ID is missing

            bat_order = safe_get(batting, ['batOrder'])
            bat_instance = safe_get(batting, ['batInstance'])
            balls_faced = safe_get(batting, ['ballsFaced'], 0)
            fours_scored = safe_get(batting, ['foursScored'], 0)
            sixes_scored = safe_get(batting, ['sixesScored'], 0)
            runs_scored = safe_get(batting, ['runsScored'], 0)
            batting_minutes = safe_get(batting, ['battingMinutes'], 0)
            strike_rate = safe_get(batting, ['strikeRate'], 0.0)
            dismissal_type_id = safe_get(batting, ['dismissalTypeId'])
            dismissal_type = safe_get(batting, ['dismissalType'])
            dismissal_text = safe_get(batting, ['dismissalText'])

            cursor.execute('''
                INSERT OR IGNORE INTO batting_stats (
                    id, innings_id, player_id, bat_order, bat_instance, balls_faced,
                    fours_scored, sixes_scored, runs_scored, batting_minutes, 
                    strike_rate, dismissal_type_id, dismissal_type, dismissal_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"{innings_id}-{participant_id}",  # Unique ID
                innings_id,
                participant_id,
                bat_order,
                bat_instance,
                balls_faced,
                fours_scored,
                sixes_scored,
                runs_scored,
                batting_minutes,
                strike_rate,
                dismissal_type_id,
                dismissal_type,
                dismissal_text
            ))
    except Exception as e:
        logging.error(f"Error inserting batting stats for innings ID {innings_id}: {e}")


def insert_bowling_stats(cursor, innings_id, bowling_data):
    try:
        for bowling in bowling_data:
            participant_id = safe_get(bowling, ['participantId'])
            if not participant_id:
                continue  # Skip if participant ID is missing

            bowl_order = safe_get(bowling, ['bowlOrder'])
            overs_bowled = safe_get(bowling, ['oversBowled'], 0.0)
            maidens_bowled = safe_get(bowling, ['maidensBowled'], 0)
            runs_conceded = safe_get(bowling, ['runsConceded'], 0)
            wickets_taken = safe_get(bowling, ['wicketsTaken'], 0)
            wide_balls = safe_get(bowling, ['wideBalls'], 0)
            no_balls = safe_get(bowling, ['noBalls'], 0)
            economy = safe_get(bowling, ['economy'], 0.0)

            cursor.execute('''
                INSERT OR IGNORE INTO bowling_stats (
                    id, innings_id, player_id, bowl_order, overs_bowled, maidens_bowled,
                    runs_conceded, wickets_taken, wide_balls, no_balls, economy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"{innings_id}-{participant_id}",  # Unique ID
                innings_id,
                participant_id,
                bowl_order,
                overs_bowled,
                maidens_bowled,
                runs_conceded,
                wickets_taken,
                wide_balls,
                no_balls,
                economy
            ))
    except Exception as e:
        logging.error(f"Error inserting bowling stats for innings ID {innings_id}: {e}")


def insert_fielding_stats(cursor, innings_id, fielding_data):
    try:
        for fielding in fielding_data:
            participant_id = safe_get(fielding, ['participantId'])
            if not participant_id:
                continue  # Skip if participant ID is missing

            catches = safe_get(fielding, ['catches'], 0)
            wicket_keeper_catches = safe_get(fielding, ['wicketKeeperCatches'], 0)
            total_catches = safe_get(fielding, ['totalCatches'], 0)
            unassisted_run_outs = safe_get(fielding, ['unassistedRunOuts'], 0)
            assisted_run_outs = safe_get(fielding, ['assistedRunOuts'], 0)
            run_outs = safe_get(fielding, ['runOuts'], 0)
            stumpings = safe_get(fielding, ['stumpings'], 0)

            cursor.execute('''
                INSERT OR IGNORE INTO fielding_stats (
                    id, innings_id, player_id, catches, wicket_keeper_catches, 
                    total_catches, unassisted_run_outs, assisted_run_outs, 
                    run_outs, stumpings
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"{innings_id}-{participant_id}",  # Unique ID
                innings_id,
                participant_id,
                catches,
                wicket_keeper_catches,
                total_catches,
                unassisted_run_outs,
                assisted_run_outs,
                run_outs,
                stumpings
            ))
    except Exception as e:
        logging.error(f"Error inserting fielding stats for innings ID {innings_id}: {e}")


def insert_fall_of_wickets(cursor, innings_id, fall_of_wickets_data):
    try:
        for fall in fall_of_wickets_data:
            participant_id = safe_get(fall, ['participantId'])
            if not participant_id:
                continue  # Skip if participant ID is missing

            order = safe_get(fall, ['order'], 0)
            runs = safe_get(fall, ['runs'], 0)

            cursor.execute('''
                INSERT OR IGNORE INTO fall_of_wickets (
                    id, innings_id, player_id, "order", runs
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                f"{innings_id}-{participant_id}",  # Unique ID
                innings_id,
                participant_id,
                order,
                runs
            ))
    except Exception as e:
        logging.error(f"Error inserting fall of wickets for innings ID {innings_id}: {e}")


def insert_match_schedule(cursor, match_id, schedule_data):
    try:
        for schedule in schedule_data:
            schedule_id = safe_get(schedule, ['id'])
            match_day = safe_get(schedule, ['matchDay'])
            start_datetime = safe_get(schedule, ['startDateTime'])

            cursor.execute('''
                INSERT OR IGNORE INTO match_schedule (
                    id, match_id, match_day, start_datetime
                ) VALUES (?, ?, ?, ?)
            ''', (
                f"{match_id}-{match_day}",  # Unique ID
                match_id,
                match_day,
                start_datetime
            ))
    except Exception as e:
        logging.error(f"Error inserting match schedule for match ID {match_id}: {e}")


def insert_ball_by_ball(cursor, ball, innings_id, fetched_time,innings_data):
    try:
        ball_id = safe_get(ball, ['id'])
        if not ball_id:
            logging.warning("Ball without ID encountered. Skipping insertion.")
            return

        # Check if the ball already exists
        cursor.execute("SELECT id FROM ball_by_ball WHERE id = ?", (ball_id,))
        if cursor.fetchone():
            logging.info(f"Ball ID {ball_id} already exists. Skipping insertion.")
            return

        # Insert the ball
        cursor.execute('''
                            INSERT INTO ball_by_ball (
                                id, innings_id, innings_number, innings_order, innings_name,
                                batting_team_id, progress_runs, progress_wickets, progress_score,
                                striker_participant_id, striker_short_name, striker_runs_scored, striker_balls_faced,
                                non_striker_participant_id, non_striker_short_name, non_striker_runs_scored, non_striker_balls_faced,
                                bowler_participant_id, bowler_short_name, over_number, ball_display_number, ball_time,
                                runs_bat, wides, no_balls, leg_byes, byes, penalty_runs, short_description, description
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
            safe_get(ball, ['id']),
            safe_get(innings_data, ['id']),
            safe_get(innings_data, ['inningsNumber']),
            safe_get(innings_data, ['inningsOrder']),
            safe_get(innings_data, ['inningsName']),
            safe_get(innings_data, ['battingTeamId']),
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
            safe_get(ball, ['description'])
        ))
        logging.info(f"Inserted new ball with ID {ball_id} at {safe_get(ball, ['ballTime'])}")
    except Exception as e:
        logging.error(f"Error inserting ball ID {ball_id}: {e}")

def do_insertion(cursor, conn, match_json, match_id, season):
    try:
        # Remove ball-by-ball insertion from here
        insert_round(cursor, safe_get(match_json, ['round']))
        insert_grade(cursor, safe_get(match_json, ['grade']))
        insert_playing_surface(cursor, safe_get(match_json, ['venue', 'playingSurface'], {}))
        insert_venue(cursor, safe_get(match_json, ['venue'], {}))
        insert_match(cursor, match_json, season)  # No longer need to check is_ball_by_ball here

        insert_teams(cursor, match_id, safe_get(match_json, ['matchSummary', 'teams'], []))

        # Insert organisations
        organisations = safe_get(match_json, ['teams'], [])
        for team_org in organisations:
            insert_organisations(cursor, safe_get(team_org, ['owningOrganisation'], {}))

        # Insert players
        teams = safe_get(match_json, ['teams'], [])
        for team in teams:
            team_id = safe_get(team, ['id'])
            players = safe_get(team, ['players'], [])
            insert_players(cursor, team_id, players)

        # Insert innings structure without stats
        innings_list = safe_get(match_json, ['innings'], [])
        insert_innings(cursor, match_id, innings_list)

        # Insert match schedule
        insert_match_schedule(cursor, match_id, safe_get(match_json, ['matchSchedule'], []))

        conn.commit()
    except Exception as e:
        logging.error(f"Error during insertion for match ID {match_id}: {e}")
        conn.rollback()
        raise

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


def fill_a_first_db(match_id, season_name, conn,match_json,cursor):
    do_insertion(cursor, conn, match_json, match_id, season_name)


def calculate_bowling_partnerships(conn, bowler_map):

    query = """
        SELECT
            innings_id,
            over_number,
            bowler_participant_id,
            bowler_short_name,
            progress_wickets,
            runs_bat,
            wides,
            no_balls,
            ball_display_number
        FROM ball_by_ball
        ORDER BY innings_id, over_number, ball_display_number
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        logging.warning("No bowling data found in 'ball_by_ball' table.")
        return pd.DataFrame(columns=['Bowler 1', 'Bowler 2', 'Wickets', 'Dot Ball %', 'Economy Rate'])

    # Compute 'runs_conceded'
    df['runs_conceded'] = df['runs_bat'] + df['wides'] + df['no_balls']

    # Compute 'wickets_taken' as the difference in 'progress_wickets' within the same innings
    df['wickets_taken'] = df.groupby('innings_id')['progress_wickets'].diff().fillna(df['progress_wickets']).astype(int)
    df['wickets_taken'] = df['wickets_taken'].apply(lambda x: x if x >= 0 else 0)

    # Shift the bowler to get the next bowler within the same innings
    df['next_bowler_id'] = df.groupby('innings_id')['bowler_participant_id'].shift(-1)
    df['next_bowler_name'] = df.groupby('innings_id')['bowler_short_name'].shift(-1)

    # Exclude rows where bowler IDs or names are None
    partnerships = df.dropna(subset=['bowler_participant_id', 'next_bowler_id', 'bowler_short_name', 'next_bowler_name'])

    # Further exclude rows where the next bowler is the same as the current bowler
    partnerships = partnerships[partnerships['bowler_participant_id'] != partnerships['next_bowler_id']]

    # Now, group by bowler pairs
    partnerships['bowler_pair'] = partnerships.apply(
        lambda row: tuple(sorted([row['bowler_participant_id'], row['next_bowler_id']])), axis=1
    )

    # Aggregate partnership statistics
    partnership_stats = partnerships.groupby('bowler_pair').agg(
        Wickets=('wickets_taken', 'sum'),
        Dot_Balls=('runs_conceded', lambda x: (x == 0).sum()),
        Total_Balls=('runs_conceded', 'count'),
        Runs_Conceded=('runs_conceded', 'sum')
    ).reset_index()

    # Calculate Dot Ball Percentage and Economy Rate
    partnership_stats['Dot_Ball_%'] = (partnership_stats['Dot_Balls'] / partnership_stats['Total_Balls']) * 100
    partnership_stats['Economy_Rate'] = (partnership_stats['Runs_Conceded'] / partnership_stats['Total_Balls']) * 6  # Per over

    # Map bowler IDs to names using bowler_map
    partnership_stats['Bowler 1'] = partnership_stats['bowler_pair'].apply(lambda x: bowler_map.get(x[0], f"Bowler {x[0]}"))
    partnership_stats['Bowler 2'] = partnership_stats['bowler_pair'].apply(lambda x: bowler_map.get(x[1], f"Bowler {x[1]}"))

    # Select and reorder columns
    final_df = partnership_stats[['Bowler 1', 'Bowler 2', 'Wickets', 'Dot_Ball_%', 'Economy_Rate']]

    return final_df


def identify_best_worst_partnerships(partnerships_df, top_n=20):

    if partnerships_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Normalize the metrics with safeguards against division by zero
    partnerships_df['Wickets_norm'] = partnerships_df['Wickets'].apply(
        lambda x: (x - partnerships_df['Wickets'].min()) / (partnerships_df['Wickets'].max() - partnerships_df['Wickets'].min())
        if partnerships_df['Wickets'].max() != partnerships_df['Wickets'].min() else 0
    )
    partnerships_df['Dot_Ball_%_norm'] = partnerships_df['Dot_Ball_%'].apply(
        lambda x: (x - partnerships_df['Dot_Ball_%'].min()) / (partnerships_df['Dot_Ball_%'].max() - partnerships_df['Dot_Ball_%'].min())
        if partnerships_df['Dot_Ball_%'].max() != partnerships_df['Dot_Ball_%'].min() else 0
    )
    partnerships_df['Economy_Rate_norm'] = partnerships_df['Economy_Rate'].apply(
        lambda x: 1 - (x - partnerships_df['Economy_Rate'].min()) / (partnerships_df['Economy_Rate'].max() - partnerships_df['Economy_Rate'].min())
        if partnerships_df['Economy_Rate'].max() != partnerships_df['Economy_Rate'].min() else 0
    )

    # Compute a composite score
    partnerships_df['Composite_Score'] = partnerships_df[['Wickets_norm', 'Dot_Ball_%_norm', 'Economy_Rate_norm']].mean(axis=1)

    # Best partnerships
    best_partnerships = partnerships_df.nlargest(top_n, 'Composite_Score')

    # Worst partnerships
    worst_partnerships = partnerships_df.nsmallest(top_n, 'Composite_Score')

    return best_partnerships, worst_partnerships


def visualize_bowling_partnerships(best_df, worst_df):
    """
    Visualize the best and worst bowling partnerships.
    """
    st.subheader("Bowling Partnerships")

    if best_df.empty and worst_df.empty:
        st.info("No bowling partnerships data available.")
        return

    # Display Best Partnerships
    if not best_df.empty:
        st.markdown("### Best Partnerships")
        st.dataframe(best_df[['Bowler 1', 'Bowler 2', 'Wickets', 'Dot_Ball_%', 'Economy_Rate']])

        # Plot Best Partnerships


    # Display Worst Partnerships
    if not worst_df.empty:
        st.markdown("### Worst Partnerships")
        st.dataframe(worst_df[['Bowler 1', 'Bowler 2', 'Wickets', 'Dot_Ball_%', 'Economy_Rate']])

        # Plot Worst Partnerships



def get_bowler_map(conn):
    """
    Returns a dictionary mapping {bowler_participant_id: bowler_short_name} for all bowlers in the match.
    """
    query = """
        SELECT DISTINCT bowler_participant_id, bowler_short_name
        FROM ball_by_ball
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        logging.warning("No bowler data found in 'ball_by_ball' table.")
        return {}

    bowler_map = {row['bowler_participant_id']: row['bowler_short_name'] for _, row in df.iterrows()}
    return bowler_map

def fetch_and_store_ball_data(conn, match_id):
    cursor = conn.cursor()
    try:
        # Fetch latest ball-by-ball data
        balls_url = f"https://grassrootsapiproxy.cricket.com.au/scores/matches/{match_id}/balls?jsconfig=eccn%3Atrue"
        data = call_api(balls_url)

        if not data:
            logging.warning(f"No ball data returned for match ID {match_id}")
            return
        do_insertion(cursor, conn, data, match_id, "N/A")
        # Process innings and balls
        for innings in safe_get(data, ['innings'], []):
            innings_id = safe_get(innings, ['id'])

            # Update innings metadata if needed
            cursor.execute('''
                UPDATE innings SET
                    name = ?,
                    innings_close_type = ?,
                    batting_team_id = ?,
                    runs_scored = ?,
                    number_of_wickets_fallen = ?
                WHERE id = ?
            ''', (
                safe_get(innings, ['name']),
                safe_get(innings, ['inningsCloseType']),
                safe_get(innings, ['battingTeamId']),
                safe_get(innings, ['runsScored'], 0),
                safe_get(innings, ['numberOfWicketsFallen'], 0),
                innings_id
            ))

            # Insert/update batting, bowling, and fielding stats
            insert_batting_stats(cursor, innings_id, safe_get(innings, ['batting'], []))
            insert_bowling_stats(cursor, innings_id, safe_get(innings, ['bowling'], []))
            insert_fielding_stats(cursor, innings_id, safe_get(innings, ['fielding'], []))
            insert_fall_of_wickets(cursor, innings_id, safe_get(innings, ['fallOfWickets'], []))

            # Process individual balls
            for ball in safe_get(innings, ['balls'], []):
                insert_ball_by_ball(cursor, ball, innings_id, datetime.utcnow(),innings)

        conn.commit()
        logging.info(f"Ball-by-ball data updated for match ID {match_id}")

    except Exception as e:
        logging.error(f"Error in ball-by-ball update: {e}")
        conn.rollback()

def fetch_initial_data(match_id, conn, season="N/A"):
    """Fetch and store all non-ball data using existing insertion functions"""
    try:
        cursor = conn.cursor()

        # Fetch main match data
        match_url = f"https://grassrootsapiproxy.cricket.com.au/scores/matches/{match_id}?jsconfig=eccn%3Atrue"
        match_data = call_api(match_url)

        # Fetch additional required data
        teams_data = safe_get(match_data, ['matchSummary', 'teams'], [])
        schedule_data = safe_get(match_data, ['matchSchedule'], [])
        # Combine data into match_json structure expected by do_insertion
        combined_data = {
            **match_data,
            "teams": teams_data,  # Directly use the teams list
            "matchSchedule": schedule_data,  # Directly use the schedule list
            "innings": []
        }

        # Perform all database insertions
        do_insertion(cursor, conn, combined_data, match_id, season)

        logging.info(f"Initial data loaded for match ID {match_id}")
        return True
    except Exception as e:
        logging.error(f"Failed to load initial data: {e}")
        conn.rollback()
        return False
async def async_live_data_fetcher(conn, match_id, stop_event):
    while not stop_event.is_set():
        try:
            fetch_and_store_ball_data(conn, match_id)
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"Async ball data fetch error: {str(e)}")
            await asyncio.sleep(5)  # Prevent tight loop on errors


def run_async_loop(loop):
    """Run the asyncio event loop in a background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()
st.set_page_config(layout="wide", page_title="Decidr - The Decisions Maker", page_icon="🏏")

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
def get_db_connection():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    return conn


conn = get_db_connection()
create_db()

# User input
match_url = st.text_input("Enter match URL:", "")
start_button = st.button("Start Live")
stop_button = st.button("Stop")

match_id = extract_match_id(match_url) if match_url else None

if start_button:
    if not match_id:
        st.error("Invalid match URL.")
    elif st.session_state.live_task and not st.session_state.live_task.done():
        st.warning("Live data collection is already running.")
    else:
        try:
            # Clear existing data
            clear_database(conn)

            # Step 1: Fetch and store initial data
            if fetch_initial_data(match_id, conn):
                st.success("Initial match data loaded successfully!")
            else:
                raise Exception("Failed to fetch initial data")

            # Step 2: Start async ball-by-ball updates
            if st.session_state.live_task:
                st.session_state.stop_event.set()
                st.session_state.live_task = None
                st.session_state.stop_event = threading.Event()

            coro = async_live_data_fetcher(conn, match_id, st.session_state.stop_event)
            task = asyncio.run_coroutine_threadsafe(coro, st.session_state.async_loop)
            st.session_state.live_task = task
            st.success("Live ball-by-ball tracking started!")

        except Exception as e:
            st.error(f"Initialization failed: {str(e)}")
            logging.error(f"Initialization error: {str(e)}")

if stop_button:
    if st.session_state.live_task and not st.session_state.live_task.done():
        st.session_state.stop_event.set()
        st.session_state.live_task = None
        st.success("Live data collection stopped.")

        # Clear the try.db database
        clear_database(conn)
        st.info("Database has been cleared.")
    else:
        st.warning("No live data collection is running.")


###############################################################################
# KPI Visualization for Batting Phases
###############################################################################

def get_team_ids_and_names(conn):
    """
    Returns a dictionary mapping {team_id: team_display_name} for all teams in the single match.
    """
    query = """
        SELECT id, display_name
        FROM teams
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        logging.warning("No teams found in the 'teams' table.")
        return {}

    team_map = {row['id']: row['display_name'] for _, row in df.iterrows()}
    return team_map


def calculate_phase_metrics(conn, team_map):
    """
    Calculate the 5 KPIs directly from 'ball_by_ball' without referencing 'innings'.

    Returns a DataFrame with columns: ['Team', 'Phase', 'Metric', 'Value'].
    """
    # 1. Base Query: Select necessary columns and categorize phases
    base_query = """
        SELECT
            CASE
                WHEN over_number BETWEEN 0 AND 10 THEN 'Powerplay'
                WHEN over_number BETWEEN 11 AND 40 THEN 'Middle'
                ELSE 'Death'
            END AS phase,
            batting_team_id,
            over_number,
            ball_display_number,
            runs_bat,
            wides,
            no_balls
        FROM ball_by_ball
    """

    df = pd.read_sql_query(base_query, conn)

    if df.empty:
        logging.warning("No ball-by-ball data found in 'ball_by_ball' table.")
        return pd.DataFrame(columns=['Team', 'Phase', 'Metric', 'Value'])

    # 2. Define KPI conditions
    kpi_definitions = {
        'Dot Balls': "(runs_bat = 0 AND wides = 0 AND no_balls = 0)",
        'Singles': "(runs_bat = 1)",
        'Boundaries': "(runs_bat IN (4,6))",
        'Boundaries (First & Last Ball)': "(runs_bat IN (4,6) AND ball_display_number IN (1,6))",
    }

    results = []

    # 3. Calculate each KPI
    for metric_name, condition in kpi_definitions.items():
        query = f"""
            SELECT
                phase,
                batting_team_id,
                COUNT(*) AS total_balls,
                SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) AS met_count
            FROM (
                {base_query}
            )
            GROUP BY phase, batting_team_id
        """
        metric_df = pd.read_sql_query(query, conn)

        if metric_df.empty:
            logging.info(f"No data for KPI: {metric_name}")
            continue

        for _, row in metric_df.iterrows():
            team_id = row['batting_team_id']
            team_name = team_map.get(team_id, f"Team {team_id}")
            total_balls = row['total_balls']
            met_count = row['met_count']
            value_pct = (met_count / total_balls) * 100 if total_balls else 0

            results.append({
                'Team': team_name,
                'Phase': row['phase'],
                'Metric': metric_name,
                'Value': round(value_pct, 2),
            })

    # 4. Calculate Strike Rate
    sr_query = f"""
        SELECT
            phase,
            batting_team_id,
            SUM(runs_bat) AS total_runs,
            COUNT(*) AS total_balls
        FROM (
            {base_query}
        )
        GROUP BY phase, batting_team_id
    """
    sr_df = pd.read_sql_query(sr_query, conn)

    for _, row in sr_df.iterrows():
        team_id = row['batting_team_id']
        team_name = team_map.get(team_id, f"Team {team_id}")
        total_runs = row['total_runs']
        total_balls = row['total_balls']
        sr_value = (total_runs / total_balls) * 100 if total_balls else 0

        results.append({
            'Team': team_name,
            'Phase': row['phase'],
            'Metric': 'Strike Rate',
            'Value': round(sr_value, 2),
        })

    # 5. Convert results to DataFrame
    metrics_df = pd.DataFrame(results)

    if metrics_df.empty:
        logging.warning("No KPI data was calculated.")

    return metrics_df


import plotly.express as px
import plotly.graph_objects as go
def calculate_bowling_phase_metrics(conn, team_map):
    """
    Calculate bowling KPIs (Dot Balls, Singles, Boundaries, Boundaries (First & Last Ball),
    Economy Rate) for each PHASE (Powerplay, Middle, Death).

    Returns DataFrame with columns: ['Team','Phase','Metric','Value'].
    """
    # 1) Load ball_by_ball
    base_query = """
        SELECT
            CASE
                WHEN over_number BETWEEN 0 AND 10 THEN 'Powerplay'
                WHEN over_number BETWEEN 11 AND 40 THEN 'Middle'
                ELSE 'Death'
            END AS phase,
            batting_team_id,
            over_number,
            ball_display_number,
            runs_bat,
            wides,
            no_balls
        FROM ball_by_ball
    """
    df = pd.read_sql_query(base_query, conn)

    if df.empty:
        logging.warning("No ball-by-ball data found in 'ball_by_ball'.")
        return pd.DataFrame(columns=['Team','Phase','Metric','Value'])

    # 2) We assume EXACTLY two teams in the match, so if batting_team_id == T1, then bowling is T2
    #    We build a quick "opposite" function:
    team_ids = list(team_map.keys())  # e.g. ["TeamA_ID", "TeamB_ID"]
    if len(team_ids) != 2:
        logging.warning(f"Expected 2 teams total, found {len(team_ids)}: {team_ids}")

    def get_opposite_team_id(batting_id):
        """Return the other team ID among the two."""
        return team_ids[1] if batting_id == team_ids[0] else team_ids[0]

    # Add a new column "bowling_team_id"
    df['bowling_team_id'] = df['batting_team_id'].apply(get_opposite_team_id)

    # Also define how many runs were conceded on each ball:
    # runs_conceded = runs_bat + wides + no_balls
    df['runs_conceded'] = df['runs_bat'] + df['wides'] + df['no_balls']

    # 3) We define KPI conditions from the "bowling" perspective:
    # Dot Balls, Singles, Boundaries, Boundaries(First&LastBall), plus Economy Rate
    #   - Dot ball for bowling if runs_bat=0, no wides/noBalls
    #   - Single for bowling if runs_bat=1
    #   - Boundaries for runs_bat in (4,6)
    #   - Boundaries (First & Last) for runs_bat in (4,6) AND ball_display_number in (1,6)
    #   - Economy Rate = sum(runs_conceded) / total_deliveries * 100 (per 100 balls)

    # We group by (phase, bowling_team_id)
    group_cols = ['phase','bowling_team_id']
    grouped = df.groupby(group_cols, dropna=True)

    # Count total deliveries
    total_balls = grouped.size()  # Series: (phase, bowlerTeamID) -> count
    # Dot balls:
    dot_condition = (df['runs_bat'] == 0) & (df['wides'] == 0) & (df['no_balls'] == 0)
    dot_balls = grouped.apply(lambda g: g[dot_condition].shape[0])
    # Singles:
    singles_condition = (df['runs_bat'] == 1)
    singles = grouped.apply(lambda g: g[singles_condition].shape[0])
    # Boundaries:
    boundary_condition = df['runs_bat'].isin([4,6])
    boundaries = grouped.apply(lambda g: g[boundary_condition].shape[0])
    # Boundaries (First & Last Ball):
    boundary_first_last_condition = boundary_condition & df['ball_display_number'].isin([1,6])
    boundaries_fl = grouped.apply(lambda g: g[boundary_first_last_condition].shape[0])
    # Sum of runs conceded:
    total_runs_conceded = grouped['runs_conceded'].sum()

    results = []

    # 4) For each group, compute metrics
    for idx in total_balls.index:
        ph, bowl_team_id = idx
        bowl_team_name = team_map.get(bowl_team_id, f"Team {bowl_team_id}")

        t_balls = total_balls[idx]
        if t_balls == 0:
            continue

        # Dot Ball %
        dot_count = dot_balls.get(idx, 0)
        dot_pct = (dot_count / t_balls)*100

        # Singles %
        sing_count = singles.get(idx, 0)
        singles_pct = (sing_count / t_balls)*100

        # Boundaries %
        bound_count = boundaries.get(idx, 0)
        bound_pct = (bound_count / t_balls)*100

        # Boundaries (First&Last) %
        bound_fl_count = boundaries_fl.get(idx, 0)
        bound_fl_pct = (bound_fl_count / t_balls)*100

        # Economy Rate (runs conceded per 100 balls)
        runs_cons = total_runs_conceded.get(idx, 0)
        economy_rate = (runs_cons / t_balls)*100

        # Append results as separate rows for each KPI
        results.append({'Team': bowl_team_name, 'Phase': ph, 'Metric': 'Dot Balls', 'Value': round(dot_pct,2)})
        results.append({'Team': bowl_team_name, 'Phase': ph, 'Metric': 'Singles', 'Value': round(singles_pct,2)})
        results.append({'Team': bowl_team_name, 'Phase': ph, 'Metric': 'Boundaries', 'Value': round(bound_pct,2)})
        results.append({'Team': bowl_team_name, 'Phase': ph, 'Metric': 'Boundaries (First & Last Ball)', 'Value': round(bound_fl_pct,2)})
        results.append({'Team': bowl_team_name, 'Phase': ph, 'Metric': 'Economy Rate', 'Value': round(economy_rate,2)})

    df_res = pd.DataFrame(results, columns=['Team','Phase','Metric','Value'])
    return df_res


import pandas as pd
import logging
import sqlite3





# ... [Your existing code up to the KPI Visualization] ...

# ###############################################################################
# KPI Visualization for Batting and Bowling Phases with Historical Averages
# ###############################################################################

# ###############################################################################
# KPI Visualization for Batting and Bowling Phases with Historical Averages
# ###############################################################################

def create_comparison_chart_with_historical(df, metric_name, hist_df, is_bowling=False):

    # Filter the DataFrame for the specified metric
    dff = df[df['Metric'] == metric_name]

    if dff.empty:
        # If there's no data for the metric, return an empty figure with a message
        fig = go.Figure()
        fig.add_annotation(
            text=f"No data available for {metric_name}",
            xref="paper", yref="paper",
            showarrow=False,
            font=dict(size=20)
        )
        return fig

    # Create the grouped bar chart
    fig = px.bar(
        dff,
        x='Phase',
        y='Value',
        color='Team',
        barmode='group',
        title=f"{metric_name} Comparison (per 100 balls)",
        labels={'Value': metric_name},
        category_orders={"Phase": ["Powerplay", "Middle", "Death"]}
    )

    # Prepare historical averages
    # Define the mapping from metric to historical average column
    if is_bowling:
        avg_column_map = {
            'Dot Balls': 'avg_dot_pct',
            'Singles': 'avg_singles_pct',
            'Boundaries': 'avg_boundaries_pct',
            'Boundaries (First & Last Ball)': 'avg_boundaries_fl_pct',
            'Economy Rate': 'avg_economy_rate'
        }
    else:
        avg_column_map = {
            'Dot Balls': 'avg_dot_pct',
            'Singles': 'avg_singles_pct',
            'Boundaries': 'avg_boundaries_pct',
            'Boundaries (First & Last Ball)': 'avg_boundaries_fl_pct',
            'Strike Rate': 'avg_strike_rate'
        }

    phases = ["Powerplay", "Middle", "Death"]
    for phase in phases:
        # Average for losing teams
        loser_avg = hist_df[
            (hist_df['phase'] == phase) & (hist_df['is_winner'] == False)
            ][avg_column_map.get(metric_name, 'avg_strike_rate')].mean()

        # Average for winning teams
        winner_avg = hist_df[
            (hist_df['phase'] == phase) & (hist_df['is_winner'] == True)
            ][avg_column_map.get(metric_name, 'avg_strike_rate')].mean()

        # Add red line for losing average
        if not pd.isna(loser_avg):
            fig.add_trace(go.Scatter(
                x=[phase],
                y=[loser_avg],
                mode='markers+lines',
                name=f'Avg Loser - {phase}',
                line=dict(color='red', dash='dash'),
                marker=dict(size=10)
            ))

        # Add green line for winning average
        if not pd.isna(winner_avg):
            fig.add_trace(go.Scatter(
                x=[phase],
                y=[winner_avg],
                mode='markers+lines',
                name=f'Avg Winner - {phase}',
                line=dict(color='green', dash='dash'),
                marker=dict(size=10)
            ))

    # Update layout to ensure legends are displayed correctly
    fig.update_layout(showlegend=True)

    return fig


# ------------------------------------------------------------------------
# EXAMPLE USAGE IN YOUR STREAMLIT APP
# This code snippet is meant to replace the KPI visualization portion
# ------------------------------------------------------------------------

@st.cache_data
def load_historical_data(batting_csv='historical_batting_averages.csv', bowling_csv='historical_bowling_averages.csv'):
    try:
        batting_df = pd.read_csv(batting_csv)
        bowling_df = pd.read_csv(bowling_csv)

        # Ensure 'is_winner' is boolean
        batting_df['is_winner'] = batting_df['is_winner'].astype(bool)
        bowling_df['is_winner'] = bowling_df['is_winner'].astype(bool)

        logging.info("Successfully loaded historical CSV files.")
        return batting_df, bowling_df
    except FileNotFoundError as fe:
        logging.error(f"CSV file not found: {fe}")
        st.error(f"CSV file not found: {fe}")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        logging.error(f"Error loading historical CSV files: {e}")
        st.error(f"Error loading historical CSV files: {e}")
        return pd.DataFrame(), pd.DataFrame()


# Load historical averages
hist_batting_df, hist_bowling_df = load_historical_data()

if match_id:
    st.header("Team Performance Comparison")

    # 1) Team map
    team_map = get_team_ids_and_names(conn)
    if not team_map:
        st.warning("No teams found for this match.")
    else:
        batting_df = calculate_phase_metrics(conn, team_map)
        if batting_df.empty:
            st.warning("No batting data found in ball_by_ball.")
        else:
            st.subheader("Batting KPIs by Phase")
            batting_kpis = [
                "Dot Balls",
                "Singles",
                "Boundaries",
                "Boundaries (First & Last Ball)",
                "Strike Rate"
            ]
            # Fetch historical batting averages

            for i in range(0, len(batting_kpis), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    kpi_index = i + j
                    if kpi_index < len(batting_kpis):
                        kpi_name = batting_kpis[kpi_index]
                        fig = create_comparison_chart_with_historical(
                            df=batting_df,
                            metric_name=kpi_name,
                            hist_df=hist_batting_df,
                            is_bowling=False
                        )
                        col.plotly_chart(fig, use_container_width=True)

        # ---------------------------
        #   B) BOWLING KPIs
        # ---------------------------
        bowling_df = calculate_bowling_phase_metrics(conn, team_map)
        if bowling_df.empty:
            st.warning("No bowling data found in ball_by_ball.")
        else:
            st.subheader("Bowling KPIs by Phase")
            bowling_kpis = [
                "Dot Balls",
                "Singles",
                "Boundaries",
                "Boundaries (First & Last Ball)",
                "Economy Rate"
            ]


            for i in range(0, len(bowling_kpis), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    kpi_index = i + j
                    if kpi_index < len(bowling_kpis):
                        kpi_name = bowling_kpis[kpi_index]
                        fig = create_comparison_chart_with_historical(
                            df=bowling_df,
                            metric_name=kpi_name,
                            hist_df=hist_bowling_df,
                            is_bowling=True
                        )
                        col.plotly_chart(fig, use_container_width=True)
                    # ---------------------------
                    #   C) BOWLING PARTNERSHIPS
                    # ---------------------------

            bowler_map = get_bowler_map(conn)

            partnerships_df = calculate_bowling_partnerships(conn, bowler_map)
            best_partnerships, worst_partnerships = identify_best_worst_partnerships(partnerships_df, top_n=20)
            visualize_bowling_partnerships(best_partnerships, worst_partnerships)
