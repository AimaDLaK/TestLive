import streamlit as st
import openai
import psycopg2
import pandas as pd
import json
import altair as alt  # For plotting

# Instantiate the OpenAI client using your API key from Streamlit secrets
client = openai.OpenAI(api_key=st.secrets["openai"]["OPENAI_API_KEY"])

def clean_sql(sql_query: str) -> str:
    """Remove markdown code fences and any leading 'sql ' prefix."""
    if sql_query.startswith("```"):
        parts = sql_query.split("\n")
        if parts[0].startswith("```"):
            parts = parts[1:]
        if parts and parts[-1].startswith("```"):
            parts = parts[:-1]
        sql_query = "\n".join(parts).strip()
    if sql_query.lower().startswith("sql "):
        sql_query = sql_query[4:].strip()
    return sql_query

def translate_to_sql(natural_query, schema):
    messages = [
        {
            "role": "user",
            "content": (
                f"Given the following PostgreSQL table schema (stick strictly to this schema, don't add extra tables):\n{schema}\n"
                "Return only a valid SQL query without any markdown formatting or extra text. Also note that this query "
                "will be displayed to a cricket coach who may not be well-versed in databases. For example, if the coach says "
                "'show me data for a grade called gps first xi', remember that in our database it is stored as 'GPS First XI'. "
                "So the query should be flexible (e.g., checking for lower-case matches) and show names instead of IDs. "
                "The resulting table should be easy for a cricket coach to read."
            )
        },
        {
            "role": "user",
            "content": f"Convert this natural language query into SQL: '{natural_query}'"
        }
    ]
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.2,
        max_completion_tokens=8000
    )
    raw_sql = response.choices[0].message.content.strip()
    sql_query = clean_sql(raw_sql)
    return sql_query

def run_query(sql_query):
    """
    Attempt to run the SQL query.
    Returns a tuple: (DataFrame, error_message).
    If execution succeeds, error_message is None.
    """
    try:
        conn = psycopg2.connect(
            host="readreplica.cl26gcqkaenv.ap-southeast-1.rds.amazonaws.com",
            dbname="cricket_rds",
            user="postgres",
            password="34poh2aGaHybsKxPB07B",
            port="5432"
        )
        df = pd.read_sql_query(sql_query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

def fix_sql_query(current_sql, error_message, natural_query, schema, iteration):
    """
    Given the current SQL query, error message, and context, ask the LLM to revise the SQL query.
    """
    additional_context = (
        f"This is iteration {iteration}. The current SQL query failed with error:\n{error_message}\n"
        "Please revise the SQL query to fix the error. Remember to include player names and grade names where appropriate, "
        "and be flexible with case (e.g., use lower-case comparisons where needed). "
        "Return only a valid SQL query without any markdown formatting or extra text."
    )
    messages = [
        {
            "role": "system",
            "content": additional_context + f"\nThe original natural language query was: {natural_query}\n"
                                            f"Using the following PostgreSQL schema:\n{schema}"
        }
    ]
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.2,
        max_completion_tokens=8000
    )
    revised_sql = response.choices[0].message.content.strip()
    return clean_sql(revised_sql)

def modify_sql_query_with_feedback(current_sql, feedback, table_sample, natural_query, schema):
    """
    Given the current SQL query, a sample of the results, and user feedback,
    ask the LLM to modify the SQL query to address the feedback.
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert SQL query modifier. The current SQL query is:\n"
                f"{current_sql}\n\n"
                "A sample of the result table (limited rows) is shown below:\n"
                f"{table_sample}\n\n"
                "User feedback: {feedback}\n"
                "The original natural language query was:\n"
                f"{natural_query}\n\n"
                f"Using the following PostgreSQL schema:\n{schema}\n\n"
                "Please modify the SQL query to address the feedback. Return only a valid SQL query without any markdown formatting or extra text."
            ).format(feedback=feedback)
        }
    ]
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.2,
        max_completion_tokens=8000
    )
    st.write("LLM raw response:", response.choices[0].message.content)
    new_sql = response.choices[0].message.content.strip()
    return clean_sql(new_sql)

def get_visualization_spec(natural_query, df):
    """
    Ask the LLM to choose an appropriate chart type and map columns.
    Expects a JSON response with keys: "chart_type", "x_axis", and "y_axis".
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert data visualization assistant. Based on the provided natural language query and "
                "the list of columns from a SQL query result, choose the most appropriate chart type (e.g., bar, line, scatter, histogram) "
                "and suggest which column should be used for the x-axis and which for the y-axis. Return only a valid JSON object "
                "with keys 'chart_type', 'x_axis', and 'y_axis'."
            )
        },
        {
            "role": "user",
            "content": f"Natural language query: {natural_query}\nData table columns: {', '.join(df.columns)}"
        }
    ]
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            temperature=0,
            max_completion_tokens=200
        )
        json_str = response.choices[0].message.content.strip()
        viz_spec = json.loads(json_str)
        return viz_spec
    except Exception as e:
        st.error(f"Error getting visualization specification: {e}")
        return {}

def plot_data(df, viz_spec):
    """
    Create and display a chart based on the LLM's visualization specification.
    Supports bar, line, scatter, and histogram.
    """
    if not viz_spec:
        st.info("No visualization spec provided; displaying raw data only.")
        return

    chart_type = viz_spec.get("chart_type")
    x_axis = viz_spec.get("x_axis")
    y_axis = viz_spec.get("y_axis")

    if not chart_type or not x_axis or not y_axis:
        st.info("Incomplete visualization spec; displaying raw data only.")
        return

    try:
        if chart_type.lower() == "bar":
            chart = alt.Chart(df).mark_bar().encode(x=x_axis, y=y_axis)
        elif chart_type.lower() == "line":
            chart = alt.Chart(df).mark_line().encode(x=x_axis, y=y_axis)
        elif chart_type.lower() == "scatter":
            chart = alt.Chart(df).mark_point().encode(x=x_axis, y=y_axis)
        elif chart_type.lower() == "histogram":
            chart = alt.Chart(df).mark_bar().encode(x=alt.X(x_axis, bin=True), y='count()')
        else:
            st.info(f"Chart type '{chart_type}' is not supported. Displaying raw data only.")
            return

        st.altair_chart(chart, use_container_width=True)
    except Exception as e:
        st.error(f"Error generating chart: {e}")

# -----------------------------
# Main Dashboard UI
# -----------------------------

st.title("Decidr AI Agent")
st.write("Enter a natural language query to fetch data from our database tables with over 50 grades and at least 10 seasons worth of data.")

# Query input section (always visible)
user_query = st.text_input("Enter your query (e.g., 'Show best teams in grade \"a2353f2c-11a8-42f0-b54a-0d5598bd9afa\"'):")
schema = """
    -- Table: public.ball_by_ball
    CREATE TABLE public.ball_by_ball (
        id text,
        innings_id text,
        innings_number integer,
        innings_order integer,
        innings_name text,
        batting_team_id text,
        progress_runs integer,
        progress_wickets integer,
        progress_score text,
        striker_participant_id text,
        striker_short_name text,
        striker_runs_scored integer,
        striker_balls_faced integer,
        non_striker_participant_id text,
        non_striker_short_name text,
        non_striker_runs_scored integer,
        non_striker_balls_faced integer,
        bowler_participant_id text,
        bowler_short_name text,
        over_number integer,
        ball_display_number integer,
        ball_time timestamp with time zone,
        runs_bat integer,
        wides integer,
        no_balls integer,
        leg_byes integer,
        byes integer,
        penalty_runs integer,
        short_description text,
        description text
    );

    -- Table: public.batting_stats
    CREATE TABLE public.batting_stats (
        id text,
        innings_id text,
        player_id text,
        bat_order integer,
        bat_instance integer,
        balls_faced integer,
        fours_scored integer,
        sixes_scored integer,
        runs_scored integer,
        batting_minutes integer,
        strike_rate double precision,
        dismissal_type_id integer,
        dismissal_type text,
        dismissal_text text
    );

    -- Table: public.bowling_stats
    CREATE TABLE public.bowling_stats (
        id text,
        innings_id text,
        player_id text,
        bowl_order integer,
        overs_bowled double precision,
        maidens_bowled integer,
        runs_conceded integer,
        wickets_taken integer,
        wide_balls integer,
        no_balls integer,
        economy double precision
    );

    -- Table: public.fall_of_wickets
    CREATE TABLE public.fall_of_wickets (
        id text,
        innings_id text,
        player_id text,
        "order" integer,
        runs integer
    );

    -- Table: public.feedback_feedback
    CREATE TABLE public.feedback_feedback (
        id integer,
        feedback_text text
    );

    -- Table: public.fielding_stats
    CREATE TABLE public.fielding_stats (
        id text,
        innings_id text,
        player_id text,
        catches integer,
        wicket_keeper_catches integer,
        total_catches integer,
        unassisted_run_outs integer,
        assisted_run_outs integer,
        run_outs integer,
        stumpings integer
    );

    -- Table: public.grades
    CREATE TABLE public.grades (
        id text,
        name text
    );

    -- Table: public.innings
    CREATE TABLE public.innings (
        id text,
        match_id text,
        name text,
        innings_close_type text,
        innings_number integer,
        innings_order integer,
        batting_team_id text,
        is_declared numeric,
        is_follow_on numeric,
        byes_runs integer,
        leg_byes_runs integer,
        no_balls integer,
        wide_balls integer,
        penalties integer,
        total_extras integer,
        overs_bowled double precision,
        runs_scored integer,
        number_of_wickets_fallen integer
    );

    -- Table: public.ladder
    CREATE TABLE public.ladder (
        id text,
        grade_id text,
        team_id text,
        rank integer,
        played integer,
        points integer,
        bonus_points integer,
        quotient double precision,
        net_run_rate double precision,
        won integer,
        lost integer,
        ties integer,
        no_results integer,
        byes integer,
        forfeits integer,
        disqualifications integer,
        adjustments integer,
        runs_for integer,
        overs_faced double precision,
        wickets_lost integer,
        runs_against integer,
        overs_bowled double precision,
        wickets_taken integer
    );

    -- Table: public.match_schedule
    CREATE TABLE public.match_schedule (
        id text,
        match_id text,
        match_day text,
        start_datetime timestamp with time zone
    );

    -- Table: public.matches
    CREATE TABLE public.matches (
        id text,
        status text,
        status_id text,
        team_a text,
        team_b text,
        season text,
        match_type text,
        match_type_id text,
        is_ball_by_ball text,
        result_text text,
        round_id text,
        grade_id text,
        venue_id text,
        start_datetime timestamp with time zone
    );

    -- Table: public.organisations
    CREATE TABLE public.organisations (
        id text,
        name text,
        short_name text,
        logo_url timestamp without time zone
    );

    -- Table: public.players
    CREATE TABLE public.players (
        id text,
        team_id text,
        name text,
        short_name text,
        role text
    );

    -- Table: public.playing_surfaces
    CREATE TABLE public.playing_surfaces (
        id text,
        name text,
        latitude double precision,
        longitude double precision
    );

    -- Table: public.rounds
    CREATE TABLE public.rounds (
        id text,
        name text,
        short_name text
    );

    -- Table: public.teams
    CREATE TABLE public.teams (
        id text,
        match_id text,
        display_name text,
        result_type_id integer,
        result_type text,
        won_toss numeric,
        batted_first numeric,
        is_home numeric,
        score_text text,
        is_winner numeric
    );

    -- Table: public.users
    CREATE TABLE public.users (
        id integer,
        email text,
        password text,
        full_name text,
        role text,
        goal text,
        email_confirmed numeric,
        created_at timestamp with time zone
    );

    -- Table: public.venues
    CREATE TABLE public.venues (
        id text,
        name text,
        line1 text,
        suburb text,
        post_code text,
        state_name text,
        country text,
        playing_surface_id text
    );
    """# Button to submit new query
if st.button("Submit Query"):
    # Reset session state variables for a new query
    st.session_state.iteration = 0
    st.session_state.current_sql = translate_to_sql(user_query, schema)
    df, error_message = None, None
    max_iterations = 4
    # Auto-fix loop for errors
    while st.session_state.iteration < max_iterations:
        st.write("Generated SQL Query:")
        st.code(st.session_state.current_sql, language="sql")
        df, error_message = run_query(st.session_state.current_sql)
        if error_message is None:
            break
        else:
            st.error(f"Error executing query: {error_message}")
            revised_sql = fix_sql_query(
                st.session_state.current_sql,
                error_message,
                user_query,
                schema,
                st.session_state.iteration + 1
            )
            st.write("Revised SQL Query from LLM:", revised_sql)
            if revised_sql.strip() == st.session_state.current_sql.strip():
                st.info("LLM did not modify the query. Retrying...")
            st.session_state.current_sql = revised_sql
            st.session_state.iteration += 1
    if st.session_state.iteration >= max_iterations:
        st.error("Unable to fix the query after several attempts. Please rephrase your prompt.")
    elif df is not None and df.empty:
        st.warning("No data returned by the query.")
    elif df is not None:
        st.subheader("Query Results")
        st.dataframe(df)
        # Save results in session state for feedback modification
        st.session_state.df = df

# If a query result is available (from session_state), display feedback form
if "df" in st.session_state and not st.session_state.df.empty:
    st.markdown("### Modify Query with Feedback")
    st.write("If you have remarks (for example, if you notice duplicated rows), enter them below and specify how many rows of the result should be sent as a sample to the LLM to modify the query.")

    with st.form(key="feedback_form"):
        feedback = st.text_area("Enter your feedback (e.g., 'The rows are duplicated; please remove duplicates')")
        sample_rows = st.number_input("Number of rows to include as sample", min_value=1, max_value=20, value=5, step=1)
        submit_feedback = st.form_submit_button(label="Update Query with Feedback")

    if submit_feedback:
        st.write("Feedback received:", feedback)
        st.write("Including sample of first", sample_rows, "rows.")
        table_sample = st.session_state.df.head(sample_rows).to_csv(index=False)
        new_sql = modify_sql_query_with_feedback(
            st.session_state.current_sql,
            feedback,
            table_sample,
            user_query,
            schema
        )
        st.write("Updated SQL Query from feedback:")
        st.code(new_sql, language="sql")
        st.session_state.current_sql = new_sql  # Update the current SQL
        df_new, error_message_new = run_query(new_sql)
        if error_message_new is None and not df_new.empty:
            st.subheader("Updated Query Results")
            st.dataframe(df_new)
            viz_spec = get_visualization_spec(user_query, df_new)
            st.write("Visualization specification:", viz_spec)
            st.subheader("Data Visualization")
            plot_data(df_new, viz_spec)
        else:
            st.error(f"Error executing updated query: {error_message_new}")
