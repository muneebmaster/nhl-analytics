"""
Load MoneyPuck NHL CSV data into Snowflake.
Schema: MUNEEB_MASTER_RAW_DATA.MONEYPUCK
"""

import os
import snowflake.connector

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "claude_token-token-secret.txt")
RAW_DATA = os.path.join(os.path.dirname(__file__), "raw_data")

ACCOUNT = "cmvgrnf-zna84829"
USER = "MUNEEB_MASTER"
WAREHOUSE = "transforming"
DATABASE = "MUNEEB_MASTER_RAW_DATA"
SCHEMA = "MONEYPUCK"
ROLE = "TRANSFORMER"
STAGE = "MONEYPUCK_STAGE"


def connect():
    token = open(TOKEN_FILE).read().strip()
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        authenticator="programmatic_access_token",
        token=token,
        warehouse=WAREHOUSE,
        database=DATABASE,
        role=ROLE,
    )


def load_table(cur, table_name, csv_paths, truncate=True):
    """PUT csv files to stage and COPY INTO table, inferring schema on first load."""
    print(f"\n--- {table_name} ---")

    # PUT all files to stage
    for path in csv_paths:
        print(f"  PUT {os.path.basename(path)}")
        cur.execute(f"PUT 'file://{path}' @{STAGE}/{table_name}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

    # Infer schema from first file to create table if not exists
    first = csv_paths[0]
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '@{STAGE}/{table_name}/',
                    FILE_FORMAT => 'MONEYPUCK_CSV_FORMAT',
                    FILES => '{os.path.basename(first)}.gz'
                )
            )
        )
    """)
    print(f"  Table ready.")

    if truncate:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

    # COPY INTO
    cur.execute(f"""
        COPY INTO {table_name}
        FROM @{STAGE}/{table_name}/
        FILE_FORMAT = (FORMAT_NAME = 'MONEYPUCK_CSV_FORMAT')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        PURGE = FALSE
    """)
    results = cur.fetchall()
    total_rows = sum(r[3] for r in results if r[3])
    print(f"  Loaded {total_rows:,} rows across {len(results)} file(s).")


def main():
    print("Connecting to Snowflake...")
    conn = connect()
    cur = conn.cursor()

    # Setup
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    cur.execute(f"USE SCHEMA {SCHEMA}")
    cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE}")
    cur.execute(f"DROP FILE FORMAT IF EXISTS MONEYPUCK_CSV_FORMAT")
    cur.execute(f"""
        CREATE FILE FORMAT MONEYPUCK_CSV_FORMAT
        TYPE = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        PARSE_HEADER = TRUE
        NULL_IF = ('', 'NULL', 'null')
        EMPTY_FIELD_AS_NULL = TRUE
        DATE_FORMAT = AUTO
        TIMESTAMP_FORMAT = AUTO
    """)
    print("Schema, stage, and file format ready.")

    # --- Season Summary ---
    load_table(cur, "RAW_SKATERS_SEASON_SUMMARY", [
        f"{RAW_DATA}/season_summary/skaters_2008_to_2024/skaters_2008_to_2024.csv",
        f"{RAW_DATA}/season_summary/skaters_2025.csv",
    ])

    load_table(cur, "RAW_GOALIES_SEASON_SUMMARY", [
        f"{RAW_DATA}/season_summary/goalies_2008_to_2024/goalies_2008_to_2024.csv",
        f"{RAW_DATA}/season_summary/goalies_2025.csv",
    ])

    load_table(cur, "RAW_TEAMS_SEASON_SUMMARY", [
        f"{RAW_DATA}/season_summary/teams_2008_to_2024/teams_2008_to_2024.csv",
        f"{RAW_DATA}/season_summary/teams_2025.csv",
    ])

    load_table(cur, "RAW_LINES_SEASON_SUMMARY", [
        f"{RAW_DATA}/season_summary/lines_2008_to_2024/lines_2008_to_2024.csv",
        f"{RAW_DATA}/season_summary/lines_2025.csv",
    ])

    # --- Game by Game ---
    load_table(cur, "RAW_SKATERS_GAME_BY_GAME", [
        f"{RAW_DATA}/game_by_game/skaters_2008_to_2024/2008_to_2024.csv",
        f"{RAW_DATA}/game_by_game/skaters_2025/2025.csv",
    ])

    load_table(cur, "RAW_GOALIES_GAME_BY_GAME", [
        f"{RAW_DATA}/game_by_game/goalies_2008_to_2024/2008_to_2024.csv",
        f"{RAW_DATA}/game_by_game/goalies_2025/2025.csv",
    ])

    load_table(cur, "RAW_LINES_GAME_BY_GAME", [
        f"{RAW_DATA}/game_by_game/lines_2008_to_2024/2008_to_2024.csv",
        f"{RAW_DATA}/game_by_game/lines_2025/2025.csv",
    ])

    load_table(cur, "RAW_TEAMS_GAME_BY_GAME", [
        f"{RAW_DATA}/game_by_game/all_teams.csv",
    ])

    # --- Player Bios ---
    load_table(cur, "RAW_PLAYER_BIOS", [
        f"{RAW_DATA}/player_bios/allPlayersLookup.csv",
    ])

    # --- Shots ---
    load_table(cur, "RAW_SHOTS", [
        f"{RAW_DATA}/shots/shots_2023/shots_2023.csv",
        f"{RAW_DATA}/shots/shots_2024/shots_2024.csv",
    ])

    cur.close()
    conn.close()
    print("\nAll done!")


if __name__ == "__main__":
    main()
