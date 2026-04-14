import pandas as pd
import psycopg2
from io import StringIO
from pathlib import Path

DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "user":     "postgres",
    "password": "postgres",
    "database": "casino_db"
}

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "online_casino_games_dataset_v2.csv"

def create_table(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS casino_games_raw")
    cur.execute("""
        CREATE TABLE casino_games_raw (
            id                   SERIAL PRIMARY KEY,
            casino               TEXT,
            game                 TEXT,
            provider             TEXT,
            rtp                  FLOAT,
            volatility           TEXT,
            jackpot              TEXT,
            country_availability TEXT,
            min_bet              FLOAT,
            max_win              FLOAT,
            game_type            TEXT,
            game_category        TEXT,
            license_jurisdiction TEXT,
            release_year         INT,
            currency             TEXT,
            mobile_compatible    TEXT,
            free_spins_feature   TEXT,
            bonus_buy_available  TEXT,
            max_multiplier       FLOAT,
            languages            TEXT,
            last_updated         TEXT
        )
    """)
    conn.commit()
    print(" Table 'casino_games_raw' ready")
    cur.close()

def load_data(conn):
    print("Reading CSV...")
    df = pd.read_csv(CSV_PATH, low_memory=False)

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    cur = conn.cursor()
    cur.copy_expert("""
        COPY casino_games_raw (
            casino, game, provider, rtp, volatility, jackpot,
            country_availability, min_bet, max_win, game_type,
            game_category, license_jurisdiction, release_year,
            currency, mobile_compatible, free_spins_feature,
            bonus_buy_available, max_multiplier, languages, last_updated
        ) FROM STDIN WITH CSV NULL '\\N'
    """, buffer)
    conn.commit()
    print(f" {len(df):,} rows loaded into PostgreSQL")
    cur.close()

def main():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    print(" Connected")
    create_table(conn)
    load_data(conn)
    conn.close()
    print("\nDone. Table: casino_db → casino_games_raw")

if __name__ == "__main__":
    main()