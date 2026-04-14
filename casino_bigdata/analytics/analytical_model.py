"""
analytical_model.py
====================
Phase 7 - Analytical Data Model
Reads curated Parquet datasets and loads them into PostgreSQL
as structured analytical tables ready for querying and reporting.

Run:
    python analytics/analytical_model.py
"""

import psycopg2
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

BASE_DIR    = Path(r"C:\Users\caro_\OneDrive\Desktop\UP\1OCTAVO\datosMasivos\casino_bigdata")
CURATED_DIR = BASE_DIR / "data" / "curated"

PG_CONFIG = {
    "host"    : "localhost",
    "port"    : 5433,
    "dbname"  : "casino_db",
    "user"    : "postgres",
    "password": "postgres",
}

print("Analytical Data Model - Phase 7")
print("loading curated data into PostgreSQL as analytical tables")
print()

# ── connect ────────────────────────────────────────────────
conn = psycopg2.connect(**PG_CONFIG)
cur  = conn.cursor()
print("connected to PostgreSQL (casino_db)")
print()

# ── helper ─────────────────────────────────────────────────
def load_parquet(subfolder: str) -> pd.DataFrame:
    path = CURATED_DIR / subfolder
    files = list(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet files in {path}")
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)

def bulk_insert(cur, table: str, df: pd.DataFrame):
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    rows = [tuple(row) for row in df.itertuples(index=False)]
    cur.executemany(sql, rows)

# ══════════════════════════════════════════════════════════
# TABLE 1: provider_stats
# Provider-level aggregated stats 
# ══════════════════════════════════════════════════════════
print("creating table: provider_stats...")
cur.execute("DROP TABLE IF EXISTS provider_stats CASCADE")
cur.execute("""
    CREATE TABLE provider_stats (
        provider              VARCHAR(100) PRIMARY KEY,
        total_games           INT,
        avg_rtp               NUMERIC(6,2),
        avg_max_win           NUMERIC(12,2),
        avg_profitability     NUMERIC(6,2),
        jackpot_games         INT
    )
""")

df_provider = load_parquet("by_provider")
bulk_insert(cur, "provider_stats", df_provider)
conn.commit()
print(f"  rows inserted: {len(df_provider):,}")

# ══════════════════════════════════════════════════════════
# TABLE 2: game_type_stats
# Game type aggregated stats 
# ══════════════════════════════════════════════════════════
print("creating table: game_type_stats...")
cur.execute("DROP TABLE IF EXISTS game_type_stats CASCADE")
cur.execute("""
    CREATE TABLE game_type_stats (
        game_type             VARCHAR(100) PRIMARY KEY,
        total_games           INT,
        avg_rtp               NUMERIC(6,2),
        avg_max_multiplier    NUMERIC(12,2)
    )
""")

df_game_type = load_parquet("by_game_type")
bulk_insert(cur, "game_type_stats", df_game_type)
conn.commit()
print(f"  rows inserted: {len(df_game_type):,}")

# ══════════════════════════════════════════════════════════
# TABLE 3: volatility_stats
# Volatility distribution 
# ══════════════════════════════════════════════════════════
print("creating table: volatility_stats...")
cur.execute("DROP TABLE IF EXISTS volatility_stats CASCADE")
cur.execute("""
    CREATE TABLE volatility_stats (
        volatility            VARCHAR(50) PRIMARY KEY,
        total_games           INT,
        avg_rtp               NUMERIC(6,2),
        avg_max_win           NUMERIC(12,2)
    )
""")

df_volatility = load_parquet("by_volatility")
bulk_insert(cur, "volatility_stats", df_volatility)
conn.commit()
print(f"  rows inserted: {len(df_volatility):,}")

# ══════════════════════════════════════════════════════════
# TABLE 4: rtp_category_stats
# RTP category distribution 
# ══════════════════════════════════════════════════════════
print("creating table: rtp_category_stats...")
cur.execute("DROP TABLE IF EXISTS rtp_category_stats CASCADE")
cur.execute("""
    CREATE TABLE rtp_category_stats (
        rtp_category          VARCHAR(50) PRIMARY KEY,
        total_games           INT,
        avg_max_win           NUMERIC(12,2)
    )
""")

df_rtp = load_parquet("by_rtp_category")
bulk_insert(cur, "rtp_category_stats", df_rtp)
conn.commit()
print(f"  rows inserted: {len(df_rtp):,}")

# ══════════════════════════════════════════════════════════
# TABLE 5: bet_tier_stats
# Bet tier distribution 
# ══════════════════════════════════════════════════════════
print("creating table: bet_tier_stats...")
cur.execute("DROP TABLE IF EXISTS bet_tier_stats CASCADE")
cur.execute("""
    CREATE TABLE bet_tier_stats (
        bet_tier              VARCHAR(50) PRIMARY KEY,
        total_games           INT,
        avg_rtp               NUMERIC(6,2)
    )
""")

df_bet = load_parquet("by_bet_tier")
bulk_insert(cur, "bet_tier_stats", df_bet)
conn.commit()
print(f"  rows inserted: {len(df_bet):,}")

# ══════════════════════════════════════════════════════════
# VERIFY — list all analytical tables with row counts
# ══════════════════════════════════════════════════════════
print()
print("analytical tables in casino_db:")
print(f"  {'table':<25} | {'rows':>8}")
print("  " + "-" * 38)

tables = ["provider_stats", "game_type_stats", "volatility_stats", "rtp_category_stats", "bet_tier_stats"]
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t:<25} | {count:>8,}")

print()
print("sample query — top 5 providers by avg profitability:")
cur.execute("""
    SELECT provider, total_games, avg_rtp, avg_profitability
    FROM provider_stats
    ORDER BY avg_profitability DESC
    LIMIT 5
""")
rows = cur.fetchall()
print(f"  {'provider':<25} | {'games':>6} | {'avg_rtp':>8} | {'profitability':>13}")
print("  " + "-" * 62)
for r in rows:
    print(f"  {str(r[0]):<25} | {r[1]:>6} | {float(r[2]):>8.2f} | {float(r[3]):>13.2f}")

print()
print("analytical data model complete.")
cur.close()
conn.close()
