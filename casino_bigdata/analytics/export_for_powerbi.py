import psycopg2
import pandas as pd
from pathlib import Path

BASE_DIR    = Path(r"C:\Users\caro_\OneDrive\Desktop\UP\1OCTAVO\datosMasivos\casino_bigdata")
EXPORT_DIR  = BASE_DIR / "data" / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

PG_CONFIG = {
    "host"    : "localhost",
    "port"    : 5433,
    "dbname"  : "casino_db",
    "user"    : "postgres",
    "password": "postgres",
}

print("Exporting tables to CSV for Power BI")
print(f"output folder: {EXPORT_DIR}")
print()

conn = psycopg2.connect(**PG_CONFIG)

# aggregated tables — small, fast
agg_tables = [
    "provider_stats",
    "game_type_stats",
    "volatility_stats",
    "rtp_category_stats",
    "bet_tier_stats",
]

for table in agg_tables:
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    out = EXPORT_DIR / f"{table}.csv"
    df.to_csv(out, index=False)
    print(f"  {table}.csv  ->  {len(df):,} rows")

print()
print("exporting casino_games_raw (1.2M rows, this may take a few minutes)...")
out_raw = EXPORT_DIR / "casino_games_raw.csv"
chunk_size = 100_000
first_chunk = True
total = 0

query = "SELECT * FROM casino_games_raw"
for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
    chunk.to_csv(out_raw, mode="w" if first_chunk else "a",
                 header=first_chunk, index=False)
    first_chunk = False
    total += len(chunk)
    print(f"  {total:,} rows written...")

print(f"  casino_games_raw.csv  ->  {total:,} rows total")

conn.close()
print()
print("all tables exported.")
print(f"import these CSV files into Power BI from:")
print(f"  {EXPORT_DIR}")