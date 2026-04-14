from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, monotonically_increasing_id, lit
from pathlib import Path
import os
import time

os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

BASE_DIR       = Path(r"C:\Users\caro_\OneDrive\Desktop\UP\1OCTAVO\datosMasivos\casino_bigdata")
PROCESSED_DIR  = BASE_DIR / "data" / "processed"
STREAM_OUTPUT  = BASE_DIR / "data" / "streaming" / "output"
CHECKPOINT_DIR = BASE_DIR / "data" / "streaming" / "checkpoint"

for d in [STREAM_OUTPUT, CHECKPOINT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

spark = SparkSession.builder \
    .appName("CasinoStreamingSimulation") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Casino Streaming Simulation - Phase 6")
print("incremental processing using foreachBatch")
print()

# load full dataset
print("loading processed parquet data...")
df_full = spark.read.parquet(str(PROCESSED_DIR))
total_rows = df_full.count()
NUM_BATCHES = 5
rows_per_batch = total_rows // NUM_BATCHES

print(f"total rows     : {total_rows:,}")
print(f"batches        : {NUM_BATCHES}")
print(f"rows per batch : {rows_per_batch:,}")
print()

# add batch_id
df_with_id = df_full \
    .withColumn("_row_id", monotonically_increasing_id()) \
    .withColumn("batch_id", (col("_row_id") % NUM_BATCHES).cast("int")) \
    .drop("_row_id")

# process each batch incrementally
print(f"  {'batch':>6} | {'input rows':>12} | {'output rows':>12} | status")
print("  " + "-" * 52)

total_written = 0

for i in range(NUM_BATCHES):
    batch_df = df_with_id.filter(col("batch_id") == i).drop("batch_id")

    # apply streaming-style transformations
    transformed = batch_df \
        .withColumn("rtp_flag",
            when(col("rtp") >= 97, "High RTP")
            .when(col("rtp") >= 94, "Medium RTP")
            .otherwise("Low RTP")
        ) \
        .withColumn("is_premium",
            when((col("has_jackpot") == True) & (col("player_friendly") == True), True)
            .otherwise(False)
        ) \
        .withColumn("stream_batch", lit(i)) \
        .select("provider", "game_type", "rtp", "rtp_flag",
                "has_jackpot", "player_friendly", "is_premium",
                "profitability_score", "stream_batch")

    input_count = transformed.count()

    # write incrementally (append mode per batch)
    transformed.write \
        .mode("append") \
        .parquet(str(STREAM_OUTPUT))

    total_written += input_count
    print(f"  {i:>6} | {input_count:>12,} | {total_written:>12,} | processed")
    time.sleep(1)

print()
print(f"incremental processing complete")
print(f"total rows written : {total_written:,}")
print(f"output location    : {STREAM_OUTPUT}")
print()

# verify output
print("reading final output...")
df_result = spark.read.parquet(str(STREAM_OUTPUT))
final_count = df_result.count()
print(f"total rows in output: {final_count:,}")
print()
print("sample (10 rows):")
df_result.show(10, truncate=False)

spark.stop()
print("done.")