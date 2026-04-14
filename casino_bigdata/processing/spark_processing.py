from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, count, round as spark_round, trim, upper
)
from pathlib import Path
import os
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += ";C:\\hadoop\\bin"

BASE_DIR      = Path(__file__).resolve().parent.parent
RAW_CSV       = str(BASE_DIR / "data" / "raw" / "online_casino_games_dataset_v2.csv")
PROCESSED_DIR = str(BASE_DIR / "data" / "processed")
CURATED_DIR   = str(BASE_DIR / "data" / "curated")


spark = SparkSession.builder \
    .appName("CasinoBatchProcessing") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session started")

# Load
print("\nLoading raw data...")
df_raw = spark.read.csv(RAW_CSV, header=True, inferSchema=True)
print("Rows loaded:", df_raw.count())
print("Columns:", len(df_raw.columns))
df_raw.printSchema()

#Cleaning
print("\nCleaning data...")
df_clean = df_raw \
    .withColumn("game",           trim(upper(col("game")))) \
    .withColumn("provider",       trim(upper(col("provider")))) \
    .withColumn("game_type",      trim(upper(col("game_type")))) \
    .withColumn("volatility",     trim(upper(col("volatility")))) \
    .withColumn("rtp",            spark_round(col("rtp").cast("double"), 2)) \
    .withColumn("min_bet",        spark_round(col("min_bet").cast("double"), 2)) \
    .withColumn("max_win",        spark_round(col("max_win").cast("double"), 2)) \
    .withColumn("max_multiplier", spark_round(col("max_multiplier").cast("double"), 2)) \
    .withColumn("jackpot",        when(col("jackpot").isNull(), "No Jackpot").otherwise(col("jackpot"))) \
    .withColumn("max_multiplier", when(col("max_multiplier").isNull(), 0.0).otherwise(col("max_multiplier"))) \
    .dropDuplicates()

print("Rows after cleaning:", df_clean.count())

#  Transformations and feature generation
print("\nTransformations and feature generation...")
df_transformed = df_clean \
    .withColumn("rtp_category",
        when(col("rtp") >= 97, "High RTP")
        .when(col("rtp") >= 94, "Medium RTP")
        .otherwise("Low RTP")
    ) \
    .withColumn("bet_tier",
        when(col("min_bet") <= 0.10, "Low Stakes")
        .when(col("min_bet") <= 1.00, "Mid Stakes")
        .otherwise("High Stakes")
    ) \
    .withColumn("has_jackpot",
        when(col("jackpot") == "No Jackpot", False).otherwise(True)
    ) \
    .withColumn("player_friendly",
        when(
            (col("mobile_compatible") == True) &
            (col("free_spins_feature") == True), True
        ).otherwise(False)
    )

print("Features added: rtp_category, bet_tier, has_jackpot, player_friendly")

# Aggregations
print("\nAggregations...")

by_provider = df_transformed.groupBy("provider").agg(
    count("*").alias("total_games"),
    spark_round(avg("rtp"), 2).alias("avg_rtp"),
    spark_round(avg("max_win"), 2).alias("avg_max_win")
).orderBy("total_games", ascending=False)

by_game_type = df_transformed.groupBy("game_type").agg(
    count("*").alias("total_games"),
    spark_round(avg("rtp"), 2).alias("avg_rtp")
).orderBy("total_games", ascending=False)

by_volatility = df_transformed.groupBy("volatility").agg(
    count("*").alias("total_games")
).orderBy("total_games", ascending=False)

by_rtp_cat = df_transformed.groupBy("rtp_category").agg(
    count("*").alias("total_games")
).orderBy("total_games", ascending=False)

print("\nTop 10 providers:")
by_provider.show(10, truncate=False)
print("Games by type:")
by_game_type.show(truncate=False)
print("Games by volatility:")
by_volatility.show(truncate=False)
print("RTP categories:")
by_rtp_cat.show(truncate=False)

# Save to Parquet
print("\n Saving to Parquet...")
df_transformed.write.mode("overwrite").parquet(PROCESSED_DIR)
print("Processed layer saved:", PROCESSED_DIR)

by_provider.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_provider")
by_game_type.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_game_type")
by_volatility.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_volatility")
by_rtp_cat.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_rtp_category")
print("Curated layer saved:", CURATED_DIR)

print("\nBatch processing complete.")
spark.stop()