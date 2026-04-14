from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, count, round as spark_round,
    trim, upper, min as spark_min, max as spark_max,
    sum as spark_sum, stddev, lit
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


# SPARK SESSION
spark = SparkSession.builder \
    .appName("CasinoBatchProcessing") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("=" * 60)
print("  CASINO BIG DATA - BATCH PROCESSING")
print("=" * 60)


# 1. LOAD
print("\n[1] Loading raw data...")
df_raw = spark.read.csv(RAW_CSV, header=True, inferSchema=True)
print(f"    Rows loaded : {df_raw.count():,}")
print(f"    Columns     : {len(df_raw.columns)}")
df_raw.printSchema()

# 2. CLEANING
print("\n[2] Cleaning data...")
df_clean = df_raw \
    .withColumn("game",           trim(upper(col("game")))) \
    .withColumn("provider",       trim(upper(col("provider")))) \
    .withColumn("game_type",      trim(upper(col("game_type")))) \
    .withColumn("volatility",     trim(upper(col("volatility")))) \
    .withColumn("rtp",            spark_round(col("rtp").cast("double"), 2)) \
    .withColumn("min_bet",        spark_round(col("min_bet").cast("double"), 2)) \
    .withColumn("max_win",        spark_round(col("max_win").cast("double"), 2)) \
    .withColumn("max_multiplier", spark_round(col("max_multiplier").cast("double"), 2)) \
    .withColumn("jackpot",
        when(col("jackpot").isNull(), "No Jackpot").otherwise(col("jackpot"))) \
    .withColumn("max_multiplier",
        when(col("max_multiplier").isNull(), 0.0).otherwise(col("max_multiplier"))) \
    .dropDuplicates()

rows_after = df_clean.count()
print(f"    Rows after cleaning: {rows_after:,}")

# 3. TRANSFORMATIONS + FEATURE GENERATION
print("\n[3] Transformations and feature generation...")
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
    ) \
    .withColumn("multiplier_tier",
        when(col("max_multiplier") >= 10000, "Mega Multiplier")
        .when(col("max_multiplier") >= 5000,  "High Multiplier")
        .when(col("max_multiplier") >= 1000,  "Mid Multiplier")
        .otherwise("Standard Multiplier")
    ) \
    .withColumn("profitability_score",
        spark_round(
            (100 - col("rtp")) *
            when(col("volatility") == "HIGH",      lit(3))
            .when(col("volatility") == "VERY HIGH", lit(4))
            .when(col("volatility") == "MEDIUM",    lit(2))
            .otherwise(lit(1)),
            2
        )
    )

print("    Features added:")
print("      + rtp_category       (High / Medium / Low RTP)")
print("      + bet_tier           (Low / Mid / High Stakes)")
print("      + has_jackpot        (boolean)")
print("      + player_friendly    (mobile + free spins)")
print("      + multiplier_tier    (Mega / High / Mid / Standard)")
print("      + profitability_score(operator edge × volatility weight)")

# 4. AGGREGATIONS
print("\n[4] Aggregations...")

# 4a. By provider
by_provider = df_transformed.groupBy("provider").agg(
    count("*").alias("total_games"),
    spark_round(avg("rtp"), 2).alias("avg_rtp"),
    spark_round(avg("max_win"), 2).alias("avg_max_win"),
    spark_round(avg("profitability_score"), 2).alias("avg_profitability"),
    spark_sum(when(col("has_jackpot"), 1).otherwise(0)).alias("jackpot_games")
).orderBy("total_games", ascending=False)

# 4b. By game type
by_game_type = df_transformed.groupBy("game_type").agg(
    count("*").alias("total_games"),
    spark_round(avg("rtp"), 2).alias("avg_rtp"),
    spark_round(avg("max_multiplier"), 0).alias("avg_max_multiplier")
).orderBy("total_games", ascending=False)

# 4c. By volatility
by_volatility = df_transformed.groupBy("volatility").agg(
    count("*").alias("total_games"),
    spark_round(avg("rtp"), 2).alias("avg_rtp"),
    spark_round(avg("max_win"), 2).alias("avg_max_win")
).orderBy("total_games", ascending=False)

# 4d. By RTP category
by_rtp_cat = df_transformed.groupBy("rtp_category").agg(
    count("*").alias("total_games"),
    spark_round(avg("max_win"), 2).alias("avg_max_win")
).orderBy("total_games", ascending=False)

# 4e. By bet tier
by_bet_tier = df_transformed.groupBy("bet_tier").agg(
    count("*").alias("total_games"),
    spark_round(avg("rtp"), 2).alias("avg_rtp")
).orderBy("total_games", ascending=False)

print("\n  Top 10 providers:")
by_provider.show(10, truncate=False)
print("  Games by type:")
by_game_type.show(truncate=False)
print("  Games by volatility:")
by_volatility.show(truncate=False)
print("  RTP categories:")
by_rtp_cat.show(truncate=False)
print("  Bet tiers:")
by_bet_tier.show(truncate=False)

# 5. JOIN
print("\n[5]JOIN: games enriched with provider-level stats...")
provider_stats = df_transformed.groupBy("provider").agg(
    spark_round(avg("rtp"), 2).alias("provider_avg_rtp"),
    count("*").alias("provider_total_games"),
    spark_round(stddev("rtp"), 4).alias("provider_rtp_stddev"),
    spark_round(avg("profitability_score"), 2).alias("provider_avg_profitability")
)

# JOIN
df_enriched = df_transformed.join(
    provider_stats,
    on="provider",
    how="left"
).withColumn("rtp_vs_provider_avg",
    spark_round(col("rtp") - col("provider_avg_rtp"), 2)
)

print(f"    Rows after join : {df_enriched.count():,}")
print("    New columns from join:")
print("      + provider_avg_rtp")
print("      + provider_total_games")
print("      + provider_rtp_stddev")
print("      + provider_avg_profitability")
print("      + rtp_vs_provider_avg  (how each game compares to its provider mean)")

print("\n  Sample — 5 rows with enriched provider stats:")
df_enriched.select(
    "game", "provider", "rtp",
    "provider_avg_rtp", "rtp_vs_provider_avg",
    "provider_total_games"
).show(5, truncate=False)

# 6. SAVE TO PARQUET
print("\n[6] Saving to Parquet...")

# Processed layer 
df_enriched.write.mode("overwrite").parquet(PROCESSED_DIR)
print(f"    Processed layer saved : {PROCESSED_DIR}")

# Curated layer 
by_provider.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_provider")
by_game_type.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_game_type")
by_volatility.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_volatility")
by_rtp_cat.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_rtp_category")
by_bet_tier.write.mode("overwrite").parquet(f"{CURATED_DIR}/by_bet_tier")
print(f"    Curated layer saved   : {CURATED_DIR}")

print("\n" + "=" * 60)
print("  BATCH PROCESSING COMPLETE")
print("=" * 60)
spark.stop()