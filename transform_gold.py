"""
transform_gold.py — Aggregate Silver data into Business-level Gold tables
Silver (Delta) → Gold (Delta)

Usage:
    python transform_gold.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# Paths
SILVER_STOCK_PATH = "s3a://silver/stock_data/vn30/"
GOLD_AVG_VOLUME_PATH = "s3a://gold/stock_data/vn30_avg_volume/"
GOLD_VOLATILITY_PATH = "s3a://gold/stock_data/vn30_volatility/"

def transform_gold(spark):
    print(" Reading data from Silver...")
    df_silver = spark.read.format("delta").load(SILVER_STOCK_PATH)
    
    # 1. Top Tickers by Average Trading Volume
    print("\n Computing Average Trading Volume...")
    df_avg_volume = df_silver.groupBy("ticker") \
        .agg(
            F.avg("volume").cast("bigint").alias("avg_daily_volume"),
            F.avg("close").cast("decimal(10,2)").alias("avg_price")
        ) \
        .orderBy(F.desc("avg_daily_volume"))

    print(f" Writing to Gold: {GOLD_AVG_VOLUME_PATH}")
    df_avg_volume.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_AVG_VOLUME_PATH)
    
    # 2. Stock Price Trend Analysis (Monthly Volatility)
    print("\n Computing Monthly Volatility...")
    df_volatility = df_silver.groupBy("year", "month") \
        .agg(
            F.avg("price_range").cast("decimal(10,2)").alias("avg_volatility")
        ) \
        .orderBy(F.desc("avg_volatility"))
        
    print(f" Writing to Gold: {GOLD_VOLATILITY_PATH}")
    df_volatility.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_VOLATILITY_PATH)

    print("\n Finished Gold Transformations!")

if __name__ == "__main__":
    spark = create_spark_session("Transform Stock Silver → Gold")
    transform_gold(spark)
    spark.stop()
