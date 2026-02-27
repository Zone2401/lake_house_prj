"""
transform_stock.py — Clean & Transform VN30 Stock Data
Bronze (Parquet) → Silver (Delta)

Usage:
    python transform_stock.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# Path 
BRONZE_PATH = "s3a://bronze/stock_data/vn30/"
SILVER_PATH = "s3a://silver/stock_data/vn30/"


def transform_stock(spark):
    print(" Reading data from Bronze...")
    df = spark.read.parquet(BRONZE_PATH)

    print(f"   Initial rows: {df.count():,}")
    print(f"   Original Schema:")
    df.printSchema()

    #  Cast Data Types 
    print("\n  Normalizing data types...")
    df = df.withColumn("time", F.to_date("time"))          # string → date
    df = df.withColumn("open",   F.col("open").cast("double"))
    df = df.withColumn("high",   F.col("high").cast("double"))
    df = df.withColumn("low",    F.col("low").cast("double"))
    df = df.withColumn("close",  F.col("close").cast("double"))
    df = df.withColumn("volume", F.col("volume").cast("long"))

    # Remove Nulls in Critical columns
    print("  Removing null rows...")
    df = df.dropna(subset=["time", "close", "volume"])
    print(f"   Rows after null removal: {df.count():,}")

    # Remove Duplicates
    print("Removing duplicate rows...")
    df = df.dropDuplicates(["ticker", "time", "open", "close"])
    print(f"   Rows after duplicate removal: {df.count():,}")

    # Filter Invalid Values 
    print("Filtering out invalid values...")
    df = df.filter(
        (F.col("close")  > 0) &
        (F.col("volume") > 0) &
        (F.col("high")   >= F.col("low")) &    # high must be >= low
        (F.col("high")   >= F.col("close")) &  # high must be >= close
        (F.col("low")    <= F.col("open"))     # low must be <= open
    )
    print(f"   Rows after filtering: {df.count():,}")

    #  Add Calculated Columns 
    print(" Adding calculated columns...")
    df = df \
        .withColumn("price_change",
            F.round(F.col("close") - F.col("open"), 2)
        ) \
        .withColumn("price_change_pct",
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 2)
        ) \
        .withColumn("price_range",
            F.round(F.col("high") - F.col("low"), 2)
        )

    #  Add Audit Columns 
    print(" Adding audit columns...")
    df = df \
        .withColumn("transformed_at", F.current_timestamp()) \
        .withColumn("year",  F.year("time")) \
        .withColumn("month", F.month("time"))

    #  Reorder Columns 
    df = df.select(
        "ticker", "time",
        "open", "high", "low", "close",
        "volume",
        "price_change",
        "price_change_pct",
        "price_range",
        "year", "month",
        "ingested_at",
        "source_name",
        "transformed_at",
    )

    #  Write to Silver (Delta) 
    print(f"\n Writing to Silver: {SILVER_PATH}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(SILVER_PATH)

    print(f"\n Finished! {df.count():,} rows → {SILVER_PATH}")
    print("\n Silver Schema:")
    df.printSchema()
    print("\n Sample 5 rows:")
    df.show(5, truncate=False)

    return df


if __name__ == "__main__":
    spark = create_spark_session("Transform Stock Bronze → Silver")
    transform_stock(spark)
    spark.stop()