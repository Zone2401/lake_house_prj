"""
transform_nike.py — Clean & Transform Nike Web Crawl Data
Bronze (JSON) → Silver (Delta)

Usage:
    python transform_nike.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# Path 
BRONZE_PATH = "s3a://bronze/web_crawl/nike/"
SILVER_PATH = "s3a://silver/web_crawl/nike/"


def transform_nike(spark):
    print(" Reading Nike data from Bronze JSON...")
    # Spark will aggregate all JSON files in the Bronze path
    df = spark.read.json(BRONZE_PATH)

    print(f"   Initial records: {df.count():,}")
    print(f"   Original Schema:")
    df.printSchema()

    # Clean Price Column (Example: "3,239,000₫" → 3239000.0) 
    print("\n Cleaning price data...")
    df = df.withColumn("price_numeric", 
        F.regexp_replace(F.col("price"), "[^0-9]", "").cast("double")
    )
    df = df.withColumn("currency", F.lit("VND"))

    # Categorize by Gender/Category from Subtitle 
    print(" Extracting gender category...")
    df = df.withColumn("gender",
        F.when(F.col("subtitle").contains("Men's"), "Men")
         .when(F.col("subtitle").contains("Women's"), "Women")
         .when(F.col("subtitle").contains("Kids'"), "Kids")
         .otherwise("Unisex/Other")
    )

    #  Remove Erroneous Data (Price <= 0 or missing title)
    print(" Filtering invalid data...")
    df = df.filter(F.col("price_numeric") > 0).dropna(subset=["title"])
    print(f"   Rows after filtering: {df.count():,}")

    #  Remove Duplicates
    print(" Removing duplicates by title and color...")
    df = df.dropDuplicates(["title", "color"])
    print(f"   Rows after deduplication: {df.count():,}")

    #  Add Audit Columns 
    print(" Adding audit columns...")
    df = df \
        .withColumn("transformed_at", F.current_timestamp()) \
        .withColumn("source_name",    F.lit("nike_web_crawl"))

    # Select & Reorder Columns
    df = df.select(
        "title",
        "subtitle",
        "gender",
        "price_numeric",
        "currency",
        "color",
        "url",
        "ingested_date",
        "transformed_at",
        "source_name"
    )

    #  Write to Silver (Delta)
    print(f"\n Writing to Silver: {SILVER_PATH}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingested_date", "gender") \
        .save(SILVER_PATH)

    print(f"\n Finished! {df.count():,} rows → {SILVER_PATH}")
    print("\n New Silver Schema:")
    df.printSchema()
    print("\n Sample 5 Nike Silver rows:")
    df.show(5, truncate=False)

    return df


if __name__ == "__main__":
    spark = create_spark_session("Transform Nike Bronze → Silver")
    transform_nike(spark)
    spark.stop()
