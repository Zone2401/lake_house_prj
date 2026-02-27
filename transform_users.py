"""
transform_users.py — Clean & Transform Users Data (PII Handling)
Bronze (Parquet) → Silver (Delta)

Usage:
    python transform_users.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# Path
BRONZE_PATH = "s3a://bronze/users/"
SILVER_PATH = "s3a://silver/users/"


def transform_users(spark):
    print(" Reading User data from Bronze Parquet...")
    df = spark.read.parquet(BRONZE_PATH)

    print(f"   Initial records: {df.count():,}")
    print(f"   Original Schema:")
    df.printSchema()

    # Normalize Phone Number (Remove special characters)
    print("\n Normalizing phone numbers...")
    df = df.withColumn("phone_clean", 
        F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    )

    # Email Masking (PII Protection) 
    # Example: abcxyz@gmail.com -> a***z@gmail.com
    print(" Masking Email (PII Security)...")
    df = df.withColumn("email_masked",
        F.regexp_replace(F.col("email"), r"(^.)(.+)(.@.+$)", r"$1***$3")
    )

    # Standardize Status 
    print("Standardizing Status...")
    df = df.withColumn("status", F.upper(F.col("status")))

    # Handle Nulls & Filter Data 
    print("Filtering invalid data...")
    df = df.dropna(subset=["id", "email"]).filter(F.col("amount") >= 0)
    print(f"   Rows after filtering: {df.count():,}")

    # Add Audit Columns & Metadata 
    print("Adding audit columns...")
    df = df \
        .withColumn("transformed_at", F.current_timestamp()) \
        .withColumn("source_name",    F.lit("internal_faker_users"))

    # Select & Reorder Columns 
    df = df.select(
        "id",
        "name",
        "email_masked",
        "phone_clean",
        "address",
        "amount",
        "status",
        "created_date",
        "ingested_date",
        "transformed_at",
        "source_name"
    )

    # Write to Silver (Delta)
    # Partition by status for easy User management (Active/Inactive)
    print(f"\n Writing to Silver: {SILVER_PATH}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("status") \
        .save(SILVER_PATH)

    print(f"\n Finished! {df.count():,} rows → {SILVER_PATH}")
    print("\n New Silver Schema:")
    df.printSchema()
    print("\n Sample 5 Users Silver rows:")
    df.show(5, truncate=False)

    return df


if __name__ == "__main__":
    spark = create_spark_session("Transform Users Bronze → Silver")
    transform_users(spark)
    spark.stop()
