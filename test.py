"""
spark_minio.py — Kết nối Spark với MinIO, đọc Parquet

Cách dùng:
    from spark_minio import create_spark_session

    spark = create_spark_session()
    df = spark.read.parquet("s3a://bronze/stock_data/vn30/")
    df.show()
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name="Lakehouse"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.nanosAsLong", "true") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark kết nối MinIO thành công!")
    return spark


# ── Test đọc Parquet khi chạy trực tiếp ─────────────────────────
if __name__ == "__main__":
    spark = create_spark_session()

    # ── Đọc Stock ────────────────────────────────────────────────
    print("\n [STOCK]")
    df_stock = spark.read.parquet("s3a://bronze/stock_data/vn30/")
    df_stock.printSchema()
    df_stock.show(5)
    print(f"Tổng rows: {df_stock.count():,}")

    # ── Đọc Users ────────────────────────────────────────────────
    print("\n [USERS]")
    df_users = spark.read.parquet("s3a://bronze/users/")
    df_users.printSchema()
    df_users.show(5)
    print(f"Tổng rows: {df_users.count():,}")

    # ── Đọc Nike (JSON) ──────────────────────────────────────────
    print("\n [NIKE]")
    df_nike = spark.read.json("s3a://bronze/web_crawl/nike/")
    df_nike.printSchema()
    df_nike.show(5)
    print(f"Tổng rows: {df_nike.count():,}")

    spark.stop()