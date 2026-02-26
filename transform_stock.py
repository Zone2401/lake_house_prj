"""
transform_stock.py — Clean & Transform VN30 Stock Data
Bronze (Parquet) → Silver (Parquet)

Cách dùng:
    python transform_stock.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# ── Path ─────────────────────────────────────────────────────────
BRONZE_PATH = "s3a://bronze/stock_data/vn30/"
SILVER_PATH = "s3a://silver/stock_data/vn30/"


def transform_stock(spark):
    print(" Đọc dữ liệu từ Bronze...")
    df = spark.read.parquet(BRONZE_PATH)

    print(f"   Tổng rows ban đầu : {df.count():,}")
    print(f"   Schema gốc:")
    df.printSchema()

    # ── Bước 1: Đổi kiểu dữ liệu ────────────────────────────────
    print("\n Bước 1: Chuẩn hóa kiểu dữ liệu...")
    df = df.withColumn("time", F.to_date("time"))          # string → date
    df = df.withColumn("open",   F.col("open").cast("double"))
    df = df.withColumn("high",   F.col("high").cast("double"))
    df = df.withColumn("low",    F.col("low").cast("double"))
    df = df.withColumn("close",  F.col("close").cast("double"))
    df = df.withColumn("volume", F.col("volume").cast("long"))

    # ── Bước 2: Xóa dòng null ở cột quan trọng ──────────────────
    print(" Bước 2: Xóa dòng null...")
    df = df.dropna(subset=["time", "close", "volume"])
    print(f"   Rows sau khi xóa null: {df.count():,}")

    # ── Bước 3: Xóa dòng trùng lặp ──────────────────────────────
    print(" Bước 3: Xóa dòng trùng lặp...")
    df = df.dropDuplicates(["ticker", "time", "open", "close"])
    print(f"   Rows sau khi xóa trùng: {df.count():,}")

    # ── Bước 4: Xóa giá trị vô lý ───────────────────────────────
    print(" Bước 4: Lọc giá trị vô lý...")
    df = df.filter(
        (F.col("close")  > 0) &
        (F.col("volume") > 0) &
        (F.col("high")   >= F.col("low")) &    # high phải >= low
        (F.col("high")   >= F.col("close")) &  # high phải >= close
        (F.col("low")    <= F.col("open"))     # low phải <= open
    )
    print(f"   Rows sau khi lọc: {df.count():,}")

    # ── Bước 5: Thêm cột tính toán ──────────────────────────────
    print(" Bước 5: Thêm cột tính toán...")
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

    # ── Bước 6: Thêm cột audit ───────────────────────────────────
    print(" Bước 6: Thêm cột audit...")
    df = df \
        .withColumn("transformed_at", F.current_timestamp()) \
        .withColumn("year",  F.year("time")) \
        .withColumn("month", F.month("time"))

    # ── Bước 7: Sắp xếp cột gọn gàng ────────────────────────────
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

    # ── Ghi ra Silver ─────────────────────────────────────────────
    print(f"\n Ghi vào Silver: {SILVER_PATH}")
    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(SILVER_PATH)

    print(f"\n Hoàn tất! {df.count():,} rows → {SILVER_PATH}")
    print("\n Schema Silver:")
    df.printSchema()
    print("\n Sample 5 dòng:")
    df.show(5, truncate=False)

    return df


if __name__ == "__main__":
    spark = create_spark_session("Transform Stock Bronze → Silver")
    transform_stock(spark)
    spark.stop()