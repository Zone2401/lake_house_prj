"""
transform_nike.py — Clean & Transform Nike Web Crawl Data
Bronze (JSON) → Silver (Parquet)

Cách dùng:
    python transform_nike.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# ── Path ─────────────────────────────────────────────────────────
BRONZE_PATH = "s3a://bronze/web_crawl/nike/"
SILVER_PATH = "s3a://silver/web_crawl/nike/"


def transform_nike(spark):
    print(" Đọc dữ liệu Nike từ Bronze JSON...")
    # Vì dữ liệu web crawl có thể nạp nhiều đợt, Spark sẽ tự gộp các file JSON
    df = spark.read.json(BRONZE_PATH)

    print(f"   Tổng bản ghi gốc : {df.count():,}")
    print(f"   Schema gốc:")
    df.printSchema()

    # ── Bước 1: Làm sạch cột Price (Ví dụ: "3,239,000₫" → 3239000.0) ───
    print("\n Bước 1: Xử lý giá tiền (Price Cleaning)...")
    df = df.withColumn("price_numeric", 
        F.regexp_replace(F.col("price"), "[^0-9]", "").cast("double")
    )
    df = df.withColumn("currency", F.lit("VND"))

    # ── Bước 2: Phân loại đối tượng (Gender/Category) từ Subtitle ─────
    print(" Bước 2: Phân loại đối tượng (Gender Extraction)...")
    df = df.withColumn("gender",
        F.when(F.col("subtitle").contains("Men's"), "Men")
         .when(F.col("subtitle").contains("Women's"), "Women")
         .when(F.col("subtitle").contains("Kids'"), "Kids")
         .otherwise("Unisex/Other")
    )

    # ── Bước 3: Xóa dòng lỗi (Price không có hoặc < 0) ─────────────────
    print(" Bước 3: Lọc bỏ dữ liệu lỗi...")
    df = df.filter(F.col("price_numeric") > 0).dropna(subset=["title"])
    print(f"   Rows sau khi lọc: {df.count():,}")

    # ── Bước 4: Xử lý trùng lặp ──────────────────────────────────────
    print(" Bước 4: Xóa trùng lặp theo title và color...")
    df = df.dropDuplicates(["title", "color"])
    print(f"   Rows sau khi xóa trùng: {df.count():,}")

    # ── Bước 5: Thêm cột Audit ───────────────────────────────────────
    print(" Bước 5: Thêm cột audit...")
    df = df \
        .withColumn("transformed_at", F.current_timestamp()) \
        .withColumn("source_name",    F.lit("nike_web_crawl"))

    # ── Bước 6: Sắp xếp & Chọn cột ────────────────────────────────────
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

    # ── Ghi ra Silver (Parquet) ──────────────────────────────────────
    print(f"\n Ghi vào Silver: {SILVER_PATH}")
    df.write \
        .mode("overwrite") \
        .partitionBy("ingested_date", "gender") \
        .parquet(SILVER_PATH)

    print(f"\n Hoàn tất! {df.count():,} rows → {SILVER_PATH}")
    print("\n Schema Silver mới:")
    df.printSchema()
    print("\n Sample 5 dòng Nike Silver:")
    df.show(5, truncate=False)

    return df


if __name__ == "__main__":
    spark = create_spark_session("Transform Nike Bronze → Silver")
    transform_nike(spark)
    spark.stop()
