"""
transform_users.py — Clean & Transform Users Data (PII Handling)
Bronze (Parquet) → Silver (Parquet)

Cách dùng:
    python transform_users.py
"""

from pyspark.sql import functions as F
from spark_minio import create_spark_session

# ── Path ─────────────────────────────────────────────────────────
BRONZE_PATH = "s3a://bronze/users/"
SILVER_PATH = "s3a://silver/users/"


def transform_users(spark):
    print(" Đọc dữ liệu Users từ Bronze Parquet...")
    df = spark.read.parquet(BRONZE_PATH)

    print(f"   Tổng bản ghi gốc : {df.count():,}")
    print(f"   Schema gốc:")
    df.printSchema()

    # ── Bước 1: Chuẩn hóa Phone Number (Xóa ký tự đặc biệt) ───────────
    print("\n Bước 1: Chuẩn hóa số điện thoại...")
    df = df.withColumn("phone_clean", 
        F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    )

    # ── Bước 2: Masking Email (Bảo mật thông tin - PII Masking) ────────
    # Ví dụ: abcxyz@gmail.com -> a***z@gmail.com
    print(" Bước 2: Masking Email (Bảo mật PII)...")
    df = df.withColumn("email_masked",
        F.regexp_replace(F.col("email"), r"(^.)(.+)(.@.+$)", r"$1***$3")
    )

    # ── Bước 3: Chuẩn hóa trạng thái (Status Standardizing) ────────────
    print(" Bước 3: Chuẩn hóa Status...")
    df = df.withColumn("status", F.upper(F.col("status")))

    # ── Bước 4: Xử lý Null & lọc dữ liệu lỗi ──────────────────────────
    print(" Bước 4: Lọc dữ liệu lỗi...")
    df = df.dropna(subset=["id", "email"]).filter(F.col("amount") >= 0)
    print(f"   Rows sau khi lọc: {df.count():,}")

    # ── Bước 5: Thêm cột Audit & Processing Metadata ─────────────────
    print(" Bước 5: Thêm cột audit...")
    df = df \
        .withColumn("transformed_at", F.current_timestamp()) \
        .withColumn("source_name",    F.lit("internal_faker_users"))

    # ── Bước 6: Sắp xếp cột ──────────────────────────────────────────
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

    # ── Ghi ra Silver (Parquet) ──────────────────────────────────────
    # Phân vùng theo status để dễ dàng quản lý User Active/Inactive
    print(f"\n Ghi vào Silver: {SILVER_PATH}")
    df.write \
        .mode("overwrite") \
        .partitionBy("status") \
        .parquet(SILVER_PATH)

    print(f"\n Hoàn tất! {df.count():,} rows → {SILVER_PATH}")
    print("\n Schema Silver mới:")
    df.printSchema()
    print("\n Sample 5 dòng Users Silver:")
    df.show(5, truncate=False)

    return df


if __name__ == "__main__":
    spark = create_spark_session("Transform Users Bronze → Silver")
    transform_users(spark)
    spark.stop()
