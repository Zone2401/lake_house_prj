

from pyspark.sql import SparkSession


def create_spark_session(app_name="Lakehouse"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.nanosAsLong", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark connected to MinIO successfully!")
    return spark


# Test reading Delta tables when run directly
if __name__ == "__main__":
    spark = create_spark_session()

    # Read Stock (Delta)
    print("\n [STOCK SILVER]")
    try:
        df_stock = spark.read.format("delta").load("s3a://silver/stock_data/vn30/")
        df_stock.printSchema()
        df_stock.show(5)
        print(f"Total rows: {df_stock.count():,}")
    except Exception as e:
        print(f"Could not read Stock Delta table: {e}")

    # Read Users (Delta) 
    print("\n [USERS SILVER]")
    try:
        df_users = spark.read.format("delta").load("s3a://silver/users/")
        df_users.printSchema()
        df_users.show(5)
        print(f"Total rows: {df_users.count():,}")
    except Exception as e:
        print(f"Could not read Users Delta table: {e}")

    # Read Nike (Delta)
    print("\n [NIKE SILVER]")
    try:
        df_nike = spark.read.format("delta").load("s3a://silver/web_crawl/nike/")
        df_nike.printSchema()
        df_nike.show(5)
        print(f"Total rows: {df_nike.count():,}")
    except Exception as e:
        print(f"Could not read Nike Delta table: {e}")

    spark.stop()
