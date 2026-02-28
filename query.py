"""
query_silver.py — Analytics & Insights from Silver Layer
Using Spark SQL to query structured data from MinIO
"""

from spark_minio import create_spark_session

# Define paths for Gold Delta Lake tables
GOLD_AVG_VOLUME_PATH = "s3a://gold/stock_data/vn30_avg_volume/"
GOLD_VOLATILITY_PATH = "s3a://gold/stock_data/vn30_volatility/"

def load_gold_data(spark):
    print(" Reading data from Gold Delta Lake...")
    
    df_avg_volume = None
    df_volatility = None

    print("\n [GOLD AVERAGE VOLUME]")
    try:
        df_avg_volume = spark.read.format("delta").load(GOLD_AVG_VOLUME_PATH)
        df_avg_volume.show(5)
    except Exception as e:
        print(f"Error reading Gold Average Volume: {e}")

    print("\n [GOLD VOLATILITY]")
    try:
        df_volatility = spark.read.format("delta").load(GOLD_VOLATILITY_PATH)
        df_volatility.show(5)
    except Exception as e:
        print(f"Error reading Gold Volatility: {e}")

    return df_avg_volume, df_volatility

def run_analytics(spark):
    print("\n--- LOADING GOLD DATA ---")
    # Load Gold Delta Lake tables
    df_avg_volume, df_volatility = load_gold_data(spark)

    #  Top 5 Stock Symbols with Highest Avg Trading Volume
    if df_avg_volume:
        print("\n Top 5 Tickers by Average Trading Volume:")
        df_avg_volume.limit(5).show()
    else:
        print("\n[ANALYSIS 1] Skipped: 'gold_avg_volume' table not found.")

    #  Stock Price Trend Analysis (Monthly Volatility)
    if df_volatility:
        print("\n Top 5 Month-Year with highest average Price Range (Volatility):")
        df_volatility.limit(5).show()
    else:
        print("\n[ANALYSIS 4] Skipped: 'gold_volatility' table not found.")

if __name__ == "__main__":
    spark = create_spark_session("Lakehouse Analytics - Silver Layer")
    run_analytics(spark)
    spark.stop()
