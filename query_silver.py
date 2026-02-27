"""
query_silver.py — Analytics & Insights from Silver Layer
Using Spark SQL to query structured data from MinIO
"""

from spark_minio import create_spark_session

# Define paths for Silver Delta Lake tables
SILVER_STOCK_PATH = "s3a://silver/stock_data/vn30/"
SILVER_NIKE_PATH = "s3a://silver/web_crawl/nike/"
SILVER_USERS_PATH = "s3a://silver/users/"

def load_silver_data(spark):
    print(" Reading data from Silver Delta Lake...")
    
    df_stock = None
    df_users = None
    df_nike = None

    # Read Stock 
    print("\n [STOCK SILVER]")
    try:
        df_stock = spark.read.format("delta").load(SILVER_STOCK_PATH)
        df_stock.show(5)
        print(f"Total Stock rows: {df_stock.count():,}")
    except Exception as e:
        print(f"Error reading Stock: {e}")

    # Read Users 
    print("\n [USERS SILVER]")
    try:
        df_users = spark.read.format("delta").load(SILVER_USERS_PATH)
        df_users.show(5)
        print(f"Total Users rows: {df_users.count():,}")
    except Exception as e:
        print(f"Error reading Users: {e}")

    # Read Nike 
    print("\n [NIKE SILVER]")
    try:
        df_nike = spark.read.format("delta").load(SILVER_NIKE_PATH)
        df_nike.show(5)
        print(f"Total Nike rows: {df_nike.count():,}")
    except Exception as e:
        print(f"Error reading Nike: {e}")
    
    return df_stock, df_nike, df_users

def run_analytics(spark):
    print("\n--- LOADING SILVER DATA ---")
    # Load Silver Delta Lake tables
    df_stock, df_nike, df_users = load_silver_data(spark)

    # Create Temporary Views for SQL querying
    views_created = []
    if df_stock:
        df_stock.createOrReplaceTempView("silver_stock")
        views_created.append("silver_stock")
    if df_nike:
        df_nike.createOrReplaceTempView("silver_nike")
        views_created.append("silver_nike")
    if df_users:
        df_users.createOrReplaceTempView("silver_users")
        views_created.append("silver_users")

    
    # ANALYTICS 1: Top 5 Stock Symbols with Highest Avg Trading Volume
    
    if "silver_stock" in views_created:
        print("\n[ANALYSIS 1] Top 5 Tickers by Average Trading Volume:")
        spark.sql("""
            SELECT ticker, 
                   CAST(AVG(volume) AS BIGINT) as avg_daily_volume,
                   CAST(AVG(close) AS DECIMAL(10,2)) as avg_price
            FROM silver_stock
            GROUP BY ticker
            ORDER BY avg_daily_volume DESC
            LIMIT 5
        """).show()
    else:
        print("\n[ANALYSIS 1] Skipped: 'silver_stock' table not found.")

    
    # ANALYTICS 2: Nike Product Stats by Gender
    
    if "silver_nike" in views_created:
        print("\n[ANALYSIS 2] Nike Product Summary (Count & Max Price by Gender):")
        spark.sql("""
            SELECT gender, 
                   COUNT(*) as product_count, 
                   FORMAT_NUMBER(MAX(price_numeric), 0) as max_price_vnd,
                   FORMAT_NUMBER(AVG(price_numeric), 0) as avg_price_vnd
            FROM silver_nike
            GROUP BY gender
            ORDER BY product_count DESC
        """).show()
    else:
        print("\n[ANALYSIS 2] Skipped: 'silver_nike' table not found.")

    
    # ANALYTICS 3: User Spending Distribution by Status
    
    if "silver_users" in views_created:
        print("\n[ANALYSIS 3] User Financial Summary by Status:")
        spark.sql("""
            SELECT status, 
                   COUNT(*) as user_count,
                   CAST(SUM(amount) AS DECIMAL(18,2)) as total_balance,
                   CAST(AVG(amount) AS DECIMAL(10,2)) as avg_balance
            FROM silver_users
            GROUP BY status
        """).show()
    else:
        print("\n[ANALYSIS 3] Skipped: 'silver_users' table not found.")

    
    # ANALYTICS 4: Stock Price Trend Analysis (Monthly Volatility)
    
    if "silver_stock" in views_created:
        print("\n[ANALYSIS 4] Top 5 Month-Year with highest average Price Range (Volatility):")
        spark.sql("""
            SELECT year, month, 
                   CAST(AVG(price_range) AS DECIMAL(10,2)) as avg_volatility
            FROM silver_stock
            GROUP BY year, month
            ORDER BY avg_volatility DESC
            LIMIT 5
        """).show()
    else:
        print("\n[ANALYSIS 4] Skipped: 'silver_stock' table not found.")

if __name__ == "__main__":
    spark = create_spark_session("Lakehouse Analytics - Silver Layer")
    run_analytics(spark)
    spark.stop()
