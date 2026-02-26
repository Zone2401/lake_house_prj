"""
main.py - Run the full lakehouse ingestion pipeline

Steps:
  1. Fetch VN30 stock data from vnstock  -> upload to MinIO
  2. Generate fake user data             -> upload to MinIO

Usage:
  python main.py
"""

import sys
import os

# Add data_ingestion folder to path so we can import its modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_ingestion'))

from stock_api     import get_stock_data
from users_info    import get_users_data
from load_to_minio import upload_df, TODAY


# ---------------------------------------------------------
# STEP 1: VN30 Stock Data
# ---------------------------------------------------------
print("=" * 50)
print("STEP 1: Fetching VN30 stock data")
print("=" * 50)

stock_data = get_stock_data()   # returns dict {"ACB": df, "BID": df, ...}

print(f"\nUploading {len(stock_data)} tickers to MinIO...")
for ticker, df in stock_data.items():
    s3_key = f"stock_data/vn30/ingested_date={TODAY}/{ticker}.csv"
    upload_df(df, s3_key)

print(f"\n[OK] Stock: uploaded {len(stock_data)}/30 tickers\n")


# ---------------------------------------------------------
# STEP 2: Users (fake data)
# ---------------------------------------------------------
print("=" * 50)
print("STEP 2: Generating user data")
print("=" * 50)

users_df = get_users_data(n=1000)   # returns DataFrame with 1000 rows

s3_key = f"users/ingested_date={TODAY}/users_data.csv"
upload_df(users_df, s3_key)

print(f"\n[OK] Users: uploaded {len(users_df)} rows\n")


# ---------------------------------------------------------
# STEP 3: Nike Web Crawl
# ---------------------------------------------------------
print("=" * 50)
print("STEP 3: Running Nike web crawl")
print("=" * 50)

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Set the environment variable for Scrapy settings
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'web_crawl.settings')

# Run the spider
process = CrawlerProcess(get_project_settings())
process.crawl("nike")
process.start() # Blocks until finished (requires a fresh process)

print(f"\n[OK] Nike crawl finished and uploaded to MinIO\n")


# ---------------------------------------------------------
print("=" * 50)
print("Pipeline complete!")
print(f"  MinIO Console : http://localhost:9001")
print(f"  Bucket        : bronze")
print(f"  Date          : {TODAY}")
print("=" * 50)
