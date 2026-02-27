"""
main.py - Orchestrate the Lakehouse data pipeline
"""

import sys
import os
import logging
from datetime import datetime

# Add data_ingestion folder to path 
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_ingestion'))

from stock_api     import get_stock_data
from users_info    import get_users_data
from load_to_minio import upload_parquet, upload_json, TODAY

# --- Setup Logging ---
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("Starting Lakehouse Bronze Pipeline...")

    
    # VN30 Stock Data (Ingest to Bronze as Parquet)
    
    logger.info(" Fetching VN30 stock data ")
    stock_data = get_stock_data()

    logger.info(f"Uploading {len(stock_data)} tickers as Parquet to MinIO...")
    for ticker, df in stock_data.items():
        s3_key = f"stock_data/vn30/ingested_date={TODAY}/{ticker}.parquet"
        upload_parquet(df, s3_key)


    
    # Users (Ingest to Bronze as Parquet)
    
    logger.info(" Generating user data ")
    users_df = get_users_data(n=1000)

    s3_key = f"users/ingested_date={TODAY}/users_data.parquet"
    upload_parquet(users_df, s3_key)


   
    # Nike Web Crawl (Ingest to Bronze as JSON)
    
    logger.info("Running Nike web crawl")
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings
    
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'web_crawl.settings')
    process = CrawlerProcess(get_project_settings())
    process.crawl("nike")
    process.start()

    logger.info("Pipeline complete! Check MinIO at http://localhost:9001")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
