"""
load_to_minio.py - Helper functions to upload data to MinIO (Senior DE Standard)

Usage:
    from load_to_minio import upload_parquet, upload_json

    upload_parquet(df, "stock_data/vn30/ACB.parquet")
    upload_json(data, "web_crawl/nike/nike_products.json")
"""

import io
import json
import boto3
import logging
from datetime import datetime

# --- Professional Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MinIO connection settings
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

BUCKET = "bronze"
TODAY  = datetime.today().strftime("%Y-%m-%d")


def upload_parquet(df, s3_key):
    """
    Upload a DataFrame to MinIO as a Parquet file (Senior Standard).
    Parquet is faster, smaller, and keeps data types (Schema).
    """
    # Senior DE Tip: Spark 3.x explicitly hates Nanosecond timestamps from Pandas.
    # We use coerce_timestamps='ms' to force the Parquet file to use Milliseconds.
    # This is the most reliable way to fix 'Illegal Parquet type: INT64 (TIMESTAMP(NANOS))'.
    if 'ingested_at' in df.columns:
        df['ingested_at'] = df['ingested_at'].astype('datetime64[ms]')

    # Convert DataFrame to Parquet format in memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', coerce_timestamps='ms', allow_truncated_timestamps=True)
    
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(f"Uploaded Parquet -> s3://{BUCKET}/{s3_key}")


def upload_json(data, s3_key):
    """Upload a dict/list to MinIO as a JSON file (no local file saved)."""
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )
    logger.info(f"Uploaded JSON -> s3://{BUCKET}/{s3_key}")