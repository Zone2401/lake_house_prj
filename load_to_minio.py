"""
load_to_minio.py - Helper functions to upload data to MinIO

Usage:
    from load_to_minio import upload_df, upload_json

    upload_df(df, "stock_data/vn30/ACB.csv")
    upload_json(data, "web_crawl/nike/nike_products.json")
"""

import io
import json
import boto3
from datetime import datetime

# MinIO connection settings
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

BUCKET = "bronze"
TODAY  = datetime.today().strftime("%Y-%m-%d")


def upload_df(df, s3_key):
    """Upload a DataFrame to MinIO as a CSV file (no local file saved)."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"  Uploaded -> s3://{BUCKET}/{s3_key}")


def upload_json(data, s3_key):
    """Upload a dict/list to MinIO as a JSON file (no local file saved)."""
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
    )
    print(f"  Uploaded -> s3://{BUCKET}/{s3_key}")