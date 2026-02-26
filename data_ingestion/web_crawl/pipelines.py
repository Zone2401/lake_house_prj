# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


import sys
import os

# Ensure load_to_minio is available
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from load_to_minio import upload_json, TODAY

class NikePipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if self.items:
            s3_key = f"web_crawl/nike/ingested_date={TODAY}/nike_products.json"
            print(f"\nFinalizing Nike crawl: uploading {len(self.items)} items to MinIO...")
            upload_json(self.items, s3_key)
        else:
            print("\nFinalizing Nike crawl: No items found to upload.")
