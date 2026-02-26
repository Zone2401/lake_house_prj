BOT_NAME = "nike"

SPIDER_MODULES = ["web_crawl.spiders"]
NEWSPIDER_MODULE = "web_crawl.spiders"

ROBOTSTXT_OBEY = False

# Tối ưu concurrency
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 2
DOWNLOAD_DELAY = 0

FEED_EXPORT_ENCODING = "utf-8"

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

PLAYWRIGHT_BROWSER_TYPE = "chromium"

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 60000,
}

PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Enable pipeline
ITEM_PIPELINES = {
    "web_crawl.pipelines.NikePipeline": 300,
}

# Reduce log noise
import logging
logging.getLogger("scrapy-playwright").setLevel(logging.WARNING)