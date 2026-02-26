import scrapy
from scrapy_playwright.page import PageMethod


class NikeSpider(scrapy.Spider):
    name = "nike"
    start_urls = ["https://www.nike.com/vn/w"]

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Chờ card load
                    PageMethod("wait_for_selector", "div.product-grid__card"),

                    # Scroll tối ưu (KHÔNG timeout ở đây)
                    PageMethod(
                        "evaluate",
                        """
                        async () => {
                            let lastHeight = 0;
                            let sameCount = 0;
                            const maxSame = 2;

                            while (sameCount < maxSame) {
                                window.scrollTo(0, document.body.scrollHeight);
                                await new Promise(r => setTimeout(r, 800));

                                const newHeight = document.body.scrollHeight;

                                if (newHeight === lastHeight) {
                                    sameCount++;
                                } else {
                                    sameCount = 0;
                                    lastHeight = newHeight;
                                }
                            }
                        }
                        """
                    ),
                ],
            },
            callback=self.parse
        )

    def parse(self, response):
        cards = response.xpath("//div[contains(@class, 'product-card product-grid__card')]")

        for card in cards:
            title = card.xpath(".//div[contains(@class, 'product-card__title')]/text()").get()
            subtitle = card.xpath(".//div[contains(@class, 'product-card__subtitle')]/text()").get()
            color = card.xpath(".//div[contains(@class, 'product-card__product-count')]/text()").get()
            price = card.xpath(".//div[contains(@class, 'product-price')]/text()").get()
            url = card.xpath(".//a[contains(@class, 'product-card__link-overlay')]/@href").get()

            yield {
                "title": title,
                "subtitle": subtitle,        
                "color": color,
                "price": price,
                "url": response.urljoin(url) if url else None
            }
