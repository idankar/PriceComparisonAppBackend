import scrapy


class NutellaSpider(scrapy.Spider):
    name = "nutella"
    allowed_domains = ["shufersal.co.il"]
    start_urls = ["https://shufersal.co.il"]

    def parse(self, response):
        pass
