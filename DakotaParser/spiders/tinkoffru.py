import scrapy


class TinkoffruSpider(scrapy.Spider):
    name = 'tinkoffru'
    allowed_domains = ['tinkoff.ru']
    start_urls = ['http://tinkoff.ru/']

    def parse(self, response):
        pass
