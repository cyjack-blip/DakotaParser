import scrapy
from DakotaParser.items import RuinvestingcomItem
from pydispatch import dispatcher
from scrapy import signals
from scrapy.http import HtmlResponse
from pymongo import MongoClient
import re


class RuinvestingcomSpider(scrapy.Spider):
    name = 'ruinvestingcom'
    allowed_domains = ['ru.investing.com']
    start_urls = ['https://ru.investing.com/news/economy',
                  'https://ru.investing.com/news/personal-finance-news']

    def __init__(self, **kwargs):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        # self.last_item_id = kwargs.pop('last_item_id')
        self.last_item_id = self.get_ednpoint()
        self.parced_items = []
        super().__init__(**kwargs)

    def spider_closed(self):
        pass

    def get_ednpoint(self):
        pass

    def set_endpoint(self):
        pass

    def parse(self, response: HtmlResponse):
        links = response.xpath(
            "//div[@class='largeTitle']/article[contains(@class,'articleItem')]//a[@class='title']/@href").extract()
        for link in links:
            yield response.follow(link, callback=self.parse_news)

        print()
        # yield response.follow(link, callback=self.parse_news)

    def fetch_category(self, link):
        return re.search('/news/(.*)$', link).group(1)

    def fetch_post_id(self, link):
        return re.search('.*-(\d*)$', link).group(1)

    def fetch_post_type(self, link):
        return re.search('^/(.*)/.*', link).group(1)

    def fetch_published_time(self, text):
        a = re.search('^.*\((.*)\)', text)
        if a:
            return a.group(1)
        else:
            return text

    def fetch_provider(self, url):
        providers = {
            'investing-new': 'Investing',
            'abe8dc61a1f3d8817fe03af10d53f223': 'Euronews',
            'Reuters': 'Reuters',
            'IFX-new': 'Interfax'
        }
        match = re.search('^.+/(.*)\.\w\w\w$', url).group(1)
        if match in providers:
            return providers[match]
        else:
            return match

    def parse_news(self, response: HtmlResponse):
        category_link = response.xpath("//div[@class='contentSectionDetails']/a[last()]/@href").extract_first()
        published_at = self.fetch_published_time(response.xpath("//div[@class='contentSectionDetails']/span/text()").extract_first())
        category = self.fetch_category(category_link)
        post_type = self.fetch_post_type(category_link)
        post_id = self.fetch_post_id(response.url)
        title = response.xpath("//h1[@class='articleHeader']/text()").extract_first()
        img_big = response.xpath("//div[contains(@class, 'articlePage')]/div[@id='imgCarousel']//img/@src").extract_first()
        if not img_big:
            img_big = ''
        body = response.xpath("//div[contains(@class, 'articlePage')]/child::*[not(@class='imgCarousel') and not (@class='clear')]").extract()
        body = '\n'.join(map(str, body))
        provider_scr = response.xpath("//div[@class='contentSectionDetails']//img/@src").extract_first()
        provider = self.fetch_provider(provider_scr)

        item = RuinvestingcomItem(
            type=post_type,
            published_at=published_at,
            post_id=post_id,
            category=category,
            title=title,
            body=body,
            img_big=img_big,
            tickers='',
            provider=provider
        )
        yield item
