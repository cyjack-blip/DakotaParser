import scrapy
from DakotaParser.items import RuinvestingcomItem
from pydispatch import dispatcher
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy import Request
from pymongo import MongoClient
import re


class RuinvestingcomSpider(scrapy.Spider):
    name = 'ruinvestingcom'
    allowed_domains = ['ru.investing.com', 'sbcharts.investing.com']
    start_urls = [
        'https://ru.investing.com/news/forex-news',
        'https://ru.investing.com/news/stock-market-news',
        'https://ru.investing.com/news/commodities-news',
        'https://ru.investing.com/news/economy',
        'https://ru.investing.com/news/economic-indicators',
        'https://ru.investing.com/news/cryptocurrency-news',
        'https://ru.investing.com/news/personal-finance-news'
    ]
    popup_tickers_url = 'https://sbcharts.investing.com/charts_xml/jschart_sideblock_{}_area.json'

    def __init__(self, **kwargs):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        # self.last_item_id = kwargs.pop('last_item_id')
        self.last_item_id = self.get_ednpoint()
        self.parced_items = []
        super().__init__(**kwargs)

    def spider_closed(self, spider):
        print(f'spider "{self.name}" report')
        if self.parced_items:
            end = max(self.parced_items) + 1
            print(f'parsed items: {sorted(self.parced_items)}')
            print(f'lets make next parse from: {end}')
        else:
            end = self.last_item_id
            print('nothing new parsed')
            print(f'lets make next parse from: {end}')
        self.set_endpoint(str(end))

    def get_ednpoint(self):
        _client = MongoClient('localhost', 27017)
        _mongo_base = _client['parsed']
        collection = _mongo_base['settings']
        result = collection.find_one({'$and': [{'name': 'endpoint'}, {'parser': self.name}]})
        return int(result['value'])

    def set_endpoint(self, endpoint):
        _client = MongoClient('localhost', 27017)
        _mongo_base = _client['parsed']
        collection = _mongo_base['settings']
        collection.update_one({'$and': [{'name': 'endpoint'}, {'parser': self.name}]}, {"$set": {'value': endpoint}})
        return

    def parse(self, response: HtmlResponse):
        links = response.xpath(
            "//div[@class='largeTitle']/article[contains(@class,'articleItem')]//a[@class='title']/@href").extract()
        for link in links:
            post_id = int(self.fetch_post_id(link))
            if post_id > self.last_item_id:
                yield response.follow(link, callback=self.parse_news)

    def fetch_category(self, link):
        return re.search('/news/(.*)$', link).group(1)

    def fetch_post_id(self, link):
        return int(re.search('.*-(\d*)$', link).group(1))

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
            tickers=[],
            provider=provider
        )
        t = []
        tickers = response.xpath("//span[contains(@class, 'js-hover-me-wrapper')]//a/@data-pairid").extract()
        if tickers:
            for i in tickers:
                t.append(self.popup_tickers_url.format(i))
        a = self._handle_tickers(item, t)
        if not isinstance(a, RuinvestingcomItem):
            yield a
        else:
            self.parced_items.append(int(item['post_id']))
            yield item

    def parse_hidden_ticker(self, response: HtmlResponse):
        item = response.meta['item']
        item['tickers'].append(response.json())
        a = self._handle_tickers(item, response.meta['t'])
        if not isinstance(a, RuinvestingcomItem):
            yield a
        else:
            self.parced_items.append(int(item['post_id']))
            yield item

    def _handle_tickers(self, item, t):
        try:
            request = Request(t.pop(), self.parse_hidden_ticker, meta={'item': item, 't': t}, dont_filter=True)
            return request
        except:
            return item
