import scrapy
from DakotaParser.items import VtimesioItem
from pydispatch import dispatcher
from pymongo import MongoClient
from scrapy import signals
from scrapy.http import HtmlResponse
import urllib
import json
from scrapy.spiders import XMLFeedSpider
from dateutil.parser import parse
import re
import html

class VtimesioSpider(XMLFeedSpider):
    name = 'vtimesio'
    allowed_domains = ['vtimes.io']
    start_urls = ['https://www.vtimes.io/rss']
    iterator = 'iternodes'
    itertag = 'item'
    namespaces = [('n', 'http://www.sitemaps.org/schemas/sitemap/0.9')]

    def __init__(self, **kwargs):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        # self.last_item_id = kwargs.pop('last_item_id')
        self.last_item_id = self.get_ednpoint()
        self.parced_items = []
        super().__init__(**kwargs)

    def get_ednpoint(self):
        _client = MongoClient('localhost', 27017)
        _mongo_base = _client['parsed']
        collection = _mongo_base['settings']
        result = collection.find_one({'$and': [{'name': 'endpoint'}, {'parser': self.name}]})
        return int(result['value'])

    def spider_closed(self):
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

    def set_endpoint(self, endpoint):
        _client = MongoClient('localhost', 27017)
        _mongo_base = _client['parsed']
        collection = _mongo_base['settings']
        collection.update_one({'$and': [{'name': 'endpoint'}, {'parser': self.name}]}, {"$set": {'value': endpoint}})
        return

    def fetch_post_id(self, link):
        return int(re.search('.*\-a(\d*)$', link).group(1))

    def make_announce(self, text, para):
        announce = re.sub(r'<figure>.*</figure>', '', text, flags=re.DOTALL)
        cleaner = re.compile('<.*?>')
        announce = html.unescape(re.sub(cleaner, '', announce))
        announce = '.'.join(announce.split('.')[:para]).strip()+'.'
        return announce

    def parse_node(self, response, node):
        node.register_namespace('content', 'http://purl.org/rss/1.0/modules/content/')
        post_id = self.fetch_post_id(node.xpath('link/text()').extract_first())
        body = node.xpath('//content:encoded/text()').extract_first().strip().replace("\t", "")
        body = html.unescape(re.sub(r"<aside.*</aside>", '', body, flags=re.DOTALL))
        announce = self.make_announce(body, 3)
        if post_id >= self.last_item_id:
            item = VtimesioItem(
                title=node.xpath('title/text()').extract_first().strip(),
                published_at=node.xpath('pubDate/text()').extract_first(),
                img_big=node.xpath('enclosure/@url').extract_first(),
                type='article',
                announce=announce,
                category=node.xpath('category/text()').extract_first().strip(),
                post_id=post_id,
                body=body,
                tickers='',
                provider='VTimes'
            )
            self.parced_items.append(post_id)
            yield item
