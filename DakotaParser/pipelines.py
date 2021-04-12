# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from dateutil.parser import parse
from DakotaParser.items import DakotaparserIdeaItem
from DakotaParser.items import DakotaparserItem

from pymongo import MongoClient

class DakotaparserPipeline(DakotaparserItem):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _client = MongoClient('localhost', 27017)
        self._mongo_base = _client['parsed']

    def process_item(self, item, spider):
        static_brands_trading = 'http://static.tinkoff.ru/brands/traiding/'
        # print(type(item))
        if isinstance(item, DakotaparserItem):
            collection = self._mongo_base[spider.name]
            print(f"{parse(item['published_at'])} :: {item['post_id']} :: {item['visible']} :: {item['type']} :: {item['title']}")
            item['_spider'] = spider.name
            if item['title'] != 'empty':
                for n, i in enumerate(item['tickers']):
                    a = f"{static_brands_trading}{i['logo_name'].split('.')[0]}x160.{i['logo_name'].split('.')[1]}"
                    # print(n, ' ', i['logo_name'], ' ', a)
                    item['tickers'][n]['logo_url'] = a
                collection.insert_one(item)

        if isinstance(item, DakotaparserIdeaItem):
            print(f"{parse(item['published_at'])} :: {item['post_id']} :: {item['type']} +{item['target_yield']}% :: {item['title']} :: {item['provider']} ({item['provider_accuracy']}%)")
        return item


