import scrapy
from scrapy.http import HtmlResponse
from DakotaParser.items import DakotaparserItem
from urllib.parse import quote_plus
from pydispatch import dispatcher
from scrapy import signals

from pymongo import MongoClient


class TinkoffruSpider(scrapy.Spider):
    name = 'tinkoffru'
    allowed_domains = ['tinkoff.ru']
    start_urls = ['https://www.tinkoff.ru/invest/feed/']
    base_url = 'https://www.tinkoff.ru'
    feed_url = 'https://www.tinkoff.ru/api/invest/smartfeed-public/v1/feed/api/main'  # url для постраничного парса
    single_news_url = 'https://www.tinkoff.ru/api/invest/smartfeed-public/v1/feed/api/news'  # url для отдельных новостей

    def __init__(self, **kwargs):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        # self.last_item_id = kwargs.pop('last_item_id')
        self.last_item_id = self.get_endpoint()
        self.parced_items = []
        super().__init__(**kwargs)

    def get_endpoint(self):
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

    # сюда попадаем, когда парсер закончил свою работу
    def spider_closed(self, spider):
        print(f'spider "{spider.name}" report')
        if self.parced_items:
            end = max(self.parced_items)+1
            print(f'parsed items: {sorted(self.parced_items)}')
            print(f'lets make next parse from: {end}')
        else:
            end = self.last_item_id
            print('nothing new parsed')
            print(f'lets make next parse from: {end}')
        self.set_endpoint(str(end))

    def parse(self, response: HtmlResponse):
        # get sessionId
        yield response.follow('https://www.tinkoff.ru/api/common/v1/session?origin=web%2Cib5%2Cplatform',
                              callback=self.response_sid)

    def response_sid(self, response: HtmlResponse):
        j_body = response.json()
        if j_body.get('resultCode') == 'OK':
            payload = j_body.get('payload')
            trackingId = j_body.get('trackingId')
            request_url = f'{self.feed_url}?sessionId={payload}'
            yield response.follow(
                request_url,
                callback=self.parce_news,
                cb_kwargs={'payload': payload, 'trackingId': trackingId, 'cursor': ''}
            )

    def parce_news(self, response: HtmlResponse, payload, trackingId, cursor):
        j_body = response.json()
        this_parsed = []
        if j_body.get('status') == 'Ok':
            cursor = j_body.get('payload').get('meta').get('cursor')
            request_url = f'{self.feed_url}?sessionId={payload}&cursor={quote_plus(cursor)}'
            trackingId = j_body.get('trackingId')
            posts = j_body.get('payload').get('items')

            if posts[0]['type'] == 'company_news':
                start_item_id = posts[0]['item']['items'][0]['item']['id']
            else:
                start_item_id = posts[0]['item']['id']

            if start_item_id > self.last_item_id:  # падает если сверху новости компании
                yield response.follow(
                    request_url,
                    callback=self.parce_news,
                    cb_kwargs={'payload': payload, 'trackingId': trackingId, 'cursor': cursor}
                )

            borders = self.get_min_and_max(posts)

            for post in posts:
                if post['type'] == 'company_news':
                    for company_news in post['item']['items']:
                        if company_news['item']['id'] >= self.last_item_id:
                            item = DakotaparserItem(
                                type=company_news['type'],
                                visible=True,
                                published_at=company_news['published_at'],
                                post_id=company_news['item']['id'],
                                title=company_news['item']['title'],
                                body=company_news['item']['body'],
                                img_big=company_news['item']['img_big'],
                                tickers=company_news['item']['tickers'],
                                provider=company_news['item']['provider']['name'],
                                item=company_news
                            )
                            self.parced_items.append(company_news['item']['id'])
                            this_parsed.append(company_news['item']['id'])
                            yield item
                else:
                    if post['item']['id'] >= self.last_item_id:
                        if post['type'] == 'news' or post['type'] == 'review':
                            item = DakotaparserItem(
                                type=post['type'],
                                visible=True,
                                published_at=post['published_at'],
                                post_id=post['item']['id'],
                                title=post['item']['title'],
                                body=post['item']['body'],
                                img_big=post['item']['img_big'],
                                tickers=post['item']['tickers'],
                                provider=post['item']['provider']['name'],
                                item=post
                            )
                            self.parced_items.append(post['item']['id'])
                            this_parsed.append(post['item']['id'])
                            yield item
                        # elif post['type'] == 'idea':
                        #     item = DakotaparserIdeaItem(
                        #         type=post['type'],
                        #         visible=True,
                        #         published_at=post['published_at'],
                        #         post_id=post['item']['id'],
                        #         broker_id=post['item']['broker_id'],
                        #         horizon=post['item']['horizon'],
                        #         date_start=post['item']['date_start'],
                        #         date_end=post['item']['date_end'],
                        #         target_yield=post['item']['target_yield'],
                        #         title=post['item']['title'],
                        #         body=post['item']['description'],
                        #         tickers=post['item']['tickers'],
                        #         provider=post['item']['broker']['name'],
                        #         provider_accuracy=post['item']['broker']['accuracy'],
                        #     )
                            # yield item
                        else:
                            print(f"found new type: {post['type']}")


            # parse hidden news
            try:
                for i in [x for x in range(borders['min'], borders['max']) if x not in this_parsed]:
                    hidden_url = f'{self.single_news_url}/{i}?sessionId={payload}'
                    yield response.follow(
                        hidden_url,
                        callback=self.parse_hidden_news,
                        cb_kwargs={'payload': payload, 'i': i}
                    )
            except Exception as e:
                print(f'{e}')

    def get_min_and_max(self, posts):
        ids = []
        count = dict()
        for post in posts:
            if post['type'] != 'company_news':
                ids.append(post['item']['id'])
        count['min'] = min(ids)
        count['max'] = max(ids)
        if count['min'] < self.last_item_id:
            count['min'] = self.last_item_id
        return count

    def parse_hidden_news(self, response: HtmlResponse, payload, i):
        post = response.json()
        self.parced_items.append(i)
        if post['status'] == 'Ok':
            a = post['payload']['news']['id']
            if post['payload']['news']['id'] >= self.last_item_id:
                item = DakotaparserItem(
                    type='news',
                    visible=False,
                    published_at=post['payload']['news']['date'],
                    post_id=post['payload']['news']['id'],
                    title=post['payload']['news']['title'],
                    body=post['payload']['news']['body'],
                    img_big=post['payload']['news']['img_big'],
                    tickers=post['payload']['news']['tickers'],
                    provider=post['payload']['news']['provider']['name'],
                    item=post['payload']
                )
                yield item
        else:
            item = DakotaparserItem(
                type='news',
                visible=False,
                published_at='2020-01-01T00:00:01+03:00',
                post_id=i,
                title='empty',
                body='',
                img_big='',
                tickers='',
                provider='',
                item=''
            )
            yield item

