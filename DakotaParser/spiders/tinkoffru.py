import scrapy
from scrapy.http import HtmlResponse
from DakotaParser.items import DakotaparserItem
from DakotaParser.items import DakotaparserIdeaItem
import json
from urllib.parse import quote_plus
from dateutil.parser import parse
from copy import deepcopy


class TinkoffruSpider(scrapy.Spider):
    name = 'tinkoffru'
    allowed_domains = ['tinkoff.ru']
    start_urls = ['https://www.tinkoff.ru/invest/feed/']
    base_url = 'https://www.tinkoff.ru'
    feed_url = 'https://www.tinkoff.ru/api/invest/smartfeed-public/v1/feed/api/main'

    def __init__(self, **kwargs):
        self.last_item_id = kwargs.pop('last_item_id')
        self.parced_items = []
        super().__init__(**kwargs)

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
        if j_body.get('status') == 'Ok':
            cursor = j_body.get('payload').get('meta').get('cursor')
            request_url = f'{self.feed_url}?sessionId={payload}&cursor={quote_plus(cursor)}'
            trackingId = j_body.get('trackingId')
            posts = j_body.get('payload').get('items')
            if posts[0]['item']['id'] > self.last_item_id:
                yield response.follow(
                    request_url,
                    callback=self.parce_news,
                    cb_kwargs={'payload': payload, 'trackingId': trackingId, 'cursor': cursor}
                )

            for post in posts:
                if post['type'] == 'news' or post['type'] == 'review':
                    if post['item']['id'] > self.last_item_id:
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
                        yield item
                elif post['type'] == 'company_news':
                    for company_news in post['item']['items']:
                        if company_news['item']['id'] > self.last_item_id:
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
                            yield item

                elif post['type'] == 'idea':
                    item = DakotaparserIdeaItem(
                        type=post['type'],
                        visible=True,
                        published_at=post['published_at'],
                        post_id=post['item']['id'],
                        broker_id=post['item']['broker_id'],
                        horizon=post['item']['horizon'],
                        date_start=post['item']['date_start'],
                        date_end=post['item']['date_end'],
                        target_yield=post['item']['target_yield'],
                        title=post['item']['title'],
                        body=post['item']['description'],
                        tickers=post['item']['tickers'],
                        provider=post['item']['broker']['name'],
                        provider_accuracy=post['item']['broker']['accuracy'],
                    )
                        # yield item
                else:
                    print(f"found new type: {post['type']}")

    def parse_hidden_news(self):
        pass
