# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DakotaparserItem(scrapy.Item):
    _id = scrapy.Field()
    visible = scrapy.Field()
    type = scrapy.Field()
    published_at = scrapy.Field()
    post_id = scrapy.Field()
    title = scrapy.Field()
    body = scrapy.Field()
    img_big = scrapy.Field()
    tickers = scrapy.Field()
    provider = scrapy.Field()
    item = scrapy.Field()


class DakotaparserIdeaItem(scrapy.Item):
    _id = scrapy.Field()
    visible = scrapy.Field()
    type = scrapy.Field()
    published_at = scrapy.Field()
    post_id = scrapy.Field()
    broker_id = scrapy.Field()
    horizon = scrapy.Field()
    date_start = scrapy.Field()
    date_end = scrapy.Field()
    target_yield = scrapy.Field()
    title = scrapy.Field()
    body = scrapy.Field()
    tickers = scrapy.Field()
    provider = scrapy.Field()
    provider_accuracy = scrapy.Field()
