# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BusinessinsiderItem(scrapy.Item):
    indexes = scrapy.Field()
    ticker = scrapy.Field()
    insider_activity = scrapy.Field()
    analyst_opinion = scrapy.Field()
    moodys_rating = scrapy.Field()
    stock = scrapy.Field()
    max_opinion = scrapy.Field()


