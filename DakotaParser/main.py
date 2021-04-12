from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from DakotaParser import settings
from DakotaParser.spiders.tinkoffru import TinkoffruSpider

if __name__ == "__main__":
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)

    process = CrawlerProcess(settings=crawler_settings)
    with open('next_end_point.txt', 'r') as f:
        end_point = int(f.read())
    process.crawl(TinkoffruSpider, last_item_id=end_point)

    process.start()
