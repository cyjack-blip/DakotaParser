from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from DakotaParser import settings
from DakotaParser.spiders.tinkoffru import TinkoffruSpider
from DakotaParser.spiders.ruinvestingcom import RuinvestingcomSpider
from DakotaParser.spiders.vtimesio import VtimesioSpider


if __name__ == "__main__":
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)

    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(RuinvestingcomSpider)
    process.crawl(TinkoffruSpider)
    process.crawl(VtimesioSpider)

    process.start()
