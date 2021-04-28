from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

import BusinessInsider.BusinessInsider.settings

# from BusinessInsider.BusinessInsider import settings
from BusinessInsider.BusinessInsider.spiders.businessinsider import BusinessinsiderSpider


if __name__ == "__main__":
    crawler_settings = Settings()
    crawler_settings.setmodule(BusinessInsider.BusinessInsider.settings)

    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(BusinessinsiderSpider)

    process.start()
