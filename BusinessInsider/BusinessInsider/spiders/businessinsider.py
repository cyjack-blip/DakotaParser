import scrapy
from scrapy import Request
from pydispatch import dispatcher
from scrapy import signals
from BusinessInsider.BusinessInsider.items import BusinessinsiderItem
from scrapy.http import HtmlResponse
from copy import deepcopy
from datetime import datetime
import re
from dateutil.parser import parse
from pymongo import MongoClient


class BusinessinsiderSpider(scrapy.Spider):
    name = 'businessinsider'
    allowed_domains = ['markets.businessinsider.com']
    start_urls = [
        'https://markets.businessinsider.com/index/components/s&p_500',
        'https://markets.businessinsider.com/index/components/dow_jones',
        'https://markets.businessinsider.com/index/components/nasdaq_100',
        'https://markets.businessinsider.com/index/nasdaq_composite'
    ]
    additional_data = {
        'https://markets.businessinsider.com/index/components/s&p_500': {
            'index': 'S&P500'
        },
        'https://markets.businessinsider.com/index/components/dow_jones': {
            'index': 'DOW JONES'
        },
        'https://markets.businessinsider.com/index/components/nasdaq_100': {
            'index': 'NASDAQ 100'
        },
        'https://markets.businessinsider.com/index/nasdaq_composite': {
            'index': 'NASDAQ Composite'
        }
    }
    main_link = 'https://markets.businessinsider.com'

    def __init__(self, **kwargs):
        dispatcher.connect(self.businessinsider_closed, signals.spider_closed)
        # self.last_item_id = kwargs.pop('last_item_id')
        # self.last_item_id = self.get_ednpoint()
        self._client = MongoClient('localhost', 27017)
        self._mongo_base = self._client['parsed']
        self.parced_items = {}
        self.summary_items = []
        self.pages_to_parse = 2
        self.items_to_parse_on_page = 55
        for i in self.additional_data:
            self.parced_items[self.additional_data[i]['index']] = []
        super().__init__(**kwargs)

    def businessinsider_closed(self, spider):
        print(f'spider "{self.name}" report')
        print(self.parced_items)
        print(self.summary_items)

    def is_ticker_in_tinkoff(self, ticker):
        collection = self._mongo_base['stocks']
        result = collection.find_one({'ticker': ticker}, {'stocks_name': 1})
        if result:
            return True
        else:
            return False

    def get_latest_opinion_id(self, ticker):
        if self.is_ticker_in_tinkoff(ticker):
            collection = self._mongo_base['stocks']
        else:
            collection = self._mongo_base['stocksG']
        result = collection.find_one({'ticker': ticker}, {'insider': 1})
        try:
            max_opinion_id = int(result['insider']['max_opinion'])
        except:
            max_opinion_id = 0
        return max_opinion_id

    def format_opinions(self, op, ticker):
        op = op.replace("\n", "").replace("\t", "").replace("\r", "")
        t = re.findall(r">([\w\d\t\s|$/.&',-:;)(]+)<", op, re.DOTALL + re.MULTILINE)
        f = re.findall(r"<a href=\"(.+)\" title.+>", op, re.DOTALL + re.MULTILINE)
        b = t[0].split('/')
        try:
            date = datetime(int('20' + b[2]), int(b[0]), int(b[1]))
        except:
            t.pop(0)
            try:
                date = parse(t[0])
                print(f"today's new opinion on {ticker} at {date}")
            except:
                date = datetime(2000, 1, 1)
                print(f"something wrong at opinion on {ticker}")
        try:
            report_id = re.search(r"\d+$", f[0]).group(0)
        except:
            report_id = 0
        if len(t) == 4:
            t.append(t[3])
            t[3] = 0
        try:
            return report_id, {'date': date, 'analyst': t[1], 'action': t[2], 'target': t[3], 'summary': t[4], 'opinion_link': self.main_link+f[0]}
        except:
            print(f"error parsing opinion {ticker}")

    def format_insiders(self, ins):
        ins = ins.replace("\n", "").replace("\t", "").replace("\r", "")
        t = re.findall(r">([\w\d\t\s|$/.&',-:;)(]+)<", ins, re.DOTALL + re.MULTILINE)
        f = re.findall(r"<a href=\"(.+)\" on.+>", ins, re.DOTALL + re.MULTILINE)
        b = t[1].split('/')
        try:
            date = datetime(int(b[2]), int(b[0]), int(b[1]))
        except:
            print(f'error parcing string {ins}')
            date = datetime(2000, 1, 1)
        if t[2] != 'n/a':
            shares_traded = float(t[2].replace(',', ''))
        else:
            shares_traded = float(0)
        if t[3] != 'n/a':
            shares_held = float(t[3].replace(',', ''))
        else:
            shares_held = float(0)
        if t[4] != 'n/a':
            try:
                price = float(t[4].replace(',', ''))
            except:
                print(f'error parcing string {ins}')
                price = float(0)
        else:
            price = float(0)
        return {
            'name': t[0], 'date': date, 'shares_traded': shares_traded, 'shares_held': shares_held, 'price': price,
            'sell_buy': t[5].strip(), 'option': t[6].strip(), 'insider_link': self.main_link+f[0]
            }

    def parse(self, response: HtmlResponse):
        # pagination for snp500 (with word PAGE:)
        pages = response.xpath("//div[@class='finando_paging']/a/@href").extract()
        if pages:
            l = 0
            for page in pages:
                l += 1
                if l <= self.pages_to_parse:
                    page = response.url+page
                    # print()
                    yield response.follow(
                        page,
                        callback=self.parse_index_pages,
                        cb_kwargs={'url': deepcopy(response.url)}
                    )
        else:
            a = response.url
            # pagination for nasdaq composite (squared numbers)
            pages = response.xpath("//div[@class='graviton']//div[contains(@class, 'paging')]/a/@href").extract()
            # pages = response.xpath("//div[@class='simplebar-content']/div/a/@href").extract()
            # print()
            if pages:
                l = 0
                for page in pages:
                    l += 1
                    if l <= self.pages_to_parse:
                        page = response.url + page
                        yield response.follow(
                            page,
                            callback=self.parse_index_pages,
                            cb_kwargs={'url': deepcopy(response.url)}
                        )
            else:
                # no pagination - parse this page only
                # a = response.url
                # print()
                yield response.follow(
                    response.url,
                    callback=self.parse_index_pages,
                    cb_kwargs={'url': deepcopy(response.url)}
                )

    def parse_index_pages(self, response: HtmlResponse, url):
        links = response.xpath("//table[@class='table table-small']/tr/td/a/@href").extract()
        # links = response.xpath("//div[@class='graviton']/div[@data-simplebar='init']//thead/tr/th[2][contains(text(), 'Previous')]/../../../tbody/tr/td[1]/a/@href").extract()
        l = 0
        if links:
            for link in links:
                l += 1
                if l <= self.items_to_parse_on_page:
                    yield response.follow(
                        link,
                        callback=self.parse_stocks,
                        dont_filter=True,
                        cb_kwargs={'url': url}
                    )
        else:
            nlinks = response.xpath("//div[@class='graviton']/div[@class='grid--vertical-scrolling']//thead/tr/th[2][contains(text(), 'Previous')]/../../../tbody/tr/td[1]/a/@href").extract()
            for link in nlinks:
                l += 1
                if l <= self.items_to_parse_on_page:
                    yield response.follow(
                        link,
                        callback=self.parse_stocks,
                        dont_filter=True,
                        cb_kwargs={'url': url}
                    )

    def parse_stocks(self, response: HtmlResponse, url):
        ticker = response.xpath("//span[@class='price-section__category']/span/text()").extract_first()
        if ticker:
            ticker = ticker.replace(",", "").replace(" ", "")
        else:
            ticker = response.xpath("//h1/span[1]/text()").extract_first()

        mrating = response.xpath("//span[@class='moodys-rating__rating']/text()").extract_first()
        stock = response.xpath("//div[@class='price-section__additionals']/span[3]/text()").extract_first()
        latest_opinion_id = self.get_latest_opinion_id(ticker)
        if mrating:
            mrating = int(mrating)
        indxs = [self.additional_data[url]['index']]

        #  check if already parsed this time
        if ticker not in self.summary_items:
            item = BusinessinsiderItem(
                ticker=ticker,
                indexes=indxs,
                insider_activity='',
                analyst_opinion='',
                moodys_rating=mrating,
                stock=stock,
                max_opinion=''
            )

            opinions = response.xpath("//h2[contains(text(),'Analyst Opinion')]/..//tbody[@class='table__tbody']/tr").extract()
            td_op = {}
            for op in opinions:
                o_id, td = self.format_opinions(op, ticker)
                if int(o_id) > latest_opinion_id:
                    td_op[o_id] = td
            item['analyst_opinion'] = td_op

            insiders = response.xpath("//h2[contains(text(),'Insider Activity')]/..//tbody[@class='table__tbody']/tr").extract()
            td_in = []
            for ins in insiders:
                td_in.append(self.format_insiders(ins))
            item['insider_activity'] = td_in

            t = []
            k = item['analyst_opinion']
            for i in item['analyst_opinion']:
                t.append(k[i]['opinion_link'])
            a = self._handle_opinions(item, t, url)
            if not isinstance(a, BusinessinsiderItem):
                yield a
            else:
                self.summary_items.append(item['ticker'])
                self.parced_items[item['indexes'][0]].append(item['ticker'])
                yield item
        else:
            #  if already parsed this time, return without extra pages
            yield BusinessinsiderItem(
                ticker=ticker,
                indexes=indxs,
                insider_activity=[],
                analyst_opinion=[],
                moodys_rating=mrating,
                stock=stock,
                max_opinion=latest_opinion_id
            )

    def parse_opinion(self, response: HtmlResponse, item, t, opinion_id, url):
        content = response.xpath("//div[@class='content seo-text']/text()").extract_first().replace("\n", "").replace("\t", "").replace("\r", "")
        opinion_time = parse(response.xpath("//div[@class='box']/span/text()").extract_first())
        item['analyst_opinion'][opinion_id]['opinion'] = content
        item['analyst_opinion'][opinion_id]['opinion_time'] = opinion_time
        a = self._handle_opinions(item, t, url)
        if not isinstance(a, BusinessinsiderItem):
            yield a
        else:
            self.summary_items.append(item['ticker'])
            self.parced_items[item['indexes'][0]].append(item['ticker'])
            yield item

    def _handle_opinions(self, item, t, url):
        try:
            a = t.pop()
            url_opinion = a
            opinion_id = re.search(r"\d+$", a).group(0)
            request = Request(url_opinion, callback=self.parse_opinion, dont_filter=True,
                          cb_kwargs={'item': item, 't': t, 'opinion_id': opinion_id, 'url': url})
            return request
        except:
            return item
