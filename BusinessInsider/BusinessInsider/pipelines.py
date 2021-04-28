# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
from BusinessInsider.BusinessInsider.items import BusinessinsiderItem


class BusinessinsiderPipeline:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _client = MongoClient('localhost', 27017)
        self._mongo_base = _client['parsed']

    def is_ticker_in_tinkoff(self, ticker):
        collection = self._mongo_base['stocks']
        result = collection.find_one({'ticker': ticker}, {'stocks_name': 1})
        return True if result else False

    def make_new_item(self, r, item, ticker):
        #  merge analyst_opinion
        if item['analyst_opinion']:
            new_item = {**r['analyst_opinion'], **item['analyst_opinion']}
            item['analyst_opinion'] = new_item
        else:
            item['analyst_opinion'] = r['analyst_opinion']
        #  merge insider_activity
        zz = r['insider_activity']
        for i in item['insider_activity']:
            if i not in r['insider_activity']:
                zz.append(i)
        item['insider_activity'] = zz
        #  add index title if it in some index
        if item['indexes'][0] not in r['indexes']:
            # print(f"item[indexes]={item['indexes'][0]}, r[indexes]={r['indexes']}")
            print(f"added {item['indexes'][0]} to {ticker}")
            item['indexes'] = r['indexes'] + item['indexes']
            print(f"now will be {item['indexes']}")
            # print('added')
        else:
            item['indexes'] = r['indexes']
        #  find max analyst_opinion id
        m = []
        for k in item['analyst_opinion'].keys():
            m.append(int(k))
        try:
            item['max_opinion'] = max(m)
        except:
            item['max_opinion'] = 0

        if not item['moodys_rating']:
            item['moodys_rating'] = r['moodys_rating']

        return item

    def process_item(self, item, spider):
        if self.is_ticker_in_tinkoff(item['ticker']):
            print(f"{item['ticker']} is in Tinkoff [{item['indexes'][0]}]")
            self.save_ticker_to_stocks(item, 'stocks', spider)
        else:
            print(f"{item['ticker']} isn\'t in Tinkoff [{item['indexes'][0]}]")
            self.save_ticker_to_stocks(item, 'stocksG', spider)
        return item

    def save_ticker_to_stocks(self, item, collection, spider):
        ticker = item['ticker']
        collection = self._mongo_base[collection]
        result = collection.find_one({'ticker': ticker})
        if not result:
            ddd = {'ticker': ticker, 'insider': item}
            if ddd['insider']['max_opinion'] == '' and not ddd['insider']['analyst_opinion']:
                ddd['insider']['max_opinion'] = 0
            if ddd['insider']['max_opinion'] == '' and ddd['insider']['analyst_opinion']:
                try:
                    m = []
                    for k in item['analyst_opinion'].keys():
                        m.append(int(k))
                    ddd['insider']['max_opinion'] = max(m)
                except:
                    print(f'something wrong with max_opinion of {ticker}')
            collection.insert_one(ddd)
            return
        if 'insider' in result.keys():
            # update
            item.pop('ticker', None)
            r = result['insider']
            dddd = self.make_new_item(r, item, ticker)
            collection.update_one({'ticker': ticker}, {"$unset": {'insider': ''}})
            collection.update_one({'ticker': ticker}, {"$set": {'insider': dddd}})
        else:
            # insert
            m = []
            for k in item['analyst_opinion'].keys():
                m.append(int(k))
            try:
                item['max_opinion'] = max(m)
            except:
                item['max_opinion'] = 0
            item.pop('ticker', None)
            collection.update_one({'ticker': ticker}, {"$set": {'insider': item}})

