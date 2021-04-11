# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from dateutil.parser import parse
from DakotaParser.items import DakotaparserIdeaItem
from DakotaParser.items import DakotaparserItem


class DakotaparserPipeline(DakotaparserItem):
    def process_item(self, item, spider):
        # print(type(item))
        if isinstance(item, DakotaparserItem):
            print(f"{parse(item['published_at'])} :: {item['post_id']} :: {item['visible']} :: {item['type']} :: {item['title']}")
        if isinstance(item, DakotaparserIdeaItem):
            print(f"{parse(item['published_at'])} :: {item['post_id']} :: {item['type']} +{item['target_yield']}% :: {item['title']} :: {item['provider']} ({item['provider_accuracy']}%)")
        return item


