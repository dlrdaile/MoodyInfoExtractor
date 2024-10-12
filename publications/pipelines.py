# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json

class PublicationsPipeline:
    def process_item(self, item, spider):
        if spider.name == "publications":
            with open(f"{item['publication_id']}.json", "a") as f:
                json.dump(item['details'], f)
        return item
