# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PublicationsItem(scrapy.Item):
    publication_id = scrapy.Field()
    details = scrapy.Field()
    detail_path = scrapy.Field()
