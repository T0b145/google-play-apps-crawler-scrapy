# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pymongo
import dataset

class JsonPipeline(object):
    def open_spider(self, spider):
        self.file = open('items.jl', 'w')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item


class MongoPipeline(object):
    collection_name = 'scrapy_items'
    def __init__(self, uri, db):
        self.uri = uri
        self.db = db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            uri=crawler.settings.get('URI'),
            db=crawler.settings.get('DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.uri)
        self.db = self.client[self.db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item


class MySQLPipeline(object):
    def __init__(self, uri, db):
        self.uri = uri
        self.db = db
        self.user = "scrapy"
        self.password = "scrapy"
        self.table_name = 'scrapy_items'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            uri=crawler.settings.get('URI'),
            db=crawler.settings.get('DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.db = dataset.connect('postgresql://{}:{}@{}:5432/{}'.format(self.user,self.password,self.uri,self.db))
        self.table = self.db[self.table_name]

    def close_spider(self, spider):
        print ("Spider Closed")

    def process_item(self, item, spider):
        #self.table.insert({"test":"test"})
        self.table.insert(dict(item))
        return item
