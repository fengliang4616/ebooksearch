# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import datetime
import scrapy
import re
import time
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader
from w3lib.html import remove_tags

from ebooksearch.models.es_types import IshareType, PipipaneType, MebookType, BookType

from elasticsearch_dsl.connections import connections

es = connections.create_connection(BookType._doc_type.using)  # 连接es的一种用法


class EbooksearchItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


def gen_suggests(index, info_tuple):
    """
    根据字符串生成搜索建议数组
    :rtype: object
    :param index:
    :param info_tuple:
    :return: suggests
    """
    used_word = set()
    suggests = []

    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串
            words = es.indices.analyze(
                index=index,
                analyzer="ik_smart",
                params={"filter": ["lowercase"]},
                body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = anylyzed_words - used_word
        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})

    return suggests


class IshareItemLoader(ItemLoader):
    # 自定义新浪爱问分享的ItemLoader
    default_output_processor = TakeFirst()


# 爱问分享资料
class IshareItem(scrapy.Item):
    # 自定义一个item给pipelines
    url_obj_id = scrapy.Field()
    title = scrapy.Field()
    upload_people = scrapy.Field()
    score = scrapy.Field()
    load_num = scrapy.Field()
    upload_time = scrapy.Field()
    crawl_time = scrapy.Field()
    url = scrapy.Field()
    source_website = scrapy.Field()
    type = scrapy.Field()
    size = scrapy.Field()
    comment_num = scrapy.Field()
    read_num = scrapy.Field()
    collect_num = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            insert into `t_ishare` (url_obj_id, title, upload_people, score, load_num, read_num, comment_num, collect_num, upload_time, crawl_time, url, source_website, type) 
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE title=VALUES(title), load_num=VALUES(load_num),
              score=VALUES(score),read_num=VALUES(read_num),comment_num=VALUES(comment_num),collect_num=VALUES(collect_num), crawl_time=VALUES(crawl_time),
              type=VALUES(type)
        """

        score = 0.0
        load_num = int(self["load_num"])
        comment_num = int(self["comment_num"])
        read_num = int(self["read_num"])
        collect_num = int(self["collect_num"])
        type = self["type"].split(".")[1]
        type_match = re.match(".+\.(.+)", self["type"])
        if type_match:
            type = type_match.group(1)
        else:
            type = "None"
        upload_time_str = self["upload_time"]
        upload_time = time.strptime(upload_time_str, "%Y-%m-%d")
        upload_time_int = round(time.mktime(upload_time) * 1000)

        params = (self["url_obj_id"], self["title"], self["upload_people"], score,
                  load_num, read_num, comment_num, collect_num,
                  upload_time_int, self["crawl_time"], self["url"], self["source_website"],
                  type)

        return insert_sql, params

    def save_to_es(self):
        ishare = IshareType()
        ishare.meta.id = self["url_obj_id"]
        ishare.title = self["title"]
        ishare.url = self["url"]
        ishare.load_num = self["load_num"]
        ishare.read_num = self["read_num"]
        ishare.type = self["type"]
        ishare.source_website = self["source_website"]

        crawl_date = time.strftime("%Y-%m-%d", time.localtime(self["crawl_time"] / 1000))
        # upload_date = time.strftime("%Y-%m-%d", time.localtime(self["upload_time"] / 1000))

        # upload_time = time.strptime(self["upload_time"], "%Y-%m-%d")

        ishare.crawl_time = crawl_date
        ishare.upload_time = self["upload_time"]
        ishare.suggest = gen_suggests(IshareType._doc_type.index, ((ishare.title, 10), (ishare.type, 7)))

        ishare.save()
        return


# 城通网盘
class PipipanItemLoader(ItemLoader):
    # 自定义城通网盘的ItemLoader
    default_output_processor = TakeFirst()


def format_upload_time(value):
    # 处理上传时间
    match_obj1 = re.match(r'(\d+)小时.*', value)
    match_obj2 = re.match(r'(^昨天((\d+):(\d+)))', value)
    match_obj3 = re.match(r'(^前天((\d+):(\d+)))', value)
    match_obj4 = re.match(r'(\d+)天前.*', value)
    match_obj5 = re.match(r'\d+-\d+-\d+', value)

    if match_obj1:
        now_timestamp = round(time.time())
        upload_time = now_timestamp - match_obj1.group(1) * 3600000
        return upload_time
    elif match_obj2:
        hour = int(match_obj2.group(3))
        minute = int(match_obj2.group(4))
        today = datetime.date.today()
        # 0点时间戮
        today_timestamp = int(time.mktime(today.timetuple()))
        yestoday_timestamp = today_timestamp - 3600000 * 24
        upload_time = yestoday_timestamp + hour * 3600000 + minute * 60000
        return upload_time
    elif match_obj3:
        hour = int(match_obj2.group(3))
        minute = int(match_obj2.group(4))
        today = datetime.date.today()
        # 0点时间戮
        today_timestamp = int(time.mktime(today.timetuple()))
        before_yestoday_timestamp = today_timestamp - 3600000 * 24 * 2
        upload_time = before_yestoday_timestamp + hour * 3600000 + minute * 60000
        return upload_time
    elif match_obj4:
        now_timestamp = round(time.time())
        upload_time = now_timestamp - match_obj4.group(1) * 3600000 * 24
        return upload_time
    elif match_obj5:
        upload_time = time.strptime(value, "%Y-%m-%d")
        return round(time.mktime(upload_time) * 1000)
    else:
        return round(time.time() * 1000)


def get_size(value):
    size = value.replace("\r", "").replace("\n", "").replace("\t", "")
    return size


def get_type(value):
    match_obj = re.match(".*\.(.*)", value)
    if match_obj:
        type = match_obj.group(1)
    else:
        type = "unknown"
    return type


# 城通网盘
class PipipanItem(scrapy.Item):
    url_obj_id = scrapy.Field()
    title = scrapy.Field()
    read_num = scrapy.Field()
    upload_time = scrapy.Field(
        input_processor=MapCompose(format_upload_time)
    )
    crawl_time = scrapy.Field()
    url = scrapy.Field()
    source_website = scrapy.Field()
    type = scrapy.Field(
        input_processor=MapCompose(get_type)
    )
    size = scrapy.Field(
        input_processor=MapCompose(get_size)
    )
    tag = scrapy.Field(
        input_processor=Join(",")
    )
    description = scrapy.Field(
        input_processor=MapCompose(remove_tags)
    )

    def get_insert_sql(self):

        description = self["description"]
        if description:
            print("description: " + description)
            insert_sql = """
                insert into `t_pipipan` (url_obj_id, title, read_num, upload_time, crawl_time, 
                url, source_website, type, size, tag, description) 
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                  ON DUPLICATE KEY UPDATE title=VALUES(title),read_num=VALUES(read_num),
                  crawl_time=VALUES(crawl_time), tag=values(tag)
            """

            params = (self["url_obj_id"], self["title"], self["read_num"], self["upload_time"], self["crawl_time"],
                      self["url"], self["source_website"], self["type"], self["size"], self["tag"], self["description"])
        else:
            insert_sql = """
                insert into `t_pipipan` (url_obj_id, title, read_num, upload_time, crawl_time, 
                url, source_website, type, size, tag) 
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                  ON DUPLICATE KEY UPDATE title=VALUES(title),read_num=VALUES(read_num),
                  crawl_time=VALUES(crawl_time), tag=values(tag)
            """

            params = (self["url_obj_id"], self["title"], self["read_num"], self["upload_time"], self["crawl_time"],
                      self["url"], self["source_website"], self["type"], self["size"], self["tag"])

        return insert_sql, params

    def save_to_es(self):
        pipipan = PipipaneType()
        pipipan.meta.id = self["url_obj_id"]
        pipipan.title = self["title"]
        pipipan.url = self["url"]
        pipipan.read_num = self["read_num"]
        pipipan.type = self["type"]
        pipipan.source_website = self["source_website"]

        pipipan.tag = self["tag"]
        pipipan.description = self["description"]

        crawl_date = time.strftime("%Y-%m-%d", time.localtime(self["crawl_time"] / 1000))
        pipipan.crawl_time = crawl_date
        pipipan.upload_time = time.strftime("%Y-%m-%d", time.localtime(self["upload_time"] / 1000))
        pipipan.suggest = gen_suggests(PipipaneType._doc_type.index, ((pipipan.title, 10), (pipipan.tag, 7)))

        pipipan.save()
        return


# 我的小书屋
class MebookItemLoader(ItemLoader):
    # 自定义ItemLoader
    default_output_processor = TakeFirst()


class MebookItem(scrapy.Item):
    url_obj_id = scrapy.Field()
    title = scrapy.Field()
    upload_time = scrapy.Field(
        input_processor=Join(","),
        # output_processor = MapCompose(get_upload_time)
    )
    crawl_time = scrapy.Field()
    url = scrapy.Field()
    source_website = scrapy.Field()
    type = scrapy.Field()
    description = scrapy.Field()
    tag = scrapy.Field()

    def get_insert_sql(self):

        insert_sql = """
            insert into `t_mebook` (url_obj_id, title, upload_time, crawl_time, url, 
            source_website, type, description, tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
              ON DUPLICATE KEY UPDATE title=VALUES(title),crawl_time=VALUES(crawl_time), 
              tag=VALUES(tag), description=VALUES(description)
        """

        match_obj = re.match(".*?((\d+)年(\d+)月(\d+)日).*", self["upload_time"])
        if match_obj:
            date = match_obj.group(1).replace("年", "-").replace("月", "-").replace("日", "")
            upload_time = time.strptime(date, "%Y-%m-%d")
            upload_time = round(time.mktime(upload_time) * 1000)
        else:
            upload_time = round(time.time() * 1000)

        match_type = re.match(r'.*(》|）|\))([a-zA-Z].*)', self["type"])
        if match_type:
            type = match_type.group(2)
        else:
            type = "unknown"

        params = (self["url_obj_id"], self["title"], upload_time, self["crawl_time"],
                  self["url"], self["source_website"], type, self["description"], self["tag"])

        return insert_sql, params

    def save_to_es(self):

        match_obj = re.match(".*?((\d+)年(\d+)月(\d+)日).*", self["upload_time"])
        if match_obj:
            date = match_obj.group(1).replace("年", "-").replace("月", "-").replace("日", "")
            upload_time = time.strptime(date, "%Y-%m-%d")
            upload_time = round(time.mktime(upload_time) * 1000)
        else:
            upload_time = round(time.time() * 1000)

        match_type = re.match(r'.*(》|）|\))([a-zA-Z].*)', self["type"])
        if match_type:
            type = match_type.group(2)
        else:
            type = "unknown"

        mebook = MebookType()
        mebook.meta.id = self["url_obj_id"]
        mebook.title = self["title"]
        mebook.url = self["url"]
        mebook.type = type
        mebook.source_website = self["source_website"]

        mebook.tag = self["tag"]
        mebook.description = self["description"]

        crawl_date = time.strftime("%Y-%m-%d", time.localtime(self["crawl_time"] / 1000))
        mebook.crawl_time = crawl_date
        mebook.upload_time = upload_time

        mebook.save()

        mebook.suggest = gen_suggests(PipipaneType._doc_type.index, ((mebook.title, 10), (mebook.tag, 7)))

        mebook.save()
        return


class IreadweekItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class IreadweekItem(scrapy.Item):
    name = scrapy.Field()
    author = scrapy.Field()
    category = scrapy.Field()
    score = scrapy.Field()
    download_url = scrapy.Field()
    introduction = scrapy.Field()
    create_edit = scrapy.Field()

    def save_to_es(self):
        ireadweek = IreadweekItem()
        ireadweek.meta.id = self['url_obj_id']
        ireadweek.title = self['title']
        ireadweek.url = self['url']
        ireadweek.source_website = self['source_website']
        ireadweek.description = self['description']

