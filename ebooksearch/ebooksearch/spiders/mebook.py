# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import time
from w3lib.html import remove_tags

from ebooksearch.utils.common import get_md5
from ebooksearch.items import MebookItemLoader, MebookItem


class MebookSpider(CrawlSpider):
    name = 'mebook'
    allowed_domains = ['mebook.cc']
    start_urls = ['http://mebook.cc/']

    rules = (
        Rule(LinkExtractor(allow=r'category/[a-zA-Z]{1,}$'), follow=True),
        Rule(LinkExtractor(allow=r'category/.+/page/\d+'), follow=True),
        Rule(LinkExtractor(allow=r'\d+.html$'), callback='parse_item', follow=True),
    )

    def parse_item(self, response):

        description = response.css("#content").extract_first("")
        description = remove_tags(description)
        if description:
            if "内容简介" and "下载地址" not in description:
                return
        else:
            return

        item_loader = MebookItemLoader(item=MebookItem(), response=response)
        item_loader.add_value("url_obj_id", get_md5(response.url))
        item_loader.add_css("title", "#primary h1::text")
        item_loader.add_css("upload_time", ".postinfo .left::text")
        item_loader.add_value("crawl_time", round(time.time() * 1000))
        item_loader.add_value("url", response.url)
        item_loader.add_value("source_website", self.allowed_domains)
        item_loader.add_css("type", "#primary h1::text")
        item_loader.add_value("description", description)
        item_loader.add_css("tag", ".postinfo .left a::text")

        mebook_item = item_loader.load_item()

        yield mebook_item
