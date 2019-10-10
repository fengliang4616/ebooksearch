# -*- coding: utf-8 -*-
import scrapy
from urllib import parse
import time

from ebooksearch.utils import common
from ebooksearch.items import PipipanItemLoader, PipipanItem


class PipipanSpider(scrapy.Spider):
    name = 'pipipan'
    allowed_domains = ['edu.pipipan.com']
    start_urls = ['http://edu.pipipan.com/']

    def parse(self, response):
        # 解析分类
        all_category_url = response.css(".sub-nav a::attr(href)").extract()
        all_category_url = [parse.urljoin(response.url, url) for url in all_category_url]
        for category_url in all_category_url:
            yield scrapy.Request(url=category_url, callback=self.parse_category_detail)

    def parse_category_detail(self, response):
        # 分类详情
        all_category_detail_url = response.css(".sub-nav a::attr(href)").extract()
        all_category_detail_url = [parse.urljoin(response.url, url) for url in all_category_detail_url]
        for category_detail_url in all_category_detail_url:
            yield scrapy.Request(category_detail_url, callback=self.parse_book_list)

    def parse_book_list(self, response):
        # 分类详情下的书籍列表
        book_list = response.css("#resource-list a::attr(href)").extract()
        book_list = [parse.urljoin(response.url, url) for url in book_list]
        for book_detail_url in book_list:
            yield scrapy.Request(book_detail_url, callback=self.parse_book_detail)

        next_url = response.css(".p_redirect::attr(href)").extract()
        if next_url:
            # 有下一页，继续跟踪
            next_url = parse.urljoin(response.url, next_url)
            yield scrapy.Request(next_url, callback=self.parse_book_list)

    def parse_book_detail(self, response):
        if "未找到" in response.text:
            return

        # 提取书籍信息
        item_loader = PipipanItemLoader(item=PipipanItem(), response=response)

        item_loader.add_value("url_obj_id", common.get_md5(response.url))
        item_loader.add_css("title", ".view_title h3::text")
        item_loader.add_xpath("read_num", '//*[@id="main-container"]/div/div/div/div[2]/div[2]/div/div[6]/span/text()')
        item_loader.add_xpath("upload_time",
                              '//*[@id="main-container"]/div/div/div/div[2]/div[2]/div/div[4]/span/text()')
        item_loader.add_value("crawl_time", round(time.time() * 1000))
        item_loader.add_value("url", response.url)
        item_loader.add_value("source_website", self.allowed_domains)
        item_loader.add_css("type", ".item-red.clearfix .inline a::text")
        item_loader.add_css("size", ".item-red.clearfix .pull-right::text")
        item_loader.add_xpath("tag", '//*[@id="main-container"]/div/div/div/div[2]/div[3]/div/div[2]/span/text()')
        item_loader.add_css("description", "#resource_content")

        pipipan_item = item_loader.load_item()

        yield pipipan_item
