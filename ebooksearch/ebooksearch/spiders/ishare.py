# -*- coding: utf-8 -*-
import scrapy
from urllib import parse
import re
import time

from ebooksearch.utils import common
from ebooksearch.items import IshareItem, IshareItemLoader


class IshareSpider(scrapy.Spider):
    name = 'ishare'
    allowed_domains = ['ishare.iask.sina.com.cn']
    start_urls = ['http://ishare.iask.sina.com.cn']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/63.0.3239.84 Safari/537.36'
    }

    def parse(self, response):
        # 拿到所有的url
        all_urls = response.css("a::attr(href)").extract()
        all_urls = [parse.urljoin(response.url, url) for url in all_urls]
        # 过滤不是http开头的url
        all_urls = filter(lambda x: True if x.startswith("http") else False, all_urls)
        for url in all_urls:
            # 匹配资料url
            match_obj = re.match(r'(.*iask.sina.com.cn/c/(\d+).html$)', url)
            if match_obj:
                # 如果匹配到url，进行详情页提取
                yield scrapy.Request(url=url, callback=self.category_parse)
            else:
                pass
                # 匹配不到，继续跟踪
                yield scrapy.Request(url=url, callback=self.parse)

    def category_parse(self, response):
        # 没有这个轮播图的才是真正要爬取的分类页面
        if "education-banner" not in response.text:
            # 拿到类别页中所有的url
            all_urls = response.css("::attr(href)").extract()
            # 为url添加域名
            all_urls = [parse.urljoin(response.url, url) for url in all_urls]
            for url in all_urls:
                # 详情页url
                match_obj = re.match(r'(.*cn/f/[a-zA-Z0-9]*.html$)', url)
                if match_obj:
                    # 如果匹配到的url不为空，提取详情
                    yield scrapy.Request(url=url, callback=self.detail_parse)

            # 下一页的url
            next_url = response.css(".btn-page::attr(href)").extract()
            if next_url:
                next_url = parse.urljoin(response.url, next_url)
                yield scrapy.Request(url=next_url, callback=self.category_parse)

    def detail_parse(self, response):
        # 资料详情提取
        item_loader = IshareItemLoader(item=IshareItem(), response=response)

        item_loader.add_css("title", ".detail-box h1::text")
        item_loader.add_css("upload_people", ".detail-user-bar a::text")
        item_loader.add_css("score", "#starScoreId::text")
        item_loader.add_css("load_num", "#downNumId::text")
        item_loader.add_css("comment_num", "#commentNumId::text")
        item_loader.add_css("collect_num", "#collectNumId::text")
        item_loader.add_css("read_num", "#readNumId::text")

        # item_loader.add_xpath("upload_time", "//*[@id='swfPreview']/div/div[1]/div[1]/span[3]/text()")

        item_loader.add_value("crawl_time", round(time.time() * 1000))
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_obj_id", common.get_md5(response.url))
        item_loader.add_value("source_website", self.allowed_domains)
        item_loader.add_css("type", ".detail-box h1::text")

        ishare_item = item_loader.load_item()

        yield ishare_item
