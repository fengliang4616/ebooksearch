# -*- coding: utf-8 -*-
from urllib.parse import urljoin

import scrapy
from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider
import time
from w3lib.html import remove_tags

from ebooksearch.utils.common import get_md5
from ebooksearch.items import IreadweekItemLoader, IreadweekItem


class IreadweekSpider(CrawlSpider):
    name = 'ireadweek'
    allowed_domains = ['www.ireadweek.com']
    start_urls = ['http://www.ireadweek.com/']

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/63.0.3239.84 Safari/537.36'
    }

    def parse(self, response):
        domain = ['http://www.ireadweek.com/']
        html_doc = response.body
        soup = BeautifulSoup(html_doc, 'html.parser')

        for i in soup.find_all('div', class_="hanghang-list-name"):
            if i.parent.parent.attrs.get('href'):
                url = i.parent.parent.attrs.get('href')
                full_url = urljoin(domain, url)
                yield scrapy.Request(full_url, callback=self.parse_news)

        next_url = urljoin(domain, soup.find_all('a')[-2].attrs.get('href'))
        yield scrapy.Request(next_url, callback=self.parse)
        print(domain, next_url)

    @staticmethod
    def parse_news(response):
        item = IreadweekItem()
        html_doc = response.body
        soup = BeautifulSoup(html_doc, 'html.parser')

        download_url = soup.find('a', class_='downloads').attrs.get('href')
        title = soup.find_all('div', class_='hanghang-za-title')
        name = title[0].text

        info = soup.find('div', class_='hanghang-shu-content-font').find_all('p')

        author = info[0].text.split('作者：')[1]
        category = info[1].text.split('分类：')[1]
        introduction = info[4].text

        item['name'] = name
        item['download_url'] = download_url
        item['author'] = author
        item['category'] = category
        item['introduction'] = introduction

        print(name, download_url, author, introduction)

        yield item
