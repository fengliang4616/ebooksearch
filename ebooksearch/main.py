__author__ = 'FengLiang'

from scrapy.cmdline import execute
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# execute(["scrapy", "crawl", "ishare"])
# execute(["scrapy", "crawl", "pipipan"])
execute(["scarpy", "crawl", "mebook"])