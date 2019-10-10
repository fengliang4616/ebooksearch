# Automatically created by: scrapyd-deploy

from setuptools import setup, find_packages

setup(
    name='ebooksearch',
    version='1.0',
    packages=find_packages(),
    entry_points={'scrapy': ['settings = ebooksearch.settings']}, install_requires=['scrapy', 'w3lib', 'redis',
                                                                                    'django', 'beautifulsoup4',
                                                                                    'elasticsearch', 'requests']
)
