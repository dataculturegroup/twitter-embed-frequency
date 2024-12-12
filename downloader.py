import scrapy
from scrapy.crawler import CrawlerProcess
import hashlib
import os

MEDIA_CLOUD_USER_AGENT = "Mozilla/5.0 (compatible; mediacloud academic archive; mediacloud.org)"


def url_hash(url:str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def download_to_dir(urls, dest_dir):
    # Create downloads directory if it doesn't exist
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    class UrlSpider(scrapy.Spider):
        name = 'url_spider'

        def _dest_file(self, url):
            hash = url_hash(url)
            return f'{dest_dir}/{hash}.html'

        def start_requests(self):
            for url in urls:
                file_path = self._dest_file(url)
                if not os.path.exists(file_path):
                    yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            file_path = self._dest_file(response.url)
            with open(file_path, 'wb') as f:
                f.write(response.body)

    # Run the spider
    process = CrawlerProcess(settings={
        'DOWNLOAD_TIMEOUT': 30,
        'USER_AGENT': MEDIA_CLOUD_USER_AGENT
    })
    process.crawl(UrlSpider)
    process.start()
