# -*- coding: utf-8 -*-

import scrapy
from scrapy.http import Request
import urlparse

from ArticleSpider.items import JobBoleArticleItem, ArticleItemLoader
from ArticleSpider.utils.common import get_md5


class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']

    def parse(self, response):
        """
        1.获取文章列表页中文章的url,交给scrapy下载并解析
        2.获取下一页并交给scrapy进行下载,下载完成后交给parse
        """
        # 获取列表页文章链接和封面图链接
        post_nodes = response.xpath('//div[@class="post floated-thumb"]/div[@class="post-thumb"]/a')
        for post_node in post_nodes:
            image_url = post_node.xpath('./img/@src').extract_first('')
            post_url = post_node.xpath('./@href').extract_first('')
            yield Request(url=urlparse.urljoin(response.url, post_url),  meta={"front_image_url": image_url}, callback=self.parse_detail)
        # 获取下一页链接
        next_url = response.xpath('//a[@class="next page-numbers"]/@href').extract_first('')
        if next_url:
            yield Request(url=urlparse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        """
        提取文章信息
        """
        # 通过自定义的item_loader加载item
        item_loader = ArticleItemLoader(item=JobBoleArticleItem(), response=response)
        item_loader.add_xpath("title", '//div[@class="entry-header"]/h1/text()')
        front_image_url = response.meta.get("front_image_url", "")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_xpath("create_date", "//p[@class='entry-meta-hide-on-mobile']/text()")
        item_loader.add_value("front_image_url", [front_image_url])
        item_loader.add_xpath("praise_nums", "//span[contains(@class, 'vote-post-up')]/h10/text()")
        item_loader.add_xpath("comment_nums", "//a[@href='#article-comment']/span/text()")
        item_loader.add_xpath("fav_nums", "//span[contains(@class, 'bookmark-btn')]/text()")
        item_loader.add_xpath("tags", "//p[@class='entry-meta-hide-on-mobile']/a/text()")
        item_loader.add_xpath("content", "//div[@class='entry']")

        article_item = item_loader.load_item()

        yield article_item

