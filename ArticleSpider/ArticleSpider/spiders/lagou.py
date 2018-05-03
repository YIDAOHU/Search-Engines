# -*- coding: utf-8 -*-

from datetime import datetime
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import scrapy
from ArticleSpider.items import LagouJobItemLoader, LagouJobItem
from selenium import webdriver
import time
from ArticleSpider.utils.common import get_md5


class LagouSpider(CrawlSpider):
    name = 'lagou'
    allowed_domains = ['www.lagou.com']
    start_urls = ['https://www.lagou.com']

    rules = (
        Rule(LinkExtractor(allow=("zhaopin/.*",)), follow=True),
        Rule(LinkExtractor(allow=("gongsi/j\d+.html",)), follow=True),
        Rule(LinkExtractor(allow=r'jobs/\d+.html'), callback='parse_job', follow=True),
    )

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], cookies=self.get_cookies(), callback=self.after_login)]

    def get_cookies(self):
        # 使用selenium模拟登陆并获取cookies
        browser = webdriver.Firefox(executable_path='D:/webdriver/geckodriver.exe')
        browser.get("https://passport.lagou.com/login/login.html")
        time.sleep(10)
        browser.find_element_by_xpath("//input[@type='text']").clear()
        browser.find_element_by_xpath("//input[@type='text']").send_keys("账户")
        browser.find_element_by_xpath("//input[@type='password']").clear()
        browser.find_element_by_xpath("//input[@type='password']").send_keys("密码")
        browser.find_element_by_xpath("//input[@type='submit']").click()
        time.sleep(10)
        cookie = browser.get_cookies()
        print(cookie)
        browser.close()
        if cookie:
            return cookie
        else:
            return self.get_cookies()

    def after_login(self, response):
        for url in self.start_urls:
            yield self.make_requests_from_url(url)

    def parse_job(self, response):
        # 解析拉勾网的职位
        item_loader = LagouJobItemLoader(item=LagouJobItem(), response=response)
        item_loader.add_css("title", ".job-name::attr(title)")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("salary", ".job_request .salary::text")
        item_loader.add_xpath("job_city", "//*[@class='job_request']/p/span[2]/text()")
        item_loader.add_xpath("work_years", "//*[@class='job_request']/p/span[3]/text()")
        item_loader.add_xpath("degree_need", "//*[@class='job_request']/p/span[4]/text()")
        item_loader.add_xpath("job_type", "//*[@class='job_request']/p/span[5]/text()")

        item_loader.add_xpath("tags", '//li[@class="labels"]/text()')
        item_loader.add_css("publish_time", ".publish_time::text")
        item_loader.add_css("job_advantage", ".job-advantage p::text")
        item_loader.add_css("job_desc", ".job_bt div")
        item_loader.add_css("job_addr", ".work_addr")
        item_loader.add_css("company_name", "#job_company dt a img::attr(alt)")
        item_loader.add_css("company_url", "#job_company dt a::attr(href)")
        item_loader.add_value("crawl_time", datetime.now())

        job_item = item_loader.load_item()

        return job_item
