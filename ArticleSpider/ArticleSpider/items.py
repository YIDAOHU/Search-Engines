# -*- coding: utf-8 -*-

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from ArticleSpider.utils.common import extract_num
from settings import SQL_DATETIME_FORMAT,SQL_DATE_FORMAT
import redis

import datetime
import re

from models.es_jobbole import ArticleType
from models.es_zhihu import ZhiHuAnswerType, ZhiHuQuestionType
from w3lib.html import remove_tags
from elasticsearch_dsl.connections import connections
from models.es_lagou import LagouType

redis_cli = redis.StrictRedis()

# 与es进行连接生成搜索建议
es_article = connections.create_connection(ArticleType._doc_type.using)
es_question = connections.create_connection(ZhiHuQuestionType._doc_type.using)
es_answer = connections.create_connection(ZhiHuAnswerType._doc_type.using)
es_lagou = connections.create_connection(LagouType._doc_type.using)


def gen_suggests(es_con,index, info_tuple):
    es = es_con
    # 根据字符串生成搜索建议数组
    used_words = set()
    # 去重以先来的为主
    suggests = []
    for text, weight in info_tuple:
        if text:
            # 调用es的analyze接口分析字符串：分词并做大小写的转换  
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter':["lowercase"]}, body=text)
            anylyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"])>1])
            new_words = anylyzed_words - used_words
        else:
            new_words = set()

        if new_words:
            suggests.append({"input":list(new_words), "weight":weight})

    return suggests


def date_convert(value):
    """
    日期转换
    """
    try:
        create_date = datetime.datetime.strptime(value, "%Y/%m/%d").date()
    except Exception as e:
        create_date = datetime.datetime.now().date()

    return create_date


def get_nums(value):
    """
    提取数字
    """
    match_re = re.match(".*?(\d+).*", value)
    if match_re:
        nums = int(match_re.group(1))
    else:
        nums = 0

    return nums


def return_value(value):
    return value


def remove_comment_tags(value):
    if u"评论" in value:
        return ""
    else:
        return value


class ArticleItemLoader(ItemLoader):
    """
        自定义ItemLoader
    """
    default_output_processor = TakeFirst()


class JobBoleArticleItem(scrapy.Item):
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_convert),
    )
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()
    praise_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comment_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_nums = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    content = scrapy.Field()

    def get_insert_sql(self):
        """
        存储到MySQL
        """
        insert_sql = """insert into jobbole_article(title, create_date, url, url_object_id, front_image_url, 
                        front_image_path,comment_nums, fav_nums, praise_nums, tags, content)
                        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        params = (self["title"], self["create_date"], self["url"], self["url_object_id"], self["front_image_url"],
                  self["front_image_path"], self["comment_nums"], self["fav_nums"], self['praise_nums'], self['tags'],self['content'])

        return insert_sql, params

    def save_to_es(self):
        """
        存储到elasticsearch
        """
        article = ArticleType()
        article.title = self['title']
        article.create_date = self["create_date"]
        article.content = remove_tags(self["content"])
        article.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            article.front_image_path = self["front_image_path"]
        article.praise_nums = self["praise_nums"]
        article.fav_nums = self["fav_nums"]
        article.comment_nums = self["comment_nums"]
        article.url = self["url"]
        article.tags = self["tags"]
        article.meta.id = self["url_object_id"]

        article.suggest = gen_suggests(es_article, ArticleType._doc_type.index,((article.title, 10), (article.tags, 7), (article.content, 3)))

        article.save()
        redis_cli.incr("jobbole_count")
        return


class ZhihuQuestionItem(scrapy.Item):
    # 知乎的问题 item
    zhihu_id = scrapy.Field()
    topics = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    answer_num = scrapy.Field()
    comment_num = scrapy.Field()
    watch_user_num = scrapy.Field()
    click_num = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎question表的sql语句
        insert_sql = """
            insert into zhihu_question(zhihu_id, topics, url, title, content, answer_num, comment_num,
              watch_user_num, click_num, crawl_time)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE content=VALUES(content), answer_num=VALUES(answer_num), comment_num=VALUES(comment_num),
              watch_user_num=VALUES(watch_user_num), click_num=VALUES(click_num)
        """
        zhihu_id = self["zhihu_id"][0]
        topics = ",".join(self["topics"])
        url = self["url"][0]
        title = "".join(self["title"])
        content = "".join(self["content"])
        answer_num = 0
        try:
            answer_num = self["answer_num"][0]
        except:
            answer_num = 0
        comment_num = extract_num("".join(self["comment_num"]))

        if len(self["watch_user_num"]) == 2:
            watch_user_num = self["watch_user_num"][0]
            click_num = self["watch_user_num"][1]
        else:
            watch_user_num = self["watch_user_num"][0]
            click_num = 0
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        params = (zhihu_id, topics, url, title, content, answer_num, comment_num, watch_user_num, click_num, crawl_time)
        return insert_sql, params

    def save_to_es(self):
        """
        存储到elasticsearch
        """
        question = ZhiHuQuestionType()
        question.zhihu_id = self['zhihu_id'][0]
        question.topics = ",".join(self["topics"])
        question.url = self["url"][0]
        question.title = "".join(self["title"])
        question.content = content = "".join(self["content"])
        try:
            question.answer_num = self["answer_num"][0]
        except:
            question.answer_num = 0
        question.comment_num = extract_num("".join(self["comment_num"]))
        if len(self["watch_user_num"]) == 2:
            question.watch_user_num = self["watch_user_num"][0]
            question.click_num = self["watch_user_num"][1]
        else:
            question.watch_user_num = self["watch_user_num"][0]
            question.click_num = 0
        crawl_time = datetime.datetime.now().strftime(SQL_DATETIME_FORMAT)
        question.crawl_time = crawl_time

        question.suggest = gen_suggests(es_question, ZhiHuQuestionType._doc_type.index,((question.title, 10), (question.topics, 7), (question.content, 3)))

        question.save()
        redis_cli.incr("question_count")
        return


class ZhihuAnswerItem(scrapy.Item):
    # 知乎的问题回答item
    zhihu_id = scrapy.Field()
    url = scrapy.Field()
    question_id = scrapy.Field()
    author_id = scrapy.Field()
    content = scrapy.Field()
    praise_num = scrapy.Field()
    comment_num = scrapy.Field()
    create_time = scrapy.Field()
    update_time = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        # 插入知乎answer表的sql语句
        insert_sql = """
            insert into zhihu_answer(zhihu_id, url, question_id, author_id, content, praise_num, comment_num,
              create_time, update_time, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              ON DUPLICATE KEY UPDATE content=VALUES(content), comment_num=VALUES(comment_num), praise_num=VALUES(praise_num),
              update_time=VALUES(update_time)
        """

        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATE_FORMAT)
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATE_FORMAT)
        params = (
            self["zhihu_id"], self["url"], self["question_id"],self["author_id"], self["content"], self["praise_num"],
            self["comment_num"], create_time, update_time,self["crawl_time"].strftime(SQL_DATETIME_FORMAT),
        )

        return insert_sql, params

    def save_to_es(self):
        """
        存储到elasticsearch
        """
        answer = ZhiHuAnswerType()
        answer.zhihu_id = self['zhihu_id']
        answer.url = remove_tags(self["url"])
        answer.question_id = self["question_id"]
        answer.author_id = self["author_id"]
        answer.content = self["content"]
        answer.praise_num = self["praise_num"]
        answer.comment_num = self["comment_num"]
        answer.create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime(SQL_DATE_FORMAT)
        answer.update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime(SQL_DATE_FORMAT)
        answer.crawl_time = self["crawl_time"].strftime(SQL_DATETIME_FORMAT)

        answer.suggest = gen_suggests(es_answer, ZhiHuAnswerType._doc_type.index, ((answer.content, 3),))
        answer.save()
        redis_cli.incr("answer_count")
        return


def replace_splash(value):
    return value.replace("/", "")


def handle_strip(value):
    return value.strip()


def handle_jobaddr(value):
    addr_list = value.split("\n")
    addr_list = [item.strip() for item in addr_list if item.strip() != u"查看地图"]
    return "".join(addr_list)


class LagouJobItemLoader(ItemLoader):
    """
        自定义item loader
    """
    default_output_processor = TakeFirst()


class LagouJobItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(replace_splash),
    )
    work_years = scrapy.Field(
        input_processor=MapCompose(replace_splash),
    )
    degree_need = scrapy.Field(
        input_processor=MapCompose(replace_splash),
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field(
        input_processor=MapCompose(handle_strip),
    )
    job_addr = scrapy.Field(
        input_processor=MapCompose(handle_jobaddr),
    )
    company_name = scrapy.Field(
        input_processor=MapCompose(handle_strip),
    )
    company_url = scrapy.Field()
    tags = scrapy.Field(
        input_processor=Join(",")
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need, job_type,
                      publish_time,job_advantage, job_desc, job_addr, company_name, company_url,tags,crawl_time) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)"""
        params = (self["title"], self["url"], self["url_object_id"], self["salary"], self["job_city"],self["work_years"],
                  self["degree_need"], self["job_type"], self['publish_time'], self['job_advantage'],self['job_desc'], self['job_addr'],
                  self['company_name'],self['company_url'],self['tags'],self['crawl_time'])

        return insert_sql, params

    # 保存拉勾网职位到es中
    def save_to_es(self):
        job = LagouType()
        job.title = self["title"]
        job.url = self["url"]
        job.url_object_id = self["url_object_id"]
        job.job_city = self["job_city"]
        job.degree_need = self["degree_need"]
        job.job_desc = remove_tags(self["job_desc"]).strip().replace("\r\n", "").replace("\t", "")
        job.job_advantage = self["job_advantage"]
        job.tags = self["tags"]
        job.job_type = self["job_type"]
        job.publish_time = self["publish_time"]
        job.job_addr = self["job_addr"]
        job.company_name = self["company_name"]
        job.company_url = self["company_url"]
        job.crawl_time = self['crawl_time']

        # 在保存数据时便传入suggest
        job.suggest = gen_suggests(es_lagou, LagouType._doc_type.index,
                                   ((job.title, 10), (job.tags, 7), (job.job_advantage, 6), (job.job_desc, 5),
                                    (job.job_addr, 4), (job.company_name, 8), (job.degree_need, 3),
                                    (job.job_city, 9)))

        redis_cli.incr("job_count")
        job.save()


