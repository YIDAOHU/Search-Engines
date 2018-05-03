# _*_ coding: utf-8 _*_

from django.shortcuts import render
import json
from django.views.generic.base import View
from search.models import ArticleType, ZhiHuQuestionType, LagouType
from django.http import HttpResponse
from datetime import datetime
import redis
import re

from elasticsearch import Elasticsearch
client = Elasticsearch(hosts=["127.0.0.1"])
# 使用redis实现排行榜
redis_cli = redis.StrictRedis()


class IndexView(View):
    """
    首页
    """
    def get(self, request):
        # 获取前三个
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=3)
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    """
    搜索建议
    """
    def get(self, request):
        key_words = request.GET.get('s', '')
        current_page = request.GET.get('s_type', '')
        # 搜索文章
        if current_page == "article":
            suggest_list = []
            if key_words:
                s = ArticleType.search()
                s = s.suggest('my_suggest', key_words, completion={
                    "field": "suggest",
                    "fuzzy": {"fuzziness": 2},
                    "size": 10
                })
                suggestions = s.execute_suggest()
                for match in suggestions.my_suggest[0].options[:10]:
                    source = match._source
                    suggest_list.append(source["title"])
            return HttpResponse(json.dumps(suggest_list), content_type="application/json")

        # 搜索问答
        elif current_page == "question":
            suggest_list = []
            if key_words:
                s = ZhiHuQuestionType.search()
                s = s.suggest('my_suggest', key_words, completion={
                    "field": "suggest",
                    "fuzzy": {"fuzziness": 2},
                    "size": 10
                })
                suggestions = s.execute_suggest()
                if suggestions:
                    for match in suggestions.my_suggest[0].options[:10]:
                        if match._type == "question":
                            source = match._source
                            suggest_list.append(source["title"])

            return HttpResponse(json.dumps(suggest_list), content_type="application/json")

        # 搜索职位
        elif current_page == "job":
            suggest_list = []
            if key_words:
                s = LagouType.search()
                s = s.suggest('my_suggest', key_words, completion={
                    "field": "suggest",
                    "fuzzy": {"fuzziness": 2},
                    "size": 10
                })
                suggestions = s.execute_suggest()
                for match in suggestions.my_suggest[0].options[:10]:
                    source = match._source
                    suggest_list.append(source["title"])
            return HttpResponse(json.dumps(suggest_list), content_type="application/json")


class SearchView(View):

    def get(self, request):
        key_words = request.GET.get("q", "")
        # 实现搜索关键词keyword加1操作
        redis_cli.zincrby("search_keywords_set", key_words)
        topn_search = redis_cli.zrevrangebyscore("search_keywords_set", "+inf", "-inf", start=0, num=8)
        # 获取爬去数量
        jobbole_count = redis_cli.get("jobbole_count")
        question_count = redis_cli.get("question_count")
        job_count = redis_cli.get("job_count")
        # 当前要获取第几页的数据
        page = request.GET.get("p", "1")
        try:
            page = int(page)
        except :
            page = 1
        start_time = datetime.now()
        hit_list = []
        error_nums = 0
        s_type = request.GET.get("s_type", "")
        if s_type == "article":
            response = client.search(
                index="jobbole",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["tags", "title", "content"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {},
                        }
                    }
                }
            )
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                try:
                    if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                    else:
                        hit_dict["title"] = hit["_source"]["title"]
                    if "content" in hit["highlight"]:
                        hit_dict["content"] = "".join(
                            hit["highlight"]["content"])
                    else:
                        hit_dict["content"] = hit["_source"]["content"]
                    hit_dict["create_date"] = hit["_source"]["create_date"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    hit_dict["source_site"] = "伯乐在线"
                    hit_list.append(hit_dict)
                except:
                    error_nums = error_nums + 1
        elif s_type == "question":
            response = client.search(
                index="zhihu",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": [
                                "topics",
                                "title",
                                "content"]}},
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "content": {},
                        }}})
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                if hit["_type"] == "answer":
                    if "content" in hit["highlight"]:
                        hit_dict["content"] = "".join(
                            hit["highlight"]["content"])
                    else:
                        hit_dict["content"] = hit["_source"]["content"]
                    hit_dict["create_date"] = hit["_source"]["update_time"]
                    hit_dict["score"] = hit["_score"]
                    data_url = hit["_source"]["url"]
                    match_url = re.match(".*answers/(\d+)", data_url)
                    question_id = hit["_source"]["question_id"]
                    answer_id = match_url.group(1)
                    hit_dict["url"] = "https://www.zhihu.com/question/{0}/answer/{1}".format(
                        question_id, answer_id)
                    hit_dict["source_site"] = "知乎问答"
                elif hit["_type"] == "question":
                    if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                    else:
                        hit_dict["title"] = hit["_source"]["title"]
                    if "content" in hit["highlight"]:
                        hit_dict["content"] = "".join(hit["highlight"]["content"])
                    else:
                        hit_dict["content"] = hit["_source"]["content"]
                    hit_dict["create_date"] = hit["_source"]["crawl_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    hit_dict["source_site"] = "知乎问答"
                    hit_list.append(hit_dict)
        elif s_type == "job":
            response = client.search(
                index="lagou",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": [
                                "title",
                                "tags",
                                "job_desc",
                                "job_advantage",
                                "company_name",
                                "job_addr",
                                "job_city",
                                "degree_need"]}},
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "job_desc": {},
                            "company_name": {},
                        }}})
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                try:
                    if "title" in hit["highlight"]:
                        hit_dict["title"] = "".join(hit["highlight"]["title"])
                    else:
                        hit_dict["title"] = hit["_source"]["title"]
                    if "job_desc" in hit["highlight"]:
                        hit_dict["content"] = "".join(
                            hit["highlight"]["job_desc"])
                    else:
                        hit_dict["content"] = hit["_source"]["job_desc"]
                    hit_dict["create_date"] = hit["_source"]["publish_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    hit_dict["company_name"] = hit["_source"]["company_name"]
                    hit_dict["source_site"] = "拉勾网"
                    hit_list.append(hit_dict)
                except:
                    hit_dict["title"] = hit["_source"]["title"]
                    hit_dict["content"] = hit["_source"]["job_desc"]
                    hit_dict["create_date"] = hit["_source"]["publish_time"]
                    hit_dict["url"] = hit["_source"]["url"]
                    hit_dict["score"] = hit["_score"]
                    hit_dict["company_name"] = hit["_source"]["company_name"]
                    hit_dict["source_site"] = "拉勾网"
                    hit_list.append(hit_dict)
        total_nums = int(response["hits"]["total"])

        end_time = datetime.now()
        last_seconds = (end_time - start_time).total_seconds()

        # 计算出总页数
        if (page % 10) > 0:
            page_nums = int(total_nums / 10) + 1
        else:
            page_nums = int(total_nums / 10)
        return render(request, "result.html", {"page": page,
                                               "all_hits": hit_list,
                                               "key_words": key_words,
                                               "total_nums": total_nums,
                                               "page_nums": page_nums,
                                               "last_seconds": last_seconds,
                                               "topn_search": topn_search,
                                               "jobbole_count": jobbole_count,
                                               "s_type": s_type,
                                               "job_count": job_count,
                                               "zhihu_count": question_count
                                               })


