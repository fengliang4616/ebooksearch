import re
import json
import redis
from django.shortcuts import render, redirect
from django.views.generic.base import View
from esearch.models import BookType
from django.http import HttpResponse
from datetime import datetime
from ebooksearch_web.utils.common import OrderedSet
from w3lib.html import remove_tags
from elasticsearch import Elasticsearch

from elasticsearch_dsl.connections import connections

es = connections.create_connection(BookType._doc_type.using)  # 连接es的一种用法

client = Elasticsearch(hosts=["127.0.0.1"])
redis_cli = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, password=None, charset='utf-8')


def page_not_found(request):
    return render(request, '404.html')


def page_error(request):
    return render(request, '500.html')


def permission_denied(request):
    return render(request, '403.html')


class IndexView(View):
    # 首页
    @staticmethod
    def get(request):
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5)
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean
        return render(request, "index.html", {"topn_search": topn_search})


class SearchSuggest(View):
    # 搜索建议
    def get(self, request):
        key_words = request.GET.get('s', '')
        if not key_words:
            return redirect("index.html")
        re_datas = []
        if key_words:
            s = BookType.search()
            # fuzzy模糊搜索  fuzziness:编辑距离 prefix_length:前面不变化的前缀长度
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest",
                "fuzzy": {
                    "fuzziness": 1  # 可具体匹配到词
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options[:10]:
                source = match._source
                re_datas.append(source["title"])
                # re_datas.append(source["content"])
        return HttpResponse(
            json.dumps(re_datas),
            content_type="application/json")


class SearchView(View):
    # 搜索结果
    @staticmethod
    def get(request):
        global upload_time
        key_words = request.GET.get("q", "")

        print("key_words: " + key_words)

        # 热门搜索
        redis_cli.zincrby("search_keywords_set", value=key_words, amount=5)
        # 获取topn个搜索词
        topn_search_clean = []
        topn_search = redis_cli.zrevrangebyscore(
            "search_keywords_set", "+inf", "-inf", start=0, num=5)
        for topn_key in topn_search:
            topn_key = str(topn_key, encoding="utf-8")
            topn_search_clean.append(topn_key)
        topn_search = topn_search_clean

        page = request.GET.get("p", "1")
        try:
            page = int(page)
        except:
            page = 1

        start_time = datetime.now()
        response = client.search(
            index="ebooksearch",
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
                        "tags": {},
                    }
                }
            }
        )

        end_time = datetime.now()
        last_seconds = (end_time - start_time).total_seconds()
        total_nums = response["hits"]["total"]
        if (page % 10) > 0:
            page_nums = int(total_nums / 10) + 1
        else:
            page_nums = int(total_nums / 10)
        hit_list = []
        for hit in response["hits"]["hits"]:
            hit_dict = {}
            if "title" in hit["highlight"]:
                hit_dict["title"] = "".join(hit["highlight"]["title"])
            else:
                hit_dict["title"] = hit["_source"]["title"]
            if "tag" in hit["highlight"]:
                hit_dict["tag"] = "".join(hit["highlight"]["tags"])[:500]
            else:
                hit_dict["tag"] = hit["_source"]["tag"][:500]

            hit_dict["upload_time"] = hit["_source"]["upload_time"]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["source_website"] = hit["_source"]["source_website"]
            hit_dict["score"] = hit["_score"]

            upload_time = hit["_source"]["upload_time"]
            m = re.match(r'(\d+-\d+-\d+)T.*', upload_time)
            if m:
                upload_time = m.group(1)
            else:
                upload_time = hit["_source"]["upload_time"]

            hit_list.append(hit_dict)

            print(upload_time)

        return render(request, "result.html", {"page": page,
                                               "all_hits": hit_list,
                                               "key_words": key_words,
                                               "total_nums": total_nums,
                                               "page_nums": page_nums,
                                               "last_seconds": last_seconds,
                                               "topn_search": topn_search,
                                               "upload_time": upload_time
                                               })
