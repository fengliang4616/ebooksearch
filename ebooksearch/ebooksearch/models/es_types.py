# encoding: utf-8
from elasticsearch_dsl import DocType, Date, Nested, Boolean, analyzer, Completion, Keyword, Text, Integer
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

# 连接服务器，可连接多个
connections.create_connection(hosts=["localhost"])


# 自定义分词器
class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


# 创建分析器对象 filter忽略大小写
ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])


# 创建数据Model IshareType类型
class IshareType(DocType):
    suggest = Completion(analyzer=ik_analyzer)  # 词条推荐
    title = Text(analyzer="ik_max_word")
    url = Keyword()
    url_object_id = Keyword()
    load_num = Integer()
    read_num = Integer()
    crawl_time = Date()
    type = Text(analyzer="ik_smart")
    source_website = Keyword()
    upload_time = Date()

    class Meta:
        index = "ebooksearch"
        doc_type = "ishare"


# 创建数据Model PipipaneType类型
class PipipaneType(DocType):
    # pipipan 类型
    suggest = Completion(analyzer=ik_analyzer)  # 词条推荐
    title = Text(analyzer="ik_max_word")
    url = Keyword()
    url_object_id = Keyword()
    read_num = Integer()
    type = Text(analyzer="ik_smart")
    source_website = Keyword()
    upload_time = Date()
    crawl_time = Date()
    tag = Text(analyzer="ik_smart")
    description = Text(analyzer="ik_smart")

    class Meta:
        index = "ebooksearch"
        doc_type = "pipipan"


class MebookType(DocType):
    # mebook 类型
    suggest = Completion(analyzer=ik_analyzer)  # 词条推荐
    title = Text(analyzer="ik_max_word")
    url = Keyword()
    url_object_id = Keyword()
    type = Text(analyzer="ik_smart")
    source_website = Keyword()
    upload_time = Date()
    crawl_time = Date()
    tag = Text(analyzer="ik_smart")
    description = Text(analyzer="ik_smart")

    class Meta:
        index = "ebooksearch"
        doc_type = "mebook"


class IreadweekType(DocType):
    suggest = Completion(analyzer=ik_analyzer)  # 词条推荐
    title = Text(analyzer="ik_max_word")
    url = Keyword()
    url_object_id = Keyword()
    type = Text(analyzer="ik_smart")
    source_website = Keyword()
    crawl_time = Date()
    description = Text(analyzer="ik_smart")

    class Meta:
        index = "ebooksearch"
        doc_type = "ireadweek"


class BookType(DocType):
    IshareType.init()
    PipipaneType.init()
    MebookType.init()


if __name__ == "__main__":
    """
    初始化步骤先执行 在elasticsearch中创建索引
    """
    BookType()
