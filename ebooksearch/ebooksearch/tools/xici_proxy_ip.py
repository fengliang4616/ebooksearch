# encoding: utf-8

import requests
import re
from scrapy.selector import Selector
# import MySQLdb        //python3将MySQLdb指向pymysql
import pymysql

pymysql.install_as_MySQLdb()

conn = pymysql.connect(host="localhost", user="root", passwd="0000", database="ebooksearch", charset="utf8")
cursor = conn.cursor()


def get_xici_total_page():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/63.0.3239.84 Safari/537.36'
    }

    range_response = requests.get("http://www.xicidaili.com/nn/", headers=headers)
    range_selector = Selector(text=range_response.text)
    total_page = int(range_selector.xpath('//*[@id="body"]/div[2]/a[10]/text()').extract_first(2858))
    return total_page


def crawl_ips(total_page):
    global live_time
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/63.0.3239.84 Safari/537.36'
    }

    for i in range(total_page):
        response = requests.get("http://www.xicidaili.com/nn/{0}".format(i), headers=headers)

        selector = Selector(text=response.text)
        all_trs = selector.css("#ip_list tr")

        ip_list = []
        for tr in all_trs[1:]:
            speed_str = tr.css(".bar::attr(title)").extract_first("")
            if speed_str:
                speed = float(speed_str.split("秒")[0])
            else:
                speed = float(0)
            all_text = tr.css("td::text").extract()
            if all_text:
                ip = all_text[0]
                port = all_text[1]
                type = all_text[5]
                live_time_str = all_text[10]
                if "分钟" in live_time_str:
                    live_time = int(live_time_str.split("分钟")[0]) * 1000 * 60
                elif "小时" in live_time_str:
                    live_time = int(live_time_str.split("小时")[0]) * 1000 * 60 * 60
                elif "天" in live_time_str:
                    live_time = int(live_time_str.split("天")[0]) * 1000 * 60 * 60 * 24

                match_obj1 = re.match(".*'HTTP'.*", str(all_text))
                match_obj2 = re.match(".*'HTTPS'.*", str(all_text))
                if match_obj1:
                    type = 'http'
                if match_obj2:
                    type = 'https'

                ip_list.append((ip, port, type, speed, live_time))
                # 插入数据库
                for ip_info in ip_list:
                    insert_sql = """
                        insert into xici_ip (ip, port, type, speed, live_time) VALUES ('{0}', '{1}', '{2}', {3}, '{4}')
                        ON DUPLICATE KEY UPDATE live_time=VALUES(live_time)
                    """.format(ip_info[0], ip_info[1], ip_info[2], ip_info[3], ip_info[4])
                    cursor.execute(insert_sql)
                    conn.commit()


class GetIP(object):
    def delete_ip(self, ip):
        delete_sql = " delete from xici_ip where ip = '{0}'".format(ip)
        cursor.execute(delete_sql)
        conn.commit()
        return True

    def judge_ip(self, ip, port, type):
        # 判断ip是否可用
        http_url = "https://www.baidu.com"
        proxy_url = "{0}://{1}:{2}".format(type, ip, port)
        try:
            proxy_dict = {
                type: proxy_url,
            }
            response = requests.get(http_url, proxies=proxy_dict)
        except Exception as e:
            print(ip)
            print(e)
            print("invalid ip and port")
            self.delete_ip(ip)
            return False
        else:
            if 200 <= response.status_code < 300:
                return True
            else:
                print(ip)
                print("invalid ip and port")
                self.delete_ip(ip)
                return False

    def get_random_ip(self):
        # 获取随机ip
        random_sql = """
            select ip, port, type from xici_ip order by rand() limit 1
        """
        result = cursor.execute(random_sql)
        print(result)
        for ip_info in cursor.fetchall():
            ip = ip_info[0]
            port = ip_info[1]
            type = ip_info[2]

            judge_re = self.judge_ip(ip, port, type)
            if judge_re:
                url = "{0}://{1}:{2}".format(type, ip, port)
                print("proxy success: ", url)
                return "{0}://{1}:{2}".format(type, ip, port)
            else:
                return self.get_random_ip()


if __name__ == "__main__":
    get_ip = GetIP()
    print("success: ", get_ip.get_random_ip())
    total_page = get_xici_total_page()
    crawl_ips(total_page)
