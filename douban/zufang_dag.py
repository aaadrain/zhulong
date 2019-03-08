import re
import time
from datetime import timedelta
from threading import Thread
from selenium import webdriver
from lxml import etree
import requests
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from queue import Queue
import random
import sys
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

agents = [
    "Avant Browser/1.2.789rel1 (http://www.avantbrowser.com)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/532.9 (KHTML, like Gecko) Chrome/5.0.310.0 Safari/532.9",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.514.0 Safari/534.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/9.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.14 (KHTML, like Gecko) Chrome/10.0.601.0 Safari/534.14",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.20 (KHTML, like Gecko) Chrome/11.0.672.2 Safari/534.20",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.27 (KHTML, like Gecko) Chrome/12.0.712.0 Safari/534.27",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.24 Safari/535.1",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.36 Safari/535.7",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0 x64; en-US; rv:1.9pre) Gecko/2008072421 Minefield/3.0.2pre",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-GB; rv:1.9.0.11) Gecko/2009060215 Firefox/3.0.11 (.NET CLR 3.5.30729)",
    "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 GTB5",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; tr; rv:1.9.2.8) Gecko/20100722 Firefox/3.6.8 ( .NET CLR 3.5.30729; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0a2) Gecko/20110622 Firefox/6.0a2",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101 Firefox/7.0.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b4pre) Gecko/20100815 Minefield/4.0b4pre",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
    "Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
    "Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
    "Mozilla/2.02E (Win95; U)",
    "Mozilla/3.01Gold (Win95; I)",
    "Mozilla/4.8 [en] (Windows NT 5.1; U)",
    "Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
    "Mozilla/5.0 (Linux; U; Android 3.0.1; fr-fr; A500 Build/HRI66) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13", ]
conp = ['postgres', 'zhulong.com.cn', '192.168.3.174', "douban", 'zufang']
default_args ={
    'owner':'anbang',
    'depends_on_past':False,
    'start_date':'2019-01-28',
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=10),
}
dag = DAG(
    'douban_zufang_info_task',
    default_args=default_args,
    schedule_interval='@once'
)

class DoubanZufangSpider(object):
    def __init__(self, group_num, list_num, conp):
        self.group_queue = Queue()
        self.page_queue = Queue()
        # 以下两种获取ip的方式。二选一即可。
        # self.__init_proxies=self.get_proxies()
        # self.__init_proxies = self.get_ip_proxy()
        # self.proxies = self.get_ip_proxy()

        self.conp = conp
        self.group_page_queue = Queue()
        self.__init_temp_q(group_num, list_num)

    def __init_temp_q(self, group_num, list_num):
        for i in range(0, group_num):
            self.group_page_queue.put(i)
        for i in range(0, list_num):
            self.page_queue.put(i)

    def get_driver(self, url, page,conp):
        '''
        获取豆瓣目标小组列表
        :param url:
        :return: df
        '''
        params = {
            'cat': 1019,
            'sort': 'relevance',
            'q': '深圳租房',
            'start':page*20
        }

        headers = {
            "Host":"www.douban.com",
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            'User-Agent': random.choice(agents),
            "Cache-Control":"max-age=0",
        }
        time.sleep(random.randint(3,5))
        content = requests.get(url, params=params, headers=headers, proxies=self.get_ip_proxy())
        content = content.content.decode()
        page_html = etree.HTML(content)
        content_list = page_html.xpath("//div[@class='groups']/div[@class='result']")
        data = []
        for cont in content_list:
            name = cont.xpath("./div[@class='content']/div[@class='title']/h3/a/text()")[0]
            href = cont.xpath("./div[@class='content']/div[@class='title']/h3/a/@href")[0]
            temp = [name, href]
            data.append(temp)
            # print(temp,self.group_queue.qsize())
            temp_dict = [name, href]
            self.group_queue.put(temp_dict)

        df = pd.DataFrame(data=data, columns=['name', 'href'])
        self.df_to_pg('group_tb', df, conp)
        # df.to_csv('./group_tb',index=False,mode='a')
        print('小组列表获取完成。')

    def df_to_pg(slef, tbname, df, conp):
        con = create_engine('postgresql://%s:%s@%s/%s' % (conp[0], conp[1], conp[2], conp[3]), encoding='utf-8')
        df.to_sql(tbname, con, if_exists='append', schema=conp[4], index=False)

    def connect_do_to_pg(slef, conp, sql):
        con = psycopg2.connect(user=conp[0], password=conp[1], host=conp[2], port='5432', database=conp[3])
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()

    def get_info_list(self, group_url, page_num, conp):
        """
        爬取豆瓣小组信息列表
        :param group_url: 需要爬取的小组链接
        :param page_num: 需要爬取第几页内容
        :return: df
        """
        print(sys._getframe().f_code.co_name)
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Host": "www.douban.com",
            'User-Agent': random.choice(agents),
            "Referer": "https://www.douban.com/",
            "Upgrade-Insecure-Requests": "1"
        }
        time.sleep(random.randint(3,5))
        new_url = group_url + 'discussion?start=%d' % (page_num * 20)
        content = requests.get(new_url, proxies=self.get_ip_proxy(), headers=headers).content.decode()
        # print('get_info_list')
        page_html = etree.HTML(content)
        content_list = page_html.xpath(
            '//div[@id="group-new-topic-bar"]/following-sibling::div[@class=""]//tr[@class=""]')
        data = []
        # print(content_list)
        for content in content_list:
            name = content.xpath('./td[@class="title"]/a/@title')[0]
            href = content.xpath('./td[@class="title"]/a/@href')[0]
            text = self.get_detail_info(href)
            try:
                export_time = re.findall("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text)[0]
            except:
                export_time = "xxxx-xx-xx"
            # print(name,href,text)
            temp = [name, href, export_time, text]
            data.append(temp)
        df = pd.DataFrame(data=data, columns=['name', 'href', 'export_time', 'text'])
        self.df_to_pg("information_tb", df, conp)
        # df.to_csv('./information_tb.csv',index=False,mode='a')

        print('info获取完成%s' % (sys._getframe().f_code.co_name))

    def get_detail_info(self, url):
        """
        爬取帖子文本内容
        :param url: 帖子url
        :return: 帖子text
        """
        # print("<%s> 爬取帖子文本内容。"%(sys._getframe().f_code.co_name))
        time.sleep(random.randint(3, 5))
        try:
            content = requests.get(url, proxies=self.get_ip_proxy(),timeout=8).content.decode()
        except:
            # self.get_ip_proxy()
            content = requests.get(url, proxies=self.get_ip_proxy()).content.decode()
        # print(content)
        page_html = etree.HTML(content)
        text = page_html.xpath("string(//div[@class='topic-doc'])")
        new_tttt = re.sub('\s+', ' ', text)
        return new_tttt

    def start_get_info(self):
        while not self.group_queue.empty():
            new_url = self.group_queue.get(block=False)
            # print(new_url)
            while not self.page_queue.empty():
                i = self.page_queue.get(block=False)
                try:
                    self.get_info_list(new_url[1], i, self.conp)

                except Exception as e:
                    print(sys._getframe().f_code.co_name, '=异常=', e, '=异常=')
                    self.group_queue.put(new_url)
                    self.page_queue.put(i)
                    # self.get_proxies()
                    # self.get_ip_proxy()

        print("线程结束")

    def run(self):
        # 前5页的组数据
        while not self.group_page_queue.empty():
            group_page = self.group_page_queue.get(block=False)
            # print('group_num:',group_page)
            try:
                url = "https://www.douban.com/group/search"
                self.get_driver(url,group_page, self.conp)
                ths = []
                for i in range(2):
                    th = Thread(target=self.start_get_info)
                    ths.append(th)
                for t in ths:
                    time.sleep(random.randint(3, 5))
                    t.start()
                for t in ths:
                    t.join()
            except Exception as e:
                print(sys._getframe().f_code.co_name, '=异常=', e, '=异常=')

                self.group_page_queue.put(group_page)
                # self.get_proxies()
                # self.get_ip_proxy()
        #
        sql = """
        delete from zufang.group_tb where ctid in (select ctid from (select row_number() over(partition by name,href ) as rn,ctid,* from zufang.group_tb) as t where t.rn <>1);
        delete from zufang.information_tb where ctid in (select ctid from (select row_number() over(partition by name,href order by export_time desc ) as rn,ctid,* from zufang.information_tb) as t where t.rn <>1);
        """
        self.connect_do_to_pg(self.conp,sql)

    def get_ip_proxy(self):
        ip_addr = {1: 'http://www.kuaidaili.com/proxylist/1', 2: 'http://www.xicidaili.com/nt/%s'%(random.randint(1,10)),3:"http://www.66ip.cn/%s.html"%random.randint(1,10)}
        n = 0
        while True:
            random_num = random.randint(1, 3)
            print('从第%s个网站中寻找代理ip' % random_num)
            try:
                ip_url = ip_addr[random_num]
                time.sleep(0.1)
                if random_num == 1:
                    html = requests.get(ip_url).content.decode()
                    page = etree.HTML(html)
                    content_list = page.xpath("//tbody[@class='center']/tr")
                    content = random.choice(content_list)
                    get_ip = content.xpath("./td[@data-title='IP']/text()")[0]
                    get_port = content.xpath("./td[@data-title='PORT']/text()")[0]
                elif random_num == 2:
                    # print(random.choice(agents))
                    html = requests.get(ip_url, headers={'User-Agent': random.choice(agents)}).content.decode()
                    # print(html)
                    page = etree.HTML(html)
                    content_list = page.xpath("//table[@id='ip_list']//tr")
                    content = random.choice(content_list)
                    get_ip = content.xpath('./td[2]/text()')[0]
                    get_port = content.xpath('./td[3]/text()')[0]

                else:
                    html = requests.get(ip_url, headers={'User-Agent': random.choice(agents)}).content.decode('gb2312')
                    page = etree.HTML(html)
                    content_list = page.xpath("//table[@width='100%' and @border='2px']//tr[position()!=1]")
                    # print(html)
                    content = random.choice(content_list)
                    get_ip = content.xpath('./td[1]/text()')[0]
                    get_port = content.xpath('./td[2]/text()')[0]
                ip = get_ip + ':' + get_port
                self.proxies = {
                    'http': 'http://%s' % (ip),
                }
                time.sleep(0.1)
                if requests.get('http://www.baidu.com', proxies=self.proxies, timeout=8).status_code == 200:
                    print("ip可用:",ip)
                    return self.proxies
            except Exception as e:
                print(sys._getframe().f_code.co_name, '=异常=', e,'=异常=')
                n += 1
                if n > 20: break

    def get_proxies(self):
        try:
            print('<%s>获取代理ip和创建代理' % sys._getframe().f_code.co_name)
            url = """http://ip.11jsq.com/index.php/api/entry?method=proxyServer.generate_api_url&packid=0&fa=0&fetch_key=&qty=1&time=1&pro=&city=&port=1&format=txt&ss=1&css=&dt=1&specialTxt=3&specialJson="""
            r = requests.get(url)
            time.sleep(1)
            ip = r.text
            self.proxies = {
                'http': 'http://%s' % (ip),
            }
        except:
            ip = "ip失败"
            self.proxies = {}
        return

    def getDfFromSQL(self,sql,conp):

        con = psycopg2.connect(user=conp[0], password=conp[1], host=conp[2], port='5432', database=conp[3])
        df = pd.read_sql(sql,con)
        return df
doubanzufang_spider = DoubanZufangSpider(3, 10, conp)




t = PythonOperator(task_id='douban_zufang_task',python_callable=doubanzufang_spider.run,dag=dag)
if __name__ == '__main__':
    # get_proxies()
    # sql = "select table_name from information_schema.tables "
