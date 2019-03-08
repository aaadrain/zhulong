import re
import time
from threading import Thread
from selenium import webdriver
from lxml import etree
import requests
import psycopg2
import pandas as pd
from selenium.webdriver import DesiredCapabilities
from sqlalchemy import create_engine
from queue import Queue
import random
import sys
import traceback
import multiprocessing

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


class DoubanZufangSpider(object):
    def __init__(self, group_num, num_per_group, conp, headless=True):
        self.page_queue = Queue()
        self.group_queue = Queue()  # 小组name和href队列
        self.conp = conp
        self.headless = headless
        self.group_page_queue = Queue()  # 每个小组总页数
        self.total_queue = Queue()  # 这个对列包括所有的name和href
        self.__init_temp_q(group_num, num_per_group)
        self.df = Queue()
        self.ip_pool = Queue()

        # self.__init_driver(ip,headless=self.headless)

    def __init_temp_q(self, group_num, num_per_group):
        for i in range(0, group_num):
            self.group_page_queue.put(i)
        for i in range(0, num_per_group):
            self.page_queue.put(i)

    def get_driver(self, dirver, page, conp):
        '''
        获取豆瓣目标小组列表
        :param driver,page,coon
        :return: none
        '''
        url = 'https://www.douban.com/group/search?q=深圳租房&cat=1019&sort=relevance&start=%s' % (page * 20)
        content = self.chrome_driver(dirver, url)
        page_html = etree.HTML(content)
        content_list = page_html.xpath("//div[@class='groups']/div[@class='result']")
        if content_list == []:
            raise Exception
        data = []
        for cont in content_list:
            name = cont.xpath("./div[@class='content']/div[@class='title']/h3/a/text()")[0]
            href = cont.xpath("./div[@class='content']/div[@class='title']/h3/a/@href")[0]
            temp = [name, href]
            data.append(temp)
            temp_dict = [name, href]
            self.group_queue.put(temp_dict)
        df = pd.DataFrame(data=data, columns=['name', 'href'])
        self.df_to_pg('group_tb', df, conp)
        # df.to_csv('./group_tb',index=False,mode='a')
        print('小组列表获取完成。')

    def df_to_pg(slef, tbname, df, conp):
        con = create_engine('postgresql://%s:%s@%s/%s' % (conp[0], conp[1], conp[2], conp[3]), encoding='utf-8')
        df.to_sql(tbname, con, if_exists='append', schema=conp[4], index=False)
        print('"%s"写入完成。' % tbname)

    def connect_do_to_pg(slef, conp, sql):
        con = psycopg2.connect(user=conp[0], password=conp[1], host=conp[2], port='5432', database=conp[3])
        cur = con.cursor()
        cur.execute(sql)
        con.commit()
        cur.close()
        con.close()

    def get_info_list(self, driver, group_url, page_num, conp):
        """
        爬取豆瓣小组信息列表
        :param group_url: 需要爬取的小组链接
        :param page_num: 需要爬取第几页内容
        :return: df
        """
        time.sleep(random.randint(3, 5))
        new_url = group_url + 'discussion?start=%d' % (page_num * 20)
        driver.get(new_url)
        content = driver.page_source
        page_html = etree.HTML(content)
        content_list = page_html.xpath(
            '//div[@id="group-new-topic-bar"]/following-sibling::div[@class=""]//tr[@class=""]')
        # print(content_list)
        for content in content_list:
            name = content.xpath('./td[@class="title"]/a/@title')[0]
            href = content.xpath('./td[@class="title"]/a/@href')[0]
            temp = [name, href]
            # print(temp)
            self.total_queue.put(temp)


    def pre_get_total_data(self):
        # ip = self.get_ip()
        driver = self.__init_driver(headless=self.headless)
        while not self.total_queue.empty():
            temp = self.total_queue.get(block=False)
            try:
                self.get_total_data(temp, driver)
            except:
                traceback.print_exc()
                ip = self.get_ip()
                driver.quit()
                driver = self.__init_driver(ip, headless=self.headless)
                self.total_queue.put(temp)

    def get_total_data(self, temp, driver):
        tmp = list(temp)
        url = tmp[1]
        # driver = self.__init_driver(self.ip,headless=self.headless)
        content = self.chrome_driver(driver, url)
        page_html = etree.HTML(content)
        text = page_html.xpath("string(//div[@class='topic-doc'])")
        text = re.sub('\s+', ' ', text)
        try:
            export_time = re.findall("\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", text)[0]
        except:
            export_time = "xxxx-xx-xx"
        tmp.append(export_time)
        tmp.append(text)
        self.df.put(tmp)
        # df.to_csv('./information_tb.csv',index=False,mode='a')

    def get_detail_info(self, driver, url):
        """
        爬取帖子文本内容
        :param url: 帖子url
        :return: 帖子text
        """
        time.sleep(random.randint(3, 5))
        content = self.chrome_driver(driver, url)
        page_html = etree.HTML(content)
        text = page_html.xpath("string(//div[@class='topic-doc'])")
        new_tttt = re.sub('\s+', ' ', text)
        return new_tttt

    def start_get_info(self):
        # ip = self.get_ip()
        driver = self.__init_driver(headless=self.headless)
        while not self.group_queue.empty():
            new_url = self.group_queue.get(block=False)
            try:
                while not self.page_queue.empty():
                    i = self.page_queue.get(block=False)
                    try:
                        self.get_info_list(driver, new_url[1], i, self.conp)
                    except:
                        ip = self.get_ip()
                        driver = self.__init_driver(ip)
                        self.page_queue.put(i)
            except:
                driver = self.__init_driver(headless=self.headless)
                self.group_queue.put(new_url)
        driver.quit()
        print("线程结束")

    def run(self):
        # 前5页的组数据
        ip = self.get_ip()
        # ip = self.ip_pool.get()
        driver = self.__init_driver(ip, headless=self.headless)
        while not self.group_page_queue.empty():
            group_page = self.group_page_queue.get(block=False)
            try:
                self.get_driver(driver, group_page, self.conp)
                # driver = self.__init_driver(self.ip,headless=self.headless)
            except Exception as e:
                # print(sys._getframe().f_code.co_name, '=异常=', e, '=异常=')
                ip = self.get_ip()
                driver.quit()
                self.group_page_queue.put(group_page)
                driver = self.__init_driver(ip, headless=self.headless)
        print('self.group_queue.queue', self.group_queue.queue)
        driver.quit()
        ths = []
        for i in range(2):
            th = Thread(target=self.start_get_info)
            ths.append(th)
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        print('self.total_queue.queue', self.total_queue.queue)
        ths = []
        for i in range(2):
            th = Thread(target=self.pre_get_total_data)
            ths.append(th)
        for t in ths:
            t.start()
        for t in ths:
            t.join()

        print('self.df.queue', self.df.queue)
        dfs = list(self.df.queue)
        df = pd.DataFrame(data=dfs, columns=['name', 'href', 'export_time', 'text'])
        self.df_to_pg("information_tb", df, conp)
        #
        sql = """
        delete from zufang.group_tb where ctid in (select ctid from (select row_number() over(partition by name,href ) as rn,ctid,* from zufang.group_tb) as t where t.rn <>1);
        delete from zufang.information_tb where ctid in (select ctid from (select row_number() over(partition by name,href order by export_time desc ) as rn,ctid,* from zufang.information_tb) as t where t.rn <>1);
        delete from nanshan_zufang where ctid in (
        select ctid from (
        select *,ctid,row_number() over(partition by name,href )as rn from nanshan_zufang) as t where t.rn <>1);
        """
        self.connect_do_to_pg(self.conp, sql)

    def start_get_ip(self):
        ths = []
        for i in range(3):
            th = Thread(target=self.get_ip)
            ths.append(th)
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        while not self.ip_pool.empty():
            print(self.ip_pool.get())

    def get_ip_proxy(self):
        ip_addr = {1: 'http://www.kuaidaili.com/proxylist/%s' % random.randint(1, 10),
                   4: 'http://www.xicidaili.com/nt/%s' % random.randint(1, 10),
                   3: "http://www.66ip.cn/%s.html" % random.randint(1, 10),
                   2: "https://www.kuaidaili.com/free/inha/%s/" % random.randint(1, 10),
                   5: "http://www.ip3366.net/free/?stype=1&page=%s" % random.randint(1, 5)}
        while True:
            random_num = random.randint(1, 5)
            print('从第%s个网站中寻找代理ip' % random_num)

            ip_url = ip_addr[random_num]
            time.sleep(0.1)
            try:
                content = requests.get(ip_url, headers={'User-Agent': random.choice(agents)}).content.decode()
            except:
                content = requests.get(ip_url, headers={'User-Agent': random.choice(agents)}).content.decode('gb2312')
            content = re.sub('\s+', '', content)
            ip_port_list = re.findall('<td>([.\d]{10,20})</td><td>(\d{1,5})</td>', content)

            for get_ip, get_port in ip_port_list:
                ip = get_ip + ':' + get_port
                time.sleep(0.1)
                proxies = {"http": "http://%s" % ip}
                print('proxy', proxies)
                try:
                    validate_ip = requests.get('http://icanhazip.com', proxies=proxies, timeout=8).text
                    print('return_ip', validate_ip)
                    print('get_ip', get_ip)
                    if validate_ip == get_ip:
                        print("ip可用:", ip)
                        self.ip_pool.put(ip)

                except Exception as e:
                    traceback.print_exc()
                    # print(sys._getframe().f_code.co_name, '=异常=', e, '=异常=')
                    pass
                # n += 1
                # if n > 20: break

    def get_ip(self):
        try:
            print('<%s>获取代理ip和创建代理' % sys._getframe().f_code.co_name)
            url = """http://ip.11jsq.com/index.php/api/entry?method=proxyServer.generate_api_url&packid=0&fa=0&fetch_key=&qty=1&time=1&pro=&city=&port=1&format=txt&ss=1&css=&dt=1&specialTxt=3&specialJson="""
            r = requests.get(url)
            time.sleep(1)
            self.ip = r.text
        except:
            self.ip = ""
            self.proxies = {}
        return self.ip

    def getDfFromSQL(self, sql, conp):
        con = psycopg2.connect(user=conp[0], password=conp[1], host=conp[2], port='5432', database=conp[3])
        df = pd.read_sql(sql, con)
        return df

    def __init_driver(self, ip=None, headless=True):
        if ip == None: ip = self.ip
        chrome_option = webdriver.ChromeOptions()
        # proxies, ip = self.get_ip_proxy()
        chrome_option.add_argument('--proxy-server=http://%s' % ip)
        if headless is True:
            chrome_option.add_argument('--headless')
            chrome_option.add_argument("--no-sandbox")
        desired_caps = DesiredCapabilities().CHROME
        desired_caps['pageloadStrategy'] = 'normal'
        args = {'desired_capabilities': desired_caps, 'chrome_options': chrome_option}
        driver = webdriver.Chrome(**args)
        return driver

    def chrome_driver(self, driver, url):

        driver.get(url)
        # driver.save_screenshot('./douban.png')
        content = driver.page_source
        return content


    def select_area(self):
        pass





if __name__ == '__main__':
    conp = ['postgres', 'zhulong.com.cn', '192.168.3.174', "douban", 'zufang']
    doubanzufang_spider = DoubanZufangSpider(1, 2, conp)
    # doubanzufang_spider.chrome_driver(headless=False)
    # doubanzufang_spider.start_get_ip()
    doubanzufang_spider.run()
    # sql = "select table_name from information_schema.tables "
