import json
import re
import time

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import etree
import pandas as pd
from zhulong.util.etl import est_meta, est_html

_name_ = "hulunbeier"
global categoryzhu,xxlx

def f1(driver, num):

    url = "http://www.hlbeggzyjy.org.cn/Epoint_hlbeggzy/jyxxlistaction.action?cmd=getInfolist&pageIndex=1&pageSize=16&siteGuid=7eb5f7f1-9041-43ad-8e13-8fcb82ea831a&xxlx="+xxlx+"&fbsj=&dqfb=&jylx=&categoryzhu="+categoryzhu+"&_="+str(int(time.time()*1000))
    n_url = re.sub(r'pageIndex=\d+','pageIndex='+str(num),url)
    # print(url)
    driver.get(n_url)
    data = []
    page = driver.page_source.encode('utf-8').decode("unicode_escape")
    page_str = re.findall(r": \[([^]]+)",page)[0]
    page_content = re.sub(r'},{','},_1+{',page_str)
    page_content = page_content.split(',_1+')
    for content in page_content:
        page_json = json.loads(content)
        name = page_json["title"]
        href = "http://www.hlbeggzyjy.org.cn" + re.sub(r'\\','',page_json["href"])
        gg_time = page_json['date']
        temp = [name, gg_time, href]
        # print(temp)
        data.append(temp)

    # locator = (By.XPATH, "//div[@class='tab-panel']//ul[@class='wb-data-item']/li[2]/div/a")
    # WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located(locator))
    # val = driver.find_element_by_xpath("//div[@class='tab-panel']//ul[@class='wb-data-item']/li[1]/div/a").get_attribute("href")[-50:]
    # locator = (By.XPATH, "//div[@class='tab-panel']//span[@class='pg_maxpagenum']")
    # WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
    # cnum = \
    # re.findall('(\d+)\/', driver.find_element_by_xpath("//div[@class='tab-panel']//span[@class='pg_maxpagenum']").text)[
    #     0]
    # if int(cnum) != int(num):
    #     driver.find_element_by_xpath("//div[@class='tab-panel']//input[@class='pg_num_input']").clear()
    #     driver.find_element_by_xpath("//div[@class='tab-panel']//input[@class='pg_num_input']").send_keys(num)
    #     driver.find_element_by_xpath("//div[@class='tab-panel']//a[@class='pg_gobtn']").click()
    #     locator = (By.XPATH, "//div[@class='tab-panel']//ul[@class='wb-data-item']/li[2]/div/a")
    #     WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located(locator))
    #     locator = (By.XPATH,
    #                "//div[@class='tab-panel']//ul[@class='wb-data-item']/li[1]/div/a[not(contains(@href,'%s'))]" % val)
    #     WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
    # data = []
    # page = driver.page_source
    # body = etree.HTML(page)
    # content_list = body.xpath("//div[@class='tab-panel']//ul[@class='wb-data-item']/li")
    # for content in content_list:
    #     name = content.xpath("./div/a/text()")[0].strip()
    #     ggstart_time = content.xpath("./span/text()")[0]
    #     url = "http://www.hlbeggzyjy.org.cn" + content.xpath("./div/a/@href")[0]
    #     temp = [name, ggstart_time, url]
    #     data.append(temp)
    df = pd.DataFrame(data=data)
    df["info"] = None
    driver.refresh()
    return df


def f2(driver):
    locator = (By.XPATH, "//div[@class='tab-panel']//span[@class='pg_maxpagenum']")
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
    total_page = \
    re.findall('\/(\d+)', driver.find_element_by_xpath("//div[@class='tab-panel']//span[@class='pg_maxpagenum']").text)[
        0]
    driver.quit()
    return int(total_page)


def f3(driver, url):
    driver.get(url)
    try:
        locator = (By.XPATH, "//div[@class='ewb-main']")
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located(locator))
        flag = True
    except:
        locator = (By.XPATH, "//div[@class='news-article']")
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located(locator))
        flag = False
    before = len(driver.page_source)
    time.sleep(0.1)
    after = len(driver.page_source)
    i = 0
    while before != after:
        before = len(driver.page_source)
        time.sleep(0.1)
        after = len(driver.page_source)
        i += 1
        if i > 5: break
    page = driver.page_source
    soup = BeautifulSoup(page, 'lxml')
    if flag:
        div = soup.find('div', class_='ewb-main')
    else:
        div = soup.find('div', class_='news-article')
    return div


def switch(driver, ggtype, xmtype):
    script = gg_type[ggtype][xmtype]
    global categoryzhu, xxlx
    categoryzhu = re.findall(r"'(\d{3})'",script)[0]
    xxlx = re.findall(r"'(\d{12})'",script)[0]
    locator = (By.XPATH, "//div[@class='tab-panel']//ul[@class='wb-data-item']/li[2]/div/a")
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
    val = driver.find_element_by_xpath("//div[@class='tab-panel']//ul[@class='wb-data-item']/li[1]/div/a").get_attribute("href")
    driver.execute_script(gg_type[ggtype][xmtype])
    locator = (By.XPATH, "//div[@class='tab-panel']//ul[@class='wb-data-item']/li[2]/div/a")
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
    locator = (By.XPATH,
               "//div[@class='tab-panel']//ul[@class='wb-data-item']/li[1]/div/a[not(contains(@href,'%s'))]" % val)
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))


def before(f, ggtype, xmtype):
    def wrap(*args):
        driver = args[0]
        switch(driver, ggtype, xmtype)
        return f(*args)

    return wrap


data = [
    ["gcjs_zhaobiao_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "工程建设", "招标公告"), before(f2, "工程建设", "招标公告")],
    ["gcjs_biangen_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "工程建设", "变更公告"), before(f2, "工程建设", "变更公告")],
    ["gcjs_zhongbiaohx_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "工程建设", "中标候选人公示"), before(f2, "工程建设", "中标候选人公示")],
    ["gcjs_zhongbiao_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "工程建设", "中标结果公告"), before(f2, "工程建设", "中标结果公告")],
    ["gcjs_liulbiao_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "工程建设", "招标异常公告"), before(f2, "工程建设", "招标异常公告")],

    ["zfcg_zhaobiao_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "政府采购", "采购公告"), before(f2, "政府采购", "采购公告")],
    ["zfcg_biangen_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "政府采购", "变更公告"), before(f2, "政府采购", "变更公告")],
    ["zfcg_zhongbiao_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "政府采购", "中标公示"), before(f2, "政府采购", "中标公示")],
    ["zfcg_liubiao_gg",
     "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html",
     ["name", "ggstart_time", "href", "info"], before(f1, "政府采购", "废标公告"), before(f2, "政府采购", "废标公告")],

]

gg_type = {
    "工程建设": {
        "招标公告": "searchjy('021','008001001001','','','',this)",
        "变更公告": "searchjy('021','008001001002','','','',this)",
        "中标候选人公示": "searchjy('021','008001001005','','','',this)",
        "中标结果公告": "searchjy('021','008001001006','','','',this)",
        "招标异常公告": "searchjy('021','008001001007','','','',this)",
    },
    "政府采购": {
        "采购公告": "searchjy('021','008001001001','','','',this)",
        "变更公告": "searchjy2('022','008001002002','','','',this)",
        "中标公示": "searchjy2('022','008001002005','','','',this)",
        "废标公告": "searchjy2('022','008001002006','','','',this)",
    },
}


def work(conp, **kwargs):
    est_meta(conp, data=data, diqu="内蒙古自治区呼伦贝尔市", **kwargs)
    est_html(conp, f=f3, **kwargs)


if __name__ == "__main__":

    work(conp=["postgres", "since2015", "192.168.3.171", "neimenggu", "hulunbeier"])
    # url = "http://www.hlbeggzyjy.org.cn/gcjs/subpage-jyxx.html"
    # driver = webdriver.Chrome()
    # driver.get(url)
    # print(before(f2, "工程建设", "中标结果公告")(driver))
    # # print(before(f1, "工程建设", "中标结果公告")(driver,1))
    # # print(before(f1, "工程建设", "中标结果公告")(driver,6))
    # driver = webdriver.Chrome()
    # driver.get(url)
    # print(f1(driver,1))
    # print(f1(driver,6))
    # driver.quit()