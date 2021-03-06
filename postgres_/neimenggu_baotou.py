import os

import pandas as pd
import re
from lxml import etree
from selenium import webdriver
from bs4 import BeautifulSoup
from lmf.dbv2 import db_write, db_command, db_query
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image
import sys
import time
import pytesseract
from zhulong.util.etl import est_meta, est_html

_name_ = 'baotou'

def save_yzm_img(driver,num):
    img = driver.find_element_by_xpath('//img[@class="yzmimg y"]')
    driver.maximize_window()
    driver.save_screenshot("full_snap.png")
    location = img.location
    size = img.size
    left = location['x']
    top = location['y']
    right = left + size['width']
    bottom = top + size['height']
    page_snap_obj = Image.open('full_snap.png')
    image_obj = page_snap_obj.crop((left, top, right, bottom))
    # image_obj.show()
    image_obj.save("yzm"+str(num)+".png")
    yzm_img = Image.open("yzm"+str(num)+".png")
    return yzm_img

def parse_img(img):
    text = pytesseract.image_to_string(img,lang="eng")
    return text



def f3(driver, url):
    driver.get(url)
    try:
        locator = (By.XPATH, "//td[@valign='top']")
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
        flag = 2

    except:
        locator = (By.XPATH, '//table[@class="table"]')
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located(locator))
        flag = 1
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
    soup = BeautifulSoup(page, 'html.parser')
    if flag == 1:
        div = soup.find('table', class_='table')
    else:
        div = soup.find('table', valign='top')
    return div


def f1(driver, num):
    locator = (By.XPATH, '//tbody[@class="tableBody"]/tr')
    WebDriverWait(driver, 20).until(EC.presence_of_element_located(locator))
    val = driver.find_element_by_xpath('//tbody[@class="tableBody"]/tr[1]//a').get_attribute("href")[-20:]
    cnum = int(re.findall('(\d+)', driver.find_element_by_xpath(
        "//td[@class='compactToolbar']//select[@name='__ec_pages']/option[@selected='selected']").text)[0])
    # print('val', val, 'cnum', cnum,'num',num)
    if int(cnum) != int(num):
        driver.execute_script(
            "javascript:document.forms.topicChrList_20070702.topicChrList_20070702_p.value='%s';document.forms.topicChrList_20070702.setAttribute('action','');document.forms.topicChrList_20070702.setAttribute('method','post');document.forms.topicChrList_20070702.submit()" % str(num))
        # driver.execute_script("javascript:document.forms.topicChrList.topicChrList_p.value='%s';document.forms.topicChrList.setAttribute('action','');document.forms.topicChrList.setAttribute('method','post');document.forms.topicChrList.submit()" % str(num))
        while "请输入验证码查看公告列表！" in driver.page_source:
            img = save_yzm_img(driver, num)
            text = parse_img(img)
            os.remove("yzm"+str(num)+".png")
            text = re.sub(r"\s+",'',text)
            # print(text)
            flag = 0
            if text == '':
                driver.refresh()
                if flag >3:break
                flag += 1
                continue
            driver.find_element_by_xpath('//input[@id="verify"]').send_keys(text)
            driver.find_element_by_xpath('//input[@class="yzmbox_submit roundimgx"]').click()

        locator = (By.XPATH, '//tbody[@class="tableBody"]/tr[1]//a[not(contains(@href,"%s"))]' % val)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located(locator))

    data = []
    page = driver.page_source
    body = etree.HTML(page)
    content_list = body.xpath('//tbody[@class="tableBody"]/tr')
    for content in content_list:
        name = content.xpath(".//a/text()")[0].strip()
        ggstart_time = content.xpath("./td[last()]/text()")[0].split(" ")[0]
        url = "http://www.btzfcg.gov.cn" + content.xpath(".//a/@href")[0]
        temp = [name, ggstart_time, url]
        # print(temp)
        data.append(temp)
    df = pd.DataFrame(data=data)
    df['info'] = None
    return df


def f2(driver):
    locator = (By.XPATH, "//td[@class='compactToolbar']//select[@name='__ec_pages']/option[last()]")
    WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located(locator))
    total_page = driver.find_element_by_xpath(
        "//td[@class='compactToolbar']//select[@name='__ec_pages']/option[last()]").text
    driver.quit()
    return int(total_page)


data = [
    #
    ["zfcg_zhaobiao_benji_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1660&stockModeIdType=666&ver=2",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_biangeng_benji_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1663&ver=2",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_benji_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=2014&ver=2",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_benji_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555845&ver=2",
     ["name", "ggstart_time", "href", "info"], f1, f2],

    ["zfcg_zhaobiao_danyilaiyuan_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1660&stockModeIdType=60&ver=2",
     ["name", "ggstart_time", "href", "info"], f1, f2],

    ["zfcg_zhaobiao_qixian_donghe_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=2&num=10",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_qingshan_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=3&num=11",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_kunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=4&num=12",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_jiuyuanqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=5&num=13",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_kaifaqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=6&num=14",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_tuyouqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=7&num=15",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_youguaiqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=8&num=16",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_guyangqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=9&num=17",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_baiyunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=10&num=18",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_qixian_damaoqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=666&ver=2&siteId=11&num=19",
     ["name", "ggstart_time", "href", "info"], f1, f2],

    ["zfcg_zhaobiao_danyilaiyuan_qixian_donghe_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=2&num=20",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_qingshan_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=3&num=21",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_kunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=4&num=22",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_jiuyuanqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=5&num=23",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_tuyouqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=7&num=25",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_youguaiqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=8&num=26",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_guyangqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=9&num=27",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_baiyunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=10&num=28",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhaobiao_danyilaiyuan_qixian_damaoqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1662&stockModeIdType=60&qxFlag=1&ver=2&siteId=11&num=29",
     ["name", "ggstart_time", "href", "info"], f1, f2],

    ["zfcg_zhongbiao_qixian_donghe_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=2&num=30",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_qingshan_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=3&num=31",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_kunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=4&num=32",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_jiuyuanqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=5&num=33",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_tuyouqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=7&num=35",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_youguaiqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=8&num=36",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_guyangqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=9&num=37",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_baiyunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=10&num=38",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_zhongbiao_qixian_damaoqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1664&ver=2&siteId=11&num=39",
     ["name", "ggstart_time", "href", "info"], f1, f2],

    ["zfcg_gengzheng_qixian_donghe_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=2&num=40",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_qingshan_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=41",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_kunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=42",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_jiuyuanqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=43",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_kaifaqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=44",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_tuyouqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=45",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_youguaiqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=46",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_guyangqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=47",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_baiyunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=48",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_gengzheng_qixian_damaoqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=1666&ver=2&siteId=3&num=49",
     ["name", "ggstart_time", "href", "info"], f1, f2],

    ["zfcg_yanshou_qixian_donghe_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=2&num=60",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_qingshan_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=3&num=61",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_kunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=4&num=62",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_jiuyuanqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=5&num=63",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_tuyouqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=7&num=65",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_youguaiqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=8&num=66",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_guyangqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=9&num=67",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_baiyunqu_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=10&num=68",
     ["name", "ggstart_time", "href", "info"], f1, f2],
    ["zfcg_yanshou_qixian_damaoqi_gg",
     "http://www.btzfcg.gov.cn/portal/topicView.do?method=view&view=stockBulletin&id=555846&ver=2&siteId=11&num=69",
     ["name", "ggstart_time", "href", "info"], f1, f2],
]


def work(conp, **arg):
    est_meta(conp, data=data, diqu="内蒙古包头市", **arg)
    est_html(conp, f=f3, **arg)


if __name__ == '__main__':
    work(conp=["postgres", "since2015", "192.168.3.171", "anbang", "neimenggu_baotou"])
    # url = "http://czj.dg.gov.cn/dggp/portal/topicView.do?method=view&id=1662"
    # d = webdriver.Chrome()
    # d.get(url)
    # for i in range(1,40):
    #     f1(d, i)
