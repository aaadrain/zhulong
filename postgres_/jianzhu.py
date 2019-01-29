import random
from collections import OrderedDict

import pandas as pd
import re

import requests
from lxml import etree
from selenium import webdriver
from bs4 import BeautifulSoup
from lmf.dbv2 import db_write
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json

import time

from zhulong.util.etl import est_html, est_meta, add_info

from fake_useragent import agents

_name_ = "guangzhou"

total = 0
tt_url = None
tt = None
payloadData = None
def f1(driver, num):
    print(num)
    user_agent = random.choice(agents)
    headers = {
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'User-Agent': user_agent,
        }
    data_dict = {
        "$total": total,
        "$pgsz": 15,
        "$reload": 0,
        "$pg": num,
    }
    Datas = {**data_dict, **payloadData}
    # 下载超时
    timeOut = 25
    sesion = requests.session()
    time.sleep(random.uniform(1, 3))
    res = sesion.post(url=tt_url, headers=headers, data=Datas, timeout=timeOut)
    # 需要判断是否为登录后的页面
    if res.status_code == 200:
        cnn = 0
        html = res.text
        html_data = BeautifulSoup(html, 'html.parser')
        tbody = html_data.find('tbody', class_='cursorDefault')
        trs = tbody.find_all('tr')
        data = []
        for tr in trs:
            try:
                td = tr.find('td', class_='text-left complist-num').text.strip()
            except:
                td = '-'
            title = tr.find('td', class_='text-left primary').text.strip()
            href = tr.find('a')['href'].strip()
            link = 'http://jzsc.mohurd.gov.cn'+href
            try:
                person = tr.find_all('td')[-2].text.strip()
            except:
                person = '-'
            try:
                place = tr.find_all('td')[-1].text.strip()
            except:
                place = '-'
            tmp = [td, title, link, person, place]
            data.append(tmp)

        df = pd.DataFrame(data=data)
        df['info'] = None
        return df


def f2(driver):
    """
    tt_url：http://jzsc.mohurd.gov.cn/dataservice/query/comp/list
    tt:资质类别(例如QY_ZZ_ZZZD_003)
    region：省市与对应的id
    :param driver:
    :return:
    """
    global tt_url,tt
    tt_url = None
    tt = None
    start_url = driver.current_url

    tt_url = start_url.rsplit('/', maxsplit=2)[0]
    tt = start_url.rsplit('/', maxsplit=2)[1]
    regions = start_url.rsplit('/', maxsplit=2)[2]

    qy_region = re.findall(r'qy_region=(.*),', regions)[0]
    qy_reg_addr = re.findall(r'qy_reg_addr=(.*)', regions)[0]
    region = {'qy_region':qy_region,'qy_reg_addr':qy_reg_addr}

    # 按省市获取资质类别种类
    data_dict = get_dict(driver, region)
    print(data_dict)
    # 考虑怎么获取总页数，测试取第一个
    num = get_pageall(tt_url, tt, data_dict[0])
    if num != None:
        df = f1(driver, num+1)
        driver.quit()
        return df


def get_pageall(tt_url, tt, data_dict):
    global total,payloadData
    total = 0
    payloadData = None
    user_agent = random.choice(agents)
    headers = {
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'User-Agent': user_agent,
       }
    Data_dict = {
        "qy_type": tt,
        "qy_name": "",
        "qy_code": "",
        "apt_certno": "",
        "qy_fr_name": "",
        "qy_gljg": "",
    }
    payloadData = {**Data_dict, **data_dict}
    # 下载超时
    timeOut = 25
    sesion = requests.session()
    time.sleep(random.uniform(1, 3))
    res = sesion.post(url=tt_url, headers=headers, data=payloadData, timeout=timeOut)
    # 需要判断是否为登录后的页面
    if res.status_code == 200:
        html = res.text
        num = re.findall(r'sf:data="(.*)"', html)[0]
        total = re.findall(r'tt:(\d+),', num)[0]
        total = int(total)
        if total / 15 == int(total / 15):
            page_all = int(total / 15)
        else:
            page_all = int(total / 15) + 1
        return page_all
    else:
        return None


def get_dict(driver, region):
    kinds = get_kind(driver, tt)
    time.sleep(random.uniform(1, 2))
    data_list = []
    for w1 in kinds:
        if type(region).__name__ != 'dict':
            region = eval(region)
        elif type(w1).__name__ != 'dict':
            w1 = eval(w1)
        d_dict = {**region, **w1}
        data_list.append(d_dict)

    return data_list


def get_kind(driver, kurl):
    """
    获取资质类别
    :param driver:
    :param kurl:
    :return:
    """
    kind_url = 'http://jzsc.mohurd.gov.cn/asite/qualapt/aptData?apt_type={}'.format(kurl)
    driver.get(kind_url)
    locator = (By.XPATH, "//tr[@class='data_row']")
    WebDriverWait(driver, 20).until(EC.presence_of_element_located(locator))
    dt_total = int(re.findall(r'"\$total":(\d+),', driver.page_source)[0])
    try:
        dt_num = int(driver.find_element_by_xpath("//div[@class='quotes']/a[last()]").get_attribute('dt'))
    except:
        dt_num = 1
    apt_data = []
    for dt in range(1, dt_num+1):
        user_agent = random.choice(agents)
        headers = {
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'User-Agent': user_agent,
            }
        payloadData = {
            "$total": dt_total,
            "$reload": 0,
            "$pg": dt,
            "$pgsz": 10,
        }
        # 下载超时
        timeOut = 25
        sesion = requests.session()
        time.sleep(random.uniform(1, 3))
        res = sesion.post(url=kind_url, headers=headers, data=payloadData, timeout=timeOut)
        # 需要判断是否为登录后的页面
        if res.status_code == 200:
            html = res.text
            soup = BeautifulSoup(html, 'html.parser')
            trs = soup.find_all('tr', class_='data_row')
            for tr in trs:
                apt = tr.find('input', class_='icheck')['value']
                # {"apt_code":"B20302B", "apt_scope":"工程勘察岩土工程专业（岩土工程设计）乙级"}
                apt_data.append(apt)
    return apt_data


def f3(driver, url):
    # driver.get(url)
    if '对不起，未查询到任何企业数据' in driver.page_source:
        return None
    try:
        locator = (By.XPATH, "//div[@class='user_info spmtop']")
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located(locator))
    except:
        if '对不起，未查询到任何企业数据' in driver.page_source:
            return None
    time.sleep(random.uniform(1, 3))
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

    div = soup.find('div', class_='plr')
    # div=div.find_all('div',class_='ewb-article')[0]

    return div


def get_region():
    """
    获取行政区域划分
    :return:
    """
    region_url = 'http://jzsc.mohurd.gov.cn/asite/region/index'
    user_agent = random.choice(agents)
    headers = {
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'User-Agent': user_agent,
    }
    # 下载超时
    timeOut = 25
    sesion = requests.session()
    time.sleep(random.uniform(1, 2))
    res = sesion.post(url=region_url, headers=headers, timeout=timeOut)
    # 需要判断是否为登录后的页面
    if res.status_code == 200:
        html = res.text
        html_data = json.loads(html)
        provinces = html_data['json']['category']['provinces']
        datas = []
        for province in provinces:
            pdict = {}
            pdict["qy_reg_addr"] = province["region_fullname"]
            pdict["qy_region"] = province["region_id"]
            datas.append(pdict)
        return datas


def get_data():
    data=[]
    regions = get_region()
    zztype = OrderedDict([("kancha", "QY_ZZ_ZZZD_003"), ("sheji", "QY_ZZ_ZZZD_004"), ("jianzhu", "QY_ZZ_ZZZD_001"), ("jianli", "QY_ZZ_ZZZD_002"),
                          ("zbdl", "QY_ZZ_ZZZD_006"), ("sjsg", "QY_ZZ_ZZZD_005"), ("zjzx", "QY_ZZ_ZZZD_007")])

    for w1 in regions:
        for w2 in zztype.keys():
            p1 = "%s" % (zztype[w2])
            p2 = "%s" % (w1['qy_region'])
            href="http://jzsc.mohurd.gov.cn/dataservice/query/comp/list/%s/qy_region=%s,qy_reg_addr=%s" % (p1,p2,w1['qy_reg_addr'])
            tmp=["cgjs_%s_dq%s" % (w2, w1['qy_region']), href, ["td", "name", "href", "person", "place", "info"], f1, f2]
            data.append(tmp)

    data1=data.copy()
    # for w in data:
    #     if w[0] in remove_arr:data1.remove(w)
    return data1

data=get_data()


def work(conp, **args):
    est_meta(conp, data=data, diqu="jianzhu", **args)
    est_html(conp, f=f3, **args)


if __name__ == '__main__':
    work(conp=["postgres", "since2015", "192.168.3.171", "guoziqiang", "guangzhou"],pageloadtimeout=60,pageLoadStrategy="none")


    # driver=webdriver.Chrome()
    # url = "http://jzsc.mohurd.gov.cn/dataservice/query/comp/list/QY_ZZ_ZZZD_003/qy_region=110000,qy_reg_addr=北京市"
    # driver.get(url)
    # df=f2(driver)
    # print(df.values)
    # driver = webdriver.Chrome()
    # url = "http://jzsc.mohurd.gov.cn/dataservice/query/comp/list/QY_ZZ_ZZZD_003"
    # driver.get(url)
    # for i in range(1, 3):
    #     df = f1(driver, i)
    #     print(df.values)
