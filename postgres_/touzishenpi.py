import re
import requests
import time

from bs4 import BeautifulSoup
from lxml import etree
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from threading import Semaphore
from zhulong.util.etl import est_meta,est_html
_name_="touzishenpi"



def f4(driver):
    # 点击全国项目
    driver.find_element_by_xpath('//option[@id="03"]').click()

    #筛选起始时间
    driver.find_element_by_xpath('//input[@id="apply_date_begin"]').clear()
    driver.find_element_by_xpath('//input[@id="apply_date_begin"]').send_keys("2000-01-01")
    # driver.save_screenshot("touzi.png")
    driver.find_element_by_xpath('//input[@id="btnQuery"]').click()



def f1(driver,num):
    try:
        driver.find_element_by_xpath('//img[@class="demoIcon"]').click()
        time.sleep(1)
    except:
        pass
    try:
        iframe1 = driver.find_element_by_id("iframepage")
        driver.switch_to.frame(iframe1)
    except:
        pass
    # 获取起始时间
    driver.find_element_by_xpath('//input[@id="apply_date_begin"]').click()

    real_year = int(driver.find_element_by_xpath('//*[@id="apply_date_begin"]').get_attribute("realvalue").split("-")[0])
    if real_year > 2010:
        f4(driver)

    locator = (By.XPATH,'//ul[@style="border-bottom: 0;"]/table/tbody/tr[2]/td[@width="350px"]/a')
    WebDriverWait(driver,20).until(EC.visibility_of_all_elements_located(locator))
    val = driver.find_element_by_xpath('//ul[@style="border-bottom: 0;"]/table/tbody/tr[2]/td[@width="350px"]/a').get_attribute('onclick')[-50:]

    WebDriverWait(driver,20).until(EC.visibility_of_element_located(locator))
    cnum = driver.find_element_by_xpath('//input[@id="pageNum"]').get_attribute('value')

    if int(cnum) != int(num):
        time.sleep(6)
        driver.execute_script("""goPage(%s);"""%num)

        locator = (By.XPATH, '//ul[@style="border-bottom: 0;"]/table/tbody/tr[2]/td[@width="350px"]/a[not(contains(@href,"%s"))]'%val)
        WebDriverWait(driver, 20).until(EC.visibility_of_all_elements_located(locator))
    page = driver.page_source
    body = etree.HTML(page)
    data = []
    content_list = body.xpath('//ul[@style="border-bottom: 0;"]/table/tbody/tr[position()!=1]')
    for content in content_list:
        xiangmu_code = content.xpath("./td[@width='290px']/a/text()")[0].strip().strip("【").strip("】")
        name = content.xpath("./td[@width='350px']/@title")[0]
        ggstart_time = content.xpath("./td[last()]/text()")[0].replace('/','-')
        # print(content.xpath("./td[@width='290px']/a/@onclick"))
        url = "https://www.tzxm.gov.cn:8081" + re.findall(r"'([^']+?)'",content.xpath("./td[@width='290px']/a/@onclick")[0])[0]
        shenpishixiang = content.xpath('./td[@width="270px"]/@title')[0]
        shenpibumen = content.xpath('./td[@width="210px"]/text()')[0]
        status = content.xpath('./td[@width="80px"]/text()')[0]
        temp = [name, ggstart_time, url,xiangmu_code,shenpishixiang,shenpibumen,status]
        data.append(temp)
        # print(temp)
    print('val',val,'num',num)

    df = pd.DataFrame(data=data)
    df["info"]=None
    return df




def f2(driver):
    try:
        # 去弹窗，否则无头模式影响点击查询
        driver.find_element_by_xpath('//img[@class="demoIcon"]').click()
        # print("点击icon")
        time.sleep(1)
    except:
        pass
    try:
        iframe1 = driver.find_element_by_id("iframepage")
        driver.switch_to.frame(iframe1)
    except:
        pass
    locator = (By.XPATH, '//form[@id="serviceForm"]')
    WebDriverWait(driver, 20).until(EC.presence_of_element_located(locator))
    f4(driver)
    total = driver.find_element_by_xpath("//div[@class='qmanu']").text

    total_page = re.findall("共(\d+)页",total.replace(' ',''))[0]
    driver.quit()
    return int(total_page)

def f3(driver, url):
    driver.get(url)
    locator = (By.XPATH, "//div[@id='regmain']")
    WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located(locator))
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

    div = soup.find('div', id='regmain')
    # print(div)
    return div


data =[

    ["quanguoxiangmu_gg",
     "https://www.tzxm.gov.cn:8081/tzxmspweb/tzxmweb/pages/portal/publicinformation/examine_new.jsp",
     ["name", "ggstart_time", "href","xiangmu_code","shenpishixiang","shenpibumen","status", "info"], f1, f2],
]

def work(conp,**arg):
    est_meta(conp, data=data, diqu="全国",**arg)
    est_html(conp, f=f3,**arg)


if __name__ == "__main__":

    work(conp=["postgres", "since2015", "192.168.3.171", "anbang", "touzishenpi"],num=4,headless=False)
    # url = "https://www.tzxm.gov.cn:8081/tzxmspweb/tzxmweb/pages/portal/publicinformation/examine_new.jsp"
    # driver = webdriver.Chrome()
    # driver.get(url)
    # f1(driver,3)
    # f1(driver,8)
    # print(f2(driver))
    # print(f3(driver,"https://www.tzxm.gov.cn:8081/tzxmspweb/portalopenPublicInformation.do?method=queryExamineDetailByUUID&projectuuid=f26175ddae514f27b89625d8efa4454d&item_id=F026DA2CA7BEC8C743B6AC3B39A0B064"))

