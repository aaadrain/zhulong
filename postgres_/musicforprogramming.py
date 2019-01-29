import queue
import time
import threading
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import socket
import traceback

socket.setdefaulttimeout(20)

# print(len(content_list))
num_Que = queue.Queue()
Que = queue.Queue()
data = []

url = "http://musicforprogramming.net/"
driver = webdriver.Chrome()
driver.get(url)
locator = (By.XPATH, '//*[@id="episodes"]')
WebDriverWait(driver, 10).until(EC.visibility_of_element_located(locator))

content_list = driver.find_elements_by_xpath('//*[@id="episodes"]/a')
for i in range(1, len(content_list) +1):
    # print(i)
    num_Que.put(i)
driver.quit()

def get_data():

    url = "http://musicforprogramming.net/"
    driver = webdriver.Chrome()
    driver.get(url)
    locator = (By.XPATH, '//*[@id="episodes"]')
    WebDriverWait(driver, 10).until(EC.visibility_of_element_located(locator))
    while not num_Que.empty():
        num = num_Que.get()
        print("num",num)
        try:
            time.sleep(1)
            locator = (By.XPATH, '//*[@id="episodes"]')
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located(locator))
            content = driver.find_element_by_xpath('//*[@id="episodes"]/a[%s]' % num)
            name = content.text.strip()
            content.click()
            href = driver.find_element_by_xpath("//div[@class='pad']/a[1]").get_attribute('href')
            driver.back()
            temp = [name, href]
            data.append(temp)
            Que.put(temp)
            print(temp)
        except Exception as e:
            print(e)
            num_Que.put(num)
    driver.quit()


temp_list = []

def work():
    s_time = time.time()
    while not Que.empty():
        temp = Que.get()
        try:
            name = temp[0].split(":")[1].split('.')[0] + '.mp3'
            url = temp[1]
            if not os.path.exists(name):
                time.sleep(3)
                print(name, "开始下载。")
                with open(name, 'wb') as f:
                    response = requests.get(url)
                    con = response.content
                    f.write(con)
                    response.close()
                e_time = time.time()
                t_time = e_time - s_time
                temp_list.append(temp)
                print(name, "保存完。花了：{:.2f}/Sec".format(t_time))
            else:

                print(name + '--已存在。')
        except Exception as e:
            traceback.print_exc(e)
            Que.put(temp)

for i in data:
    with open("data.txt", 'a') as f:
        print(i)
        f.write(i + '\n')

threads = []
for i in range(10):
    t = threading.Thread(target=get_data)
    threads.append(t)
for i in threads:
    i.start()

for i in threads:
    i.join()


threads = []
for i in range(10):
    t = threading.Thread(target=work)
    threads.append(t)

for i in threads:
    i.start()

for i in threads:
    i.join()
