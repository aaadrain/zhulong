from PIL import Image
import pytesseract
import os
# print(os.getcwd())
# img = Image.open('1.png')
# text = pytesseract.image_to_string(img,lang="eng")
# print(text)
from selenium import webdriver
url = "http://czj.dg.gov.cn/dggp/portal/topicView.do?method=view&id=51530072"
d = webdriver.Chrome()
d.maximize_window()
d.get(url)
p=3
d.execute_script("javascript:document.forms.topicChrList.topicChrList_p.value='%s';document.forms.topicChrList.setAttribute('action','');document.forms.topicChrList.setAttribute('method','post');document.forms.topicChrList.submit()"%p)
d.find_element_by_xpath('//img[@class="yzmimg y"]')
img = d.find_element_by_xpath("//img[@class='yzmimg y']")

location = img.location
size = img.size
left = location['x']
top = location['y']
right = left + size['width']
bottom = top + size['height']

d.save_screenshot("full_snap.png")

page_snap_obj = Image.open('full_snap.png')
# page_snap_obj.show()
image_obj = page_snap_obj.crop((left,top,right,bottom))
# image_obj.show()
# image = Image.open(image_obj)
image_obj.save("yzm.png")
yzm_img = Image.open('yzm.png')
text = pytesseract.image_to_string(image_obj,lang="eng")

print(text)
# d.quit()