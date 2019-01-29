#!/usr/bin/env python
# encoding: utf-8

import re
import time
import requests
from lxml import etree
# import airflow
# from airflow import DAG
# from datetime import timedelta
# from airflow.operators.email_operator import EmailOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.trigger_rule import TriggerRule
global text
text = 111

def collect_info():

    params = {"wd":"港币汇率"}
    response = requests.get("http://www.baidu.com/s",params=params).content

    body = etree.HTML(response)
    exchange_rate_hk = body.xpath("//div[@class='c-border']//div[@class='op_exrate_result']/div[1]/text()")
    params = {"wd":"深圳天气"}
    response = requests.get("http://www.baidu.com/s",params=params).content
    body = etree.HTML(response)
    weather_sz = body.xpath("//div[@class='op_weather4_twoicon_container_div']/div/a[1]")[0].xpath("string(.)").strip()

    weather_sz =re.sub("\s+"," ",weather_sz)
    response = requests.get("http://www.weather.com.cn/weather1d/101280601.shtml").content.decode("utf-8")
    body_s = etree.HTML(response)
    weather_suggest = body_s.xpath("//div[@class='livezs']/ul[@class='clearfix']")[0].xpath("string(.)").strip()
    weather_suggest =re.sub("\s+"," ",weather_suggest)
    weather_suggest = re.sub("。 ","。\n",weather_suggest)
    message_part_1 = time.asctime(time.localtime(time.time())) + "\n" + "\t" + exchange_rate_hk[0] +"\n"+weather_sz + weather_suggest
    global text
    text = message_part_1
    try:
        with open("/home/zluser/Desktop/INFO/info.txt","w") as f:
            f.write(message_part_1)
    except:
        with open("./info.txt","w") as f:
            f.write(message_part_1)


collect_info()
print(text)



# default_args = {
#     "owner": "anbang",
#     'depends_on_past': False,
#     'start_date': airflow.utils.dates.days_ago(2),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
# }
#
# dag= DAG("inform",default_args=default_args,schedule_interval=timedelta(days=1))
#
# collet_message = PythonOperator(
#     task_id="collet_message",
#     python_callable=collect_info,
#     dag=dag
# )


# send_message = EmailOperator(
#     task_id="send_message",
#     dag=dag,
#     to="anbang_li@163.com",
#     trigger_rule=TriggerRule.ONE_SUCCESS,
#     subject="Have a new day!",
#     html_content="",
#     files=["/home/zluser/Desktop/INFO/info.txt"]
# )
#
# collet_message >> send_message