import os
import threading
import time
import os
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime,timedelta

default_args = {
        "owner": "anbang",
        'depends_on_past':False,
        'start_date':airflow.utils.dates.days_ago(2),
        'email_on_failure':True,
        'email_on_retry':True,
        'retries':1,
        'retry_delay':timedelta(minutes=5),
        }

dag = DAG(
        'start_test_four',
        default_args=default_args,
        schedule_interval=timedelta(hours=4)
        )


my_tesk_province = ["neimenggu","guangxi","jiangsu","liaoning"]
class StartAllTasks(object):
    def __init__(self,path):
        self.path = path
        self.province_city_dict = {}
        self.path_list = []
        self.my_tesk_province = ["neimenggu", "guangxi", "jiangsu-李安榜", "liaoning-李安榜"]
        self.province_list = []
        os.chdir(self.path)

    def filter_files(self,olist):
        # cdocu = os.getcwd().rsplit('/', maxsplit=1)[1]
        # if cdocu in olist:
        #     olist.remove(cdocu)
        if "__init__.py" in olist:
            olist.remove("__init__.py")
        if ".idea" in olist:
            olist.remove(".idea")
        if "__pycache__" in olist:
            olist.remove("__pycache__")
        if "task.py" in olist:
            olist.remove("task.py")
        return olist

    def get_city_names(self,path):
        self.province_city_dict[path] = []
        city_list = []
        opath = os.getcwd()
        os.chdir(path)
        city_list_temp = os.listdir()
        npath = os.getcwd()

        os.chdir(opath)
        city_list_temp = self.filter_files(city_list_temp)
        for i in city_list_temp:
            abs_path = os.path.join(npath, i)
            self.province_city_dict[path].append(abs_path)
        return self.province_city_dict

    def start_tasks(self,path):
        os_path = os.getcwd()
        threads = []
        os.chdir(path)
        for i in self.province_city_dict[path]:
            t = threading.Thread(target=self.start_s, args=(i,))
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        os.chdir(os_path)

    def start_s(self,filename):
        print(filename)
        # os.system("python3 %s" % filename)

    def get_province_names(self):
        temp = os.listdir(os.getcwd())
        for name in temp:
            if name in self.my_tesk_province:
                self.province_list.append(name)
        return self.province_list

os.chdir("..")
start_ = StartAllTasks(os.getcwd())
path = start_.get_province_names()[0]
# print(path)
for province in start_.get_province_names():
    start_.get_city_names(province)
# print(start_.province_city_dict)

# for province in start_.province_city_dict:
# for city in start_.province_city_dict["jiangsu"]:
#     try:
#         t = city.rsplit('/', maxsplit=1)[1].split('.')[0]
#     except:
#         t = city.rsplit('\\',maxsplit=1)[1].split('.')[0]
    # print(t)
t1=PythonOperator(
    task_id='t1_task',
    python_callable=start_.start_s,
    op_args=(start_.province_city_dict["jiangsu"][0],),
    dag=dag,)
t2=PythonOperator(
    task_id='t2_task',
    python_callable=start_.start_s,
    op_args=(start_.province_city_dict["jiangsu"][1],),
    dag=dag,)
t3=PythonOperator(
    task_id='t3_task',
    python_callable=start_.start_s,
    op_args=(start_.province_city_dict["jiangsu"][2],),
    dag=dag,)
t4=PythonOperator(
    task_id='t4_task',
    python_callable=start_.start_s,
    op_args=(start_.province_city_dict["jiangsu"][3],),
    dag=dag,)