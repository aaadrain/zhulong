import os
import sys
import threading
import time
import os
from datetime import datetime,timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


sys.path.append('/home/zluser/Desktop')
default_args = {
        "owner": "anbang",
        'depends_on_past':False,
        'start_date':airflow.utils.dates.days_ago(2),
        'email_on_failure':True,
        'email_on_retry':True,
        'retries':1,
        'retry_delay':timedelta(hours=5),
        }

dag = DAG(
        'start_zfcg_all_task',
        default_args=default_args,
        schedule_interval='@once'
        )


my_tesk_province = ['zfcg','Python_zfcg']
class StartAllTasks(object):
    def __init__(self,path):
        self.path = path
        self.province_city_dict = {}
        self.path_list = []
        self.my_tesk_province = ["zfcg",'Python_zfcg']
        self.province_list = []
        os.chdir(self.path)

    def filter_files(self,olist):
        # 文件过滤
        newlist = []
        if "__init__.py" in olist:
            olist.remove("__init__.py")
        if ".idea" in olist:
            olist.remove(".idea")
        if "__pycache__" in olist:
            olist.remove("__pycache__")
        if "task.py" in olist:
            olist.remove("task.py")
        if "test.sh" in olist:
            olist.remove("test.sh")
        if "zfcgstart.py" in olist:
            olist.remove("zfcgstart.py")
        for o in olist:
            if '.py' in o:
                newlist.append(o)

        return newlist

    def get_city_names(self,path):
        # 获得所有城市名
        self.province_city_dict[path] = []
        city_list_temp = os.listdir(os.getcwd()+'/'+path)
        # print(city_list_temp)
        self.province_city_dict[path] = self.filter_files(city_list_temp)
        self.province_city_dict[path] = self.filter_files(self.province_city_dict[path])
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
        # print(filename)
        os.system("python3 %s" % filename)

    def get_province_names(self):
        temp = os.listdir(os.getcwd())
        for name in temp:
            if name in self.my_tesk_province:
                self.province_list.append(name)
        return self.province_list

    # def init__init__(self):


try:
    os.chdir("/home/zluser/Desktop")
except:
    os.chdir("D:/Python")


start_ = StartAllTasks(os.getcwd())
paths = start_.get_province_names()
# print(paths)
for province in start_.get_province_names():
    start_.get_city_names(province)
# print(start_.province_city_dict)





# print(start_.province_city_dict)
# t1=PythonOperator(task_id="chenzhou",start_date=datetime(2019,1,12,20,10),python_callable=lambda :chenzhou.work(conp=["postgres","since2015","192.168.3.171","lch","hunan_chenzhou"]),dag=dag)
for province in start_.province_city_dict:

    os.chdir(os.path.join(os.getcwd(), province))

    if not os.path.exists('./__init__.py'):
        f=open('./__init__.py' ,'w')
        f.close()
    #print(os.getcwd())
    with open('./__init__.py','r') as f:
        content = f.read()
    for city in start_.province_city_dict[province]:
        city_task_name = city.split('.')[0]
        if city_task_name not in content:
            with open('./__init__.py','a') as f:
                f.write("from %s.%s import work as %s_work\n"%(province,city_task_name,city_task_name))
from zfcg import *

#         try:
#             t = city.rsplit('/', maxsplit=1)[1].split('.')[0]
#         except:
#             t = city.rsplit('\\',maxsplit=1)[1].split('.')[0]
        # print(t)
for province in start_.province_city_dict:
    for city in start_.province_city_dict[province]:
        city = city.split('.')[0]
        city_task_func = city+"_work"
        # print(city)
        conp = ["postgres","since2015","192.168.3.171","anbang",city]
        t=PythonOperator(task_id=city+"_task",python_callable=locals()[city_task_func],op_args=((conp),),dag=dag)

            # t=PythonOperator(
            #     task_id=t+'_task',
            #     python_callable=start_.start_s,
            #     op_args=(city,),
            #     dag=dag,)
            # start_.start_s(city)
# jilin_changchun_work(conp=["postgres","since2015","192.168.3.171",'anbang','jilin_changchun'])
