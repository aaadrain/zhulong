import os
import threading
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta



def filter_files(olist):
    cdocu = os.getcwd().rsplit('/',maxsplit=1)[1]
    if cdocu in olist:
        olist.remove(cdocu)
    if "__init__.py" in olist:
        olist.remove("__init__.py")
    if ".idea" in olist:
        olist.remove(".idea")
    if "__pycache__" in olist:
        olist.remove("__pycache__")
    return olist

def get_city_names(path):
    opath = os.getcwd()
    os.chdir(path)
    city_list = os.listdir()
    os.chdir(opath)
    city_list = filter_files(city_list)

    return city_list

def get_province_name():
    pass

def start_tasks(path):
    os_path = os.getcwd()
    threads = []
    os.chdir(path)
    for i in province_city_dict[path]:
        t = threading.Thread(target=start_s,args=(i,))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    os.chdir(os_path)

def start_s(filename):
    os.system("python3 %s"%filename)


if __name__ == "__main__":

    # province_city_dict = {}
    # path_list = []
    # os_path = os.getcwd()
    # p_path = os.path.dirname(os.getcwd())
    # os.chdir("..")
    # p_files = os.listdir()
    # os.chdir(os_path)
    # p_files = filter_files(p_files)

    province = "guangxi"
    try:
        os.chdir("/home/zluser/Desktop/python_"+"/"+province)
    except:
        pass


    default_args ={
        'owner':province,
        'depends_on_pash':False,
        'start_date':"2018/12/21",
        'email_on_failure':True,
        'email_on_retry':True,
        'retries':3,
        'retry_delay':timedelta(hours=4),
    }

    dag = DAG(
        'start_all_tasks',
        default_args=default_args,
        schedule_interval=timedelta(days=1)
    )





    for c_name in p_files:
        new_path = os.path.join(p_path,c_name)
        path_list.append(new_path)


    for i in path_list:
        city_list = get_city_names(i)
        province_city_dict[i]=city_list

#    print(province_city_dict)
    for i in province_city_dict:
#        print(i)
        start_tasks(i)
