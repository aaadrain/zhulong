#!/usr/bin/env python
# encoding: utf-8

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
        'start_test_baise',
        default_args=default_args,
        schedule_interval=timedelta(hours=4)
        )
def start_task():
    or_path = os.getcwd()
    os.chdir('/home/zluser/Desktop/airflow')
    os.system('python3 baise.py')
    os.chdir(or_path)

t1=PythonOperator(
        task_id="test_baise",
        python_callable=start_task,
        dag=dag,)

t2=EmailOperator(
        task_id="send_email",
        dag=dag,
        trigger_rule=TriggerRule.ONE_SUCCESS,
        to='414804619@qq.com',
        subject='Baise task is ok',
        html_content='<h3>Hi,dear,your task is Completed Successfully! </h3>\n%s'%time.asctime(time.localtime(time.time())))
t2.set_upstream(t1)


