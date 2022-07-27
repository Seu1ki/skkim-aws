from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import os
import requests
import csv
import pandas as pd

local_file = "/home/ec2-user/airflow/data/function_durations_percentiles.anon.d01.csv"
table_name = "skkim_table"
def get_mysql():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    res = hook.get_records("""
    SELECT * FROM s3_rds_test WHERE id = 1
     """)
    with open('./data/rds-output.csv','w') as out:
        csv_out = csv.writer(out)
        for row in res:
            csv_out.writerow(row)
data = pd.read_csv(filename)
for i in data.columns:
    print(i)

def create_mysql():
    data = pd.read_csv(local_file)
    hook = MySqlHook(mysql_conn_id='mysql_default')
    msg = "CREATE TABLE " + table_name + "("
    for i in data.columns:
        msg += (i + " int")
    hook.run(msg)


def insert_mysql():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    hook.run("""
    LOAD DATA LOCAL INFILE './data/rds-input.csv' 
    INTO TABLE s3_rds_test 
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'; 
    """)


def truncate_mysql():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    hook.run("TRUNCATE TABLE s3_rds_test")


with DAG(
        dag_id='rds_dag',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        catchup=False,
    ) as dag:
        task_create_table = PythonOperator(
            task_id='create_table',
            python_callable=create_mysql,
        )
        task_insert_to_mysql = PythonOperator(
            task_id='insert_rds',
            python_callable=insert_mysql,
        )
        task_get_from_mysql = PythonOperator(
            task_id='get_rds',
            python_callable=get_mysql,
        )
        task_truncate_mysql = PythonOperator(
            task_id='truncate_rds',
            python_callable=truncate_mysql,
        )

        task_truncate_mysql >> task_insert_to_mysql >> task_get_from_mysql
        




