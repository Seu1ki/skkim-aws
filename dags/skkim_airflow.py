from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import requests
import csv

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)

def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('aws_default')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name

def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

def get_mysql():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    res = hook.get_records("""
    SELECT * FROM s3_rds_test WHERE id = 1
     """)
    with open('./data/rds-output.csv','w') as out:
        csv_out = csv.writer(out)
        for row in res:
            csv_out.writerow(row)

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
        dag_id='skkim_airflow',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        catchup=False,
    ) as dag:
        task_download_from_s3 = PythonOperator(
            task_id='download_from_s3',
            python_callable=download_from_s3,
            op_kwargs={
                'key': 'dir-skkim/rds-input.csv',
                'bucket_name': 'skkim-airflow-s3',
                'local_path': './'
            }
        )

        task_rename_file = PythonOperator(
            task_id='rename_file',
            python_callable=rename_file,
            op_kwargs={
                'new_name': './data/rds-input.csv'
            }
        )

        task_upload_to_s3 = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': './data/rds-output.csv',
                'key': 'dir-skkim/skkim-output.csv',
                'bucket_name': 'skkim-airflow-s3'
            }
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

        task_download_from_s3 >> task_rename_file >> task_truncate_mysql >> task_insert_to_mysql >> task_get_from_mysql >> task_upload_to_s3 
        




