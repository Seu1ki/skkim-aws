from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
import os
import requests

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

with DAG(
        dag_id='s3_dag',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        catchup=False,
    ) as dag:
        task_download_from_s3 = PythonOperator(
            task_id='download_from_s3',
            python_callable=download_from_s3,
            op_kwargs={
                'key': 'data-dir/도로교통공단_도로종류별 기상상태별 교통사고 통계_20211231.csv',
                #'key': 'data-dir/function_durations_percentiles.anon.d01.csv',
                'bucket_name': 'skkim-bucket-02',
                'local_path': '../data/'
            }
        )

        task_rename_file = PythonOperator(
            task_id='rename_file',
            python_callable=rename_file,
            op_kwargs={
                'new_name': 'function_durations_percentiles.anon.d01.csv'
            }
        )

        task_upload_to_s3 = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': '../data/function_durations_percentiles.anon.d01.csv',
                'key': 'data-dir/function_durations_percentiles.anon.d01.csv',
                'bucket_name': 'skkim-bucket-02'
            }
        )
       
        task_download_from_s3 >> task_rename_file >> task_upload_to_s3 
