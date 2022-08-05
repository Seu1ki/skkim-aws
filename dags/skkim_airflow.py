from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import requests
import csv

def get_data_from_url(url: str, outfile: str) -> None:
    req = requests.get(url)
    req.encoding='euc-kr'
    url_content = req.content.decode('euc-kr')
    csv_file = open(outfile,'wb')
    csv_file.write(url_content.encode('utf-8'))
    csv_file.close()

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
        dag_id='skkim_airflow',
        schedule_interval='@daily',
        start_date=datetime(2022, 3, 1),
        catchup=False,
    ) as dag:
        task_download_from_url = PythonOperator(
            task_id='get_data_from_url',
            python_callable=get_data_from_url,
            op_kwargs={
                'filename': './new_l0.csv',
                'url': 'https://www.data.go.kr/cmm/cmm/fileDownload.do?fileDetailSn=1&atchFileId=FILE_000000002316643&dataNm=%EB%8F%84%EB%A1%9C%EA%B5%90%ED%86%B5%EA%B3%B5%EB%8B%A8_%EB%8F%84%EB%A1%9C%EC%A2%85%EB%A5%98%EB%B3%84_%EA%B8%B0%EC%83%81%EC%83%81%ED%83%9C%EB%B3%84_%EA%B5%90%ED%86%B5%EC%82%AC%EA%B3%A0%282013%29'
            }
        )


        task_download_from_s3 = PythonOperator(
            task_id='download_from_s3',
            python_callable=download_from_s3,
            op_kwargs={
                'key': 'data-dir/function_durations_percentiles.anon.d01.csv',
                'bucket_name': 'skkim-bucket-02',
                'local_path': './'
            }
        )

        task_rename_file = PythonOperator(
            task_id='rename_file',
            python_callable=rename_file,
            op_kwargs={
                'new_name': './function_durations_percentiles.anon.d01.csv'
            }
        )

        task_upload_to_s3 = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': './function_durations_percentiles.anon.d01.csv',
                'key': 'data-dir/function_durations_percentiles.anon.d01.csv',
                'bucket_name': 'skkim-bucket-02'
            }
        )
 
        task_create_mysql = PythonOperator(
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

        task_dwonload_from_url
        #task_download_from_s3 >> task_rename_file >> task_truncate_mysql >> task_insert_to_mysql >> task_get_from_mysql >> task_upload_to_s3



