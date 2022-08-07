from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.baseoperator import chain
import pandas as pd
import os
import requests
import csv

def empty_function():
    pass
def get_data_from_url(url: str, outfile: str) -> None:
    req = requests.get(url)
    req.encoding='euc-kr'
    url_content = req.content.decode('euc-kr')
    csv_file = open(outfile,'wb')
    csv_file.write(url_content.encode('utf-8'))
    csv_file.close()

def upload_to_s3(ti,dir_name: str, bucket_name: str) -> None:
    file_name = ti.xcom_pull(task_ids=['rename_file'])
    hook = S3Hook('aws_default')
    key = dir_name+file_name[0]
    hook.load_file(filename=file_name[0], key=key, bucket_name=bucket_name, replace=True)

def download_from_s3(dir_name: str, year:str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('aws_default')
    key = dir_name + 'data' + year +'.csv'
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name

def find_filename(ti, year:str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=[f'download_from_s3_{year}'])
    #downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    new_name = 'data' + year + '.csv'
    os.rename(src=downloaded_file_name[0], dst=f"{new_name}")
    #os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

def rename_file(file_name: str, year: str) -> None:
    currentDateTime = datetime.now()
    date = currentDateTime.date()
    #new_name = 'data'+str(date.strftime("%Y"))+'.csv'
    new_name = 'data'+year+'.csv'
    os.rename(src=file_name, dst=new_name)
    return new_name

def get_mysql():
    hook = MySqlHook(mysql_conn_id='mysql_default')
    res = hook.get_records("""
    SELECT * FROM s3_rds_test WHERE id = 1
     """)
    with open('./data/rds-output.csv','w') as out:
        csv_out = csv.writer(out)
        for row in res:
            csv_out.writerow(row)

def create_mysql(table_name: str, table_col: str):
    hook = MySqlHook(mysql_conn_id='mysql_default')
    msg = f"CREATE TABLE if not exists {table_name} " + table_col
    hook.run(msg)

def truncate_mysql(table_name: str):
    hook = MySqlHook(mysql_conn_id='mysql_default')
    hook.run(f"TRUNCATE TABLE {table_name}")

def sync_index(year: str):
    new_data = pd.read_csv('data'+year+'.csv', delimiter=',',encoding='utf-8')
    last_data = pd.read_csv('data'+str(int(year)-1)+'.csv', delimiter=',',encoding='utf-8')
    last_idx = []
    for idx in last_data.columns:
        last_idx.append(idx)
    for idx in new_data.columns:
        change_flag = 0
        for last in last_idx:
            if idx in last or last in idx:
                new_data.rename(columns={idx:last}, inplace=True)
                change_flag = 1
                break
        if change_flag is 0:
            new_data.drop(labels=idx, axis=1, inplace=True)
    return new_data, last_data

def calculate_increase(ti):
    new_data, last_data = ti.xcom_pull(task_ids=['sync_index'])[0]
    res_data = pd.concat([new_data.iloc[:,0:2],new_data.iloc[:,2:] - last_data.iloc[:,2:]],axis=1)
    return res_data

def predict_risk(ti, year:str):
    # 5-year: 2017, 2018, 2019, 2020, 2021
    new_data, last_data = ti.xcom_pull(task_ids=['sync_index'])[0]
    past_data = []
    for i in [4,3,2]:
        past_data.append(pd.read_csv('data'+str(int(year)-i)+'.csv', delimiter=',',encoding='utf-8'))
    past_data.append(last_data)
    past_data.append(new_data)
    return new_data #TODO return predict dataframe

def load_accumulate(table_name: str):
    hook = MySqlHook(mysql_conn_id='mysql_default')
    sql_query = f"SELECT * FROM {table_name}"
    return hook.get_pandas_df(sql_query)

def calculate_accumulate(ti):
    accum_data = ti.xcom_pull(task_ids=['load_accumulate'])[0]
    new_data, last_data = ti.xcom_pull(task_ids=['sync_index'])[0]
    for kor,eng in zip(new_data.columns,['Road', 'Weather', 'Accident', 'Death', 'Serious', 'Minor', 'Injured']):
        new_data.rename(columns={kor:eng}, inplace=True)
    print("seulki debug acc", accum_data)
    print("seulki debug new", new_data)
    if accum_data.empty:
        return new_data
    else:
        res_data = pd.concat([new_data.iloc[:,0:2],new_data.iloc[:,2:] + accum_data.iloc[:,2:]],axis=1)
        print("seulki debug sum",res_data)
        return res_data

def update_accumulate(ti, table_name: str):
    data = ti.xcom_pull(task_ids=['calculate_accumulate'])[0]
    hook = MySqlHook(mysql_conn_id='mysql_default')
    for idx, row in data.iterrows():
        sql_query = f"INSERT INTO {table_name} (Road, Weather, Accident, Death, Serious, Minor, Injured) VALUES " + str(tuple(row.values))
        print("seulki debug" , sql_query)
        hook.run(sql_query)



def select_risk(ti, pre_task: str):
    data = ti.xcom_pull(task_ids=[pre_task])[0]
    road = data.columns[0]
    column = data.columns[1]
    results=[]
    for weather in set(data.iloc[:,1]):
        con = data[column] == weather
        filtered_df = data.loc[con,:]
        tmp = filtered_df.iloc[:,3:].max() #사망자수, ... , 부상신고자수
        max_idx = filtered_df[tmp.idxmax()].argmax()
        results.append([weather, tmp.max(), data.loc[max_idx, road], tmp.idxmax()])
    res = pd.DataFrame(results, columns=['Weather','Num','Road','Accident'])
    return res

def insert_mysql(ti, table_name: str):
    if table_name == "increase_table":
        data = ti.xcom_pull(task_ids=['select_increase_risk'])[0]
    elif table_name == "predict_table":
        data = ti.xcom_pull(task_ids=['select_predict_risk'])[0]

    hook = MySqlHook(mysql_conn_id='mysql_default')
    
    for idx, row in data.iterrows():
        sql_query = f"INSERT INTO {table_name} (Weather, Num, Road, Accident) VALUES " + str(tuple(row.values))
        print("seulki debug" , sql_query)
        hook.run(sql_query)



with DAG(
        dag_id='skkim_airflow',
        schedule_interval='@daily',
        start_date=datetime(2022, 8, 5),
        catchup=False,
    ) as dag:

        currentDateTime = datetime.now()
        date = currentDateTime.date()
        year = str(int(date.strftime("%Y"))-1)

        task_download_from_url = PythonOperator(
            task_id='get_data_from_url',
            python_callable=get_data_from_url,
            op_kwargs={
                'outfile': './new_l0.csv',
                'url': 'https://www.data.go.kr/cmm/cmm/fileDownload.do?fileDetailSn=1&atchFileId=FILE_000000002316643&dataNm=%EB%8F%84%EB%A1%9C%EA%B5%90%ED%86%B5%EA%B3%B5%EB%8B%A8_%EB%8F%84%EB%A1%9C%EC%A2%85%EB%A5%98%EB%B3%84_%EA%B8%B0%EC%83%81%EC%83%81%ED%83%9C%EB%B3%84_%EA%B5%90%ED%86%B5%EC%82%AC%EA%B3%A0%282013%29'
            }
        )

        task_rename_file = PythonOperator(
            task_id='rename_file',
            python_callable=rename_file,
            op_kwargs={
                'file_name': 'new_l0.csv',
                'year':year
            }
        )

        task_upload_to_s3 = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'dir_name': 'data-dir/',
                'bucket_name': 'skkim-bucket-02'
            }
        )
 
        download_task_list = []
        rename_task_list = []

        for i in range(1,5):
            past_year = str(int(year)-i)

            task_download_from_s3 = PythonOperator(
                task_id=f'download_from_s3_{past_year}',
                python_callable=download_from_s3,
                op_kwargs={
                    'dir_name': 'data-dir/',
                    'year': past_year,
                    'bucket_name': 'skkim-bucket-02',
                    'local_path': './'
                }
            )
            task_find_filename = PythonOperator(
                task_id=f'find_filename_{past_year}',
                python_callable=find_filename,
                op_kwargs={
                    'year':past_year
                }
            )
            download_task_list.append(task_download_from_s3)
            rename_task_list.append(task_find_filename)

        start_task = PythonOperator(task_id="start_task", python_callable=empty_function)
        end_get_task = PythonOperator(task_id="end_get_task", python_callable=empty_function)
        start_db_task = PythonOperator(task_id="start_db_task", python_callable=empty_function)
        end_db_task = PythonOperator(task_id="end_db_task", python_callable=empty_function)

        task_sync_index = PythonOperator(
            task_id='sync_index',
            python_callable=sync_index,
            op_kwargs={
                'year': year
            }
        )

        task_cal_increase = PythonOperator(
            task_id='calculate_increase',
            python_callable=calculate_increase,
        )
        task_predict = PythonOperator(
            task_id='predict_risk',
            python_callable=predict_risk,
            op_kwargs={
                'year': year
            }
        )
        task_select_increase = PythonOperator(
            task_id='select_increase_risk',
            python_callable=select_risk,
            op_kwargs={
                'pre_task': 'calculate_increase',
            }
        )
        task_select_predict = PythonOperator(
            task_id='select_predict_risk',
            python_callable=select_risk,
            op_kwargs={
                'pre_task': 'predict_risk',
            }
        )
        task_create_increase = PythonOperator(
            task_id='create_increase_table',
            python_callable=create_mysql,
            op_kwargs={
                'table_name': 'increase_table',
                'table_col': '(Weather text, Num int, Road text, Accident text)'
            }
        )
        task_insert_to_increase = PythonOperator(
            task_id='insert_increase',
            python_callable=insert_mysql,
            op_kwargs={
                'table_name': 'increase_table'
            }
        )
        task_create_predict = PythonOperator(
            task_id='create_predict_table',
            python_callable=create_mysql,
            op_kwargs={
                'table_name': 'predict_table',
                'table_col': '(Weather text, Num int, Road text, Accident text)'
            }
        )
        task_insert_to_predict = PythonOperator(
            task_id='insert_predict',
            python_callable=insert_mysql,
            op_kwargs={
                'table_name': 'predict_table'
            }
        )
        task_init_increase = PythonOperator(
            task_id='init_increase',
            python_callable=truncate_mysql,
            op_kwargs={
                'table_name': 'increase_table'
            }
        )
        task_init_predict = PythonOperator(
            task_id='init_predict',
            python_callable=truncate_mysql,
            op_kwargs={
                'table_name': 'predict_table'
            }
        )
        task_check_table = PythonOperator(
            task_id='check_accumulate_table',
            python_callable=create_mysql,
            op_kwargs={
                'table_name': 'accum_table',
                'table_col': '(Road text, Weather text, Accident int, Death int, Serious int, Minor int, Injured int )'
            }
        )
        task_load_accumulate = PythonOperator(
            task_id='load_accumulate',
            python_callable=load_accumulate,
            op_kwargs={
                'table_name': 'accum_table',
            }
        )
        task_calculate_accumulate = PythonOperator(
            task_id='calculate_accumulate',
            python_callable=calculate_accumulate,
        )

        task_init_accumulate = PythonOperator(
            task_id='init_accumulate',
            python_callable=truncate_mysql,
            op_kwargs={
                'table_name': 'accum_table'
            }
        )
        task_update_accumulate = PythonOperator(
            task_id='update_accumulate',
            python_callable=update_accumulate,
            op_kwargs={
                'table_name': 'accum_table',
            }
        )
        chain(start_task, task_download_from_url,task_rename_file, task_upload_to_s3,end_get_task)
        chain(start_task, download_task_list, rename_task_list, end_get_task)
        chain(end_get_task, task_sync_index, start_db_task)
        chain(start_db_task, task_cal_increase, task_select_increase, task_create_increase, task_init_increase, task_insert_to_increase, end_db_task)
        chain(start_db_task, task_predict, task_select_predict, task_create_predict, task_init_predict, task_insert_to_predict, end_db_task)
        chain(start_db_task, task_check_table, task_load_accumulate, task_calculate_accumulate, task_init_accumulate, task_update_accumulate, end_db_task)


        '''
        task_insert_to_mysql = PythonOperator(
            task_id='insert_rds',
            python_callable=insert_mysql,
        )
        task_get_from_mysql = PythonOperator(
            task_id='get_rds',
            python_callable=get_mysql,
        )
        '''

        #task_download_from_s3 >> task_update_accum_mysql
        #>> task_find_filename
        #task_truncate_mysql >> task_insert_to_mysql >> task_get_from_mysql >> task_upload_to_s3



