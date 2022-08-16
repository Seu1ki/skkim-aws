from sqlalchemy import create_engine
import pymysql
import pandas as pd
import csv
import requests

#src info
url = 'https://www.data.go.kr/cmm/cmm/fileDownload.do?fileDetailSn=1&atchFileId=FILE_000000002316643&dataNm=%EB%8F%84%EB%A1%9C%EA%B5%90%ED%86%B5%EA%B3%B5%EB%8B%A8_%EB%8F%84%EB%A1%9C%EC%A2%85%EB%A5%98%EB%B3%84_%EA%B8%B0%EC%83%81%EC%83%81%ED%83%9C%EB%B3%84_%EA%B5%90%ED%86%B5%EC%82%AC%EA%B3%A0%282013%29'
outfile = 'L0_file.csv'

#db info
host=MYSQL_ENDPOINT
user=MYSQL_USER
password=MYSQL_PASSWORD
db=MYSQL_DATABASE
     
db_connection_str = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()
table_name = 'accum_sum'

req = requests.get(url)
req.encoding='euc-kr'
#req.encoding='cp949'
#url_content = req.content
url_content = req.content.decode('euc-kr')
csv_file = open(outfile,'wb')
print(url_content)
#save url to csv
csv_file.write(url_content.encode('utf-8'))
csv_file.close()

#save data at db
data = pd.read_csv(outfile, delimiter=',')

data.to_sql(name=table_name, con=db_connection, if_exists='append', index=False)
#data.to_sql(name=table_name, con=db_connection, if_exists='replace', index=False)


