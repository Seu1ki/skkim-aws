from datetime import datetime
import pymysql
import pandas as pd
from sqlalchemy import types, create_engine
import records

pymysql.install_as_MySQLdb()
def query_sql():
    try:
        #conn = create_engine('mysql+pymysql://admin:intern19@skkim-db.cshvzopeiwd9.ap-northeast-2.rds.amazonaws.com:3306/skkim_db')
        conn = records.Database('mysql+pymysql://admin:intern19@skkim-db.cshvzopeiwd9.ap-northeast-2.rds.amazonaws.com:3306/skkim_db')
        #data = pd.read_sql_query("select * from accum_sum",conn)
        #data = conn.query("select * from accum_sum",conn)
        print("success")
    except Exception as err:
        print(err)
    return "hello"
print(query_sql())
