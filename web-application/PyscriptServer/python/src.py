from datetime import datetime
import pymysql
import pandas as pd
from sqlalchemy import types, create_engine
import records

pymysql.install_as_MySQLdb()
def query_sql():
    try:
        host=MYSQL_ENDPOINT
        user=MYSQL_USER
        password=MYSQL_PASSWORD
        db=MYSQL_DATABASE

        db_connection_str = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
        conn = create_engine(db_connection_str)
        #data = pd.read_sql_query("select * from accum_sum",conn)
        #data = conn.query("select * from accum_sum",conn)
        print("success")
    except Exception as err:
        print(err)
    return "hello"
print(query_sql())
