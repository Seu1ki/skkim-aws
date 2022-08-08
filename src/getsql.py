import pymysql
import pandas as pd
import matplotlib.pyplot as plt

def weather_data():
    conn = pymysql.connect(host='skkim-db.cshvzopeiwd9.ap-northeast-2.rds.amazonaws.com',
            user='admin',
            password='intern19',
            db='skkim_db',
            charset='utf8')
    today_weather = '맑음'
    curs = conn.cursor()

    sql = f"SELECT * FROM increase_table WHERE Weather LIKE '{today_weather}'"
    curs.execute(sql)
    increase_list = curs.fetchall()

    sql = f"SELECT * FROM predict_table WHERE Weather LIKE '{today_weather}'"
    curs.execute(sql)
    predict_list = curs.fetchall()


    predict_data = pd.DataFrame(predict_list, columns = ['Weather','Num','Road','Accident'])
    increase_data = pd.DataFrame(increase_list, columns = ['Weather','Num','Road','Accident'])
    predict_num = predict_data.loc[0,'Num']
    predict_road = predict_data.loc[0,'Road']
    predict_Accident = predict_data.loc[0,'Accident']
    print(f"올 해 날씨 {today_weather}에 {predict_road}의 예상 {predict_Accident}: {predict_num}")
    #print(increase_data)
    conn.close()


def accum_data():
    conn = pymysql.connect(host='skkim-db.cshvzopeiwd9.ap-northeast-2.rds.amazonaws.com',
            user='admin',
            password='intern19',
            db='skkim_db',
            charset='utf8')
    today_weather = '맑음'

    sql = f"SELECT * FROM accum_table WHERE Weather LIKE '{today_weather}'"
    curs = conn.cursor()
    curs.execute(sql)
    get_list = curs.fetchall()
    db_data = pd.DataFrame(get_list, columns = ['Road', 'Weather', 'Accident', 'Death', 'Serious', 'Minor', 'Injured'])
    db_data.plot.barh()
    plt.show()
    conn.close()

if __name__=="__main__":
    accum_data()
    #weather_data()
