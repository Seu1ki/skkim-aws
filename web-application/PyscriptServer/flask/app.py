import random
from pathlib import Path
import pymysql

import flask
import bs4
import requests

app = flask.Flask(__name__)


@app.get('/')
def index():
    return flask.render_template('index.html')

@app.get('/weather/get_data')
def weather_get_data():
    naver_weather, naver_temp = get_weather()
    data = get_weather_report(naver_weather,naver_temp)
    conn = pymysql.connect(host=MYSQL_ENDPOINT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE,
            charset='utf8')
    curs = conn.cursor()
    today_weather = data['kor']
    data['today_weather'] = today_weather


    sql = f"SELECT * FROM predict_table WHERE Weather LIKE '{today_weather}'"
    curs.execute(sql)
    rows = curs.fetchall()
    data['predict_table'] = rows

    sql = f"SELECT * FROM increase_table WHERE Weather LIKE '{today_weather}'"
    curs.execute(sql)
    rows = curs.fetchall()
    data['increase_table'] = rows
    
    conn.close()
    return flask.jsonify(data)

@app.get('/weather/get_accum')
def weather_get_accum():
    naver_weather,naver_temp = get_weather()
    data = get_weather_report(naver_weather, naver_temp)

    conn = pymysql.connect(host=MYSQL_ENDPOINT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            db=MYSQL_DATABASE,
            charset='utf8')
    curs = conn.cursor()

    
    #today_weather = "맑음"
    today_weather = data['kor']
    data['today_weather'] = today_weather

    #sql = f"SELECT * FROM accum_table"
    sql = f"SELECT * FROM accum_table WHERE Weather LIKE '{today_weather}'"
    curs.execute(sql)
    rows = curs.fetchall()
    data['accum_table'] = rows

    conn.close()
    return flask.jsonify(data)


@app.get('/serviceWorker.js')
def worker():
    js = Path(__file__).parent / 'static' / 'js' / 'serviceWorker.js'
    text = js.read_text()
    resp = flask.make_response(text)
    resp.content_type = 'application/javascript'
    resp.headers['Service-Worker-Allowed'] = '/'

    return resp


def get_weather_report(weather:str, temp: str) -> dict:
    weather_to_eng = {'맑음':'sunny','비':'rain','흐림':'cloudy'}
    reports = {
            'sky': weather_to_eng[weather],
            'temp': int(float(temp)),
            'kor': weather
    }
    return reports

def get_weather() -> str:
    html = requests.get('https://www.weather.go.kr/weather/observation/currentweather.jsp')
    soup = bs4.BeautifulSoup(html.text, 'html.parser')
    table = soup.find('div',{'class':'over-scroll'})
    weather_list=['맑음','흐림','비']
    alt_weather_list=['구름']
    alt_weather = {'구름':'흐림'}
    ##skkim for test
    return '비','25.0' 
    for tr in table.find_all('tr'):
        tds = list(tr.find_all('td'))
        for td in tds:
            if td.find('a'):
                if '서울' == tds[0].text:
                    for weather in weather_list:
                        if weather in tds[1].text:
                            return weather, tds[5].text
                    for weather in alt_weather_list:
                        if weather in tds[1].text:
                            return alt_weather[weather], tds[5].text
    return '맑음', '30.0'
