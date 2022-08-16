from bs4 import BeautifulSoup
import requests

#html = requests.get('https://search.naver.com/search.naver?query=날씨')
html = requests.get('https://www.weather.go.kr/weather/observation/currentweather.jsp')
soup = BeautifulSoup(html.text, 'html.parser')
table = soup.find('div',{'class':'over-scroll'})
weather_list=['맑음','흐림','비']
for tr in table.find_all('tr'):
    tds = list(tr.find_all('td'))
    for td in tds:
        if td.find('a'):
            if '서울' == tds[0].text:
            #if '서울' in str(tds):
                for weather in weather_list:
                    #if weather in str(tds):
                    if weather in tds[1].text:
                        print(weather)
                        print(tds[5].text)
                #print(tds[5].text)
