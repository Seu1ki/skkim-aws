import io
import json
# noinspection PyUnresolvedReferences

import pyodide
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib as mpl
import numpy as np

class Report:
    # noinspection PyDefaultArgument
    def __init__(self, data={}):
        self.report_summary: str = data.get('report')
        self.sky: str = data.get('sky')
        self.temp: int = int(data.get('temp', 0))

def plot_db():
    resp: io.StringIO = pyodide.open_url('/weather/get_accum')
    forecast = json.loads(resp.read())
    get_list = forecast['accum_table']

    db_data = pd.DataFrame(get_list, columns = ['Road', 'Weather', 'Accident', 'Death', 'Serious', 'Minor', 'Injured'])

    '''
    path = '/static/images/weather/NanumGothic.ttf'
    font_name = fm.FontProperties(fname=path, size=50).get_name()
    plt.rc('font', family='NanumGothic')
    '''
    #plt.rcParams["font.family"] = 'NanumGothic'
    fig = plt.figure(figsize=(8,5.5))
    fig.subplots_adjust(bottom=0.2,top=0.95)

    
    before=[0]*7
    colors = ['firebrick','coral','orange','gold']
    for idx,i in enumerate(db_data.columns[3:]):
        tmp_y = db_data[i].tolist()
        plt.bar(range(len(tmp_y)),tmp_y,bottom=before, color=colors[idx])
        before = [x+y for x,y in zip(before,tmp_y)]
    import numpy as np
    maxnum = (db_data['Accident'].max()//40000+1)*40000
    #yticklist = np.arange(0,maxnum,step=150000)
    yticklist = np.linspace(0,maxnum,4)
    ytickname = [str(x//1000)+"K" for x in yticklist]



    plt.xticks(np.arange(0,7),['General-national\nRoad','Local Road','Metropolitan-\ncity Road','City Road','Town road','Interstate\nHighway','ETC.'], fontsize=15, rotation=30)
    #plt.xticks(np.arange(0,7),db_data['Road'], fontsize=15, rotation=30)
    #plt.yticks([0,150000,300000,450000],[0,"150K","300K","450K"], fontsize=15)
    plt.yticks(yticklist, ytickname, fontsize=15)
    plt.legend(db_data.columns.tolist()[3:],fontsize=16)

    return fig

def download_report() -> Report:
    resp: io.StringIO = pyodide.open_url('/weather/get_data')
    forecast = json.loads(resp.read())
    today_weather = forecast['today_weather']

    predict_list = forecast['predict_table']
    increase_list = forecast['increase_table']

    predict_data = pd.DataFrame(predict_list, columns = ['Weather','Num','Road','Accident'])
    increase_data = pd.DataFrame(increase_list, columns = ['Weather','Num','Road','Accident'])
    from datetime import datetime
    #today_info = str(datetime.today().strftime("%Y/%m/%d %H:%M:%S"))+"\n"
    #skkim for test
    today_info = str("2022/08/09 11:33:02")+"\n"


    predict_num = predict_data.loc[0,'Num']//365
    predict_road = predict_data.loc[0,'Road']
    predict_Accident = predict_data.loc[0,'Accident']
    predict_out = f"오늘 날씨 {today_weather}에 {predict_road}의 예상 {predict_Accident}: {predict_num}\n"

    increase_num = increase_data.loc[0,'Num']
    increase_road = increase_data.loc[0,'Road']
    increase_Accident = increase_data.loc[0,'Accident']

    increase_out = f"날씨 {today_weather}에 {increase_road}의 {increase_Accident} 전년도 대비 {increase_num} 명 증가"
    forecast['report'] = str(today_info + predict_out + increase_out)
    #forecast['report'] = predict_out

    fig = plt.figure()
    plt.plot([1,2,3],[4,5,6])
#    print("skkim_fig_test",mpld3.fig_to_html(f, figid='THIS_IS_FIGID'))
    return Report(forecast)
