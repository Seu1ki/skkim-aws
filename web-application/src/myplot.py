import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

import numpy as np

data = np.random.randint(-100, 100, 50).cumsum()
path = '/home/ec2-user/skkim-aws/src/PyscriptServer/pyscript-pwa-example/venv/lib64/python3.7/site-packages/matplotlib/mpl-data/fonts/ttf/NanumGothic.ttf'
font_name = fm.FontProperties(fname=path, size=50).get_name()
print(font_name)
#plt.rc('font', family=font_name)
#plt.rcParams["font.family"] = 'NanumGothic'
plt.plot(range(50), data, 'r')
mpl.rcParams['axes.unicode_minus'] = False
plt.title('시간별 가격 추이')
plt.ylabel('주식 가격')
plt.xlabel('시간(분)')

print ('버전: ', mpl.__version__)
print ('설치 위치: ', mpl.__file__)
print ('설정 위치: ', mpl.get_configdir())
print ('캐시 위치: ', mpl.get_cachedir())
print([(f.name, f.fname) for f in fm.fontManager.ttflist if 'Nanum' in f.name])

plt.show()

