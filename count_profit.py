import pandas as pd
import datetime
import numpy as np
info=pd.read_csv('//192.168.0.197/data/temp/info.csv',dtype={'code':str})
path='000017_2020-04-03_2.csv'
code_info=pd.read_csv('C:/data/time_collection/bigorder/000017_2020-04-03_2.csv')
time_info=pd.read_csv('C:/data/time.csv')
def chek(bigorder,threshold,datatime,time,rate):
    datatime=datetime.datetime.strptime(datatime,"%H:%M:%S.%f")
    time=datetime.datetime.strptime(time,"%H:%M:%S.%f")
    if bigorder<=threshold:
        if datatime<time:
            return rate
        else:
            return 0
    else:
        return 1
code=path[0:6]
date=path[7:17]
datatime,bigorder=code_info.iloc[0,0],code_info.iloc[0,1]
datatime=datatime[11:]
time=time_info.iloc[0,1]
rate=float(info[(info['code']==code) & (info['date']==date)]['yield_rate'].values)
thre=np.linspace(0,10,11)
for threshold in thre:
    print(chek(bigorder,threshold,datatime,time,rate))