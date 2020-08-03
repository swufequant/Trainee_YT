import pandas as pd
import datetime
import numpy as np
import os
import random
info=pd.read_csv('//192.168.0.197/data/temp/info.csv',dtype={'code':str})#所有涨停信息包括收益率的记录
#path='000030_2020-02-26_1.csv'
#code_info=pd.read_csv('C:/data/time_collection/bigorder/000030_2020-02-26_1.csv')
time_info=pd.read_csv('C:/data/time.csv')#所有涨停五种破板点的时间记录
url_list=os.listdir('C:/data/time_collection/bigorder')#所有涨停大单量信息的文件夹
length=len(url_list)
sample_num=int(length*0.1)
num=range(length)
random.seed(1)  
code_list=random.sample(num, sample_num)#生成一个占原数据集百分之十的子数据集

def chek(bigorder,threshold,datatime,time,rate):
    datatime=datetime.datetime.strptime(datatime,"%H:%M:%S.%f")
    time=datetime.datetime.strptime(time,"%H:%M:%S.%f")
    if bigorder<=threshold:#大单量小于阈值
        if datatime<time:#采样点时间早于成交时间
            return 0
        else:
            return rate
    else:
        if time.hour>=15:
            return 0
        else:
            return rate

def compute_rate(threshold=0):
    sum_rate=0
    for index,path in enumerate(url_list):
        if index in code_list:
            code=path[0:6]
            date=path[7:17]
            uplimit_times=int(float(path[18:20]))
            code_info=pd.read_csv('C:/data/time_collection/bigorder/'+path)
            datatime,bigorder=code_info.iloc[0,0],code_info.iloc[0,1]
            datatime=datatime[11:]
            time=time_info.iloc[index,1]
            try:
                rate=float(info[(info['code']==code) & (info['date']==date) & (info['min_bigorder']==0) &(info['uplimit_times']==uplimit_times)]['yield_rate'].values)
            except:
                print(code,date,uplimit_times)
            sum_rate+=chek(bigorder,threshold,datatime,time,rate)
    return sum_rate


thre=np.linspace(0,10,11)
list_rate=[]
for threshold in thre:
    newrate=compute_rate(threshold)
    list_rate.append(newrate)
print(list_rate.index(max(list_rate)))