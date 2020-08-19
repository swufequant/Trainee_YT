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
code_set=set(random.sample(num, sample_num))#生成一个占原数据集百分之十的子数据集

def chek(bigorder,threshold,datatime,time,rate):
    try:
        datatime=datetime.datetime.strptime(datatime,"%H:%M:%S.%f")
    except:
        datatime=datetime.datetime.strptime(datatime,"%H:%M:%S")
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

def compute_rate(threshold=1,buy_time=1,i=0):
    sum_rate=0
    for index,path in enumerate(url_list):
        if index in code_set:
            code=path[0:6]
            date=path[7:17]
            uplimit_times=int(float(path[18:20]))
            code_info=pd.read_csv('C:/data/time_collection/bigorder/'+path)
            try:
                datatime,bigorder=code_info.iloc[i,2*buy_time-2],code_info.iloc[i,2*buy_time-1]
                datatime=datatime[11:]
            except:
                continue
            time=time_info.iloc[index,buy_time]
            try:
                rate=float(info[(info['code']==code) & (info['date']==date) & (info['min_bigorder']==0) &(info['uplimit_times']==uplimit_times)]['yield_rate'].values)
            except:
                print(code,date,uplimit_times)
                continue
            sum_rate+=chek(bigorder,threshold,datatime,time,rate)
    return sum_rate

time_start=datetime.datetime.now()
buy_time=1
thre=[0 for i in range(20)]
list_rate=[]
for i in range(20):
    newrate=compute_rate(threshold=5,buy_time=buy_time,i=i)
    list_rate.append(newrate)
maxindex=list_rate.index(max(list_rate))
print(maxindex)
print(list_rate)
thre[0:maxindex+1]=[5 for i in range(maxindex+1)]

list_rate1=[]
for i in range(20):
    if i<=maxindex:
        newrate=list_rate[i]
        list_rate1.append(newrate)
    else:
        newrate=compute_rate(threshold=4,buy_time=buy_time,i=i)
        list_rate1.append(newrate)
maxindex1=list_rate1.index(max(list_rate1))
print(maxindex1)
print(list_rate1)
thre[maxindex+1:maxindex1+1]=[4 for i in range(maxindex1-maxindex)]

list_rate2=[]
for i in range(20):
    if i<=maxindex:
        newrate=list_rate[i]
        list_rate2.append(newrate)
    elif i>maxindex and i<=maxindex1:
        newrate=list_rate1[i]
        list_rate2.append(newrate)
    else:
        newrate=compute_rate(threshold=3,buy_time=buy_time,i=i)
        list_rate2.append(newrate)
maxindex2=list_rate2.index(max(list_rate2))
print(maxindex2)
print(list_rate2)
thre[maxindex1+1:maxindex2+1]=[3 for i in range(maxindex2-maxindex1)]

list_rate3=[]
for i in range(20):
    if i<=maxindex:
        newrate=list_rate[i]
        list_rate3.append(newrate)
    elif i>maxindex and i<=maxindex1:
        newrate=list_rate1[i]
        list_rate3.append(newrate)
    elif i>maxindex1 and i<=maxindex2:
        newrate=list_rate2[i]
        list_rate3.append(newrate)
    else:
        newrate=compute_rate(threshold=2,buy_time=buy_time,i=i)
        list_rate3.append(newrate)
maxindex3=list_rate3.index(max(list_rate3))
print(maxindex3)
print(list_rate3)
thre[maxindex2+1:maxindex3+1]=[2 for i in range(maxindex3-maxindex2)]

list_rate4=[]
for i in range(20):
    if i<=maxindex:
        newrate=list_rate[i]
        list_rate4.append(newrate)
    elif i>maxindex and i<=maxindex1:
        newrate=list_rate1[i]
        list_rate4.append(newrate)
    elif i>maxindex1 and i<=maxindex2:
        newrate=list_rate2[i]
        list_rate4.append(newrate)
    elif i>maxindex2 and i<=maxindex3:
        newrate=list_rate3[i]
        list_rate4.append(newrate)
    else:
        newrate=compute_rate(threshold=1,buy_time=buy_time,i=i)
        list_rate4.append(newrate)
maxindex4=list_rate4.index(max(list_rate4))
print(maxindex4)
print(list_rate4)
thre[maxindex3+1:maxindex4+1]=[1 for i in range(maxindex4-maxindex3)]
print(thre)
time_end=datetime.datetime.now()
print(time_end-time_start)