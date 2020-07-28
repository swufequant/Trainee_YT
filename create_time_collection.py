import FakeBreakB1amt_to_csv
import pandas as pd
import os
import time
import datetime
import numpy as np

col=['datatime','b1vol','b1amt','bigorder','b1vol2','b1amt2','bigorder2','b1vol3','b1amt3','bigorder3',
'b1vol4','b1amt4','bigorder4','b1vol5','b1amt5','bigorder5','b1vol6','b1amt6','bigorder6']


pot=np.linspace(-2.3025850929940455,7.495541943884256,30)
time_collection=np.exp(pot)

#print(time_collection)

url=os.listdir('C:/data/B1amt') 
#path=url[4]
#df=pd.read_csv('//192.168.0.197/data/temp/000629_2018-08-24_14737.csv')

def findtime(n,df):
    k=0
    #cur_time=0
    for idx in df.index:
        if (df.loc[idx,'bigorder6']==2 or df.loc[idx,'bigorder6']==1) and k ==0:
            k=1
        if df.loc[idx,'bigorder6']==0 and k ==1:
            k=2
        if df.loc[idx,'bigorder6']>=n and k ==2:
            cur_time=df.loc[idx,'datatime']
            return cur_time,idx
        
    return cur_time,idx

def get_n_time(n,df):
    datatime=[]
    b1amt=[]
    bigorder=[]
    for i in range(20):
        curtime,idx=findtime(n,df)
        curtime=datetime.datetime.strptime(curtime,"%Y-%m-%d %H:%M:%S.%f")
        end_time=curtime+datetime.timedelta(seconds=time_collection[i])
        while curtime<=end_time:
            idx +=1
            if idx in df.index:
                curtime=df.loc[idx,'datatime']
                curtime=datetime.datetime.strptime(curtime,"%Y-%m-%d %H:%M:%S.%f")
            else:
                b1amt.append(-1)
                bigorder.append(-1)
                datatime.append(end_time)
                return  datatime,b1amt,bigorder
        idx -=1
        curtime=df.loc[idx,'datatime']
        curtime=datetime.datetime.strptime(curtime,"%Y-%m-%d %H:%M:%S.%f")
        b1=df.loc[idx,col[3*n-1]]
        big_or=df.loc[idx,col[3*n]]
        datatime.append(curtime)
        b1amt.append(b1)
        bigorder.append(big_or)

    return datatime,b1amt,bigorder

def main(url):
    for index,path in enumerate(url):
        
        df=pd.read_csv('C:/data/B1amt/'+path)
        try:
            datatime1,b1amt1,bigorder1=get_n_time(1,df)
        except:
            datatime1,b1amt1,bigorder1=[],[],[]
        try:
            datatime2,b1amt2,bigorder2=get_n_time(2,df)
        except:
            datatime2,b1amt2,bigorder2=[],[],[]
        try:
            datatime3,b1amt3,bigorder3=get_n_time(3,df)
        except:
            datatime3,b1amt3,bigorder3=[],[],[]
        try:
            datatime4,b1amt4,bigorder4=get_n_time(4,df)
        except:
            datatime4,b1amt4,bigorder4=[],[],[]
        try:
            datatime5,b1amt5,bigorder5=get_n_time(5,df)
        except:
            datatime5,b1amt5,bigorder5=[],[],[]


        try :
            b1amt_info=pd.DataFrame({'datatime1':datatime1,'b1amt1':b1amt1,'datatime2':datatime2,'b1amt2':b1amt2,
            'datatime3':datatime3,'b1amt3':b1amt3,'datatime4':datatime4,'b1amt4':b1amt4,
            'datatime5':datatime5,'b1amt5':b1amt5})
            bigorder_info=pd.DataFrame({'datatime1':datatime1,'bigorder1':bigorder1,
            'datatime2':datatime2,'bigorder2':bigorder2,'datatime3':datatime3,'bigorder3':bigorder3,
            'datatime4':datatime4,'bigorder4':bigorder4,'datatime5':datatime5,'bigorder5':bigorder5})
        except ValueError:
            b1amt_info=pd.DataFrame.from_dict({'datatime1':datatime1,'b1amt1':b1amt1,'datatime2':datatime2,'b1amt2':b1amt2,
            'datatime3':datatime3,'b1amt3':b1amt3,'datatime4':datatime4,'b1amt4':b1amt4,
            'datatime5':datatime5,'b1amt5':b1amt5},orient='index')
            bigorder_info=pd.DataFrame.from_dict({'datatime1':datatime1,'bigorder1':bigorder1,
            'datatime2':datatime2,'bigorder2':bigorder2,'datatime3':datatime3,'bigorder3':bigorder3,
            'datatime4':datatime4,'bigorder4':bigorder4,'datatime5':datatime5,'bigorder5':bigorder5},orient='index')
            b1amt_info = pd.DataFrame(b1amt_info.values.T, index=b1amt_info.columns, columns=b1amt_info.index)
            bigorder_info = pd.DataFrame(bigorder_info.values.T, index=bigorder_info.columns, columns=bigorder_info.index)

        csv_name = 'C:/data/time_collection/b1amt/'+path
        csv_name_='C:/data/time_collection/bigorder/'+path
        b1amt_info.to_csv(csv_name, index=False)
        bigorder_info.to_csv(csv_name_, index=False)
        

if __name__ == "__main__":
    main(url)
