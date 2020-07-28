import os
import pandas as pd
url=os.listdir('C:/data/B1amt')
length=len(url) 
end_time='15:00:00.000'
col=['datatime','b1vol','b1amt','bigorder','b1vol2','b1amt2','bigorder2','b1vol3','b1amt3','bigorder3',
'b1vol4','b1amt4','bigorder4','b1vol5','b1amt5','bigorder5','b1vol6','b1amt6','bigorder6']
cur_time=['time1','time2','time3','time4','time5']
time1=[end_time for i in range(length)]
time2=time1
time3=time1
time4=time1
time5=time1
time=pd.DataFrame({'code':url,'time1':time1,'time2':time2,'time3':time3,'time4':time4,'time5':time5})
for index,path in enumerate(url):
    #print('C:/data/B1amt/'+path)
    #pd.read_csv('C:/data/B1amt/'+path)
    df=pd.read_csv('C:/data/B1amt/'+path)
    for i in range(5):
        k=0
        for idx,b1amt in enumerate(df[col[3*i+2]]):
            if b1amt>0 and k ==0 :
                k=1
            if b1amt==0 and k ==1 :
                k=2
                zero_time=df.loc[idx,'datatime']
                time.loc[index,cur_time[i]]=zero_time[11:]
                break

        
time.to_csv('C:/data/time.csv',index=False)