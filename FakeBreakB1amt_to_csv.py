import FindFakeBreak
import pandas as pd
import json
from MongoDBReader import MongoDBReader
import datetime

def return_data():
    data=pd.read_csv('//192.168.0.197/Cache/Time_Info.csv',dtype={'code':str})
    #data.loc[861]=['000004','2020-02-17',4]
    #data.columns=['code','time','uplimit_times']
    length=data.shape[0]
    return data,length


def main(readdb):
    '''
    向数据库写入指定时间段的涨停信息
    :param readdb: 需读取数据的数据库的IP 端口号
    :param date_st: 开始日期 datetime.date
    :param date_ed: 结束日期 datetime.date
    :return:
    '''
    reader = MongoDBReader()
    reader.login(*readdb)
    df,length=return_data()
    df["data"] = pd.to_datetime(df["data"])
    df["uplimit_time"] = pd.to_datetime(df["uplimit_time"])
    df["data"]=df["data"].dt.date #去掉00：00：00的后缀
    
    
    # 获取指定日期涨停股票信息
    for i in range(length):
        date=df.loc[i]['data']
        code=df.loc[i]['code']
        uplimit_times=df.loc[i]['uplimit_times']
        uplimit_time=df.loc[i]['uplimit_time']

        #date=date.strftime("%Y/%m/%d")
        if date.year==2020:
            if uplimit_time.hour>9 or (uplimit_time.hour==9 and uplimit_time.minute>30) or (uplimit_time.hour==9 and uplimit_time.minute==30 and uplimit_time.second>30):
                datenum = date.year * 10000 + date.month * 100 + date.day  
                uplimit_info = reader.QueryUplimitInfo(date=datenum,code=code) 
                for idx in uplimit_info.index:
                    if uplimit_info.loc[idx,'uplimit_times']==uplimit_times:
                        seq_st = int(uplimit_info.loc[idx,"uplimit_order"])
                        seq_ed = int(uplimit_info.loc[idx,"break_trade"])
                dayline_info = reader.QueryStockDayLine(date_st=datenum, date_ed=datenum, code="SZ" + code)
                uplimit_pr = round(dayline_info.loc[0, "pre_close"] * 1.10, 2)        
                
                # 查询订单流数据
                order_info = reader.QueryStockTickOrder(date=datenum, code=code, seq_st=seq_st, seq_ed=seq_ed)
                # 查询成交流数据
                trade_info = reader.QueryStockTickTrade(date=datenum, code=code, seq_st=seq_st, seq_ed=seq_ed)
                # 计算各个时刻的买一量 大单量
                b1amt_info = FindFakeBreak.GetB1amtInfo(uplimit_pr, order_info, trade_info)
        
                csv_name = "C:\\data\\B1amt\\{}_{}_{}.csv".format(code, date, uplimit_times)
                b1amt_info.to_csv(csv_name, index=False)

if __name__ == "__main__":
    with open("config.json","rb") as fp:
        conf=json.load(fp)
    main((conf["server"],conf["port"],conf["user"],conf["pwd"]))
