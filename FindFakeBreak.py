import datetime
from copy import deepcopy
import pandas as pd
from MongoDBReader import MongoDBReader
from data_struct import OrderType, OrderSide, OrderFinalStatus
import os
'''
查找是否存在有封板后订单量减少 后又突然增加的情况
'''
MAX_SEQ = 9999_9999


def CheckFakeBreak(df_b1amt):
    '''
    判断一次涨停-破板过程是否满足假破板的要求
    :param df_b1amt: 涨停-破板过程 时间 买一量 大单数
    :return: bool 是否满足条件
    '''
    SendOrder = False
    CancelOrder = False
    for idx in df_b1amt.index:
        if df_b1amt.loc[idx, "bigorder"] >= 2:
            if not SendOrder:
                SendOrder = True
            if SendOrder and CancelOrder:
                return True
        elif df_b1amt.loc[idx, "bigorder"] <= 0:
            if SendOrder:
                CancelOrder = True
    return False


def GetB1amtInfo(uplimit_pr, df_order, df_trade):
    '''
    :param uplimit_pr: 涨停价
    :param order_info: 委托流
    :param trade_info: 成交流
    :return: b1amt_info: 涨停-破板过程 时间 买一量 大单数
    '''
    uplimit_pr -= 0.00001
    # 筛选买入订单
    df_order = df_order[df_order["side"] == OrderSide.buy]
    # 筛选市价报单与涨停价报价的限价单
    df_order = df_order[(df_order["type"] > OrderType.limit) | \
                        ((df_order["type"] == OrderType.limit) & (df_order["price"] >= uplimit_pr))]
    df_order.index = range(len(df_order))
    df_trade.index = range(len(df_trade))
    order_set = set(df_order["seq"])
    big_order_min_vol = min(80_0000, 140_0000 / uplimit_pr)
    big_order = deepcopy(df_order[df_order["vol"] >= big_order_min_vol])
    big_order["resdual_vol"] = big_order["vol"]
    big_order_map = dict(zip(big_order["seq"], big_order.index))
    # 循环变量初始化
    big_order_num = 0
    b1vol = 0
    data_list = []
    idx_o = 0
    idx_t = 0
    max_idx_o = len(df_order) - 1
    max_idx_t = len(df_trade) - 1
    next_order_time = df_order.loc[idx_o, "datatime"]
    next_trade_time = df_trade.loc[idx_t, "datatime"]
    end_time = df_trade.loc[idx_t, "datatime"] + datetime.timedelta(hours=8)
    while idx_o <= max_idx_o or idx_t <= max_idx_t:
        cur_time = min(next_order_time, next_trade_time)
        # 委托流
        while idx_o <= max_idx_o and df_order.loc[idx_o, "datatime"] <= cur_time:
            b1vol += df_order.loc[idx_o, "vol"]
            if df_order.loc[idx_o, "vol"] >= big_order_min_vol:
                big_order_num += 1
            idx_o += 1
        if idx_o <= max_idx_o:
            next_order_time = df_order.loc[idx_o, "datatime"]
        else:
            next_order_time = end_time
        # 成交流
        while idx_t <= max_idx_t and df_trade.loc[idx_t, "datatime"] <= cur_time:
            if df_trade.loc[idx_t, "type"] == OrderType.deal or df_trade.loc[idx_t, "seq_buy"] in order_set:
                b1vol -= df_trade.loc[idx_t, "vol"]
            else:
                idx_t += 1
                continue
            seq = df_trade.loc[idx_t, "seq_buy"]
            if seq in big_order_map.keys():
                idx_big = big_order_map[df_trade.loc[idx_t, "seq_buy"]]
                big_order.loc[idx_big, "resdual_vol"] -= df_trade.loc[idx_t, "vol"]
                if big_order.loc[idx_big, "resdual_vol"] < big_order_min_vol and \
                    big_order.loc[idx_big, "resdual_vol"] + df_trade.loc[idx_t, "vol"] >= big_order_min_vol:
                    big_order_num -= 1
            idx_t += 1
        if idx_t <= max_idx_t:
            next_trade_time = df_trade.loc[idx_t, "datatime"]
        else:
            next_trade_time = end_time
        data_list.append((cur_time, b1vol, b1vol*uplimit_pr / 10000.0, big_order_num))
    # end while
    df_b1amt = pd.DataFrame(data_list, columns=["datatime", "b1vol", "b1amt", "bigorder"])
    return df_b1amt


def load_df(date, code, uplimit_times):
    '''
    读取指定文件
    :param date: 日期 "yyyy-mm-dd"格式 或 datetime.datetime 或 datetime.date 类型
    :param code: 六位证券代码
    :param uplimit_times: 涨停次数 自然数
    :return:
    '''
    dir_ = "\\\\192.168.0.197\\data\\temp"
    filename = "{}\\{}_{}_{}.csv".format(dir_, code, date, uplimit_times)
    if not os.path.exists(filename):
        print("{} not exists".format(filename))
        return pd.DataFrame()
    df = pd.read_csv(filename)
    df["datatime"] = pd.to_datetime(df["datatime"])
    return df, code, uplimit_times


def plot_df(df, code, uplimit_times):
    '''
    绘制指定 pandas文件
    :param df:
    :return:
    '''
    if df is None or len(df) == 0:
        print("df is empty")
        return
    import matplotlib.pyplot as plt
    from matplotlib import ticker
    from matplotlib.dates import num2date
    year, month, day = df["datatime"].iloc[0].year, df["datatime"].iloc[0].month, df["datatime"].iloc[0].day
    noon_st = datetime.datetime(year, month, day, 11, 30)
    noon_end = datetime.datetime(year, month, day, 13, 0)
    time_delta = datetime.timedelta(minutes=90)
    x_data = df["datatime"].apply(lambda x: x - time_delta if x > noon_end else x if x < noon_st else noon_st)
    title = "{:04d}-{:02d}-{:02d} {} {}rd Uplimit".format(year, month, day, code, uplimit_times)
    # 坐标轴显示函数
    def format_datetime(x, pos=None):
        '''
        用户自定义坐标轴显示函数
        :param x:
        :param pos:
        :return:
        '''
        # 下午时间
        if x - int(x) > 41400 / 86400:
            x += 0.0625
        x = num2date(x)
        # if pos == 0:
        #     fmt = '%Y-%m-%d %H:%M:%S.%f'
        # else:
        #     fmt = '%H:%M:%S.%f'
        fmt = '%H:%M:%S.%f'
        label = x.strftime(fmt)
        label = label.rstrip("0")
        label = label.rstrip(".")
        return label
    # end fun
    # fig, axs = plt.subplots(2, 1)
    fig = plt.figure(figsize=(12, 6))
    # 绘制第一个子图
    axs = fig.add_subplot(2, 1, 1)
    axs.plot(x_data, df["b1amt"])
    axs.set_xlabel("time")
    axs.set_ylabel("b1amt(w)")
    axs.set_title(title)
    plt.xticks(rotation=30)  # x轴坐标值旋转30度
    axs.grid()
    axs.xaxis.set_major_formatter(ticker.FuncFormatter(format_datetime))
    # 绘制第二个子图
    axs = fig.add_subplot(2, 1, 2)
    axs.plot(x_data, df["bigorder"])
    axs.set_xlabel("time")
    axs.set_ylabel("bigorder")
    axs.grid()
    axs.xaxis.set_major_formatter(ticker.FuncFormatter(format_datetime))
    plt.xticks(rotation=30)  # x轴坐标值旋转30度
    plt.show()


def main(readdb, date_st, date_ed):
    '''
    向数据库写入指定时间段的涨停信息
    :param readdb: 需读取数据的数据库的IP 端口号
    :param date_st: 开始日期 datetime.date
    :param date_ed: 结束日期 datetime.date
    :return:
    '''
    reader = MongoDBReader()
    reader.login(*readdb)
    date = date_st
    df_list = []
    # 日期循环
    while date <= date_ed:
        datenum = date.year * 10000 + date.month * 100 + date.day
        # 获取指定日期涨停股票信息
        uplimit_info = reader.QueryUplimitInfo(date=datenum)
        uplimit_info_gp = uplimit_info.groupby(by="code")
        total = len(uplimit_info)
        count = 0
        # 证券代码循环
        for code, df in uplimit_info_gp:
            # 查询日线行情数据
            dayline_info = reader.QueryStockDayLine(date_st=datenum, date_ed=datenum, code="SZ" + code)
            if len(dayline_info) == 0:
                continue
            uplimit_pr = round(dayline_info.loc[0, "pre_close"] * 1.10, 2)
            # 每一次涨停的循环
            for idx in df.index:
                seq_st = int(df.loc[idx, "uplimit_order"])
                seq_ed = int(df.loc[idx, "break_trade"])
                if seq_ed == 0:
                    seq_ed = MAX_SEQ
                uplimit_times = df.loc[idx, "uplimit_times"]
                # 查询订单流数据
                order_info = reader.QueryStockTickOrder(date=datenum, code=code, seq_st=seq_st, seq_ed=seq_ed)
                # 查询成交流数据
                trade_info = reader.QueryStockTickTrade(date=datenum, code=code, seq_st=seq_st, seq_ed=seq_ed)
                # 计算各个时刻的买一量 大单量
                b1amt_info = GetB1amtInfo(uplimit_pr, order_info, trade_info)
                # 检查是否存在假破板 后又封板的情况
                fake_break = CheckFakeBreak(b1amt_info)
                count += 1
                if fake_break:
                    print("{}/{} date:{} code:{} uplimit_times:{} is fake_break".format(count, total, code, date, uplimit_times))
                else:
                    print("{}/{} date:{} code:{} uplimit_times:{} is not fake_break".format(count, total, code, date, uplimit_times))
                csv_name = "C:\\data\\temp\\{}_{}_{}.csv".format(code, date, uplimit_times)
                b1amt_info.to_csv(csv_name, index=False)
                # 下一次涨停
            # 下一只股票
        # 日期递增
        date += datetime.timedelta(days=1)
    # 日期循环结束
    if len(df_list) > 0:
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.DataFrame()


if __name__ == "__main__":
    # main(readdb=("localhost", 27017),
    #      date_st=datetime.date(2020, 2, 18),
    #      date_ed=datetime.date(2020, 2, 18))
    df, code, up_times = load_df("2016-08-17", "000019", 6)
    plot_df(df, code, up_times)
