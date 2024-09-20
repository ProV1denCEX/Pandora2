import datetime
import datetime as dt
import warnings
from enum import Enum
from typing import Union, List

import pandas as pd
from dateutil import parser

from Pandora.constant import DbConn, TP
from Pandora.helper.database import DbManager


class DateFmt(Enum):
    YMD = '%Y%m%d'
    HMS = '%H%M%S'
    Y_M_D = '%Y-%m-%d'
    H_M_S = '%H:%M:%S'
    YMDHMS = f'{YMD}{HMS}'
    YMD_H_M_S = f'{YMD} {H_M_S}'
    Y_M_D_H_M_S = f'{Y_M_D} {H_M_S}'
    dolphin_datetime = "%Y.%m.%dT%H:%M:%S.%f"


class Dates:
    """日期相关处理"""

    @staticmethod
    def now() -> dt.date:
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=+8)))

    @staticmethod
    def now_day() -> str:
        return Dates.now().strftime(DateFmt.YMD.value)

    @staticmethod
    def now_std_day() -> str:
        return Dates.now().strftime(DateFmt.Y_M_D.value)

    @staticmethod
    def now_time() -> str:
        return Dates.now().strftime(DateFmt.Y_M_D_H_M_S.value)

    @staticmethod
    def convert(date: Union[str, datetime.date], fmt=DateFmt.Y_M_D) -> str:
        """转换任意格式的date为指定pat格式的日期,默认返回yyyy-mm-dd格式"""
        assert date, "date can't be none or empty!"
        assert isinstance(date, (str, datetime.date)), "date type is error!"
        if isinstance(date, str):
            date = parser.parse(timestr=date, fuzzy=True)
        return date.strftime(fmt.value)

    @staticmethod
    def add(date: Union[str, datetime.date] = None, days=1, fmt=DateFmt.Y_M_D) -> str:
        date = date or Dates.now()
        if isinstance(date, str):
            date = parser.parse(timestr=date, fuzzy=True)
        # 日期计算
        date += dt.timedelta(days)
        return date.strftime(fmt.value)


class TDays:
    """交易日相关处理, 数据来源wind"""

    @staticmethod
    def get_trading_calendar(exchange='SHFE'):
        sql = f"SELECT DISTINCT Date FROM Calendar WHERE {exchange}=1 ORDER BY Date"
        dbm = DbManager(DbConn.MSSQL_165)
        calendar = dbm.query(sql)

        return calendar

    @staticmethod
    def wrap_tdays(df: pd.DataFrame, col_dt='DateTime', col_tdate='TradeDate'):
        col_daten = "__DateN"
        col_date = "__Date"
        col_next = "__NextDate"

        calendar = TDays.get_trading_calendar(exchange='SHFE')
        calendar.loc[:, col_date] = calendar.loc[:, 'Date']
        calendar.loc[:, col_next] = calendar.loc[:, col_date].shift(-1)
        calendar = calendar.loc[:, [col_date, col_next]]

        df[col_daten] = pd.to_datetime(df.loc[:, col_dt].dt.date)
        df = pd.merge_asof(df.sort_values(col_dt), calendar, left_on=col_daten, right_on=col_date, direction='forward')

        loc = (df.loc[:, col_dt].dt.time <= dt.time(16))
        df.loc[loc, col_tdate] = df.loc[loc, col_date]
        df.loc[~loc, col_tdate] = df.loc[~loc, col_next]

        return df.drop(columns=[col_daten, col_date, col_next], errors="ignore")

    @staticmethod
    def is_tday(t_date: TP.TDate, exchange='SHFE') -> bool:
        """ 判断是否为工作日

        :param t_date: 需要判断的日期
        :param exchange: 交易所标识
        :return: bool
        """
        assert t_date, "date cannot be empty!"
        t_date = Dates.convert(t_date, DateFmt.YMD)

        sql = f"SELECT COUNT(*) FROM Calendar WHERE {exchange}=1 and Date='{t_date}'"
        dbm = DbManager(DbConn.MSSQL_165)
        result = dbm.query(sql)
        return result.iat[0, 0] != 0

    @staticmethod
    def get_tday(t_datetime: TP.TDate = None, end_hour=21, fmt=DateFmt.Y_M_D, exchange='SHFE') -> str:
        """ 依据end_hour与当前时间的小时数进行对比, 如果超过end_hour切换到下一工作日.
            如果t_date非交易日, 将t_date等于下一个交易日

        :param t_datetime: 需要计算的工作日,可以带时间戳, 默认当天
        :param end_hour: T日结束的小时点,默认T日21点. 如果需要按自然日切换, 传入0即可.
        :param fmt: 需要输出的日期格式
        :param exchange: 交易所标识
        :return: yyyy-mm-dd
        """
        hours = [0] + list(range(15, 24))
        if end_hour not in hours:
            raise ValueError("end_hour value error! Must be equal to 0 or in the interval [15,23]")

        t_datetime = t_datetime or Dates.now_std_day()
        t_time = pd.to_datetime(t_datetime).time()
        # 如果时间不为 00:00:00 则分解出时间的小时数与end_hour进行比对
        if t_time == dt.time(0):
            t_hour = datetime.datetime.now().hour
        else:
            t_hour = t_time.hour
        # 当前时点大于end_hour, 取下一交易日. 否则取当日交易日(非工作日默认取下一工作日)
        days = 1 if end_hour != 0 and t_hour >= end_hour else 0

        return TDays.add(t_datetime, days, fmt, exchange)

    @staticmethod
    def add(t_date: TP.TDate, days, fmt=DateFmt.Y_M_D, exchange='SHFE') -> str:
        """ 以t_date做为T日, 往前或后推算交易日

        :param t_date: 日期/实例日期
        :param days: ±N 个交易日
        :param fmt: 需要输出的日期格式
        :param exchange: 交易所标识
        :return: 计算后的交易日 yyyymmdd
        """
        if not t_date:
            raise ValueError("trading date can't be empty!")

        t_date = Dates.convert(t_date, DateFmt.YMD)
        compare_symbol, order_symbol = (">=", "ASC") if days >= 0 else ("<", "DESC")
        # row_number()是从1开始的序列. days=0 默认当前工作日
        days = 1 if days == 0 else abs(days) if days < 0 else days + 1

        sql = (f"SELECT Date FROM " 
              f"(" 
              f"    SELECT row_number() over (order by Date {order_symbol}) rn, Date " 
              f"    FROM Calendar WHERE {exchange}=1 and Date {compare_symbol} '{t_date}'" 
              f") AS a " 
              f"WHERE a.rn = {days}")

        dbm = DbManager(DbConn.MSSQL_165)
        result = dbm.query(sql)

        if fmt:
            return Dates.convert(str(result.iat[0, 0]), fmt)

        else:
            return result.iat[0, 0]

    @staticmethod
    def period(begin_date: TP.TDate, end_date: TP.TDate = None, fmt=DateFmt.Y_M_D, exchange='SHFE') -> List:
        """
           取交易日闭区间. 如果begin_date不是交易日, 默认取下一交易日做为基准T日.
           如果begin与end都是交易日, 在结果集里存在 begin=result[0], end=result[-1]

        :param begin_date: 开始日期
        :param end_date:  结束日期, 默认当前日期
        :param fmt: 需要输出的日期格式
        :param exchange:  交易所标识
        :return:  区间内的交易日 [yyyymmdd,...,yyyymmdd]
        """
        assert begin_date, "Begin date cannot be empty!"
        begin_date = Dates.convert(begin_date, DateFmt.YMD)
        end_date = Dates.convert(end_date or Dates.now_day(), DateFmt.YMD)
        if begin_date > end_date:
            raise ValueError("BeginDate must be less than EndDate")

        sql = (f"SELECT Date FROM Calendar "
               f"WHERE {exchange}=1 AND Date between '{begin_date}' and '{end_date}' "
               f"ORDER BY Date ")

        dbm = DbManager(DbConn.MSSQL_165)
        result = dbm.query(sql)
        if result.empty:
            warnings.warn(f"trading days not exists between [{begin_date}] and [{end_date}]")
            return []

        dates = result.iloc[:, 0].to_list()

        if fmt:
            return list(map(lambda d: Dates.convert(d, fmt), dates))

        else:
            return dates

    @staticmethod
    def interval(t_date: TP.TDate = None, days=1, end_hour=21, fmt=DateFmt.Y_M_D, exchange='SHFE') -> List[str]:
        """ 依据t_date取出 ±days 天. 如果t_date非交易日,则T日为t_date的下一个交易日.
            结果集与内部add调用等同 period(begin=TDays.add(t_date, -days),end=TDays.add(t_date, days).
            内部只查SQL一次, 计算实际区间依赖于索引

        :param t_date:T日
        :param days: 向前后取的天数 ±days
        :param end_hour: T日的结束小时数
        :param fmt: 格式化
        :param exchange: 交易所标识
        :return: list([T-1]yyyy-mm-dd,[T]yyyy-mm-dd,[T+1]yyyy-mm-dd) 交易日区间段
        """
        if days <= 0:
            raise ValueError("days Must be greater than 0")
        # 计算基准T日
        t_date = TDays.get_tday(t_date, end_hour, fmt, exchange)
        # 根据days计算起始日期 ±(days+10)
        begin, end = Dates.add(t_date, -(days + 20)), Dates.add(t_date, days + 20)
        periods = TDays.period(begin, end, fmt, exchange)
        idx_tday = periods.index(t_date)

        return periods[idx_tday - days:idx_tday + days + 1]

    @staticmethod
    def get_tday_start(t_date: TP.TDate = None, start_hour = 21):
        tday = TDays.add(t_date, -1, fmt=None)

        return datetime.datetime.combine(tday, datetime.time(hour=start_hour))

    @staticmethod
    def get_tday_end(t_date: TP.TDate = None, end_hour = 21):
        tday = TDays.get_tday(t_date, fmt=None)

        return datetime.datetime.combine(tday, datetime.time(hour=end_hour))
