from datetime import datetime, timedelta
from typing import Dict, Callable
from zoneinfo import ZoneInfo

import pandas as pd
from pyTSL import Client, DoubleToDatetime

from Pandora.constant import Frequency, Exchange, Interval, OptionType
from Pandora.helper import TDays, Symbol, DateFmt
from Pandora.trader.object import HistoryRequest

EXCHANGE_MAP: Dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ"
}

EXCHANGE_CHINESE_MAP: Dict[str, Exchange] = {
    "大连商品交易所": Exchange.DCE,
    "郑州商品交易所": Exchange.CZCE,
    "上海期货交易所": Exchange.SHFE,
    "上海国际能源交易中心": Exchange.INE,
    "中国金融期货交易所": Exchange.CFFEX,
    "广州期货交易所": Exchange.GFEX,

    "上海证券交易所": Exchange.SSE,
    "深圳证券交易所": Exchange.SZSE,
}


INTERVAL_MAP = {
    Frequency.Min_1: "cy_1m",
    Frequency.Min_5: "cy_5m",
    Frequency.Min_15: "cy_15m",
    Frequency.Min_30: "cy_30m",
    Frequency.Min_60: "cy_60m",

    Frequency.Sec_1: "cy_1s",
    Frequency.Sec_2: "cy_2s",
    Frequency.Sec_3: "cy_3s",
    Frequency.Sec_4: "cy_4s",
    Frequency.Sec_5: "cy_5s",
    Frequency.Sec_10: "cy_10s",
    Frequency.Sec_15: "cy_15s",
    Frequency.Sec_30: "cy_30s",

    Frequency.Daily: "cy_day",

    Interval.MINUTE: "cy_1m",
    Interval.HOUR: "cy_60m",
    Interval.DAILY: "cy_day",
}


SHIFT_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
}

OPTION_TYPE_MAP = {
    "认购": OptionType.CALL,
    "认沽": OptionType.PUT,
}


INDEX_SYMBOL_MAP = {
    "IO": "000300",
    "MO": "000852",
    "HO": "000016",
}


CHINA_TZ = ZoneInfo("Asia/Shanghai")

MC_SYMBOL = "mc_only"
LISTING_SYMBOL = "listing_only"
ALL_SYMBOL = "all"


def get_option_symbol(symbol):
    if symbol.startswith("OP"):
        return symbol.replace("OP", "")

    else:
        return symbol


def get_contract(symbol: str):
    return ''.join((i for i in symbol if i.isalpha()))


def get_option_product_info(series, exchange: Exchange):
    if exchange == Exchange.CFFEX:
        symbol = series["StockID"]
        underlying = symbol.split("-")[0]
        portfolio = get_contract(underlying)

    elif exchange in {Exchange.SSE, Exchange.SZSE}:
        symbol = series["标的证券代码"]
        underlying = symbol[2:]
        portfolio = underlying + "_O"

    else:
        underlying = series["标的证券代码"]
        portfolio = get_contract(underlying) + "_o"

    return underlying, portfolio


QUOTE_MAP = {
    'StockID': 'Ticker',
    'yclose': 'PrevClose',
    'syl1': 'Settle',
    'syl2': 'PrevSettle',
    'vol': 'Volume',
    'cjbs': 'DealAmount',
    'sectional_cjbs': 'OpenInterest',
    'ret': 'CloseRet'
}


class TinysoftDatafeed(object):
    """天软数据服务接口"""

    def __init__(self):
        """"""
        self.username: str = "cxfundqa"
        self.password: str = "cxfund888888"

        self.client: Client = None
        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        self.client = Client(
            self.username,
            self.password,
            "tsl.tinysoft.com.cn",
            443
        )

        n: int = self.client.login()
        if n != 1:
            output("天软数据服务初始化失败：用户名密码错误！")
            return False

        self.inited = True
        return True

    def query_warehouse(self, req: HistoryRequest, output: Callable = print):
        pass