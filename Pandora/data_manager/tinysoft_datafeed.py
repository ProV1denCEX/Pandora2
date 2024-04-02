from datetime import datetime
from typing import Dict, Callable

import pandas as pd
from pyTSL import Client, DoubleToDatetime

from Pandora.constant import Frequency, Exchange
from Pandora.helper import TDays, Symbol, DateFmt

EXCHANGE_MAP: Dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ"
}

INTERVAL_MAP: Dict[Frequency, str] = {
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
}


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

    def query_bar_history(
            self,
            symbol: str,
            exchange: Exchange,
            freq: Frequency,
            start: datetime,
            end: datetime,
            output: Callable = print
    ):
        """查询K线数据"""
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return

        tsl_exchange: str = EXCHANGE_MAP.get(exchange, "")
        tsl_interval: str = INTERVAL_MAP[freq]

        start_str: str = TDays.add(start, -1, DateFmt.YMD)
        end_str: str = end.strftime("%Y%m%d")

        cmd: str = (
            f"setsysparam(pn_cycle(),{tsl_interval}());"
            "return select * from markettable "
            f"datekey {start_str}.210000T to {end_str}.153000T "
            f"of '{tsl_exchange}{symbol}' end;"
        )
        result = self.client.exec(cmd)

        if not result.error():
            data = result.value()

            quote = pd.DataFrame(data)

            quote.columns = [QUOTE_MAP.get(
                i, i.capitalize()) for i in quote.columns]
            quote = quote[
                ['Date', 'Ticker', 'Open', 'Close', 'High', 'Low', 'PrevClose', 'Volume', 'Amount', 'DealAmount', 'OpenInterest']]

            # Data formatting
            quote['Ticker'] = [i.upper() for i in quote['Ticker']]
            quote['DateTime'] = quote['Date'].apply(DoubleToDatetime)
            quote['Date'] = quote['DateTime'].dt.date

            quote = TDays.wrap_tdays(quote)
            quote['Date'] = quote['DateTime'].dt.date

            quote['Contract'] = quote['Ticker'].apply(Symbol.get_contract)

            return quote
