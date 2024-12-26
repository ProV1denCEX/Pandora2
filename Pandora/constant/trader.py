from enum import Enum
import re

from Pandora.constant import BaseEnum


class Product(Enum):
    """
    Product class.
    """
    EQUITY = "股票"
    FUTURES = "期货"
    OPTION = "期权"
    INDEX = "指数"
    FOREX = "外汇"
    SPOT = "现货"
    ETF = "ETF"
    BOND = "债券"
    WARRANT = "权证"
    SPREAD = "价差"
    FUND = "基金"


class Exchange(Enum):
    """
    Exchange.
    """
    # Chinese
    CFFEX = "CFFEX"         # China Financial Futures Exchange
    SHFE = "SHFE"           # Shanghai Futures Exchange
    CZCE = "CZCE"           # Zhengzhou Commodity Exchange
    DCE = "DCE"             # Dalian Commodity Exchange
    INE = "INE"             # Shanghai International Energy Exchange
    GFEX = "GFEX"           # Guangzhou Futures Exchange
    SSE = "SSE"             # Shanghai Stock Exchange
    SZSE = "SZSE"           # Shenzhen Stock Exchange
    BSE = "BSE"             # Beijing Stock Exchange
    SHHK = "SHHK"           # Shanghai-HK Stock Connect
    SZHK = "SZHK"           # Shenzhen-HK Stock Connect
    SGE = "SGE"             # Shanghai Gold Exchange
    WXE = "WXE"             # Wuxi Steel Exchange
    CFETS = "CFETS"         # CFETS Bond Market Maker Trading System
    XBOND = "XBOND"         # CFETS X-Bond Anonymous Trading System

    # Global
    SMART = "SMART"         # Smart Router for US stocks
    NYSE = "NYSE"           # New York Stock Exchnage
    NASDAQ = "NASDAQ"       # Nasdaq Exchange
    ARCA = "ARCA"           # ARCA Exchange
    EDGEA = "EDGEA"         # Direct Edge Exchange
    ISLAND = "ISLAND"       # Nasdaq Island ECN
    BATS = "BATS"           # Bats Global Markets
    IEX = "IEX"             # The Investors Exchange
    AMEX = "AMEX"           # American Stock Exchange
    TSE = "TSE"             # Toronto Stock Exchange
    NYMEX = "NYMEX"         # New York Mercantile Exchange
    COMEX = "COMEX"         # COMEX of CME
    GLOBEX = "GLOBEX"       # Globex of CME
    IDEALPRO = "IDEALPRO"   # Forex ECN of Interactive Brokers
    CME = "CME"             # Chicago Mercantile Exchange
    ICE = "ICE"             # Intercontinental Exchange
    SEHK = "SEHK"           # Stock Exchange of Hong Kong
    HKFE = "HKFE"           # Hong Kong Futures Exchange
    SGX = "SGX"             # Singapore Global Exchange
    CBOT = "CBT"            # Chicago Board of Trade
    CBOE = "CBOE"           # Chicago Board Options Exchange
    CFE = "CFE"             # CBOE Futures Exchange
    DME = "DME"             # Dubai Mercantile Exchange
    EUREX = "EUX"           # Eurex Exchange
    APEX = "APEX"           # Asia Pacific Exchange
    LME = "LME"             # London Metal Exchange
    BMD = "BMD"             # Bursa Malaysia Derivatives
    TOCOM = "TOCOM"         # Tokyo Commodity Exchange
    EUNX = "EUNX"           # Euronext Exchange
    KRX = "KRX"             # Korean Exchange
    OTC = "OTC"             # OTC Product (Forex/CFD/Pink Sheet Equity)
    IBKRATS = "IBKRATS"     # Paper Trading Exchange of IB

    BINANCE = "BINANCE"
    OKX = "OKX"
    BYBIT = "BYBIT"
    DERIBIT = "DERIBIT"

    # Special Function
    LOCAL = "LOCAL"         # For local generated data



class Interval(BaseEnum):
    """
    Interval of bar data.
    """
    MINUTE = "1m"
    MINUTE_1 = "1m"
    MINUTE_2 = "2m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"

    HOUR = "1h"
    DAILY = "d"
    WEEKLY = "w"
    TICK = "tick"

    @staticmethod
    def from_window(interval, window: int):
        if interval == Interval.MINUTE or Interval.MINUTE_1:
            if window == 1: return Interval.MINUTE_1
            if window == 2: return Interval.MINUTE_2
            if window == 3: return Interval.MINUTE_3
            if window == 5: return Interval.MINUTE_5
            if window == 15: return Interval.MINUTE_15

        raise NotImplementedError

    @staticmethod
    def to_window(interval) -> int:
        if interval == Interval.MINUTE_1: return 1
        if interval == Interval.MINUTE_2: return 2
        if interval == Interval.MINUTE_3: return 3
        if interval == Interval.MINUTE_5: return 5
        if interval == Interval.MINUTE_15: return 15

        raise NotImplementedError

    @staticmethod
    def from_str(interval):
        interval = interval.lower()
        if interval.endswith('d') or 'daily' in interval:
            return Interval.DAILY

        elif interval.endswith('m') or 'min' in interval:
            minute = re.sub("\D", "", interval)
            return Interval.from_window(Interval.MINUTE, int(minute))

        else:
            raise NotImplementedError

    @staticmethod
    def from_freq(freq):
        if freq == Frequency.Min_1: return Interval.MINUTE_1
        if freq == Frequency.Min_2: return Interval.MINUTE_2
        if freq == Frequency.Min_3: return Interval.MINUTE_3
        if freq == Frequency.Min_5: return Interval.MINUTE_5
        if freq == Frequency.Min_15: return Interval.MINUTE_15
        if freq == Frequency.Daily: return Interval.DAILY
        if freq == Frequency.Weekly: return Interval.WEEKLY

        raise NotImplementedError


class Frequency(BaseEnum):
    Sec_1 = 1 / 60
    Sec_2 = 2 / 60
    Sec_3 = 3 / 60
    Sec_4 = 4 / 60
    Sec_5 = 5 / 60
    Sec_6 = 6 / 60
    Sec_10 = 10 / 60
    Sec_12 = 12 / 60
    Sec_15 = 15 / 60
    Sec_20 = 20 / 60
    Sec_30 = 30 / 60

    Min_1 = 1
    Min_2 = 2
    Min_3 = 3
    Min_5 = 5
    Min_15 = 15
    Min_30 = 30
    Min_60 = 60
    Daily = "daily"
    Weekly = "weekly"
    Monthly = "monthly"
    Quarterly = "quarterly"
    Yearly = "yearly"

    UNKNOWN = -1

    @staticmethod
    def from_str(freq):
        freq = freq.lower()
        if 'daily' in freq:
            return Frequency.Daily

        elif 'weekly' in freq:
            return Frequency.Weekly

        elif 'monthly' in freq:
            return Frequency.Monthly

        elif 'quarterly' in freq:
            return Frequency.Quarterly

        elif 'yearly' in freq:
            return Frequency.Yearly

        elif freq.endswith('m') or 'min' in freq:
            minute = re.sub("\D", "", freq)
            return Frequency.from_val(minute)

        elif freq.endswith('s') or 'sec' in freq:
            second = re.sub("\D", "", freq)
            second = int(second) / 60

            return Frequency.from_val(second)

        else:
            return Frequency.UNKNOWN

    @staticmethod
    def from_val(val):
        for i in Frequency:
            if i.value == val or str(i.value) == val:
                return i

        return Frequency.UNKNOWN

    def to_str(self):
        vals = self.name.lower().split('_')
        if len(vals) == 1:
            return vals[0]

        else:
            return vals[1] + vals[0]

    @property
    def unit(self):
        return self.name.lower().split('_')[0]


class OptionType(Enum):
    """
    Option type.
    """
    CALL = "看涨期权"
    PUT = "看跌期权"

    @staticmethod
    def from_str(type: str):
        if type.upper() in {"看涨期权", "CALL", "C"}:
            return OptionType.CALL

        if type.upper() in {"看跌期权", "PUT", "P"}:
            return OptionType.PUT

        raise ValueError(f"Invalid OptionType {type}")


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "Long"
    SHORT = "Short"
    NET = "Net"


class EngineType(Enum):
    LIVE = "Live"
    SIGNAL = "Signal"
    BACKTESTING = "Backtest"


class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "开"
    CLOSE = "平"
    CLOSETODAY = "平今"
    CLOSEYESTERDAY = "平昨"


class Status(Enum):
    """
    Order status.
    """
    SUBMITTING = "提交中"
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "拒单"


class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "限价"
    MARKET = "市价"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"
    RFQ = "询价"


class Currency(Enum):
    """
    Currency.
    """
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"
    CAD = "CAD"
