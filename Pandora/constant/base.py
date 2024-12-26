import datetime as dt
import platform
from collections import namedtuple
from enum import Enum
from os import PathLike
from typing import Type, Union, List, Dict, AnyStr, Sequence


Unit_To_Ton = {
    '金衡盎司': 31.1034768 / 1e6,
    '常衡盎司': 28.3495 / 1e6,
    '盎司': 28.3495 / 1e6,
    '千克': 1 / 1e3,
    '克': 1 / 1e6,
    '斤': 0.5 / 1e3,
    '500克': 0.5 / 1e3,
    '公斤': 1 / 1e3,
    '短吨': 0.907,
    '长吨': 1.016,
    '湿吨': 1,
    '公吨': 1,
    '千吨': 1e3,
    'kt': 1e3,
    '万吨': 1e4,
    '桶': 1 / 7.33,
    '千桶': 1000 / 7.33,
    '万桶': 1e4 / 7.33,
    '万重量箱': 1e4 * 50 / 1e3,
    '天': 1,
}

COM_EXCHANGE = ['SHFE', 'CZCE', 'DCE', 'INE', 'GFEX']

FIN_EXCHANGE = ['CFFEX']

SYMBOL_MAP = {'RO': 'OI', 'WS': 'WH', 'WT': 'PM', 'ER': 'RI', 'ME': 'MA', 'TC': 'ZC'}

DATA_START = 20100101


class BaseEnum(Enum):
    """
        提供类似Dict的操作
    """

    @classmethod
    def mapping(cls: Type[Enum], value: Union[int, float, str, bytes]) -> Enum:
        """根据Enum的值返回enum实例"""
        members = [member for _, member in cls.__members__.items() if member.value == value]
        return members[0] if members else None

    @classmethod
    def keys(cls: Type[Enum]) -> List[str]:
        """与Dict表现一致"""
        return list(cls.__members__.keys())

    @classmethod
    def values(cls: Type[Enum]) -> List[Union[int, float, str, bytes]]:
        """与Dict表现一致"""
        return [mem.value for _, mem in cls.__members__.items()]

    @classmethod
    def items(cls: Type[Enum]) -> Dict[str, Union[int, float, str, bytes]]:
        """与Dict表现一致"""
        return {mem.name: mem.value for _, mem in cls.__members__.items()}


class EdbType(Enum):
    BASIC = "basic"
    ADV = "adv"


class MatchMethod(Enum):
    this_bar_close = 1
    next_bar_open = 2
    next_bar_close = 3


class Method(Enum):
    static = 1
    dynamic = 2

    @staticmethod
    def from_str(method):
        method = method.lower()
        if 'static' in method:
            return Method.static
        elif 'dynamic' in method:
            return Method.dynamic


class Types(Enum):
    FutureSet = 1
    TradePeriod = 2
    TradeElement = 3


class Sample(Enum):
    Insample = 1
    Outsample = 2
    Fullsample = 3

    @staticmethod
    def from_str(sample):
        sample = sample.lower()
        if 'insample' in sample:
            return Sample.Insample
        elif 'outsample' in sample:
            return Sample.Outsample
        elif 'fullsample' in sample:
            return Sample.Fullsample


class Label(Enum):
    Liquidity_1B = 1
    Period_falling = 2
    Period_rising = 3
    Sample_1Y1Q = 4
    Sample_3Y1Q = 5
    Sample_3Y1Y = 6
    Sample_FX1Q = 7
    Sample_FX1Y = 8
    Illiquidity = 9
    TradingDaytime = 10
    TradingDaytimeLiquidity = 11
    TradingNight = 12
    TradingNightLiquidity = 13
    Black = 14
    Metal = 15
    PreciousMetal = 16
    Agricultural = 17
    Chemical = 18
    Financial = 19
    Slippage = 20
    RiskFreeRate = 21
    MatchingMethod = 22
    Fee = 23
    YearTradeDate = 24
    CommInter = 25
    CommMM = 26
    StockIndex = 27
    TreasuryBond = 28
    CommTradeble = 29

    @staticmethod
    def from_str(label):
        label = label.lower()

        if label == 'liquidity_1b':
            return Label.Liquidity_1B

        elif label == 'CommInter':
            return Label.CommInter

        elif label == 'CommMM':
            return Label.CommMM

        elif label == 'CommTradeble':
            return Label.CommTradeble

        elif label == 'period_falling':
            return Label.Period_falling
        elif label == 'period_rising':
            return Label.Period_rising

        elif label == 'sample_1y1q':
            return Label.Sample_1Y1Q
        elif label == 'sample_3y1q':
            return Label.Sample_3Y1Q
        elif label == 'sample_3y1y':
            return Label.Sample_3Y1Y
        elif label == 'sample_fx1q':
            return Label.Sample_FX1Q
        elif label == 'sample_fx1y':
            return Label.Sample_FX1Y

        elif label == 'illiquidity':
            return Label.Illiquidity

        elif label == 'tradingdaytime':
            return Label.TradingDaytime
        elif label == 'tradingdaytimeliquidity':
            return Label.TradingDaytimeLiquidity
        elif label == 'tradingnight':
            return Label.TradingNightLiquidity
        elif label == 'TradingNightLiquidity':
            return Label.Period_rising

        elif label == 'black':
            return Label.Black
        elif label == 'metal':
            return Label.Metal
        elif label == 'preciousmetal':
            return Label.PreciousMetal
        elif label == 'agricultural':
            return Label.Agricultural
        elif label == 'chemical':
            return Label.Chemical
        elif label == 'financial':
            return Label.Financial
        elif label == 'stockindex':
            return Label.StockIndex
        elif label == 'treasurybond':
            return Label.TreasuryBond

        elif label == 'slippage':
            return Label.Slippage
        elif label == 'riskfreerate':
            return Label.RiskFreeRate
        elif label == 'matchingmethod':
            return Label.MatchingMethod
        elif label == 'fee':
            return Label.Fee
        elif label == 'yeartradedate':
            return Label.YearTradeDate


class GroupType(BaseEnum):
    Top = 'top'
    Bottom = 'bottom'
    TMB = 'TMB'
    BMT = 'BMT'
    All = 'all'

    @staticmethod
    def from_str(group_type: str):
        group_str = group_type.lower()

        if 'top' in group_str or group_str == GroupType.Top.value:
            return GroupType.Top

        elif 'bottom' in group_str or group_str == GroupType.Bottom.value:
            return GroupType.Bottom

        elif 'tmb' in group_str or group_str == GroupType.TMB.value:
            return GroupType.TMB

        elif 'bmt' in group_str or group_str == GroupType.BMT.value:
            return GroupType.BMT

        else:
            return GroupType.All


class Consts:
    """
        常量相关值定义
    """

    IS_WIN_OS = platform.system() == "Windows"
    CHAR_UTF8 = "utf-8"
    CHAR_GBK = "gbk"
    MASK755 = 0o755

    """符号定义"""
    SYM_V_LINE = "|"
    SYM_H_LINE = "-"
    SYM_DOWN_LINE = "_"
    SYM_NONE = "--"
    SYM_COMMA = ","
    SYM_DOT = "."
    SYM_SHARP = "#"
    SYM_STAR = "*"
    SYM_NEWLINE = "\n"


class SymbolSuffix:
    MC = "00"
    MNC = "01"


class Section:
    MAIL = "notice-mail"


class DbConn:
    """数据库连接标识, 与配置文件中的Section对应"""
    MSSQL_65 = "db_65"
    MSSQL_165 = "db_165"
    MSSQL_162 = "db_162"
    TSDB_169 = "db_tsdb"
    ORCL_WIND = "db_wind"
    DOLPHIN = "db_dolphindb"


class DbName:
    """常用数据库名称定义"""
    REALTRADE = "CTA_REALTRADE"
    TESTTRADE = "CTA_TESTTRADE"
    RESEARCH = "CTA_RESEARCH"
    RESEARCH_5MIN = "CTA_RESEARCH_5MIN"
    RESEARCH_15MIN = "CTA_RESEARCH_15MIN"
    RESEARCH_30MIN = "CTA_RESEARCH_30MIN"
    TSDB_QUOTE = "gplshqdb"


class DateFmt(Enum):
    YMD = '%Y%m%d'
    HMS = '%H%M%S'
    Y_M_D = '%Y-%m-%d'
    H_M_S = '%H:%M:%S'
    YMDHMS = f'{YMD}{HMS}'
    YMD_H_M_S = f'{YMD} {H_M_S}'
    Y_M_D_H_M_S = f'{Y_M_D} {H_M_S}'
    dolphin_datetime = "%Y.%m.%dT%H:%M:%S.%f"


class TP:
    """ 定义常用类型 """
    # 数值
    TNum = Union[int, float]
    # 路径
    TPath = Union[PathLike, AnyStr]
    # 日期
    TDate = Union[dt.date, AnyStr]

    TSeqPath = Sequence[TPath]
    TSeqNum = Sequence[TNum]
    TSeqStr = Sequence[AnyStr]


class Contacts:
    """组内联系方式. 用户名使用tuple支持多个不同定义"""
    _ContactUser = namedtuple("ContactUser", ("alias", "mobile", "email"), defaults=((), "", ""))

    XIANGCX = _ContactUser(("xiangcx",), "", "xiangcx@cxfund.com.cn")

    ALL_USERS = [XIANGCX, ]

    @staticmethod
    def get_email(alias_name: AnyStr) -> AnyStr:
        for user in Contacts.ALL_USERS:
            if alias_name in user.alias:
                return user.email

    @staticmethod
    def get_mobile(alias_name: AnyStr) -> AnyStr:
        for user in Contacts.ALL_USERS:
            if alias_name in user.alias:
                return user.mobile
