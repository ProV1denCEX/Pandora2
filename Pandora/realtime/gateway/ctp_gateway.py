import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from ..api import (
    MdApi
)
from ..event import EventEngine
from ..trader.constant import (
    Exchange
)
from ..trader.gateway import BaseGateway
from ..trader.object import (
    TickData,
    SubscribeRequest,
)
from ..trader.utility import get_folder_path

# 交易所映射
EXCHANGE_CTP2VT: Dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "GFEX": Exchange.GFEX
}

# 其他常量
MAX_FLOAT = sys.float_info.max  # 浮点数极限值

# 合约数据全局缓存字典
symbol_exchange_map: Dict[str, Exchange] = {}


class CtpGateway(BaseGateway):
    """
    VeighNa用于对接期货CTP柜台的交易接口。
    """

    default_name: str = "CTP"

    default_setting: Dict[str, str] = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": ""
    }

    exchanges: List[str] = list(EXCHANGE_CTP2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api: "CtpMdApi" = CtpMdApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        md_address: str = setting["行情服务器"]

        if (
                (not md_address.startswith("tcp://"))
                and (not md_address.startswith("ssl://"))
                and (not md_address.startswith("socks"))
        ):
            md_address = "tcp://" + md_address

        self.md_api.connect(md_address, "", "", "")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def close(self) -> None:
        """关闭接口"""
        self.md_api.close()

    def write_error(self, msg: str, error: dict) -> None:
        """输出错误信息日志"""
        error_id: int = error["ErrorID"]
        error_msg: str = error["ErrorMsg"]
        msg: str = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg)


class CtpMdApi(MdApi):
    """"""

    def __init__(self, gateway: CtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.reqid: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.subscribed: set = set()

        self.userid: str = ""
        self.password: str = ""
        self.brokerid: str = ""

        self.current_date: str = datetime.now().strftime("%Y%m%d")

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error("行情服务器登录失败", error)

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        """请求报错回报"""
        self.gateway.write_error("行情接口报错", error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """订阅行情回报"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error("行情订阅失败", error)

    def onRtnDepthMarketData(self, data: dict) -> None:
        """行情数据推送"""
        # 过滤没有时间戳的异常行情数据
        if not data["UpdateTime"]:
            return

        # 过滤还没有收到合约数据前的行情推送
        symbol: str = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, None)
        if not exchange:
            return

        # 对大商所的交易日字段取本地日期
        if not data["ActionDay"] or exchange == Exchange.DCE:
            date_str: str = self.current_date
        else:
            date_str: str = data["ActionDay"]

        timestamp: str = f"{date_str} {data['UpdateTime']}.{int(data['UpdateMillisec'] / 100)}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        # dt: datetime = dt.replace(tzinfo=CHINA_TZ)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=dt,
            name=symbol,
            volume=data["Volume"],
            turnover=data["Turnover"],
            open_interest=data["OpenInterest"],
            last_price=adjust_price(data["LastPrice"]),
            limit_up=data["UpperLimitPrice"],
            limit_down=data["LowerLimitPrice"],
            open_price=adjust_price(data["OpenPrice"]),
            high_price=adjust_price(data["HighestPrice"]),
            low_price=adjust_price(data["LowestPrice"]),
            pre_close=adjust_price(data["PreClosePrice"]),
            bid_price_1=adjust_price(data["BidPrice1"]),
            ask_price_1=adjust_price(data["AskPrice1"]),
            bid_volume_1=data["BidVolume1"],
            ask_volume_1=data["AskVolume1"],
            gateway_name=self.gateway_name
        )

        if data["BidVolume2"] or data["AskVolume2"]:
            tick.bid_price_2 = adjust_price(data["BidPrice2"])
            tick.bid_price_3 = adjust_price(data["BidPrice3"])
            tick.bid_price_4 = adjust_price(data["BidPrice4"])
            tick.bid_price_5 = adjust_price(data["BidPrice5"])

            tick.ask_price_2 = adjust_price(data["AskPrice2"])
            tick.ask_price_3 = adjust_price(data["AskPrice3"])
            tick.ask_price_4 = adjust_price(data["AskPrice4"])
            tick.ask_price_5 = adjust_price(data["AskPrice5"])

            tick.bid_volume_2 = data["BidVolume2"]
            tick.bid_volume_3 = data["BidVolume3"]
            tick.bid_volume_4 = data["BidVolume4"]
            tick.bid_volume_5 = data["BidVolume5"]

            tick.ask_volume_2 = data["AskVolume2"]
            tick.ask_volume_3 = data["AskVolume3"]
            tick.ask_volume_4 = data["AskVolume4"]
            tick.ask_volume_5 = data["AskVolume5"]

        self.gateway.on_tick(tick)

    def connect(self, address: str, userid: str, password: str, brokerid: str) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.brokerid = brokerid

        # 禁止重复发起连接，会导致异常崩溃
        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi((str(path) + "\\Md").encode("GBK"))

            self.registerFront(address)
            self.init()

            self.connect_status = True

    def login(self) -> None:
        """用户登录"""
        ctp_req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid
        }

        self.reqid += 1
        self.reqUserLogin(ctp_req, self.reqid)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if self.login_status:
            self.subscribeMarketData(req.symbol)
        self.subscribed.add(req.symbol)

        symbol_exchange_map[req.symbol] = req.exchange

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()

    def update_date(self) -> None:
        """更新当前日期"""
        self.current_date = datetime.now().strftime("%Y%m%d")


def adjust_price(price: float) -> float:
    """将异常的浮点数最大值（MAX_FLOAT）数据调整为0"""
    if price == MAX_FLOAT:
        price = 0
    return price
