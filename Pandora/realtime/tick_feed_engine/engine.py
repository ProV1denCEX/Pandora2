from typing import Optional, Sequence, Any

from pandas import DataFrame

from ..trader.engine import BaseEngine
from ..trader.object import (
    BaseData,
    SubscribeRequest,
    TickData,
)
from ...constant import Exchange


class TickFeedEngine(BaseEngine):
    """"""
    def __init__(self, main_engine, event_engine):
        """"""
        super().__init__(main_engine, event_engine, "TickFeedEngine")

    def connect_gateway(self, setting: dict, gateway_name: str) -> None:
        """"""
        self.main_engine.connect(setting, gateway_name)

    def subscribe(self, symbol, exchange) -> None:
        """"""
        if isinstance(exchange, str):
            exchange = Exchange.mapping(exchange)

        req: SubscribeRequest = SubscribeRequest(
            symbol=symbol,
            exchange=exchange
        )
        self.main_engine.subscribe(req, "CTP")

    def get_last_price(self, symbol, exchange):
        if isinstance(exchange, Exchange):
            exchange = exchange.value

        tick = self.main_engine.get_tick('.'.join([symbol, exchange]))
        if tick:
            return tick.last_price

    def get_tick(self, vt_symbol: str, use_df: bool = False) -> TickData:
        """"""
        return get_data(self.main_engine.get_tick, arg=vt_symbol, use_df=use_df)

    def get_ticks(self, vt_symbols: Sequence[str], use_df: bool = False) -> Sequence[TickData]:
        """"""
        ticks: list = []
        for vt_symbol in vt_symbols:
            tick: TickData = self.main_engine.get_tick(vt_symbol)
            ticks.append(tick)

        if not use_df:
            return ticks
        else:
            return to_df(ticks)


def to_df(data_list: Sequence) -> Optional[DataFrame]:
    """"""
    if not data_list:
        return None

    dict_list: list = [data.__dict__ for data in data_list]
    return DataFrame(dict_list)


def get_data(func: callable, arg: Any = None, use_df: bool = False) -> BaseData:
    """"""
    if not arg:
        data = func()
    else:
        data = func(arg)

    if not use_df:
        return data
    elif data is None:
        return data
    else:
        if not isinstance(data, list):
            data = [data]
        return to_df(data)
