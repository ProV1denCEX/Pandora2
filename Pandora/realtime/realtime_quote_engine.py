from time import sleep

from .tick_feed_engine.cli import init_cli_feeding
from .gateway import CtpGateway


class RealTimeQuoteEngine(object):
    def __init__(self):
        self.feed_engine = init_cli_feeding([CtpGateway])

    def connect(self, setting: dict = None):
        self.feed_engine.connect_gateway(setting, "CTP")
        sleep(1)

    def subscribe(self, vt_symbols: list):
        for i in vt_symbols:
            self.feed_engine.subscribe(*i.split('.'))

        sleep(1)

    def get_last_price(self, vt_symbols):
        return {i: self.feed_engine.get_last_price(*i.split('.')) for i in vt_symbols}


if __name__ == '__main__':
    engine = RealTimeQuoteEngine()

    setting = {
        "行情服务器": "218.202.237.33:10213",
    }

    engine.connect(setting)

    # sleep(1)
    tickers = ["rb2401.SHFE", ]
    engine.subscribe(tickers)
    print(engine.get_last_price(tickers))

