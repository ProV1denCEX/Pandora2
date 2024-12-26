from typing import Sequence, Type

from ..event import EventEngine, Event
from ..trader.engine import MainEngine
from ..trader.gateway import BaseGateway
from ..trader.event import EVENT_LOG
from Pandora.trader.object import LogData

from .engine import TickFeedEngine


def process_log_event(event: Event) -> None:
    """"""
    log: LogData = event.data
    print(f"{log.time}\t{log.msg}")


def init_cli_feeding(gateways: Sequence[Type[BaseGateway]]):
    """"""
    event_engine: EventEngine = EventEngine()
    event_engine.register(EVENT_LOG, process_log_event)

    main_engine: MainEngine = MainEngine(event_engine)
    for gateway in gateways:
        main_engine.add_gateway(gateway)

    script_engine = main_engine.add_engine(TickFeedEngine)

    return script_engine
