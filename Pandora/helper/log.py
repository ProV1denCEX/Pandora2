import logging
import os.path
import re
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from Pandora.helper.config import Envs


class BaseLogger(object):
    """
    Processes log event and output with logging module.
    """

    def __init__(self, name: str, config: dict, log_dir: str = None):
        """"""
        self.name = name
        self.config = config
        self.config['cls'] = self.__class__.__name__
        self.active = config.get("ACTIVE", True)
        self.level = config.get("LEVEL", logging.INFO)
        self.console = config.get("CONSOLE", True)
        self.file = config.get("FILE", True)
        self.file_rotate = config.get("FILE_ROTATE", None)

        self.logger: logging.Logger = logging.getLogger(name)
        self.logger.setLevel(self.level)

        self.log_dir = Path(log_dir) if log_dir else Envs.DIR_CONF_ROOT
        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)

        self.formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s: %(message)s"
        )

        self.add_null_handler()

        if self.console:
            self.add_console_handler()

        if self.file:
            self.add_file_handler()

    def add_null_handler(self) -> None:
        """
        Add null handler for logger.
        """
        null_handler = logging.NullHandler()
        self.logger.addHandler(null_handler)

    def add_console_handler(self) -> None:
        """
        Add console output of log.
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

    def add_file_handler(self) -> None:
        """
        Add file output of log.
        """
        if self.file_rotate:
            file_handler = TimedRotatingFileHandler(
                self.file_path,
                encoding='utf8',
                when=self.file_rotate
            )
            file_handler.suffix = "%Y%m%d"
            file_handler.extMatch = re.compile(r"^\d{8}}(\.\w+)?$", re.ASCII)

        else:
            file_handler = logging.FileHandler(
                self.file_path, mode="a", encoding="utf8"
            )

        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def write_log(self, msg: str, level=logging.INFO) -> None:
        """
        Process log event.
        """
        self.logger.log(level, msg)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    @property
    def file_name(self):
        return f"{self.name}.log"

    @property
    def file_path(self):
        return os.path.join(self.log_dir, self.file_name)


class Logs:
    """
        日志工具类, 不支持多进程.
    """

    @staticmethod
    def init_log(app_name: str, log_dir=None) -> Logger:
        assert app_name, "app log name can't be empty!"

        LEVEL_TRACE = logging.DEBUG - 5
        logging.addLevelName(LEVEL_TRACE, "TRACE")

        logger = logging.getLogger(app_name)
        logger.setLevel(level=logging.DEBUG)

        # add trace level
        def _trace(message, *args, **kwargs):
            if logger.isEnabledFor(LEVEL_TRACE):
                logger._log(LEVEL_TRACE, message, args, **kwargs)

        logger.trace = _trace

        # Formatter
        fmt = "%(asctime)s [%(levelname)s] [%(filename)s:%(funcName)s:%(lineno)d] - %(message)s"
        formatter = logging.Formatter(fmt)

        # FileHandler
        # fl = Consts.DIR_TEMP_LOGS / f"{app_name}.log"
        log_dir = Path(log_dir) if log_dir else Envs.DIR_CONF_ROOT
        fl = log_dir / f"{app_name}.log"
        # 避免logger重复添加多个handler
        if not logger.handlers:
            # FileHandler
            file_handler = TimedRotatingFileHandler(filename=fl, when='D', encoding="utf-8", backupCount=7)
            file_handler.setFormatter(formatter)
            # 调整suffix必须同步调整extMatch否则backupCount不生效
            file_handler.suffix = "%Y%m%d"
            file_handler.extMatch = re.compile(r"^\d{8}}(\.\w+)?$", re.ASCII)
            logger.addHandler(file_handler)

            # StreamHandler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        return logger
