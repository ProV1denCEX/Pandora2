import inspect
import shutil
import threading
from configparser import ConfigParser
from os import PathLike
from pathlib import Path
from typing import Dict, TypeVar, AnyStr, Union

from Pandora.constant import Consts, TP


def find_root_dir(fl_path: TP.TPath = None) -> str:
    """逐层向上查找, 检查当前目录下是否存在 __init__.py 文件"""
    cur_path = fl_path or __file__
    path = Path(cur_path)
    if len(path.parts) == 1:
        raise ValueError("Can't find project root dir!")

    if path.joinpath("__init__.py").exists():
        parent = Path(cur_path).parent
        return find_root_dir(parent)

    else:
        return str(path.absolute())


class Envs:
    # 默认配置路径及配置文件
    DIR_ROOT = find_root_dir(Path.cwd())
    PROJECT_NAME = Path(DIR_ROOT).parts[-1]
    DIR_CONF_ROOT = Path('~').expanduser().joinpath(f".{PROJECT_NAME}")

    if not DIR_CONF_ROOT.exists():
        PROJECT_NAME = 'Pandora'
        DIR_CONF_ROOT = Path('~').expanduser().joinpath(f".{PROJECT_NAME}")

    FILE_CONFIG = DIR_CONF_ROOT / "settings.ini"
    DIR_LOG_NAME = "Logs"
    DIR_LOGS = DIR_CONF_ROOT / DIR_LOG_NAME

    @classmethod
    def init_env(cls):
        """copy配置文件模板到用户家目录并重命名"""
        not Envs.DIR_CONF_ROOT.exists() and Envs.DIR_CONF_ROOT.mkdir(parents=True, exist_ok=True)
        not Envs.DIR_LOGS.exists() and Envs.DIR_LOGS.mkdir(parents=True, exist_ok=True)
        for tpl in Path(__file__).parent.rglob("*.template"):
            fl_cfg = cls.DIR_CONF_ROOT.joinpath(tpl.stem)
            not fl_cfg.exists() and shutil.copy2(tpl, fl_cfg)

        return Envs.DIR_CONF_ROOT


class Singleton(type):
    """单例模式定义
    如果某个class需要实现单例模式只需要添加标识即可. eg:

    class Example(metaclass=Singleton):
        ...

    """
    _instance_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            with Singleton._instance_lock:
                if not hasattr(cls, "_instance"):
                    cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class Configs:
    """配置文件解析工具"""

    @staticmethod
    def parse() -> Dict[str, Dict[str, str]]:
        """parse config file"""
        if not Envs.FILE_CONFIG.exists():
            raise RuntimeError(f"Config file {Envs.FILE_CONFIG} not exits!")

        parser = ConfigParser()
        parser.read(Envs.FILE_CONFIG, encoding=Consts.CHAR_UTF8)
        sections = parser.sections()
        if not sections:
            raise ValueError(f"Config file {Envs.FILE_CONFIG} can't empty!")

        return {sect: {opt: parser.get(sect, opt, raw=True) for opt in parser.options(sect)} for sect in sections}


# 解析配置文件, 模块被加载就会自动读取且只会读取一次
try:
    Settings = Configs.parse()

except (RuntimeError, ValueError):
    Settings = {}


class Models:
    """将配置文件映射到dataclass, 方便以对象方式处理配置文件"""
    Model = TypeVar('Model')

    @staticmethod
    def mapping(model_type: Model, section: AnyStr,
                rename: Dict = None, conf_file: Union[AnyStr, PathLike] = None) -> Model:
        """
            ini中配置与model模型一致的属性, 使用以下方式动态构建对象:
            db_cfg = Models.mapping_model(DbConfig, "tsdb_conn")

        :param model_type: 模型类型
        :param section: 需要读取的section,必须要保证section配置与model属性一致
        :param rename: 配置文件与模型定义不一致的可以做名称重映射
        :param conf_file: 自定义需要读取的配置文件, 默认为系统定义的配置文件

        :return model_type实例
        """
        global Settings
        if not Settings:
            Settings = Configs.parse()

        if section not in Settings:
            raise ValueError(f"section[{section}] not in config file!")
        # 使用副本处理
        item_cfg = Settings[section].copy()
        # 初始化前对属性名称进行重映射
        if rename:
            for k in rename:
                item_cfg[rename[k]] = item_cfg.pop(k)

        # 获取类名, 反射获取module_name
        cls_name = model_type.__name__
        mod_parts = Path(inspect.getmodule(model_type).__file__).relative_to(Envs.DIR_ROOT).parts
        mod_path = ".".join([*mod_parts[:-1], Path(mod_parts[-1]).stem])

        # 动态导入并构建对象
        try:
            module_meta = __import__(mod_path, globals(), locals(), [cls_name])

        except ModuleNotFoundError:
            mod_parts = mod_parts[1:]
            mod_path = ".".join([*mod_parts[:-1], Path(mod_parts[-1]).stem])
            module_meta = __import__(mod_path, globals(), locals(), [cls_name])

        class_meta = getattr(module_meta, cls_name)
        model = class_meta(**item_cfg)

        return model

    @staticmethod
    def mapping_dynamic(section: str) -> Model:
        """ 对配置文件中的每个section动态创建一个model实例. 实例属性与配置文件一致.

            :param section 配置文件中的title

            :return 动态创建的实例对象
        """
        global Settings
        if not Settings:
            Settings = Configs.parse()

        if section not in Settings:
            raise ValueError(f"section[{section}] not in config file!")
        return type(section, (object,), Settings[section])
