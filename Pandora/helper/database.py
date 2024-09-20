import warnings
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Union, List, Sequence, Dict
from urllib import parse

import cx_Oracle
import pandas as pd
import dolphindb as ddb

from sqlalchemy import text, create_engine
from sqlalchemy.exc import ResourceClosedError

from Pandora.constant import Exchange, Interval, Product, DateFmt
from Pandora.helper.config import Settings


@dataclass
class DbConfig:
    """数据库配置模型"""
    dbtype: str
    driver: str
    host: str
    port: str
    user: str
    password: str
    database: str


class DbManager:
    """数据库连接管理工具"""

    def __init__(self, conn: str, database: str = ""):
        """
            初始化连接
            :param conn:     common.py中的 DbConn
            :param database: common.py中的DbName
        """
        if conn not in Settings:
            raise ValueError(f"database connection [{conn}] not in config file!")

        self.db_engine = DbManager.step_db(conn, database)

    def query(self, sql: str) -> pd.DataFrame:
        assert sql, "query sql can't be empty!"

        with self.db_engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn)

    def execute(self, sql: str):
        """支持单条SQL语句的执行"""
        assert sql, "execute sql can't be empty!"
        with self.db_engine.connect() as conn:
            with conn.begin():
                res = conn.execute(text(sql))
                try:
                    return res.fetchall()
                except ResourceClosedError:
                    pass

    def execute_many(self, sql: Union[str, text], *params):
        """使用sqlAlchemy原生事务方式执行单条或批量的sql, 针对不同db统一的sql写法. e.g:
        dbm = DbManager("db65", dbname="yanghu")
        # 单条
        dbm.execute_sql(text("insert into dbo.accounts(username,age,address) values(:username,:age,:address)"),
                        {"username": "张三", "age": 18, "address": "上海市0x区xx路"})
        # 批量
        params = ({"username": "李四", "age": 22, "address": "上海市x2区xx路"},
                          {"username": "王五", "age": 12, "address": "上海市1x区xx路"})
        # 注意param如果是tuple就需要带  解包  操作
        dbm.execute_sql(text("insert into dbo.accounts(username,age,address) values(:username,:age,:address)"),
                        *params)
        """
        assert sql is not None, "The SQL to be executed cannot be empty!"
        params = params or ()
        sql = text(sql) if isinstance(sql, (bytes, str)) else sql
        with self.db_engine.connect() as conn:
            with conn.begin():
                res = conn.execute(sql, params)
                try:
                    return res.fetchall()
                except ResourceClosedError:
                    pass

    def upsert(
            self,
            table: str,
            data: pd.DataFrame,
            on: Union[str, List[str]],
            columns: Union[str, Sequence] = None,
            audit_columns: Dict[str, str] = None,
            identity_columns: Union[str, List[str]] = None
    ):
        """
            对指定的表使用data中的数据进行upsert操作, 当前支持Pg与Mssql.
            需要注意：pg中on字段必须有唯一索引或者是主键

        :param table: 数据库表名, 不用加 [database].[dbo] 前缀
        :param data: dataframe数据, 与table列名一致(或者指定columns参数)
        :param on: 同SQL中的on关键字, 指定需要对比的列名
        :param columns: 指定需要保存的列名. (默认使用dataframe中的所有列)
        :param audit_columns: 审计时间字段.   目前支持: {"update":"update_time", "create":"create_time"}
                              create操作时两字段都会写入值,  update时只有update_time会被更新值
        :return: None
        """
        if data.empty:
            warnings.warn("dataframe is emtpy, No other operations!")
            return

        # 当前函数支持的数据库类型
        support_db = ("postgresql", "mssql")
        db_type = self.db_engine.name
        if db_type not in support_db:
            raise ValueError(f"Currently supported database types is {support_db}")

        # 判断表名与On的空值
        if not all([table, on]):
            raise ValueError("params [table_name, on] can't be empty!")

        if db_type != 'mssql' and identity_columns:
            raise ValueError(f"identity column currently only surpported by mssql")

        # on与column包装成sequence
        on = [on] if isinstance(on, str) else on
        data_cols = data.columns
        if not columns:
            columns = data_cols
        else:
            # 判断传入的列名是否包含在dataframe columns中
            if not identity_columns:
                identity_columns = []

            if set(columns) - set(identity_columns) <= set(data_cols):
                columns = set([columns] if isinstance(columns, str) else columns + on)
            else:
                raise ValueError("[columns] Must be a column already included in the data columns!")

        if db_type == "mssql":
            exec_sql = self.join_upsert_sql_with_mssql(table, on, columns, audit_columns, identity_columns)
        else:
            exec_sql = self.join_upsert_sql_with_tsdb(table, on, columns, audit_columns)

        # 执行SQL
        data_params = data.to_dict(orient="records")
        return self.execute_many(exec_sql, *data_params)

    def insert(
            self,
            table: str,
            data: pd.DataFrame,
            columns: Union[str, Sequence] = None
    ):
        """
            对指定的表使用data中的数据进行upsert操作, 当前支持Pg与Mssql.
            需要注意：pg中on字段必须有唯一索引或者是主键

        :param table: 数据库表名, 不用加 [database].[dbo] 前缀
        :param data: dataframe数据, 与table列名一致(或者指定columns参数)
        :param on: 同SQL中的on关键字, 指定需要对比的列名
        :param columns: 指定需要保存的列名. (默认使用dataframe中的所有列)
        :param audit_columns: 审计时间字段.   目前支持: {"update":"update_time", "create":"create_time"}
                              create操作时两字段都会写入值,  update时只有update_time会被更新值
        :return: None
        """
        if data.empty:
            warnings.warn("dataframe is emtpy, No other operations!")
            return

        data_cols = data.columns
        if not columns:
            columns = data_cols

        else:
            # 判断传入的列名是否包含在dataframe columns中
            if set(columns) <= set(data_cols):
                columns = set([columns] if isinstance(columns, str) else columns)
            else:
                raise ValueError("[columns] Must be a column already included in the data columns!")

        insert_cols = ", ".join([col for col in columns])
        insert_vals = ", ".join([f":{col}" for col in columns])

        exec_sql = f"INSERT INTO {table} ({insert_cols}) VALUES ({insert_vals})"

        data_params = data.to_dict(orient="records")
        self.execute_many(exec_sql, *data_params)

    @staticmethod
    def step_db(conn: str, database: str = ""):
        """
            给定global_config中的section名称,返回连接
        :param conn: section value in config file
        :param database:  连接到新库. 如果section中已配置database, 此处可以为空.

        :return: sqlalchemy.engine.Engine
        """
        if all([not conn, conn not in Settings]):
            raise ValueError(f"database connection name [{conn}] can't find in config file!")

        # 从配置文件中获取连接
        cfg = Settings[conn]
        dbname = database or cfg["database"]

        # 防止password中包含特殊字符
        plus_pwd = parse.quote_plus(cfg["password"])
        # 构建连接串
        dsn = f"{cfg['dbtype']}+{cfg['driver']}://{cfg['user']}:{plus_pwd}@{cfg['host']}:{cfg['port']}/{dbname}"

        return create_engine(dsn, pool_size=2, pool_pre_ping=True, pool_use_lifo=True, max_overflow=1, echo_pool=True)

    @staticmethod
    def join_upsert_sql_with_mssql(table, on, columns, audits, identity) -> str:
        """拼接mssql的upsert语句"""
        query_cols = ", ".join([f":{col} {col}" for col in columns])
        on_cols = " AND ".join([f"T.{col} = S.{col}" for col in on])
        insert_cols = ", ".join([col for col in columns])
        insert_vals = ", ".join([f"S.{col}" for col in columns])
        update_cols = ", ".join([f"T.{col} = S.{col}" for col in (set(columns) - set(on))])

        if identity:
            insert_cols = ", ".join([col for col in columns if col not in identity])
            insert_vals = ", ".join([f"S.{col}" for col in columns if col not in identity])
            id_cols = ", @".join([col for col in identity])

        if not update_cols:
            raise ValueError("columns [on] cannot be equal to columns [data]!")

        # 判断是否需要添加createTime与updateTime
        audit_keys = ("create", "update")
        if audits and set(audits.keys()) <= set(audit_keys):
            # 这两字段不允许出现在columns中
            for col_name in audits.values():
                if col_name in columns:
                    raise ValueError(f"[{col_name}] is an audit field and can't exists in the data column!")

            col_create = audits.get("create")
            col_update = audits.get("update")
            if col_update:
                update_cols += f", T.[{col_update}] = GETDATE()"
            if col_create:
                insert_cols += f", {col_create}"
                insert_vals += ", GETDATE()"

        # 根据当前库的语法拼接SQL
        if identity:
            exec_sql = (f"DECLARE @{id_cols} int; "
                        f"MERGE INTO {table} T USING (SELECT {query_cols}) S ON {on_cols} "
                        f"WHEN MATCHED THEN  UPDATE SET {update_cols} "
                        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
                        f"SET @{id_cols} = scope_identity(); "
                        f"SELECT @{id_cols} AS {id_cols};")
        else:
            exec_sql = (f"MERGE INTO {table} T USING (SELECT {query_cols}) S ON {on_cols} "
                        f"WHEN MATCHED THEN  UPDATE SET {update_cols} "
                        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});")

        return exec_sql

    @staticmethod
    def join_upsert_sql_with_tsdb(table, on, columns, audits) -> str:
        """ 拼接postgres的upsert语句 """
        on_cols = ", ".join(on)
        insert_cols = ", ".join([col for col in columns])
        insert_vals = ", ".join([f":{col}" for col in columns])

        update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in (set(columns) - set(on))])
        if not update_cols:
            raise ValueError("columns [on] cannot be equal to columns [data]!")

        # 判断是否需要添加createTime与updateTime
        audit_keys = ("create", "update")
        if audits and set(audits.keys()) <= set(audit_keys):
            # 这两字段不允许出现在columns中
            for col_name in audits.values():
                if col_name in columns:
                    raise ValueError(f"[{col_name}] is an audit field and can't exists in the data column!")

            col_create = audits.get("create")
            col_update = audits.get("update")
            if col_update:
                update_cols += f", {col_update} = NOW()"
            if col_create:
                insert_cols += f", {col_create}"
                insert_vals += ", NOW()"

        exec_sql = (f"INSERT INTO {table} ({insert_cols}) VALUES ({insert_vals}) "
                    f" ON CONFLICT ({on_cols}) DO UPDATE SET {update_cols};")
        return exec_sql


class DolphinDbManager(object):
    def __init__(self, conn='db_dolphindb'):
        if all([not conn, conn not in Settings]):
            raise ValueError(f"database connection name [{conn}] can't find in config file!")
        else:
            settings = Settings[conn]

        self.user: str = settings["user"]
        self.password: str = settings["password"]
        self.host: str = settings["host"]
        self.port: int = int(settings["port"])
        self.db_path: str = "dfs://" + settings["database"]

        self.table_name = {
            'tick': 'tick',

            'bar': 'bar',
            'bar_futures': 'bar_futures',
            'bar_options': 'bar_options',

            'contract': 'contract',
            'contract_futures': 'contract_futures',
            'contract_options': 'contract_options',

            'factor': 'factor',
            'option_greeks': 'option_greeks',

            'trade_time': 'trade_time',
        }

        # 连接数据库
        self.session: ddb.session = ddb.session(keepAliveTime=600)
        self.session.connect(self.host, self.port, self.user, self.password)

        # 创建连接池（用于数据写入）
        self.pool: ddb.DBConnectionPool = ddb.DBConnectionPool(self.host, self.port, 5, self.user, self.password)

    def __del__(self) -> None:
        """析构函数"""
        if not self.session.isClosed():
            self.session.close()

    def get_table_name(self, kind, product):
        if kind == "bar":
            if product == Product.FUTURES:
                return self.table_name["bar_futures"]
            elif product == Product.OPTION:
                return self.table_name["bar_options"]
            else:
                return self.table_name["bar"]

        elif kind == "contract":
            if product == Product.FUTURES:
                return self.table_name["contract_futures"]
            elif product == Product.OPTION:
                return self.table_name["contract_options"]
            else:
                return self.table_name["contract"]

        else:
            return self.table_name[kind]

    def query(self, table, **kwargs):
        table: ddb.Table = self.session.loadTable(tableName=table, dbPath=self.db_path)

        fields = kwargs.pop("fields", "*")

        query = table.select(fields)
        for k, v in kwargs.items():
            if not v:
                continue

            if k == "start":
                start = v.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime>={start}')

            elif k == "end":
                end = v.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime<={end}')

            elif k == "where":
                query = query.where(v)

            elif isinstance(v, Enum):
                query = query.where(f'{k}="{v.value}"')

            else:
                query = query.where(f'{k}="{v}"')

        df: pd.DataFrame = query.toDF()

        return df

    def upsert(self, table, data, on):
        appender: ddb.PartitionedTableAppender = ddb.PartitionedTableAppender(self.db_path, table, on, self.pool)
        appender.append(data)

    def save_bar_data(self, df: pd.DataFrame, product=None):
        table_name = self.get_table_name("bar", product)

        self.upsert(table_name, df, "datetime")

    def load_bar_data(
        self,
        symbol: str = None,
        exchange: Exchange = None,
        product: Product = None,
        interval: Interval = None,
        start: datetime = None,
        end: datetime = None
    ) -> pd.DataFrame:
        table_name = self.get_table_name("bar", product)

        df = self.query(
            table_name,
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end
        )

        return df

    def delete_bar_data(
        self,
        symbol: Union[str, List[str]],
        exchange: Exchange = None,
        product: Product = None,
        interval: Interval = None,
        start: datetime = None,
        end: datetime = None,
    ):
        table_name = self.get_table_name("bar", product)

        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

        query = table.delete()
        if symbol:
            if isinstance(symbol, str):
                query = query.where(f'symbol="{symbol}"')

            elif isinstance(symbol, list) or isinstance(symbol, set):
                query = query.where(f'symbol in {tuple(symbol)}')

        if exchange:
            if isinstance(exchange, Exchange):
                query = query.where(f'exchange="{exchange.value}"')

            else:
                query = query.where(f'exchange="{exchange}"')

        if interval:
            if isinstance(interval, Interval):
                query = query.where(f'interval="{interval.value}"')

            else:
                query = query.where(f'interval="{interval}"')

        if start:
            start = start.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'datetime >= {start}')

        if end:
            end = end.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'datetime <= {end}')

        query.execute()

    def load_tick_data(
            self,
            symbol: str = None,
            exchange: Exchange = None,
            product: Product = None,
            start: datetime = None,
            end: datetime = None
    ) -> pd.DataFrame:
        table_name = self.get_table_name("tick", product)

        df = self.query(
            table_name,
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=end
        )

        return df

    def save_tick_data(self, df: pd.DataFrame, product=None):
        table_name = self.get_table_name("tick", product)

        self.upsert(table_name, df, "datetime")

    def save_contract_data(self, df: pd.DataFrame, product=Product.FUTURES):
        if product == Product.OPTION:
            on = "datetime"

        else:
            on = "expire_date"

        table = self.get_table_name("contract", product)

        self.upsert(table, df, on)

    def load_contract_data(
            self,
            symbol: str = None,
            product: Product = Product.FUTURES,
            start: datetime = None,
            end: datetime = None
    ) -> pd.DataFrame:
        table_name = self.get_table_name("contract", product)

        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)

        query = table.select('*')
        if symbol:
            query = query.where(f'symbol="{symbol}"')

        if product == Product.FUTURES:
            if start:
                start = start.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'expire_date>={start}')

            if end:
                end = end.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'list_date<={end}')

        elif product == Product.OPTION:
            if start:
                start = start.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime >= {start}')

            if end:
                end = end.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'datetime <= {end}')

        else:
            if product:
                query = query.where(f'product="{product.value}"')

            if start:
                start = start.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'expire_date>={start}')

            if end:
                end = end.strftime(DateFmt.dolphin_datetime.value)
                query = query.where(f'list_date<={end}')

        df: pd.DataFrame = query.toDF()

        return df

    def delete_contract_data(
            self,
            symbol: Union[str, List[str]],
            exchange: Exchange = None,
            product: Product = Product.FUTURES,
            start: datetime = None,
            end: datetime = None
    ):
        table_name = self.get_table_name("contract", product)
        if product == Product.FUTURES:
            dt_key = "list_date"

        elif product == Product.OPTION:
            dt_key = "datetime"

        else:
            raise NotImplementedError

        table: ddb.Table = self.session.loadTable(tableName=table_name, dbPath=self.db_path)
        query = table.delete()
        if symbol:
            if isinstance(symbol, str):
                query = query.where(f'symbol="{symbol}"')

            elif isinstance(symbol, list) or isinstance(symbol, set):
                query = query.where(f'symbol in {tuple(symbol)}')

        if exchange:
            if isinstance(exchange, Exchange):
                query = query.where(f'exchange="{exchange.value}"')

            else:
                query = query.where(f'exchange="{exchange}"')

        if start:
            start = start.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'{dt_key} >= {start}')

        if end:
            end = end.strftime(DateFmt.dolphin_datetime.value)
            query = query.where(f'{dt_key} <= {end}')

        query.execute()


class WindDbManager:
    """
    Deal with oracle-based database: query or non-query codes
    pymssql：http://www.lfd.uci.edu/~gohlke/pythonlibs/#pymssql
    require to open TCP/IP in Sql Server Configuration Manager
    """

    def __init__(self, conn):
        # 从配置文件中获取连接
        if all([not conn, conn not in Settings]):
            raise ValueError(f"database connection name [{conn}] can't find in config file!")
        else:
            cfg = Settings[conn]

        self.host = cfg['host']
        self.port = cfg['port']
        self.user = cfg['user']
        # 防止password中包含特殊字符
        self.password = parse.quote_plus(cfg["password"])
        self.dbname = cfg['database']
        # initialize connection variable
        self.conna = None
        self.get_connect()

    # ----------------------------------------------------------------------------------------
    """Oracle database connection"""

    def get_connect(self):
        # login to local database
        conn_info = f"{self.user}/{self.password}@{self.host}:{self.port}/{self.dbname}"
        conna = cx_Oracle.connect(conn_info)
        # close auto submit
        conna.autocommit = False
        self.conna = conna
        return conna

    # Create cursor
    def __get_cur(self, conna):
        cur = conna.cursor()
        if not cur:
            raise (NameError, "Fail to connect Oracle database")
        else:
            return cur

    def reconnect(self):
        """"""
        if self.conna is None:
            print("Lose connection to Oracle, reconnect...")
            self.conna = self.get_connect()
        else:
            try:
                self.conna.close()
                del self.conna
            except:
                del self.conna
            self.conna = self.get_connect()

    # close database connection
    def close_connect(self):
        """"""
        if self.conna is not None:
            self.conna.close()
            self.conna = None

    # ----------------------------------------------------------------------------------------
    """Query"""

    # execute query codes
    def exec_query(self, sql):
        """Execute query action"""
        # Check connection status
        self.reconnect()
        # Create cursor
        cur = self.__get_cur(self.conna)
        # Execute query
        cur.execute(sql)
        # Formatting query data
        column_names = [item[0] for item in cur.description]
        data = pd.DataFrame(list(cur.fetchall()), columns=column_names)

        return data

    # execute non-query codes
    def exec_nonquery(self, sql):
        # Check connection status
        self.reconnect()
        # Create cursor
        cur = self.__get_cur(self.conna)
        try:
            cur.execute(sql)
        except:
            print("ExecNonQuery Errors:%s..." % (sql[:min(20, len(sql))]))
            self.conna.rollback()  # rollback
        finally:
            self.conna.commit()
