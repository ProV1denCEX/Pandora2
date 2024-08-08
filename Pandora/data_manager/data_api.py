# -*- coding:utf-8 -*-
import datetime as dt
from datetime import date
from typing import Union, Sequence, Tuple, List

import pandas as pd
from dateutil import parser

from Pandora.constant import DbConn, DbName, Frequency, EdbType, Method, Label, Sample, SYMBOL_MAP, COM_EXCHANGE, \
    FIN_EXCHANGE, Interval, SymbolSuffix, Product
from Pandora.helper.string import Strs, Symbol
from Pandora.helper.date import Dates, DateFmt
from Pandora.helper.database import DbManager, WindDbManager, DolphinDbManager


class FutureDataAPI:
    def __init__(self, real_trade=False):
        self.real_trade = real_trade

        self.mssql_65 = self.mssql_165 = DbManager(DbConn.MSSQL_165)
        # self.mssql_165 = DbManager(DbConn.MSSQL_165)
        # self.orcl_wind = WindDbManager(DbConn.ORCL_WIND)

        self.dolphindb = DolphinDbManager(DbConn.DOLPHIN)

        if self.real_trade:
            self.mssql_162 = DbManager(DbConn.MSSQL_165, DbName.REALTRADE)

        else:
            self.mssql_162 = DbManager(DbConn.MSSQL_165, DbName.TESTTRADE)

    """
       Account Risk Management 
    """

    def get_account_name(self, account: str = None):
        table_name = "dbo.FundNameInfo"
        sql = f"SELECT FundName, ChineseName FROM {table_name} WHERE 1 = 1 "

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("FundName", account)

        return self.mssql_162.query(sql)

    def get_risk_config(self,
                        account: str = None,
                        strategy: str = None,
                        begin_date=None,
                        end_date=None,
                        table_name: str = "dbo.RiskInfo_Config"
                        ):

        sql = f"SELECT TradeDate, FundName, Strategy, Section, Name, Value From {table_name} WHERE Valid = 1 "

        if begin_date or end_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("TradeDate", begin_date, end_date)

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("FundName", account)

        if strategy:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("Strategy", strategy)

        sql += f" ORDER BY TradeDate"

        return self.mssql_162.query(sql)

    def get_risk_weight(self,
                        account: str = None,
                        strategy: str = None,
                        begin_date=None,
                        end_date=None
                        ):

        table_name = "dbo.RiskInfo_Weight"
        sql = f"SELECT TradeDate, FundName, Strategy, Weight From {table_name} WHERE Valid = 1 "

        if begin_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("TradeDate", begin_date, None)

        if end_date:
            sql += " AND "
            sql += f" TradeDate < '{end_date}'"

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("FundName", account)

        if strategy:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("Strategy", strategy)

        sql += f" ORDER BY TradeDate"

        return self.mssql_162.query(sql)

    def get_risk_detail(self,
                        account: str = None,
                        strategy: str = None,
                        begin_date=None,
                        end_date=None
                        ):

        table_name = "dbo.RiskInfo_Detail"
        sql = f"SELECT TradeDate, FundName, Strategy, Name, Value From {table_name} WHERE Valid = 1 "

        if begin_date or end_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("TradeDate", begin_date, end_date)

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("FundName", account)

        if strategy:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("Strategy", strategy)

        sql += f" ORDER BY TradeDate"

        return self.mssql_162.query(sql)

    def get_account_config(self, today, table_name: str = "dbo.RiskInfo_Config"):
        sql = (
            f"SELECT {table_name}.TradeDate, "
            f"       {table_name}.FundName, "
            f"       {table_name}.Name, "
            f"       {table_name}.Value, "
            f"       {table_name}.Section "
            f"FROM {table_name} "
            f"INNER JOIN "
            f"  (SELECT FundName, Strategy, MAX(TradeDate) as TradeDate FROM {table_name} "
            f"  WHERE Name = 'active' AND Valid = 1 AND FundName = Strategy AND TradeDate <= '{today}' GROUP BY FundName, Strategy) AS tmp "
            f"ON "
            f"  {table_name}.TradeDate = tmp.TradeDate "
            f"  AND {table_name}.FundName = tmp.FundName "
            f"  AND {table_name}.Strategy = tmp.Strategy "
            f"WHERE Valid = 1 "
            f"ORDER BY {table_name}.TradeDate, {table_name}.FundName, {table_name}.Section, {table_name}.Name "
        )

        return self.mssql_162.query(sql)

    def get_strategy_config(self, today, category: str, table_name: str = "dbo.RiskInfo_Config"):
        sql = (
            f"SELECT {table_name}.TradeDate, "
            f"       {table_name}.FundName, "
            f"       {table_name}.Strategy, "
            f"       {table_name}.Name, "
            f"       {table_name}.Value, "
            f"       {table_name}.Section "
            f"FROM {table_name} "
            f"INNER JOIN "
            f"  (SELECT FundName, Strategy, MAX(TradeDate) as TradeDate FROM {table_name} "
            f"  WHERE Name = 'category' AND Value = '{category}' AND Valid = 1 AND TradeDate <= '{today}' GROUP BY FundName, Strategy) AS tmp "
            f"ON "
            f"  {table_name}.TradeDate = tmp.TradeDate "
            f"  AND {table_name}.FundName = tmp.FundName "
            f"  AND {table_name}.Strategy = tmp.Strategy "
            f"  AND {table_name}.FundName != {table_name}.Strategy "
            f"WHERE Valid = 1 "
            f"ORDER BY {table_name}.TradeDate, {table_name}.FundName, {table_name}.Strategy, {table_name}.Section, {table_name}.Name "
        )

        return self.mssql_162.query(sql)

    def delete_risk_config(self, trade_date=None, account: str = None, strategy: str = None,
                           table_name: str = "dbo.RiskInfo_Config"):
        if any((trade_date, account, strategy)):
            sql = f"DELETE FROM {table_name} WHERE 1 = 1 "

            if trade_date:
                trade_date = dt.datetime.strftime(trade_date, '%Y-%m-%d')

                sql += " AND "
                sql += f" TradeDate = '{trade_date}'"

            if account:
                sql += " AND "
                sql += FutureDataAPI.pair_equals("FundName", account)

            if strategy:
                sql += " AND "
                sql += FutureDataAPI.pair_equals("Strategy", strategy)

        else:
            sql = f"TRUNCATE TABLE {table_name}"

        return self.mssql_162.execute(sql)

    def pesu_del_risk_table(self, invalid, table_name):
        tmp = invalid.loc[:, ['TradeDate', "FundName", "Strategy"]].drop_duplicates()

        for i in tmp.index:
            trade_date = tmp.loc[i, 'TradeDate']
            fund_name = tmp.loc[i, "FundName"]
            strategy = tmp.loc[i, "Strategy"]
            sql_up = f"UPDATE {table_name} SET Valid = 0 " \
                     f"FROM {table_name} " \
                     f"WHERE TradeDate = '{trade_date}' AND FundName = '{fund_name}' AND Strategy = '{strategy}' AND Valid = 1"
            self.mssql_162.execute(sql_up)

    def pesu_del_risk_config(self, invalid):
        table_name = "dbo.RiskInfo_Config"
        self.pesu_del_risk_table(invalid, table_name)

    def save_risk_config(self, config: pd.DataFrame, table_name: str = "dbo.RiskInfo_Config"):
        # cols = ['TradeDate', 'FundName', 'Strategy', 'Section', 'Name']
        # audit_col = {"update": "UpdateTime"}
        # return self.mssql_162.upsert(table=table_name, data=config, on=cols, audit_columns=audit_col)

        return self.mssql_162.insert(table=table_name, data=config)

    def pesu_del_risk_weight(self, invalid: pd.DataFrame):
        table_name = "dbo.RiskInfo_Weight"
        self.pesu_del_risk_table(invalid, table_name)

    def save_risk_weight(self, update: pd.DataFrame):
        table_name = "dbo.RiskInfo_Weight"
        cols = ['TradeDate', 'FundName', 'Strategy', 'UpdateTime']
        return self.mssql_162.upsert(table=table_name, data=update, on=cols)

    def pesu_del_risk_detail(self, invalid: pd.DataFrame):
        table_name = "dbo.RiskInfo_Detail"
        self.pesu_del_risk_table(invalid, table_name)

    def save_risk_detail(self, data):
        table_name = "dbo.RiskInfo_Detail"
        # cols = ['TradeDate', 'FundName', 'Strategy', 'Name', 'UpdateTime']
        # return self.mssql_162.upsert(table=table_name, data=data, on=cols)
        return self.mssql_162.insert(table=table_name, data=data)

    def get_risk_cache_columns(self):
        table_name = "dbo.RiskInfo_Cache"

        ret = self.mssql_162.execute(f"SELECT name from syscolumns WHERE id = OBJECT_ID('{table_name}')")

        return [i.values()[0] for i in ret]

    def get_risk_cache(self,
                       account: str = None,
                       strategy: str = None,
                       begin_date=None,
                       end_date=None
                       ):

        table_name = "dbo.RiskInfo_Cache"
        sql = f"SELECT * From {table_name} WHERE 1 = 1 "

        if begin_date or end_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("TradeDate", begin_date, end_date)

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("FundName", account)

        if strategy:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("Strategy", strategy)

        sql += f" ORDER BY TradeDate"

        return self.mssql_162.query(sql)

    def save_risk_cache(self, data):
        table_name = "RiskInfo_Cache"
        # cols = ['TradeDate', 'FundName', 'Strategy', 'ID']
        # return self.mssql_162.upsert(table=table_name, data=data, on=cols)
        return data.to_sql(table_name, self.mssql_162.db_engine, index=False,
                           if_exists='append')  # use to sql to insert null value

    def delete_risk_cache(self, series_id: str = None, from_date=None, table_name: str = "dbo.RiskInfo_Cache"):
        if any((from_date, series_id)):
            sql = f"DELETE FROM {table_name} WHERE 1 = 1 "

            if from_date:
                trade_date = dt.datetime.strftime(from_date, '%Y-%m-%d')

                sql += " AND "
                sql += f" TradeDate >= '{trade_date}'"

            if series_id:
                sql += " AND "
                sql += FutureDataAPI.pair_equals("ID", [series_id])

        else:
            sql = f"TRUNCATE TABLE {table_name}"

        return self.mssql_162.execute(sql)

    def save_risk_operation(self, trade_date, stakeholder, item, message, level, machine_name, logdir):
        table_name = "dbo.RiskInfo_Operation"

        data = pd.DataFrame({
            "TradeDate": trade_date,
            "StakeHolder": str(stakeholder),
            "Item": item,
            "Message": message,
            "Level": str(level),
            "MachineName": machine_name,
            "LogDir": logdir,
        }, index=[0])

        return self.mssql_162.insert(table_name, data)

    def update_risk_table(self, table_name, value_mod, **kwargs):
        sql = f"UPDATE {table_name} SET Value = '{value_mod}' WHERE 1 = 1 "

        for k, v in kwargs.items():
            sql += " AND "
            sql += FutureDataAPI.pair_equals(k, v)

        self.mssql_162.execute(sql)

    def update_risk_detail(
            self,
            trade_date=None,
            account: str = None,
            strategy: str = None,
            name: str = None,
            value_to_mod: str = None,
            value_mod: str = None,
            valid: bool = None
    ):
        table_name: str = "dbo.RiskInfo_Detail"
        kwargs = {}

        if trade_date:
            kwargs['TradeDate'] = dt.datetime.strftime(trade_date, '%Y-%m-%d')

        if account:
            kwargs['FundName'] = account

        if strategy:
            kwargs['Strategy'] = strategy

        if name:
            kwargs['Name'] = name

        if value_to_mod:
            kwargs['Value'] = value_to_mod

        if valid is not None:
            kwargs['Valid'] = valid

        return self.update_risk_table(table_name, value_mod, **kwargs)

    def update_risk_config(
            self,
            trade_date=None,
            account: str = None,
            strategy: str = None,
            section: str = None,
            name: str = None,
            value_to_mod: str = None,
            value_mod: str = None,
            valid: bool = None
    ):
        table_name: str = "dbo.RiskInfo_Config"
        kwargs = {}

        if trade_date:
            kwargs['TradeDate'] = dt.datetime.strftime(trade_date, '%Y-%m-%d')

        if account:
            kwargs['FundName'] = account

        if strategy:
            kwargs['Strategy'] = strategy

        if name:
            kwargs['Name'] = name

        if value_to_mod:
            kwargs['Value'] = value_to_mod

        if section:
            kwargs['Section'] = section

        if valid is not None:
            kwargs['Valid'] = valid

        return self.update_risk_table(table_name, value_mod, **kwargs)

    def get_account_nav(self, account: str, begin_date=None, end_date=None) -> pd.DataFrame:
        table_name = "dbo.AccountNav"
        sql = f"SELECT Date as TradeDate, Share, Aum, Nav, AccumNav, AccumDividend " \
              f"From {table_name} " \
              f"WHERE Account = '{account}' "

        if begin_date or end_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("Date", begin_date, end_date)

        sql += f" ORDER BY TradeDate"

        return self.mssql_162.query(sql)

    def get_strategy_group_pnl(self,
                               account: str = None,
                               strategy: str = None,
                               begin_date=None,
                               end_date=None) -> pd.DataFrame:
        table_name = "dbo.StrategyDailyRtn"
        table_mapping = "dbo.StrategyNameMapping"
        sql = f"SELECT t1.TradeDate, " \
              f"       t1.FundName, " \
              f"       t2.StrategyAttr StrategyName, " \
              f"       SUM(t1.Pnl) PNL " \
              f"From {table_name} t1 " \
              f"INNER JOIN {table_mapping} t2 " \
              f"ON t1.StrategyName=t2.StrategyName WHERE 1 = 1 "

        if begin_date or end_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("TradeDate", begin_date, end_date)

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("t1.FundName", account)

        if strategy:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("t2.StrategyAttr", strategy)

        sql += f" GROUP BY t1.TradeDate, t1.FundName, t2.StrategyAttr"
        sql += f" ORDER BY t1.TradeDate"

        return self.mssql_162.query(sql)

    def get_strategy_pnl(self,
                         account: str = None,
                         strategy: str = None,
                         begin_date=None,
                         end_date=None) -> pd.DataFrame:

        table_name = "dbo.StrategyDailyRtn"
        sql = f"SELECT TradeDate, FundName, StrategyName, PNL From {table_name} WHERE 1 = 1 "

        if begin_date or end_date:
            sql += " AND "
            sql += FutureDataAPI.pair_dates("TradeDate", begin_date, end_date)

        if account:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("FundName", account)

        if strategy:
            sql += " AND "
            sql += FutureDataAPI.pair_equals("StrategyName", strategy)

        sql += f" ORDER BY TradeDate"

        return self.mssql_162.query(sql)

    def get_strategy_map(self) -> pd.DataFrame:
        table_name = "dbo.StrategyNameMapping"
        sql = f"SELECT StrategyAttr, StrategyName From {table_name} WHERE Mode = 'Live' "

        return self.mssql_162.query(sql)

    """
        Future Quote
    """

    def get_future_contracts(self, codes: Union[str, Sequence[str]] = "") -> pd.DataFrame:
        contract = self.dolphindb.load_contract_data(symbol=codes, product=Product.FUTURES)

        return contract

    def get_future_basic(self, codes: Union[str, Sequence[str]] = "") -> pd.DataFrame:
        """
            获取期货基本信息

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]

        :return: DataFrame
        columns=(Code ,Ticker ,Contract ,ListDate ,DelistDate ,DeliverDate,Multiplier,TickSize, Exchange,
               MinMargin ,PriceLimit ,QuoteUnit ,MultiplierUnit ,FutureClass ,ComFutureClass ,UpdateTime)
        """
        table_name = "dbo.FutureInfo_Basic"
        sql = ("SELECT Code ,Ticker ,Contract ,ListDate ,DelistDate ,DeliverDate,Multiplier,TickSize, Exchange,"
               " MinMargin ,PriceLimit ,QuoteUnit ,MultiplierUnit ,FutureClass ,ComFutureClass ,UpdateTime "
               f" FROM  {table_name}")
        if not codes:
            return self.mssql_65.query(sql)

        contracts, tickers = FutureDataAPI.split_codes(codes)
        cond = [FutureDataAPI.pair_equals("contract", contracts),
                FutureDataAPI.pair_equals("ticker", tickers)]
        cond = [c for c in cond if c.strip()]

        cond_str = " OR ".join(cond) if cond else ""
        sql += f" WHERE {cond_str}"

        return self.mssql_65.query(sql)

    def get_future_class(self) -> pd.DataFrame:
        """
        获取期货板块信息

        :return: DataFrame
        columns=(Contract ,Sector ,Subsector)
        """
        table_name = "dbo.FutureInfo_Class"
        sql = ("SELECT Contract ,FutureClass as Sector ,FutureSubClass as SubSector"
               f" FROM  {table_name}")

        return self.mssql_165.query(sql)

    def get_quote(
            self,
            codes: Union[str, Sequence[str]],
            product: Product,
            begin_date: Union[str, date],
            end_date: Union[str, date],
            fields: Union[str, Sequence[str]] = None,
            freq=Frequency.Min_1
    ) -> pd.DataFrame:
        tab_name = self.dolphindb.get_table_name('bar', product)

        cond_str = FutureDataAPI.pair_equals("symbol", codes)

        if isinstance(begin_date, str):
            begin_date = parser.parse(timestr=begin_date, fuzzy=True)

        if isinstance(end_date, str):
            end_date = parser.parse(timestr=end_date, fuzzy=True)

        if isinstance(freq, Interval):
            interval = freq

        elif isinstance(freq, Frequency):
            interval = Interval.from_freq(freq)

        elif isinstance(freq, str):
            interval = Interval.from_str(freq)

        else:
            raise NotImplementedError

        if isinstance(fields, str):
            fields = [fields]

        if fields:
            fields = [Strs.camel_to_snake(i) for i in fields]

        else:
            fields = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover', 'open_interest']

        fields = "datetime,symbol," + ','.join(fields)

        df_quote = self.dolphindb.query(
            tab_name,
            fields=fields,
            interval=interval,
            start=begin_date,
            end=end_date,
            where=cond_str
        )

        return df_quote

    def get_future_quote(
            self,
            codes: Union[str, Sequence[str]],
            begin_date: Union[str, date],
            end_date: Union[str, date],
            filter_time=True,
            fields: Union[str, Sequence[str]] = None,
            freq=Frequency.Min_1
    ) -> pd.DataFrame:

        if isinstance(freq, Interval):
            interval = freq

        elif isinstance(freq, Frequency):
            interval = Interval.from_freq(freq)

        elif isinstance(freq, str):
            interval = Interval.from_str(freq)

        else:
            raise NotImplementedError

        df_quote = self.get_quote(
            codes,
            Product.FUTURES,
            begin_date,
            end_date,
            fields=fields,
            freq=interval,
        )

        if filter_time and interval != Interval.DAILY:
            df_quote = self.filtering_time(df_quote)

        return df_quote

    def get_future_quote_main_adj(
            self,
            codes: Union[str, Sequence[str]],
            begin_date: Union[str, date],
            end_date: Union[str, date],
            adjusted=True,
            filter_time=True,
            fields: Union[str, Sequence[str]] = None,
            freq=Frequency.Daily
    ) -> pd.DataFrame:
        """
            获取主连复权/不复权行情

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]
        :param begin_date: 开始日期
        :param end_date:  结束日期
        :param adjusted: 是否为复权数据, 默认true
        :param filter_time: 是否过滤掉23点以后数据, 默认true
        :param fields: 取用的字段
        :param freq: 行情频次(enum) 1/5/15/30/60/daily(默认)

        :return: DataFrame
        columns=(Datetime,Date,TradeDate,Contract,Ticker,[OHLC],PrevClose,Volume,Amount,DealAmount,OpenInterest)
        """

        if codes:
            contracts = {Symbol.get_contract(i, upper=False) for i in codes}

            symbol_suffix = SymbolSuffix.MC if adjusted else SymbolSuffix.MNC
            symbols = [i + symbol_suffix for i in contracts]

        else:
            suffix = SymbolSuffix.MC if adjusted else SymbolSuffix.MNC
            symbols = self.dolphindb.query(
                self.dolphindb.get_table_name("contract", Product.FUTURES),
                fields="symbol",
                where=f'symbol.endsWith("{suffix}")'
            )

            symbols = symbols['symbol'].unique().tolist()

        df_quote = self.get_future_quote(
            codes=symbols,
            begin_date=begin_date,
            end_date=end_date,
            filter_time=filter_time,
            fields=fields,
            freq=freq
        )

        return df_quote

    def get_future_quote_mssql(
            self,
            codes: Union[str, Sequence[str]],
            begin_date: Union[str, date],
            end_date: Union[str, date],
            fields: Union[str, Sequence[str]] = None,
            freq=Frequency.Daily
    ):
        assert freq == Frequency.Daily

        if fields:
            if not isinstance(fields, str):
                fields = ','.join([i for i in fields])

            sql = f"SELECT {fields} FROM FutureQuote_Daily"

        else:
            sql = "SELECT * FROM FutureQuote_Daily"

        cond = [
            FutureDataAPI.pair_equals("Ticker", codes),
            FutureDataAPI.pair_dates("Date", begin_date, end_date)
        ]

        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" WHERE {cond_str}"

        df_quote = self.mssql_165.query(sql)

        return df_quote

    @staticmethod
    def filtering_time(df_quote):
        loc1 = (df_quote['datetime'].dt.hour < 23) & (df_quote['datetime'].dt.hour > 3)
        return df_quote[loc1]

    def get_future_index_quote(self, codes: Union[str, Sequence[str]],
                               begin_date: Union[str, date] = None,
                               end_date: Union[str, date] = None) -> pd.DataFrame:
        """ 获取商品指数行情

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]
        :param begin_date: 开始日期
        :param end_date:  结束日期

        :return: DataFrame
        columns=(Date,Ticker,Name,Close,UpdateTime)
        """
        table_name = "public.future_quote_index_daily"
        sql = f"SELECT date,ticker,name,close,update_time FROM {table_name} WHERE "
        sql += FutureDataAPI.pair_dates("Date", begin_date, end_date)
        cond = [FutureDataAPI.pair_equals("ticker", codes),
                FutureDataAPI.pair_equals("name", codes)]
        cond = [c for c in cond if c.strip()]
        cond_str = " OR ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""

        # 列名转换成大驼峰
        df_index = self.tsdb.query(sql)
        df_index.columns = [Strs.snake_to_camel(c) for c in df_index.columns]

        return df_index

    def get_future_main_ticker(self, codes: Union[str, Sequence[str]],
                               begin_date: Union[str, date],
                               end_date: Union[str, date]) -> pd.DataFrame:
        """
            获取主力合约

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]
        :param begin_date: 开始日期
        :param end_date:  结束日期

        :return: DataFrame
        columns=(Date,Contract,Ticker,UpdateTime)
        """
        table_name = "dbo.FutureMainTicker"
        sql = f"SELECT Date,Contract,Ticker,UpdateTime From {table_name} WHERE "
        where = FutureDataAPI.pair_dates("Date", begin_date, end_date)

        contracts, tickers = FutureDataAPI.split_codes(codes)
        cond = [FutureDataAPI.pair_equals("Contract", contracts),
                FutureDataAPI.pair_equals("Ticker", tickers)]
        cond = [c for c in cond if c.strip()]
        cond_str = " OR ".join(cond) if cond else ""
        where += f" AND ({cond_str})" if cond_str else ""
        where += " ORDER BY Date,Contract "
        sql += where

        return self.mssql_65.query(sql)

    def get_future_member_hold(self, codes: list = None, contracts: list = None,
                               start: str = None, end: str = None, info_types: list = None,
                               rank: int = 0):
        """
        获取品种会员持仓和交易信息
        params: codes:(short string ticker without exchange info)(optional)
        params: start: start time
        params: end: end time
        params: info_types: 1:Trading Volume;2: Hold Long num;3: Hold Short num
        returns: daily holding pos of futures company
        """
        self.orcl_wind.reconnect()

        table_map = {"commodity": "CCommodityFuturesPositions",
                     "equity": "CIndexFuturesPositions",
                     "treasury": "CBondFuturesPositions"}

        # Unify date format
        start = Dates.convert(start, DateFmt.YMD)
        end = Dates.convert(end, DateFmt.YMD)

        info_types = info_types or [1, 2, 3]
        info_type = ','.join(['\'' + str(x).upper() + '\'' for x in info_types])

        # Get ticker info
        ticker_info = self.get_future_basic(codes=codes or contracts)
        ticker_info['WindCode'] = [x + '.' + y for x, y in zip(ticker_info['Ticker'], ticker_info['Exchange'])]

        # Split target tickers to specific tables
        asset_info = self.split_info_byasset(ticker_info)

        data = pd.DataFrame(columns=['Code', 'Contract', 'Date', 'Member', 'Type', 'Num', 'Change'])
        for asset in asset_info:
            table = table_map[asset]
            target_temp = asset_info[asset]
            if target_temp.shape[0] == 0:
                continue

            # Prepare data
            if codes is not None:
                call_data = target_temp.WindCode.unique().tolist()
                condition = "a.S_INFO_WINDCODE"

            elif contracts is not None:
                call_data = target_temp.Contract.unique().tolist()
                condition = "b.FS_INFO_SCCODE"
            else:
                raise NotImplementedError("Input target missing!")

            targets = ','.join(['\'' + x.upper() + '\'' for x in call_data])

            # Query data from Wind oracle
            query = "SELECT a.S_INFO_WINDCODE,b.FS_INFO_SCCODE,a.TRADE_DT,a.FS_INFO_MEMBERNAME,\
                        a.FS_INFO_TYPE,sum(a.FS_INFO_POSITIONSNUM),sum(a.S_OI_POSITIONSNUMC),a.FS_INFO_RANK\
                        FROM ({} a LEFT JOIN CFUTURESDESCRIPTION b \
                              ON a.S_INFO_WINDCODE=b.S_INFO_WINDCODE) \
                        WHERE a.TRADE_DT>='{}' and a.TRADE_DT<='{}' and {} in ({})\
                        and a.FS_INFO_TYPE in ({})\
                        GROUP BY a.S_INFO_WINDCODE,b.FS_INFO_SCCODE,a.TRADE_DT,a.FS_INFO_MEMBERNAME, a.FS_INFO_TYPE, a.FS_INFO_RANK"

            data_temp = self.orcl_wind.exec_query(query.format(table, start, end, condition, targets,
                                                               info_type))
            data_temp.columns = ['Code', 'Contract', 'Date', 'Member', 'Type', 'Num', 'Change', 'Rank']
            data = pd.concat([data, data_temp])

        # Adjust data format
        data = data[['Date', 'Code', 'Contract', 'Member', 'Type', 'Num', 'Change', 'Rank']]
        data.Date = pd.to_datetime(data.Date)

        # Update contract label to latest
        for k in SYMBOL_MAP:
            data.Code = [i.replace(k, SYMBOL_MAP[k]) for i in data.Code]
            data.Contract = [i.replace(k, SYMBOL_MAP[k]) for i in data.Contract]

        if codes is not None:
            wind_to_ticker = ticker_info.set_index('windcode')['Code'].to_dict()
            data['Code'] = data['Code'].map(lambda x: wind_to_ticker[x])

        # Append rank label
        data.Num = data.Num.astype(float)
        data.Change = data.Change.astype(float)
        data.Type = data.Type.astype(int)
        data.Rank = data.Rank.astype(int)

        if rank:
            data = data.query("rank<=%s" % rank)

        data['Type'] = data['Type'].replace({1: 'vol',
                                             2: 'oi_long',
                                             3: 'oi_short'})
        return data

    @staticmethod
    def split_info_byasset(ticker_info: pd.DataFrame):
        """Split given ticker info table by asset: commodity, equity, treasury"""
        commodities = ticker_info[ticker_info.Exchange.isin(COM_EXCHANGE)]
        equities = ticker_info[(ticker_info.Exchange.isin(FIN_EXCHANGE))
                               & (ticker_info.Code.str.startswith('I'))]
        treasury = ticker_info[(ticker_info.Exchange.isin(FIN_EXCHANGE))
                               & (ticker_info.Code.str.startswith('T'))]

        asset_info = {"commodity": commodities,
                      "equity": equities,
                      "treasury": treasury}

        return asset_info

    def fetch_all_contracts(self):
        """查询所有期货合约信息"""
        query = f"select [Code] as ticker, [Ticker] as tickerFull, [Contract] as contractObject, " \
                f"CONVERT(varchar(100), [listDate], 23) as listDate , " \
                f"CONVERT(varchar(100), [delistDate], 23)  as deliDate," \
                f"[multiplier], [tickSize], [exchange], [priceLimit] as [limit] " \
                f"from [CTA_RESEARCH].[dbo].[FutureInfo_Basic]"
        data = self.mssql_162.query(query)
        data[['listDate', 'deliDate']] = data[['listDate', 'deliDate']].applymap(lambda x: x.replace('-', ''))
        return data

    def get_daily_target_pos(self, fund_name: str, date: str, engine=None) -> pd.DataFrame:
        """获取本地数据库对应fundname的最新的持仓信息"""
        query = f"select [StrategyName], [Ticker], (CASE WHEN [Direction] ='Long' then 1 ELSE -1 end) as " \
                f"[Direction], [Qty], [UpdateTime] from [dbo].[V_AdvisorPM_TargetDailyPosition_LastTradeDate] " \
                f"where FundName=\'{fund_name}\' and Date = \'{Dates.convert(date)}\'"

        engine = engine or self.mssql_162
        data = engine.query(query)
        return data

    def get_static_pos(self, fund_name: str, date: str) -> pd.DataFrame:
        table = '[dbo].[AccountPositionInfo_IndexHedge]'
        sql = (f"SELECT DateTime, Ticker, Quantity, TypeName, FundName, UpdateTime "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and DateTime = '{Dates.convert(date)}'")
        data = self.mssql_162.query(sql)
        return data

    def get_account_position(self, fund_name: str, date: str) -> pd.DataFrame:
        table = '[dbo].[AccountPositionInfo]'
        sql = (f"SELECT [Date], FundName, InvestorID, Ticker, PositionDirection, [Position], "
               f"YdPosition, TodayPosition, TodayOpenVolume, TodayCloseVolume, "
               f"FrozenClosing, HedgeFlag, PositionProfit, CloseProfit, Commission, UseMargin, "
               f"TradeDate, UpdateTime "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and [Date] = '{Dates.convert(date)}'")
        data = self.mssql_162.query(sql)
        return data

    def get_accountsettle_pos(self, fund_name: str, date: str, engine=None) -> pd.DataFrame:
        """获取结算表持仓信息"""
        query = f"select [Instrument], [LongPos.], [ShortPos.] from [dbo].[V_AccountSettleInfo_Position_AdvisorPM] " \
                f"where Date = '{Dates.convert(date)}' " \
                f"and FundName ='{fund_name}'"

        engine = engine or self.mssql_162
        data = engine.query(query)
        return data

    def get_account_target_pos(self, fund_name: str) -> pd.DataFrame:
        """获取账户目标持仓信息"""
        query = f"select [DateTime], [Ticker], [Quantity], [FundName] from [dbo].[TotalTargetPosition] " \
                f"where DateTime=(select max(DateTime) from [dbo].[TotalTargetPosition]) " \
                f"and FundName='{fund_name}'"
        data = self.mssql_162.query(query)
        return data

    def get_risk_degree(self, fund_name: str, date: str) -> pd.DataFrame:
        """获取账户风险度信息"""
        query = f"select [Date], [RiskDegree], [FundName], [UpdateTime] from [dbo].[AccountCashInfo] " \
                f"where UpdateTime=(select max(UpdateTime) from [dbo].[AccountCashInfo]) " \
                f"and FundName='{fund_name}' " \
                f"and Date = '{Dates.convert(date)}'"
        data = self.mssql_162.query(query)
        return data

    def get_future_coefadj(self, codes: Union[str, Sequence[str]],
                           begin_date: Union[str, date],
                           end_date: Union[str, date]) -> pd.DataFrame:
        """
            获取复权系数

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]
        :param begin_date: 开始日期
        :param end_date:  结束日期

        :return: DataFrame
        columns=(Date,Contract,Ticker,CoefAdj, UpdateTime)
        """
        table_name = "dbo.FutureCoefAdj_MainTicker"
        sql = f"SELECT Date,Contract,Ticker,CoefAdj, UpdateTime From {table_name} WHERE "

        if isinstance(begin_date, dt.datetime):
            begin_date = begin_date.date()

        if isinstance(end_date, dt.datetime):
            end_date = end_date.date()

        where = FutureDataAPI.pair_dates("Date", begin_date, end_date)

        contracts, tickers = FutureDataAPI.split_codes(codes)
        cond = [FutureDataAPI.pair_equals("Contract", contracts),
                FutureDataAPI.pair_equals("Ticker", tickers)]
        cond = [c for c in cond if c.strip()]
        cond_str = " OR ".join(cond) if cond else ""
        where += f" AND ({cond_str})" if cond_str else ""
        where += " ORDER BY Date "
        sql += where

        return self.mssql_65.query(sql)

    """
        Factor
    """

    def get_factor(
            self,
            symbols: Union[str, Sequence[str]] = None,
            freq: Frequency = Frequency.Min_1,
            ids: Union[str, Sequence[str]] = None,
            names: Union[str, Sequence[str]] = None,

            begin_date: Union[str, date] = None,
            end_date: Union[str, date] = None,
    ) -> pd.DataFrame:

        tab_name = self.dolphindb.table_name['factor']
        if isinstance(freq, Interval):
            interval = freq

        elif isinstance(freq, Frequency):
            interval = Interval.from_freq(freq)

        elif isinstance(freq, str):
            interval = Interval.from_str(freq)

        else:
            raise NotImplementedError

        cond = [
            FutureDataAPI.pair_equals("symbol", symbols),
            FutureDataAPI.pair_equals("factor_id", ids),
            FutureDataAPI.pair_equals("factor_name", names),
        ]

        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""

        df = self.dolphindb.query(
            tab_name,
            interval=interval,
            start=begin_date,
            end=end_date,
            where=cond_str
        )

        return df

    get_future_factor = get_factor

    def get_edb_data(self, ids: Union[int, Sequence[int]],
                     industry: Union[str, Sequence[str]],
                     begin_date: Union[str, date],
                     end_date: Union[str, date],
                     edb_type: EdbType = EdbType.BASIC) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
            获取基本面基础信息数据

        :param ids: 基本面数据中定义的ID, 与industry不可共存
        :param industry: 基本面数据中定义的版块类型, 与id不可共存
        :param begin_date: 开始日期
        :param end_date:  结束日期
        :param edb_type: EdbType Enum

        :return: Tuple[Dataframe(基本面基础信息), Dataframe(基本面行情信息)], 通过ID关联
        """
        if all([ids, industry]):
            raise ValueError("edb id and industry Only one value is allowed!")

        tb_edb, tb_edb_info = (("dbo.EDB_Basic", "dbo.EDBInfo_Basic")
                               if edb_type == EdbType.BASIC else ("dbo.EDB_Adv", "dbo.EDBInfo_Adv"))
        sql_edb = f"SELECT ID,Date,EconData FROM {tb_edb} WHERE "
        sql_edb_info = (f"SELECT ID ,Industry ,Description ,Source ,Ticker ,Frequency ,TimeTag ,PublishLag ,"
                        f" QuoteUnit ,Note ,UpdateTime FROM {tb_edb_info} WHERE ")
        # 依赖ID查询
        if ids:
            # 转成str调用函数构建条件
            ids = [str(_id) for _id in ids]
            sql_edb += FutureDataAPI.pair_dates("Date", begin_date, end_date)

            cond = FutureDataAPI.pair_equals("ID", ids)
            # 去掉条件中的引号
            cond = cond.replace("[\"\']+", "")
            sql_edb += f"AND +{cond} ORDER BY Date ASC"
            sql_edb_info += cond

            return self.mssql_165.query(sql_edb_info), self.mssql_165.query(sql_edb)

        # 依赖版块查询
        if industry:
            cond = FutureDataAPI.pair_equals("Industry", industry)
            sql_edb_info += cond
            df_edb_info = self.mssql_165.query(sql_edb_info)
            ids = tuple(df_edb_info["ID"].unique().tolist())
            ids = [str(_id) for _id in ids]
            cond = FutureDataAPI.pair_equals("ID", ids)
            cond = cond.replace("[\"\']+", "")
            sql_edb += cond

            return df_edb_info, self.mssql_165.query(sql_edb)

    def get_risk_free_rate(
            self,
            symbol: Union[str, Sequence[str]] = None,
            begin_date: Union[str, date] = None,
            end_date: Union[str, date] = None,
    ):
        table_name = "RiskFreeRate"

        sql = f"SELECT Date,Symbol,Rate FROM {table_name} WHERE "
        if isinstance(begin_date, dt.datetime):
            begin_date = begin_date.date()

        if isinstance(end_date, dt.datetime):
            end_date = end_date.date()

        where = FutureDataAPI.pair_dates("Date", begin_date, end_date)

        cond = [FutureDataAPI.pair_equals("Symbol", symbol),]
        cond = [c for c in cond if c.strip()]
        cond_str = " OR ".join(cond) if cond else ""
        where += f" AND ({cond_str})" if cond_str else ""
        where += " ORDER BY Date "
        sql += where

        return self.mssql_165.query(sql)

    def get_option_quote(
            self,
            codes: Union[str, Sequence[str]] = None,
            start: Union[str, date] = None,
            end: Union[str, date] = None,
            interval: Interval = Interval.DAILY,
    ) -> pd.DataFrame:

        df_quote = self.dolphindb.load_bar_data(
            symbol=codes,
            product=Product.OPTION,
            interval=interval,
            start=start,
            end=end,
        )

        return df_quote

    def get_option_chain(
            self,
            codes_underlying: Union[str, Sequence[str]] = None,
            product_underlying: Product = None,
            start: Union[str, date] = None,
            end: Union[str, date] = None,
            interval: Interval = Interval.DAILY,
            option_fields: Union[str, Sequence[str]] = "*",
    ):

        if isinstance(codes_underlying, str):
            codes_underlying = [codes_underlying]

        if isinstance(option_fields, list):
            option_fields = ",".join(option_fields)

        ret = {}
        option_contract_table = self.dolphindb.get_table_name("contract", Product.OPTION)
        option_bar_table = self.dolphindb.get_table_name("bar", Product.OPTION)
        for symbol in codes_underlying:
            ret[symbol] = {}
            ret[symbol]['contract_underlying'] = self.dolphindb.load_contract_data(symbol, product_underlying, start,
                                                                                   end)

            ret[symbol]['bar_underlying'] = self.dolphindb.load_bar_data(
                symbol=symbol,
                product=product_underlying,
                interval=interval,
                start=start,
                end=end,
            )

            ret[symbol]['contract_options'] = contract_options = self.dolphindb.query(
                option_contract_table,
                start=start,
                end=end,
                option_underlying=symbol,
            )

            symbols = tuple(contract_options["symbol"].unique().tolist())
            where_str = f"symbol in {symbols}"

            ret[symbol]['bar_options'] = self.dolphindb.query(
                option_bar_table,
                start=start,
                end=end,
                interval=interval,
                where=where_str,
                fields=option_fields
            )

        return ret

    """
        For Backtest Result reading / writing
    """

    def get_backtest_info(self, backtest_id: str = None) -> pd.DataFrame:
        table_name = "[dbo].[StrategyBackTestInfo]"
        sql = f"""SELECT * FROM {table_name} WHERE 1=1 """

        if backtest_id:
            cond = [
                FutureDataAPI.pair_equals("BackTestID", backtest_id)
            ]
            cond = [c for c in cond if c.strip()]
            cond_str = " OR ".join(cond) if cond else ""
            sql += f" AND ({cond_str})" if cond_str else ""

        return self.mssql_65.query(sql)

    def get_backtest_env(self, backtest_id: int) -> pd.DataFrame:
        table_name = "[dbo].[StrategyBacktestEnv]"
        sql = f"""SELECT [BackTestID]
                        ,[Type]
                        ,[Value]
                    FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("BackTestID", backtest_id)
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " OR ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""

        return self.mssql_65.query(sql)

    def get_backtest_log(self, backtest_id: int, source: Union[str, Sequence[str]] = None) -> pd.DataFrame:
        table_name = "[dbo].[StrategyBacktestLog]"
        sql = f"""SELECT [BackTestID]
                        ,[Source]
                        ,[DateTime]
                        ,[Log]
                    FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("BackTestID", backtest_id),
            FutureDataAPI.pair_equals("Source", source)
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""
        sql += " ORDER BY DateTime "

        return self.mssql_65.query(sql)

    def get_backtest_trade(self, backtest_id: int) -> pd.DataFrame:
        table_name = "[dbo].[StrategyBacktestTrade]"
        sql = f"""SELECT [BackTestID]
                        ,[DateTime]
                        ,[TradeDate]
                        ,[Ticker]
                        ,[Contract]
                        ,[Qty]
                        ,[Price]
                        ,[Amount]
                        ,[Portfolio]
                        ,[UpdateTime]
                  FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("BackTestID", backtest_id)
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""
        sql += " ORDER BY DateTime, Ticker "

        return self.mssql_65.query(sql)

    def get_backtest_param(self, param_id: int) -> pd.DataFrame:
        table_name = "[dbo].[StrategyParameterSet]"
        sql = f"""SELECT [ParameterSetID]
                        ,[Name]
                        ,[Type]
                        ,[Value]
                        ,[DataType]
                        ,[UpdateTime]
                  FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("ParameterSetID", param_id)
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""

        return self.mssql_65.query(sql)

    def get_backtest_performance(self, backtest_id: int, evaluator: Union[str, Sequence[str]] = None) -> pd.DataFrame:
        table_name = "[dbo].[StrategyPerformance]"
        sql = f"""SELECT [BackTestID]
                        ,[Evaluator]
                        ,[Type]
                        ,[Value]
                        ,[UpdateTime]
                      FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("BackTestID", backtest_id),
            FutureDataAPI.pair_equals("Evaluator", evaluator)
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""

        return self.mssql_65.query(sql)

    def get_backtest_ret(self, backtest_id: str) -> pd.DataFrame:
        table_name = "[dbo].[StrategyBacktestRet]"
        sql = f"""SELECT [BackTestID]
                        ,[DateTime]
                        ,[Ret]
                FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("BackTestID", backtest_id),
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""
        sql += " ORDER BY DateTime "

        return self.mssql_65.query(sql)

    def get_strategy_pack_ret(self, strategy_name: str) -> pd.DataFrame:
        table_name = "[dbo].[StrategyPackRet]"
        sql = f"""SELECT [StrategyName]
                        ,[DateTime]
                        ,[Ret]
                FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("StrategyName", strategy_name),
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""
        sql += " ORDER BY DateTime "

        return self.mssql_65.query(sql)

    def get_lastest_backtest_nav(self):
        sql = f"""
        Select BackTestID, DateTime, TradeDate, Nav, Rtn from
        (SELECT BackTestID, DateTime, TradeDate, Nav, Rtn, row_number() OVER(PARTITION BY BackTestID order by DateTime desc) rn
        FROM [dbo].[StrategyBacktestNav]) a
        where rn = 1"""

        return self.mssql_65.query(sql)

    def get_analyzer_contribution(self, backtest_id: int, contrib: Union[str, Sequence[str]] = None) -> pd.DataFrame:
        table_name = "[dbo].[StrategyAnalyzerContribution]"
        sql = f"""SELECT [BackTestID]
                        ,[Type]
                        ,[Value]
                        ,[Contribute]
                        ,[UpdateTime]
                  FROM {table_name} WHERE 1=1 """

        cond = [
            FutureDataAPI.pair_equals("BackTestID", backtest_id),
            FutureDataAPI.pair_equals("Type", contrib)
        ]
        cond = [c for c in cond if c.strip()]
        cond_str = " AND ".join(cond) if cond else ""
        sql += f" AND ({cond_str})" if cond_str else ""

        return self.mssql_65.query(sql)

    """
        backtest element 
    """

    def get_static_futureset(self, label: Union[str, Label] = Label.CommTradeble,
                             date: Union[str, date] = None):
        """
        调用静态回测品种池
        Parameters
        ----------
        label：Label=Label.CommTradeble
        date: str,数据库日期
        Returns
        -------
        futuresets：set
            期货品种池
        """
        if not (date is None):
            date = Dates.convert(date)
        query_temp = "SELECT TOP 1 be.Date,be.KeyElement " \
                     "FROM [CTA_RESEARCH].[dbo].[BacktestElement] be " \
                     "LEFT JOIN CTA_RESEARCH.dbo.ElementDefinition ed " \
                     "ON be.DefinitionId = ed.Id "

        if isinstance(label, Label):
            label_str = label.name

        else:
            label_str = label

        if date is None:
            query = query_temp + f"WHERE ed.Label='{label_str}' " \
                                 f"ORDER BY be.Date DESC"
        else:
            query = query_temp + f"WHERE be.Date = '{date}' and ed.Label='{label.name}'"

        futureset = self.mssql_65.query(query)
        futuresets = futureset['KeyElement'].values[0].strip(' ').split(';')
        return set(futuresets)

    def get_dynamic_futureset(self, label: Union[str, Label],
                              fromdate: Union[str, date],
                              todate: Union[str, date]):
        """
        调用动态回测品种池
        Parameters
        ----------
        label：str, 品种池标签
        fromdate: str, 数据库日期
        todate: str, 数据库日期
        Returns
        -------
        futuresets：pd.DataFrame
            期货品种池
        """
        fromdate, todate = Dates.convert(fromdate), Dates.convert(todate)
        query_temp = "SELECT be.Date,be.KeyElement " \
                     "FROM [CTA_RESEARCH].[dbo].[BacktestElement] be " \
                     "LEFT JOIN CTA_RESEARCH.dbo.ElementDefinition ed " \
                     "ON be.DefinitionId = ed.Id "

        if isinstance(label, Label):
            label_str = label.name

        else:
            label_str = label

        query = query_temp + f"WHERE ed.Label='{label_str}' " \
                             f"and be.Date between '{fromdate}' and '{todate}' " \
                             "ORDER BY be.Date ASC"

        future_set = self.mssql_65.query(query)
        future_set['KeyElement'] = [i.strip(' ').split(';') for i in future_set['KeyElement']]
        return future_set

    def get_basic_element(self, label=None):
        """
        调取基本回测要素
        Parameters
        ----------
        label:Label，具体的回测要素名
        Returns
        -------
        element：dict
            回测要素
        """
        query = "SELECT * " \
                "FROM [CTA_RESEARCH].[dbo].[BacktestElement] be " \
                "LEFT JOIN CTA_RESEARCH.dbo.ElementDefinition ed " \
                "ON be.DefinitionId = ed.Id " \
                "WHERE ed.Types='TradeElement'"
        elements = self.mssql_65.query(query)
        elements.set_index('Label', inplace=True)
        element = elements['KeyElement'].to_dict()
        element['Fee'] = float(element['Fee'])
        element['RiskFreeRate'] = float(element['RiskFreeRate'])
        element['Slippage'] = float(element['Slippage'])
        element['YearTradeDate'] = int(element['YearTradeDate'])

        if label is None:
            return element
        else:
            return element[label.name]

    def get_backtest_periods(self, date: Union[str, date] = None,
                             label: Label = Label.Sample_FX1Q,
                             method: Method = Method.static):
        """
        调取回测样本区间，静态划分为两段样本，样本内和样本外，动态划分为多段样本
        Parameters
        ----------
        date : str or None
            回测样本的可得时间.
        label : str or None
            标签.
        method : str, optional
            划分方法，滚动或者静态. The default is 'static'.
        Returns
        -------
        sample : dict或者DataFrame
            静态样本划分为Series，动态样本区间为Dataframe.时间均为dt.datetime
        """
        label = label.name if label else 'Sample_FX1Q'
        if not (date is None):
            date = Dates.convert(date)
        if method == Method.static:
            query_temp = "SELECT TOP 1 be.Date,be.KeyElement " \
                         "FROM [CTA_RESEARCH].[dbo].[BacktestElement] be " \
                         "LEFT JOIN CTA_RESEARCH.dbo.ElementDefinition ed " \
                         "ON be.DefinitionId = ed.Id "
            if date is None:
                query = query_temp + f"WHERE ed.Label='{label}' " \
                                     "ORDER BY be.Date DESC"
            else:
                query = query_temp + f"WHERE be.Date<='{date}' and ed.Label='{label}' " \
                                     "ORDER BY be.Date DESC"
            samples = self.mssql_65.query(query)
            samples['KeyElement'] = [x.split(';') for x in samples['KeyElement']]
            sample = {}
            period = samples.head(1)['KeyElement'].values[0]
            period = [dt.datetime.strptime(d, "%Y-%m-%d") for d in period]
            if period[0] < period[1] <= period[2] < period[3]:
                sample['insample_start'] = period[0]
                sample['insample_end'] = period[1]
                sample['outsample_start'] = period[2]
                sample['outsample_end'] = period[3]
            else:
                raise ValueError('The relative size of timesample is wrong')
        else:
            query_temp = "SELECT be.Date, be.KeyElement " \
                         "FROM [CTA_RESEARCH].[dbo].[BacktestElement] be " \
                         "LEFT JOIN CTA_RESEARCH.dbo.ElementDefinition ed " \
                         "ON be.DefinitionId = ed.Id "
            if date is None:
                query = query_temp + f"WHERE ed.Label='{label}' " \
                                     "ORDER BY be.Date ASC"
            else:
                query = query_temp + f"WHERE be.Date<='{date}' and ed.Label='{label}' " \
                                     "ORDER BY be.Date ASC"
            sample = self.mssql_65.query(query)
            sample['KeyElement'] = [[dt.datetime.strptime(d, "%Y-%m-%d") for d in x.split(';')]
                                    for x in sample['KeyElement']]
            # 如果是滚动划分回测样本
            if label.startswith('Sample'):
                sample['insample_start'] = [x[0] for x in sample['KeyElement']]
                sample['insample_end'] = [x[1] for x in sample['KeyElement']]
                sample['outsample_start'] = [x[2] for x in sample['KeyElement']]
                sample['outsample_end'] = [x[3] for x in sample['KeyElement']]
            # 如果是滚动划分上涨下跌样本
            else:
                sample['sample_start'] = [x[0] for x in sample['KeyElement']]
                sample['sample_end'] = [x[1] for x in sample['KeyElement']]
            del sample['KeyElement']

            if sample.empty:
                raise ValueError('Input label is wrong')

        return sample

    def get_backtrade_element(self, date: Union[str, date] = None,
                              missing_ratio: float = 1,
                              label: Label = Label.Sample_FX1Y,
                              pool: Label = Label.CommTradeble,
                              method: Method = Method.static,
                              sample=Sample.Insample):
        """
        获取静态或者动态回测要素合集
        Parameters
        ----------
        date: str, 日期
        missing_ratio: 合约在回测区间缺失比例上限
        label : Label, Label.Sample_3Y1Y
        pool: Label = Label.CommTradeble,品种池
        method: Method,Method.static or Method.dynamic
        sample: Sample.Insample,默认样本内

        Returns
        -------
        result : dict
            各种要素.
        """
        # 获取当前日期下的回测区间起始和终点
        if date:
            date = Dates.convert(date)
        period = self.get_backtest_periods(date, label, Method.static)
        if sample == Sample.Insample:
            fromdate = period['insample_start']
            todate = period['insample_end']
        elif sample == Sample.Outsample:
            fromdate = period['outsample_start']
            todate = period['outsample_end']
        else:
            fromdate = period['insample_start']
            todate = period['outsample_end']

        result = {}
        # 基本要素
        basic_element = self.get_basic_element()
        result['slippage_perc'] = basic_element['Slippage']
        result['fee'] = basic_element['Fee']
        # MatchMethod.next_bar_open
        result['match_method'] = basic_element['MatchingMethod']
        result['fromdate'] = fromdate
        result['todate'] = todate
        # 合约集
        future_set = self.get_dynamic_futureset(pool,
                                                fromdate.strftime('%Y-%m-%d'),
                                                todate.strftime('%Y-%m-%d'))

        contract = [i for item in future_set['KeyElement'] for i in item]
        columns = list(set(contract))
        future_set['Date'] = future_set['Date'].apply(lambda x: x.date())
        future_set.set_index('Date', inplace=True)
        # 时序品种池统计
        contracts = pd.DataFrame(index=future_set.index, columns=columns)
        for index, row in future_set.iterrows():
            contracts.loc[index, :] = [1 if i in row.values[0] else 0 for i in columns]

        if method == Method.static:
            counts = contracts[contracts == 0].count(axis=0)
            contracts = counts[counts < missing_ratio * len(contracts)].index.tolist()
            result['contract'] = set(contracts)
            result['trading_calendar'] = None
        else:
            result['contract'] = set(contracts.columns)
            contracts.insert(0, 'trade_date', future_set.index)
            result['trading_calendar'] = contracts
        return result

    def save_analyzer_performance(self, data: pd.DataFrame) -> None:
        self.mssql_65.upsert('StrategyPerformance', data, ['BackTestID', 'Evaluator', 'Type'],
                             audit_columns={'update': 'UpdateTime'})

    def save_analyzer_contribution(self, data: pd.DataFrame) -> None:
        self.mssql_65.upsert('StrategyAnalyzerContribution', data, ['BackTestID', 'Contribute', 'Type'],
                             audit_columns={'update': 'UpdateTime'})

    def save_backtest_info(self, data: pd.DataFrame):
        self.mssql_65.upsert(
            table='StrategyBacktestInfo',
            data=data,
            on=['BackTestID'],
            audit_columns={'update': 'UpdateTime'}
        )

    def save_backtest_trade(self, data: pd.DataFrame):
        self.mssql_65.upsert(table='StrategyBacktestTrade', data=data,
                             on=['BackTestID', 'DateTime', 'Ticker', 'Qty'],
                             columns=['BackTestID', 'DateTime', 'TradeDate', 'Ticker', 'Contract',
                                      'Qty', 'Price', 'Amount', 'Portfolio'],
                             audit_columns={'update': 'UpdateTime'})

    def save_backtest_env(self, data: pd.DataFrame):
        self.mssql_65.upsert(table='StrategyBacktestEnv', data=data,
                             on=['BackTestID', 'Type'],
                             columns=['BackTestID', 'Type', 'Value'])

    def save_backtest_log(self, data: pd.DataFrame):
        self.mssql_65.upsert(table='StrategyBacktestLog', data=data,
                             on=['BackTestID'],
                             columns=['BackTestID', 'Log'])

    def save_backtest_param(self, data: pd.DataFrame, parameter_set_id=None):
        if parameter_set_id is None:
            data_params = data.to_dict(orient="records")

            insert_query = ""
            for record in data_params:
                values = "'" + "', '".join(record.values()) + "'"
                insert_query += (
                    f"INSERT INTO [dbo].[StrategyParameterSet] "
                    f"({', '.join(record.keys())}, ParameterSetID) "
                    f"VALUES ({values}, @parameter_set_id);"
                )

            exec_sql = (
                f"DECLARE @parameter_set_id int;"
                f"SET @parameter_set_id = (SELECT MAX(ParameterSetId) FROM [dbo].[StrategyParameterSet]) + 1;"
                f"{insert_query}"
                f"SELECT @parameter_set_id;")

            parameter_set_id = self.mssql_65.execute(exec_sql)
            parameter_set_id = parameter_set_id[0][0]

        else:
            data.loc[:, 'ParameterSetID'] = parameter_set_id
            self.mssql_65.upsert(table='StrategyParameterSet', data=data,
                                 on=['ParameterSetID', 'Name'],
                                 columns=['ParameterSetID', 'Name', 'Type', 'Value', 'DataType'],
                                 audit_columns={'update': 'UpdateTime'})

        return parameter_set_id

    def save_backtest_ret(self, data: pd.DataFrame):
        self.mssql_65.upsert(table='StrategyBacktestRet', data=data,
                             on=['BackTestID', 'DateTime'],
                             columns=['BackTestID', 'DateTime', 'Ret'],
                             audit_columns={'update': 'UpdateTime'})

    def save_pack_ret(self, data: pd.DataFrame):
        self.mssql_65.upsert(table='StrategyPackRet', data=data,
                             on=['StrategyName', 'DateTime'],
                             columns=['StrategyName', 'DateTime', 'Ret'],
                             audit_columns={'update': 'UpdateTime'})

    def save_account_position(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[AccountPositionInfo]',
                                  data=data,
                                  on=['Date', 'FundName', 'InvestorID', 'Ticker', 'PositionDirection', 'TradeDate'],
                                  columns=['Date', 'FundName', 'InvestorID', 'Ticker', 'PositionDirection', 'Position',
                                           'YdPosition', 'TodayPosition', 'TodayOpenVolume', 'TodayCloseVolume',
                                           'FrozenClosing', 'HedgeFlag', 'PositionProfit', 'CloseProfit', 'Commission',
                                           'UseMargin', 'UpdateTime', 'TradeDate'])
        else:
            self.mssql_162.insert(table=f'[dbo].[AccountPositionInfo]',
                                  data=data,
                                  columns=['Date', 'FundName', 'InvestorID', 'Ticker', 'PositionDirection', 'Position',
                                           'YdPosition', 'TodayPosition', 'TodayOpenVolume', 'TodayCloseVolume',
                                           'FrozenClosing', 'HedgeFlag', 'PositionProfit', 'CloseProfit', 'Commission',
                                           'UseMargin', 'UpdateTime', 'TradeDate'])

    def save_account_trade_record(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[AccountTradeRecords]',
                                  data=data,
                                  on=['Date', 'TradeTime', 'FundName', 'InvestorID', 'OrderSysId', 'TradeId'],
                                  columns=['Date', 'TradeTime', 'FundName', 'InvestorID', 'Ticker', 'Side',
                                           'OpenCloseFlag',
                                           'Price', 'Quantity', 'OrderSysId', 'TradeId', 'HedgeFlag', 'UpdateTime',
                                           'TradeDate'])
        else:
            self.mssql_162.insert(table=f'[dbo].[AccountTradeRecords]',
                                  data=data,
                                  columns=['Date', 'TradeTime', 'FundName', 'InvestorID', 'Ticker', 'Side',
                                           'OpenCloseFlag',
                                           'Price', 'Quantity', 'OrderSysId', 'TradeId', 'HedgeFlag', 'UpdateTime',
                                           'TradeDate'])

    def get_account_trade_record(self, fund_name: str, date: str) -> pd.DataFrame:
        table = '[dbo].[AccountTradeRecords]'
        sql = (f"SELECT [Date], TradeTime, FundName, InvestorID, Ticker, Side, OpenCloseFlag, Price, "
               f"Quantity, OrderSysId, TradeId, HedgeFlag, UpdateTime, TradeDate "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and [Date] = '{Dates.convert(date)}'")
        data = self.mssql_162.query(sql)
        return data

    def save_account_cash_info(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[AccountCashInfo]',
                                  data=data,
                                  on=['Date', 'FundName', 'InvestorID', 'UpdateTime'],
                                  columns=['Date', 'FundName', 'InvestorID', 'DynamicBalance', 'StaticBalance',
                                           'MarginOccupied', 'Commission', 'RiskDegree', 'Deposit', 'WithDraw',
                                           'UpdateTime', 'TradeDate'])
        else:
            self.mssql_162.insert(table=f'[dbo].[AccountCashInfo]',
                                  data=data,
                                  columns=['Date', 'FundName', 'InvestorID', 'DynamicBalance', 'StaticBalance',
                                           'MarginOccupied', 'Commission', 'RiskDegree', 'Deposit', 'WithDraw',
                                           'UpdateTime', 'TradeDate']
                                  )

    def get_account_cash_info(self, fund_name: str, date: str) -> pd.DataFrame:
        table = '[dbo].[AccountCashInfo]'
        sql = (f"SELECT [Date], FundName, InvestorID, DynamicBalance, StaticBalance, MarginOccupied, "
               f"Commission, RiskDegree, Deposit, WithDraw, UpdateTime, TradeDate "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and [Date] = '{Dates.convert(date)}'")
        data = self.mssql_162.query(sql)
        return data

    def save_static_position(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[AccountPositionInfo_IndexHedge]',
                                  data=data,
                                  on=['DateTime', 'Ticker', 'FundName', 'TypeName'],
                                  columns=['DateTime', 'Ticker', 'Quantity', 'TypeName', 'FundName'],
                                  audit_columns={'update': 'UpdateTime'})
        else:
            self.mssql_162.insert(table=f'[dbo].[AccountPositionInfo_IndexHedge]',
                                  data=data,
                                  columns=['DateTime', 'Ticker', 'Quantity', 'TypeName', 'FundName', 'UpdateTime'])

    def save_account_target_position(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[TotalTargetPosition]',
                                  data=data,
                                  on=['DateTime', 'Ticker', 'Quantity', 'FundName'],
                                  columns=['DateTime', 'Ticker', 'Quantity', 'FundName', 'UpdateTime'])
        else:
            self.mssql_162.insert(table=f'[dbo].[TotalTargetPosition]',
                                  data=data,
                                  columns=['DateTime', 'Ticker', 'Quantity', 'FundName', 'UpdateTime'])

    def save_strategy_position(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[StrategyPortfolio]',
                                  data=data,
                                  on=['FundName', 'InvestorID', 'StrategyName', 'Ticker', 'Folder', 'MachineName',
                                      'TradeDate'],
                                  columns=['Date', 'TradeDate', 'UpdateTime', 'FundName', 'InvestorID',
                                           'StrategyName', 'Mode', 'Symbol', 'Ticker', 'Direction', 'Qty', 'Value',
                                           'PortfolioValue', 'PnL', 'EntryTime', 'MachineName', 'Folder'])
        else:
            self.mssql_162.insert(table=f'[dbo].[StrategyPortfolio]',
                                  data=data,
                                  columns=['Date', 'TradeDate', 'UpdateTime', 'FundName', 'InvestorID',
                                           'StrategyName', 'Mode', 'Symbol', 'Ticker', 'Direction', 'Qty', 'Value',
                                           'PortfolioValue', 'PnL', 'EntryTime', 'MachineName', 'Folder'])

    def get_strategy_position(self, fund_name: str, date) -> pd.DataFrame:
        table = '[dbo].[StrategyPortfolio]'
        sql = (f"SELECT [Date], FundName, InvestorID, Mode, StrategyName, Ticker, Direction, Qty, "
               f"Value, Pnl, EntryDate, MachineName, Folder, UpdateTime, TradeDate, PortfolioValue "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and [Date] = '{Dates.convert(date)}'")
        data = self.mssql_162.query(sql)
        return data

    def get_strategy_portfolio(
            self,
            fund_name: str,
            investor_id: str,
            strategy_name: str,
            mode: str
    ) -> pd.DataFrame:
        table = '[dbo].[StrategyPortfolio]'
        sql = (f"SELECT TOP 1 WITH TIES "
               f"[Date], TradeDate, UpdateTime, FundName, InvestorID, "
               "StrategyName, Mode, Symbol, Ticker, Direction, Qty, [Value], "
               "PortfolioValue, PnL, EntryTime, MachineName, Folder "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and InvestorID = '{investor_id}' and StrategyName = '{strategy_name}' and Mode = '{mode}' "
               f"ORDER BY UpdateTime desc")
        data = self.mssql_162.query(sql)
        return data

    def save_strategy_trade_record(self, data: pd.DataFrame, upsert=True) -> None:
        if upsert:
            self.mssql_162.upsert(table=f'[dbo].[StrategyExecutionTrades]',
                                  data=data,
                                  on=['FundName', 'InvestorID', 'StrategyName', 'TradeId', 'Ticker', 'MachineName',
                                      'TradeDate'],
                                  columns=['Date', 'FundName', 'InvestorID', 'Mode', 'StrategyName', 'OrderSysId',
                                           'TradeId',
                                           'Ticker', 'Direction', 'Qty', 'OrderPrice', 'TradePrice',
                                           'StrategyTradeType',
                                           'Text', 'Status', 'ExchangeTime', 'UpdateTime', 'MachineName', 'TradeDate'])
        else:
            self.mssql_162.insert(table=f'[dbo].[StrategyExecutionTrades]',
                                  data=data,
                                  columns=['Date', 'FundName', 'InvestorID', 'Mode', 'StrategyName', 'OrderSysId',
                                           'TradeId',
                                           'Ticker', 'Direction', 'Qty', 'OrderPrice', 'TradePrice',
                                           'StrategyTradeType',
                                           'Text', 'Status', 'ExchangeTime', 'UpdateTime', 'MachineName', 'TradeDate'])

    def get_strategy_trade_record(self, fund_name: str, date) -> pd.DataFrame:
        table = '[dbo].[StrategyExecutionTrades]'
        sql = (f"SELECT [Date], FundName, InvestorID, Mode, StrategyName, OrderSysId, TradeId, Ticker, "
               f"Direction, Qty, OrderPrice, TradePrice, StrategyTradeType,Text, Status, ExchangeTime, "
               f"UpdateTime, MachineName, TradeDate "
               f"FROM {table} "
               f"WHERE FundName = '{fund_name}' and [Date] = '{Dates.convert(date)}'")
        data = self.mssql_162.query(sql)
        return data

    def get_mock_trade(self, account: str = None, strategy_name: str = None):
        query = f"SELECT * FROM [CTA_REALTRADE].[dbo].MockStrategyExecutionTrades WHERE 1 = 1 "

        if account:
            query += f" AND FundName = '{account}' "

        if strategy_name:
            query += f" AND StrategyName = '{strategy_name}' "

        return self.mssql_65.query(query)

    def save_mock_order(self, orders: pd.DataFrame):
        self.mssql_65.upsert(table='[CTA_TESTTRADE].[dbo].MockStrategyExecutionTrades', data=orders,
                             # self.mssql_65.upsert(table='[CTA_REALTRADE].[dbo].MockStrategyExecutionTrades', data=orders,
                             on=['FundName', 'InvestorID', 'StrategyName', 'Ticker', 'ExchangeTime'],
                             columns=['Date', 'FundName', 'InvestorID', 'Mode', 'StrategyName', 'Ticker', 'Direction',
                                      'Qty', 'TradePrice', 'Status', 'ExchangeTime', 'UpdateTime', 'MachineName',
                                      'TradeDate'])

    def save_mock_position(self, position: pd.DataFrame):
        self.mssql_65.upsert(table='[CTA_TESTTRADE].[dbo].MockStrategyPortfolio', data=position,
                             # self.mssql_65.upsert(table='[CTA_REALTRADE].[dbo].MockStrategyPortfolio', data=position,
                             on=['Date', 'FundName', 'InvestorID', 'StrategyName', 'Ticker'],
                             columns=['Date', 'FundName', 'InvestorID', 'Mode', 'StrategyName', 'Ticker', 'Direction',
                                      'Qty', 'Value', 'EntryDate', 'MachineName', 'Folder', 'UpdateTime', 'TradeDate'])

    def delete_mock_order(self, account: str, strategy_name: str, trade_date):
        query = f"DELETE FROM [CTA_TESTTRADE].[dbo].MockStrategyExecutionTrades WHERE FundName = '{account}' AND InvestorID = '{account}' AND StrategyName = '{strategy_name}' AND TradeDate = '{trade_date}'"
        query = f"-- DELETE FROM [CTA_REALTRADE].[dbo].MockStrategyExecutionTrades WHERE FundName = '{account}' AND InvestorID = '{account}' AND StrategyName = '{strategy_name}' AND TradeDate = '{trade_date}'"
        self.mssql_65.execute(query)

    def delete_mock_position(self, account: str, strategy_name: str, trade_date):
        query = f"DELETE FROM [CTA_TESTTRADE].[dbo].MockStrategyPortfolio WHERE FundName = '{account}' AND InvestorID = '{account}' AND StrategyName = '{strategy_name}' AND TradeDate = '{trade_date}'"
        # query = f"DELETE FROM [CTA_REALTRADE].[dbo].MockStrategyPortfolio WHERE FundName = '{account}' AND InvestorID = '{account}' AND StrategyName = '{strategy_name}' AND TradeDate = '{trade_date}'"
        self.mssql_65.execute(query)

    def delete_backtest_trade(self, backtest_id: int):
        query = f"DELETE FROM dbo.StrategyBacktestTrade WHERE BackTestID = {backtest_id}"
        self.mssql_65.execute(query)

    def delete_backtest_nav(self, backtest_id: int):
        query = f"DELETE FROM dbo.StrategyBacktestNav WHERE BackTestID = {backtest_id}"
        self.mssql_65.execute(query)

    def delete_backtest_log(self, backtest_id: int):
        query = f"DELETE FROM dbo.StrategyBacktestLog WHERE BackTestID = {backtest_id}"
        self.mssql_65.execute(query)

    @staticmethod
    def split_codes(codes: Union[str, Sequence[str]]) -> Tuple[List[str], List[str]]:
        """拆解参数codes"""
        contracts, tickers = [], []
        if not codes:
            return contracts, tickers

        if isinstance(codes, str):
            if codes.find(",") != -1:
                codes = codes.split(",")
            else:
                codes = [codes]

        for x in codes:
            if 0 < len(x) <= 2:
                contracts.append(x.upper())
            elif 4 <= len(x) <= 6:
                tickers.append(x.upper())
            else:
                raise ValueError(f"code value has wrong value {x}")

        # 去除contract已包含的tickers
        tickers = {Strs.erase_digit(x): x for x in tickers}
        if tickers:
            # 取contract交集
            same_data = set(tickers) & set(contracts)
            if same_data:
                for same in same_data:
                    tickers.pop(same)
        tickers = list(tickers.values())

        return contracts, tickers

    @staticmethod
    def pair_equals(col_name: str, col_val: Union[str, int, float, Sequence[str]]) -> str:
        """将入参转换成SQL where条件"""
        if not all([col_name, col_val]):
            return ""

        if isinstance(col_val, str):
            if col_val.find(",") != -1:
                col_val = col_val.split(",")
            else:
                col_val = [col_val]

        elif isinstance(col_val, int) or isinstance(col_val, float):
            col_val = [col_val]

        elif isinstance(col_val, set):
            col_val = tuple(col_val)

        if len(col_val) == 1:
            cond = f" {col_name} = '{col_val[0]}'"
        else:
            cond = f" {col_name} in {tuple(col_val)}"

        return cond

    @staticmethod
    def pair_dates(col_name: str, begin_date: Union[str, date], end_date: Union[str, date]) -> str:
        """生成SQL条件匹配日期或日期区间"""
        if not col_name:
            raise ValueError("column name can't be empty!")

        if begin_date and end_date:
            str_begin, str_end = Dates.convert(begin_date), Dates.convert(end_date)
            if str_begin == str_end:
                cond = f"{col_name} = '{end_date}'"
            else:
                cond = f"{col_name} BETWEEN '{begin_date}' AND '{end_date}'"
        elif begin_date:
            cond = f"{col_name} >= '{begin_date}'"
        elif end_date:
            cond = f"{col_name} <= '{end_date}'"
        else:
            raise ValueError("begin and end dates cannot be all nulls!")

        return cond

    def get_tracking_factor_backtestid(self):
        sql = (
            f"SELECT a.BackTestID, a.ParameterSetID, b.Name, b.Value FROM [CTA_RESEARCH].[dbo].StrategyBackTestID_tmp a INNER JOIN [CTA_RESEARCH].[dbo].StrategyParameterSet b on a.ParameterSetID = b.ParameterSetID WHERE Name in ('factor_name', 'freq') and StrategyClass = 'CSFactorStrategy'")

        backtestid = self.mssql_65.query(sql)

        backtestid = backtestid.set_index(['BackTestID', 'ParameterSetID', 'Name'])['Value'].unstack().reset_index()

        return backtestid


if __name__ == '__main__':
    b = FutureDataAPI()
    start = '2022-06-01'
    end = '2022-07-19'
    contract = ['A', 'B']
    future_member_hold = b.get_future_member_hold(codes=None, contracts=contract, start=start, end=end,
                                                  info_types=[1, 2, 3])

    # 获取静态品种池
    futureset_static = b.get_static_futureset(label=Label.CommTradeble,
                                              date=dt.datetime.strptime('2022-02-25', '%Y-%m-%d'))
    print(futureset_static)
    # 获取基本回测要素
    basic_elements = b.get_basic_element()
    print(basic_elements)
    # 获取动态样本划分
    sample_3Y1Y = b.get_backtest_periods(None, Label.Sample_3Y1Y, Method.dynamic)
    # 获取上涨区间
    period_falling = b.get_backtest_periods(None, Label.Period_falling, Method.dynamic)
    # 获取动态变化的期货集
    dynamic_futureset = b.get_dynamic_futureset(label=Label.CommTradeble,
                                                fromdate=dt.datetime.strptime('2022-01-01', '%Y-%m-%d'),
                                                todate='2022-02-25')
    print(dynamic_futureset)
    # date_time = None
    # 获取动态交易要素合集
    bt_element_dynamic = b.get_backtrade_element(missing_ratio=0.1,
                                                 label=Label.Sample_FX1Y,
                                                 method=Method.dynamic,
                                                 sample=Sample.Fullsample)
    print(bt_element_dynamic)
    # 获取静态交易要素合集
    bt_element_static = b.get_backtrade_element(missing_ratio=0.1, label=Label.Sample_FX1Y, pool=Label.CommTradeble,
                                                method=Method.static, sample=Sample.Insample)
    bt_future_factor = b.get_future_factor(freq=Frequency.Min_60, begin_date='2020-01-01', db_type=DbConn.TSDB_169)
    bt_future_factor2 = b.get_future_factor(ids=['macd', 'macd_2'], freq=Frequency.Min_60, begin_date='2020-01-01',
                                            db_type=DbConn.TSDB_169)
    bt_future_factor3 = b.get_future_factor(ids=['macd', 'macd_2'], freq=Frequency.Min_60, begin_date='2020-01-01',
                                            codes='A', db_type=DbConn.TSDB_169)
    bt_future_factor4 = b.get_future_factor(ids='macd', freq=Frequency.Min_60, begin_date='2020-01-01',
                                            codes=['A', 'B'], db_type=DbConn.TSDB_169)
    print(bt_future_factor2)
