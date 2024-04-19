import datetime as dt
import itertools
import random
import re
import string
from typing import AnyStr

import pandas as pd

from Pandora.constant import DbConn
from Pandora.helper.database import DbManager


class Strs:
    """字符串操作"""

    @staticmethod
    def camel_to_snake(camel_case: str):
        """大驼峰（帕斯卡）转蛇形"""
        camel_case = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_case)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', camel_case).lower()

    @staticmethod
    def snake_to_camel(snake_case: str):
        """蛇形转大驼峰（帕斯卡）"""
        words = snake_case.split('_')
        return ''.join(word.title() for word in words)

    @staticmethod
    def is_ticker(tk: str) -> bool:
        return len(tk) >= 4 and re.match("^[a-zA-Z]{1,2}[0-9]{3,4}$", tk)

    @staticmethod
    def erase_digit(any_str: str, to_upper=True) -> str:
        rtn_str = re.sub("\d+", "", any_str)
        return rtn_str.upper() if to_upper else rtn_str

    @staticmethod
    def rand_digit(digit: int = 8) -> AnyStr:
        """ 指定长度的数字与字符的随机数
        :param digit: 位数
        :return: 随机数
        """
        seeds = f"{string.ascii_lowercase}{string.digits}"
        return "".join(random.sample(seeds, k=digit))

    @staticmethod
    def is_number(val: AnyStr) -> bool:
        """判断是否为一个数字"""
        try:
            f = float(val)
            if f != f or f == float('inf') or f == float('-inf'):
                return False
            return True
        except ValueError:
            return False


class Symbol(object):
    @staticmethod
    def get_contract(symbol: str, upper=True):
        if upper:
            return ''.join((i for i in symbol.upper() if i.isalpha()))

        else:
            return ''.join((i for i in symbol if i.isalpha()))

    @staticmethod
    def get_num_str_in_string(string: str):
        return ''.join((i for i in string if i.isnumeric()))

    @staticmethod
    def convert_to_standard(symbol):
        if isinstance(symbol, str):
            query = f"SELECT Code, Ticker, Exchange FROM FutureInfo_Basic WHERE Code = '{symbol}'"

            db = DbManager(DbConn.MSSQL_165)
            info = db.query(query)

            if info.empty:
                return symbol

            else:
                if info.at[0, 'Exchange'] in {'CZCE', 'CFFEX'}:
                    return info.at[0, 'Ticker'].upper()

                else:
                    return info.at[0, 'Ticker'].lower()

        else:
            if len(symbol) == 1:
                return Symbol.convert_to_exchange(list(symbol)[0])

            else:
                query = f"SELECT Code, Ticker, Exchange FROM FutureInfo_Basic WHERE Code in {tuple(symbol)}"
                db = DbManager(DbConn.MSSQL_165)
                info = db.query(query)

                symbol = pd.DataFrame(symbol)
                symbol.columns = ['Code']
                info = symbol.merge(info, how='left')
                loc = info.loc[:, 'Ticker'].isna()
                info.loc[loc, 'Ticker'] = info.loc[loc, 'Code']

                return info.loc[:, 'Ticker'].values

    @staticmethod
    def convert_to_exchange(symbol: str):
        if isinstance(symbol, str):
            query = f"SELECT Code, Ticker, Exchange FROM FutureInfo_Basic WHERE Ticker = '{symbol}'"

            db = DbManager(DbConn.MSSQL_165)
            info = db.query(query)

            if info.empty:
                return symbol

            else:
                return info.at[0, 'Code']

        else:
            if len(symbol) == 1:
                return Symbol.convert_to_exchange(symbol[0])

            else:
                query = f"SELECT Code, Ticker, Exchange FROM FutureInfo_Basic WHERE Ticker in {tuple(symbol)}"
                db = DbManager(DbConn.MSSQL_165)
                info = db.query(query)

                symbol = pd.DataFrame(symbol)
                symbol.columns = ['Ticker']
                info = symbol.merge(info, how='left')
                loc = info.loc[:, 'Code'].isna()
                info.loc[loc, 'Code'] = info.loc[loc, 'Ticker']

                loc = info.loc[:, 'Exchange'].isin({'CZCE', 'CFFEX'})
                info.loc[~loc, 'Code'] = info.loc[~loc, 'Code'].str.lower()

                return info.loc[:, 'Code'].values

    @staticmethod
    def get_4_digit(symbol: str):
        code = Symbol.convert_to_standard(symbol)
        return code[-4:]

    @staticmethod
    def code_to_ticker(code, date=dt.datetime.today()):
        contract = Symbol.get_contract(code)
        time = Symbol.get_num_str_in_string(code)

        if len(time) == 3:
            this_year = str(date.year)
            if int(time[0]) <= int(this_year[-1]) + 1:
                time = this_year[-2] + time
            else:
                time = str(int(this_year[-2]) - 1) + time
        elif len(time) < 3:
            time = None
            print("time in ticker shorter than 3 digits")

        return contract + time

    def get_future_all_format_codes(self, symbol: str, date=dt.datetime.today()):
        contract = self.get_contract(symbol)
        time_ = self.get_num_str_in_string(symbol)
        contracts = [contract.upper(), contract.lower()]

        if len(time_) == 4:
            times = [time_[1:], time_]

        elif len(time_) == 3:
            this_year = str(date.year)
            if int(time_[0]) <= int(this_year[-1]) + 1:
                times = [time_, this_year[-2] + time_]
            else:
                times = [time_, str(int(this_year[-2]) - 1) + time_]
        else:
            raise ValueError('Wrong code: {} parsed, please check parsed argument'.format(symbol))

        codes = [''.join(d) for d in itertools.product(contracts, times)]

        return codes
