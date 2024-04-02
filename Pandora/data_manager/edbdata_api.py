# -*- coding: utf-8 -*-

import pandas as pd
from Pandora.data_manager.data_api import FutureDataAPI
from Pandora.constant import DbConn, EdbType, Unit_To_Ton
from Pandora.helper.date import TDays, DateFmt
from Pandora.helper.database import DbManager


class EdbDataApi(object):
    data_api = FutureDataAPI()

    def __init__(self, begin_date, end_date, application_scenarios=None):
        self.begin_date = begin_date
        self.to_date = pd.to_datetime(end_date).date()
        self.end_date = TDays.add(end_date, 5)
        self.application_scenarios = application_scenarios

        self.indicators = {}
        self.infos = {}
        self.data_info = {}
        self.calendar_days = pd.DataFrame()
        self.trading_days = pd.DataFrame()

    def get_edb_data(self, ind_types=None, ind_dates=None):
        """
        获取所有处理好的基本面数据或者指定类别的基本面数据
        :param ind_types: 品种指标信息表,取值None或list
        :param ind_dates: 品种指标信息表,取值None或list,必须与ind_types一一对应
        :returns indicators: 处理好的基本面指标合集
        :returns infos: 基本面指标信息
        """
        self.get_indicator_info(ind_types, ind_dates)
        self.get_calendar_trading_days()

        indicator_types = {
            'exchange_inventory': self.process_exchange_inventory,
            'social_inventory': self.process_quantity_data,
            'other_inventory': self.process_quantity_data,
            'operating_rate': self.process_operating_rate,
            'produce': self.process_quantity_data,
            'demand': self.process_similar_data,
            'spot_price': self.process_price_data,
            'material_price': self.process_price_data,
            'bias_ratio': self.process_similar_data
        }

        composite_method_dict = {keys: 'sum' if keys in ['exchange_inventory', 'social_inventory']
                                 else 'weight' for keys in indicator_types}

        ind_types = ind_types or self.data_info.keys()

        for ind_type in ind_types:
            composite_method = composite_method_dict.get(ind_type)
            indicator_info = self.data_info[ind_type]
            indicator, info = indicator_types.get(ind_type)(composite_method, indicator_info)
            self.indicators[ind_type] = indicator
            self.infos[ind_type] = info
            print(ind_type + ' is done')

        return self.indicators, self.infos

    def get_indicator_info(self, ind_types, ind_dates):
        """
        获取基本面数据信息
        :returns data_info: 数据信息表
        """
        table = 'CTA_RESEARCH.dbo.FundamentalFactorClassify'

        if self.application_scenarios is None or self.application_scenarios == 'research':
            mssql = DbManager(DbConn.MSSQL_165)

        elif self.application_scenarios in ['real_trade', 'realtrade', 'trade']:
            mssql = DbManager(DbConn.MSSQL_162)

        if ind_types is None:
            query = f"SELECT DISTINCT Classification FROM {table} "
            types = mssql.query(query)
            ind_types = types['Classification'].unique().tolist()

        for ind_type in ind_types:
            if ind_dates is None:
                query = f"SELECT * FROM {table} WHERE Classification='{ind_type}' " \
                        f"AND Date = (SELECT MAX(Date) FROM {table} where Classification='{ind_type}') "
            else:
                index = ind_types.index(ind_type)
                query = f"SELECT * FROM {table} WHERE Classification='{ind_type}' " \
                        f"AND Date = '{ind_dates[index]}' "

            values = mssql.query(query)
            self.data_info[ind_type] = values

    def get_calendar_trading_days(self):
        """
        获取日历日和交易日信息
        """
        # 获取交易日信息
        trading_days = TDays.period(self.begin_date, self.end_date, fmt=DateFmt.Y_M_D, exchange='SHFE')
        trading_days = pd.DataFrame(trading_days, columns=['Date'])
        trading_days.loc[:, 'Date'] = pd.to_datetime(trading_days.loc[:, 'Date'])
        trading_days.loc[:, 'Date'] = trading_days.loc[:, 'Date'].apply(lambda x: x.date())
        trading_days.loc[:, 'next_trade_date'] = trading_days['Date'].shift(-1)
        self.trading_days = trading_days.reset_index(drop=True)

        # 获取日历日
        self.calendar_days = pd.date_range(self.begin_date, self.end_date, freq="D")
        self.calendar_days = [x.date() for x in self.calendar_days]

    def get_econ_data(self, indicator_info):
        """
        获取edb数据并预处理
        :param indicator_info: 品种指标信息表 
        :returns exchange_inventory: 处理好的数据
        :returns info: contract_id和edb数据滞后时间
        """
        ids = tuple(indicator_info["DbCode"].unique().tolist())
        ids_basic = [str(_id) for _id in ids if _id.startswith('B')]
        ids_adv = [str(_id) for _id in ids if _id.startswith('A')]
        is_exsit_basic_adv = ids_basic and ids_adv
        ids_ind = {'basic': ids_basic, 'adv': ids_adv} if is_exsit_basic_adv else \
            {'basic': ids_basic} if ids_basic else {'adv': ids_adv}

        ind_dict = {}
        industry = []

        for types in ids_ind:
            id_original = ids_ind[types]
            ids = [int(i[1:]) for i in id_original]

            edb_type = EdbType.BASIC if types.startswith('basic') else EdbType.ADV

            _, data = self.data_api.get_edb_data(ids, industry, self.begin_date, self.end_date,
                                                 edb_type)
            data.rename(columns={'Date': 'date'}, inplace=True)
            data = data.set_index(['date', 'ID'])['EconData'].unstack()

            # 更改列名
            data.columns = ['B' + str(x) if types == 'basic' else 'A' + str(x) for x in data.columns]
            ind_dict[types] = data

        indicator = pd.concat([ind_dict['basic'], ind_dict['adv']], axis=1) if is_exsit_basic_adv else \
            ind_dict['basic'] if ids_basic else ind_dict['adv']

        # 指标滞后信息
        publishlag = {keys: values for keys, values in zip(indicator_info['DbCode'], indicator_info['PublishLag'])}
        # 对指标进行滞后
        indicator = self.lag_econ_data(indicator, publishlag)

        contract_id = {}
        groups = indicator_info['DbCode'].groupby(indicator_info['Contract'])
        for group, value in groups:
            contract_id[group] = list(value.values)

        info = {'contract_id': contract_id, 'publishlag_indicator': publishlag}

        return indicator, info

    def lag_econ_data(self, indicator, publishlag):
        """
        对edb数据进行滞后
        :param indicator: 指标数据
        :param publishlag: 指标滞后数据
        :returns indicator: 经滞后处理的edb数据
        """
        ids = indicator.columns.tolist()
        # 数据预处理，平移
        factor = pd.DataFrame(index=self.calendar_days)
        indicator.index = [x.date() for x in indicator.index]
        for i in ids:
            tmp = indicator[[i]].copy()
            tmp = pd.merge(tmp[[i]], self.trading_days,
                           left_index=True, right_on='Date', how='left')
            tmp['next_trade_date'].fillna(method='ffill', inplace=True)
            tmp = tmp[['next_trade_date', i]]
            tmp.columns = ['date', i]
            tmp.dropna(inplace=True)
            tmp.drop_duplicates('date', keep='last', inplace=True)
            tmp.set_index('date', inplace=True)
            factor = factor.join(tmp)

        # 对指标进行滞后
        factors = pd.DataFrame(index=self.trading_days['Date'])
        for i in ids:
            # 因子滞后信息
            delay = publishlag.get(i)
            tmp = factor[i]

            # 按交易日筛选
            tmp = tmp[tmp.index.isin(self.trading_days['Date'])]
            tmp = tmp.shift(-1)

            # 按照交易日进行滞后
            if delay != 0:
                tmp = tmp.shift(delay)
            factors = factors.join(tmp)
        factors = factors.loc[:self.to_date, :]
        return factors

    def process_exchange_inventory(self, composite_method, indicator_info):
        """
        获取交易所库存数据并预处理
        :param composite_method: 聚合方法:'sum'和'weight'两种
        :param indicator_info: 品种指标信息表 
        :returns exchange_inventory: 处理好的交易所库存
        :returns info: contract_id和edb数据滞后时间
        """
        indicator, info = self.get_econ_data(indicator_info)

        ids = tuple(indicator_info["DbCode"].unique().tolist())
        ids_basic = [str(_id) for _id in ids if _id.startswith('B')]
        receipt = indicator_info[indicator_info['Description'].apply(lambda x:
                                                                     x.startswith('仓单数量') or x.startswith('注册仓单量'))]
        receipt = receipt[['Contract', 'DbCode']]
        id_contract = receipt.set_index('DbCode')['Contract'].to_dict()
        future_basic = self.data_api.get_future_basic(list(id_contract.values()))

        multiplier = future_basic[['Contract', 'Multiplier']].drop_duplicates('Contract', keep='last')
        contract_multiplier = multiplier.set_index('Contract').to_dict()['Multiplier']
        # 鸡蛋单位不一致，转为一致
        contract_multiplier['JD'] = 5
        # 分为仓单和库存两类分开处理
        basic_receipt = list(id_contract.keys())
        basic_inventory = list(set(ids_basic).difference(set(basic_receipt)))

        ids_ind = {'basic_inventory': basic_inventory,
                   'basic_receipt': basic_receipt}

        for types in ids_ind:
            ids = ids_ind[types]
            # 单位转换
            for i in ids:
                unit = indicator_info[indicator_info['DbCode'] == i]['QuoteUnit'].values[0]
                if unit == '吨':
                    continue
                elif unit in ('张', '手'):
                    contract = id_contract[i]
                    multiplier = contract_multiplier[contract]
                    indicator[i] = indicator[i] * multiplier
                else:
                    indicator[i] = indicator[i] * Unit_To_Ton[unit]

        # 将同品种不同指标相加并得到最大滞后天数
        exchange_inventory, info = self.get_composite_indicator(composite_method, indicator_info,
                                                                info, indicator)
        return exchange_inventory, info

    @staticmethod
    def get_composite_indicator(composite_method, indicator_info, info, indicator):
        """
        获取合成指标及滞后时间
        :param composite_method: 聚合方法:'sum'和'weight'两种
        :param indicator_info: 品种指标信息表
        :param info: 品种-指标对应表及滞后信息
        :param indicator: 指标数据
        :returns indicator_composite: 合成指标
        :returns publishlag_contract: 合成指标滞后时间
        """
        # 将同品种不同合约相加
        indicator = indicator.fillna(method='ffill')
        indicator_composite = pd.DataFrame(index=indicator.index)
        groups = indicator_info['DbCode'].groupby(indicator_info['Contract'])
        for group, value in groups:
            if composite_method == 'sum':
                indicator_composite[group] = indicator[value.values].sum(axis=1, min_count=1)
            else:
                indicator_composite[group] = indicator[value.values].mean(axis=1)

        # 合成指标的最大滞后天数
        publishlag_contract = {}
        groups = indicator_info['PublishLag'].groupby(indicator_info['Contract'])
        for group, value in groups:
            publishlag_contract[group] = max(value.values)

        info['publishlag_contract'] = publishlag_contract

        return indicator_composite, info

    def process_operating_rate(self, composite_method, indicator_info):
        """
        获取开工率和产能利用率数据并预处理
        :param composite_method: 聚合方法:'sum'和'weight'两种
        :param indicator_info: 品种指标信息表 
        :returns indicator: 处理好的开工率或产能利用率数据
        :returns info: contract_id和edb数据滞后时间
        """
        indicator, info = self.get_econ_data(indicator_info)
        ids = tuple(indicator_info["DbCode"].unique().tolist())
        for i in ids:
            unit = indicator_info[indicator_info['DbCode'] == i]['QuoteUnit'].values[0]
            if unit == '%':
                continue
            else:
                indicator[i] = indicator[i] * 100

        # 将同品种不同指标加权并得到最大滞后天数
        indicator, info = self.get_composite_indicator(composite_method, indicator_info,
                                                       info, indicator)
        return indicator, info

    def process_similar_data(self, composite_method, indicator_info):
        """
        获取同品种同类数据并预处理
        :param composite_method: 聚合方法:'sum'和'weight'两种
        :param indicator_info: 品种指标信息表
        :returns indicator: 处理好的数据
        :returns info: contract_id和库存b数据滞后时间
        """
        indicator, info = self.get_econ_data(indicator_info)
        # 将同品种不同指标加权并得到最大滞后天数
        indicator, info = self.get_composite_indicator(composite_method, indicator_info,
                                                       info, indicator)
        return indicator, info

    def process_quantity_data(self, composite_method, indicator_info):
        """
        获取数量数据并预处理
        :param composite_method: 聚合方法:'sum'和'weight'两种
        :param indicator_info: 品种指标信息表 
        :returns indicator: 处理好的产量数据
        :returns info: contract_id和edb数据滞后时间
        """
        indicator, info = self.get_econ_data(indicator_info)
        ids = tuple(indicator_info["DbCode"].unique().tolist())
        for i in ids:
            unit = indicator_info[indicator_info['DbCode'] == i]['QuoteUnit'].values[0]
            if unit == '吨':
                continue
            else:
                indicator[i] = indicator[i] * Unit_To_Ton[unit]

        # 将同品种不同合约相加并得到最大滞后天数
        indicator, info = self.get_composite_indicator(composite_method, indicator_info,
                                                       info, indicator)
        return indicator, info

    def process_price_data(self, composite_method, indicator_info):
        """
        获取价格类数据并预处理
        :param composite_method: 聚合方法:'sum'和'weight'两种
        :param indicator_info: 品种指标信息表 
        :returns indicator: 处理好的价格数据
        :returns info: contract_id和价格数据滞后时间
        """
        indicator, info = self.get_econ_data(indicator_info)
        # 美元对人民币汇率
        industry = []
        edb_type = EdbType.BASIC
        _, data = self.data_api.get_edb_data([10], industry, self.begin_date, self.end_date,
                                             edb_type)
        data = data.set_index(['Date', 'ID'])['EconData'].unstack()
        data.rename(columns={'Date': 'date'}, inplace=True)
        dollar = pd.DataFrame(index=indicator.index)
        dollar['B10'] = data[10].fillna(method='ffill')
        dollar['B10'] = dollar['B10'].fillna(method='bfill')

        ids = tuple(indicator_info["DbCode"].unique().tolist())

        for i in ids:
            unit = indicator_info[indicator_info['DbCode'] == i]['QuoteUnit'].values[0]
            if unit == '元/吨':
                continue

            if unit.startswith('美元'):
                indicator[i] = indicator[i] * dollar['B10']

            denominator = unit.split('/')[1]
            if denominator != '吨':
                indicator[i] = indicator[i] / Unit_To_Ton[denominator]
            else:
                pass

        # 将同品种不同指标加权并得到最大滞后天数
        indicator, info = self.get_composite_indicator(composite_method, indicator_info,
                                                       info, indicator)
        return indicator, info


if __name__ == '__main__':
    from_date = '2014-01-01'
    to_date = '2023-02-08'
    # None, 'research' or 'real_trade'
    application_scenarios = 'research'
    eda = EdbDataApi(from_date, to_date, application_scenarios)
    indicator_types = ['exchange_inventory', 'social_inventory']
    indicator_dates = ['2022-07-05', '2022-04-09']
    edb_indicator, edb_info = eda.get_edb_data(ind_types=indicator_types, ind_dates=indicator_dates)
