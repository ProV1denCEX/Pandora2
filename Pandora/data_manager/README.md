## 数据提取API


### 使用说明
1. copy 项目目录下`utility/template/settings.ini.tpl` 文件为  `C:\Users\@User@\.DataManager\settings.ini`
2. 填充 `settings.ini` 文件中的相关数据库连接信息
3. 执行 `tests/data_api_test.py` 进行单元测试 **(需要安装pytest库支持. 模块本身运行不需要, 未在requirements.txt中包含)**


### API接口列表

> 所有函数签名及注释对入参及返回值有详细描述

- [x] 期货基本信息 get_future_basic
- [x] 期货行情数据 get_future_quote
- [x] 期货主力合约 get_future_main_ticker
- [x] 期货复权系数 get_future_coefadj
- [x] 期货因子数据 get_future_factor
- [x] 基本面行情数据 get_edb_data
- [x] 期货主连/非主连复权行情 get_future_quote_main_adj
- [x] 期货商品指数 get_future_quote_index

### 函数签名
```python
def get_future_basic(self, codes: Union[str, Sequence[str]]) -> pd.DataFrame:
        """
            获取期货基本信息

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]

        :return: DataFrame
        columns=(Code ,Ticker ,Contract ,ListDate ,DelistDate ,DeliverDate,Multiplier,TickSize, Exchange,
               MinMargin ,PriceLimit ,QuoteUnit ,MultiplierUnit ,FutureClass ,ComFutureClass ,UpdateTime)
        """
    ...

    
def get_future_quote(self, codes: Union[str, Sequence[str]],
                     begin_date: Union[str, date],
                     end_date: Union[str, date],
                     freq=Frequency.Min_1) -> pd.DataFrame:
    """
        获取期货或期权的行情数据接口

    :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                  eg: "A2201,AG" or ["A2201","A2203","B","TF"]
    :param begin_date: 开始日期
    :param end_date:  结束日期
    :param freq: 行情频次(enum) 1(默认)/5/15/30/60/daily

    :return: DataFrame
    columns=(Datetime,Date,TradeDate,Contract,Ticker,[OHLC], PrevClose,Volume,Amount, DealAmount,OpenInterest,
             Buy1,Sale1,Bc1,Sc1)
    """
    ...


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
    ...

    
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
    ...

    
def get_future_factor(self, ids: Union[str, Sequence[str]],
                          tags: Union[str, Sequence[str]],
                          freq: Frequency = Frequency.Min_1,
                          begin_date: Union[str, date] = None,
                          end_date: Union[str, date] = None) -> pd.DataFrame:
        """
            获取因子数据

        :param ids: factorId.
        :param tags: 因子标签. 在factorId过滤后再次过滤
        :param freq: 行情频次(enum) 1/5/15/30/60/daily, 默认1min
        :param begin_date: 开始日期
        :param end_date: 结束日期

        :return: DataFrame
                 columns=(Datetime, Date, FactorId, Tag, FactorValue, FactorReturn)
        """
    ...

    
def get_edb_data(self, ids: Union[int, Sequence[int]],
                     industry: Union[str, Sequence[str]],
                     edb_type=EdbType.BASIC) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
            获取基本面基础信息数据

        :param ids: 基本面数据中定义的ID, 与industry不可共存
        :param industry: 基本面数据中定义的版块类型, 与id不可共存
        :param edb_type: EdbType Enum

        :return: Tuple[Dataframe(基本面基础信息), Dataframe(基本面行情信息)], 通过ID关联
        """
    ...

    
def get_future_quote_main_adj(self, codes: Union[str, Sequence[str]],
                                  begin_date: Union[str, date],
                                  end_date: Union[str, date],
                                  adjusted=True,
                                  freq=Frequency.Daily) -> pd.DataFrame:
        """
            获取主连复权/不复权行情

        :param codes: 品种或合约代码,支持混合. 以逗号分割的字符串或集合. 传None查全部.
                      eg: "A2201,AG" or ["A2201","A2203","B","TF"]
        :param begin_date: 开始日期
        :param end_date:  结束日期
        :param adjusted: 是否为复权数据, 默认true
        :param freq: 行情频次(enum) 1/5/15/30/60/daily(默认)

        :return: DataFrame
        columns=(Datetime,Date,TradeDate,Contract,Ticker,[OHLC],PrevClose,Volume,Amount,DealAmount,OpenInterest)
        """
    ...

    
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
    ...

```

### 包结构说明

- datafeed api实现模块
- tests 对api实现的所有函数单元测试
- utility 工具类

### 输出报告

- 执行tests/data_api_test.py即可在`C:\Users\@User@\.DataManager\Logs\api_timer_yyyymmdd.txt`中输出函数性能报告

