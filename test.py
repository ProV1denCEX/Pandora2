import pandas as pd
import datetime as dt
import numpy as np
from matplotlib import pyplot as plt
import bottleneck as bn
from tqdm.auto import tqdm

from Pandora.constant import Frequency, SymbolSuffix, Interval
from Pandora.data_manager import get_api
from Pandora.research import *

codes = CODES_TRADABLE

start = dt.datetime(2014, 1, 1)
quote_bt, ret = get_quote(codes, start=start, end=dt.datetime.now(), freq=Frequency.Daily)

COMMISSION = 3e-4
vol_window = 5
vol_tgt = 0.2

# weight = get_weight_by_ew(quote_bt)
weight = get_weight_by_std_ratio(quote_bt, vol_window, vol_tgt, day_count=1, n=3)

from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns

cmap = LinearSegmentedColormap.from_list("custom_green_red", ["green", "white", "red"], N=256)

def plot_perf(sharpe, calmar):
    mat1 = sharpe
    mat2 = calmar

    fig, (ax1, ax2) = plt.subplots(1,2, figsize=(18, 6))  # 1行2列的子图

    sns.heatmap(mat1, cmap=cmap, annot=True, ax=ax1, vmin=-1, vmax=1)
    ax1.set_title('sharpe')

    sns.heatmap(mat2, cmap=cmap, annot=True, ax=ax2, vmin=-1, vmax=1)
    ax2.set_title('calmar')

    plt.tight_layout()  # 自动调整子图参数，使之填充整个图像区域
    plt.show()

api = get_api()

contracts = api.get_future_contracts()

loc = contracts['product_id'].isin(codes)
contracts = contracts[loc]

loc = (contracts['symbol'] == (contracts['product_id'] + SymbolSuffix.MC)) | (contracts['symbol'] == (contracts['product_id'] + SymbolSuffix.MNC))
mc_contracts = contracts[loc]
contracts = contracts[~loc]

loc = mc_contracts['symbol'].str.endswith(SymbolSuffix.MC)
mc_contracts = mc_contracts[loc]

mc_contracts = mc_contracts.merge(contracts[['symbol', 'name']], how='left', on='name', suffixes=('', '_mc'))

LIQ_THRES = 1e7
windows = [50, 100, 150, 200, 250, 350, 500]
windows_macd = [10, 25, 50, 100, 150, 200, 250]
rebals = [1, 3, 5, 10]


quotes = []

with tqdm(total=contracts['name'].nunique()) as pbar:
    for name, group in contracts.groupby('name'):
        expire_date = group['expire_date'].iat[-1]

        quote = api.get_future_quote(
            group['symbol'].iat[0],
            begin_date=group['list_date'].iat[0],
            end_date=group['expire_date'].iat[-1],
            freq=Interval.DAILY
        )

        quote['name'] = name
        quote['product_id'] = group['product_id'].iat[0]
        quote['ptm_day'] = expire_date - quote['datetime']

        quotes.append(quote)
        pbar.update()

quote_raw = pd.concat(quotes)
quote_raw

def get_basis_mom_factor(quote_raw, window, mom='stm'):
    feat = pd.DataFrame(index=ret.index, columns=ret.columns)

    loc = quote_raw['turnover'] > LIQ_THRES
    quote = quote_raw[loc]

    col = f"{mom}_{window}"

    for (product_id, dt_), group in quote.groupby(['product_id', 'datetime']):

        group = group.sort_values('ptm_day')

        loc = pd.isna(group[col])
        group = group[~loc]

        if len(group) <= abs(1):
            continue

        feat.at[dt_, product_id + SymbolSuffix.MC] = group[col].iat[0] - group[col].iloc[1:].mean()

    return feat.sort_index()



def get_macd_dif_factor(quote_, fast):
    slow = int(fast * 26 / 12)
    signal = int(fast * 9 / 12)

    dif, dea, bar = talib.MACD(quote_['close_price'], fast, slow, signal)
    return dif

def get_macd_dea_factor(quote_, fast):
    slow = int(fast * 26 / 12)
    signal = int(fast * 9 / 12)

    dif, dea, bar = talib.MACD(quote_['close_price'], fast, slow, signal)
    return dea

def get_macd_bar_factor(quote_, fast):
    slow = int(fast * 26 / 12)
    signal = int(fast * 9 / 12)

    dif, dea, bar = talib.MACD(quote_['close_price'], fast, slow, signal)
    return bar

def get_macdn_dif_factor(quote_, fast):
    slow = int(fast * 26 / 12)
    signal = int(fast * 9 / 12)

    dif, dea, bar = talib.MACD(quote_['close_price'], fast, slow, signal)
    std = quote['close_price'].rolling(fast).std()
    ret = dif / std
    loc = std == 0
    ret[loc] = 0

    return ret

def get_macdn_dea_factor(quote_, fast):
    slow = int(fast * 26 / 12)
    signal = int(fast * 9 / 12)

    dif, dea, bar = talib.MACD(quote_['close_price'], fast, slow, signal)

    std = quote['close_price'].rolling(fast).std()
    ret = dea / std
    loc = std == 0
    ret[loc] = 0

    return ret

def get_macdn_bar_factor(quote_, fast):
    slow = int(fast * 26 / 12)
    signal = int(fast * 9 / 12)

    dif, dea, bar = talib.MACD(quote_['close_price'], fast, slow, signal)
    std = quote['close_price'].rolling(fast).std()
    ret = bar / std
    loc = std == 0
    ret[loc] = 0

    return ret


FUNCS = {
    'f_macd_dif': (get_macd_dif_factor, windows_macd),
    'f_macd_dea': (get_macd_dea_factor, windows_macd),
    'f_macd_bar': (get_macd_bar_factor, windows_macd),
    'f_macdn_dif': (get_macdn_dif_factor, windows_macd),
    'f_macdn_dea': (get_macdn_dea_factor, windows_macd),
    'f_macdn_bar': (get_macdn_bar_factor, windows_macd),
}


def get_factor(quote_raw_, name_, func, windows_):
    # cols = ['datetime', 'product_id', 'symbol', 'ptm_day', 'turnover']

    quote_raw_ = quote_raw_.sort_values('datetime')
    quotes = []
    for symbol, quote in quote_raw_.groupby('symbol'):
        for window in windows_:
            quote[f'{name_}_{window}'] = func(quote, window)
            # cols.append(f'{name_}_{window}')

        quotes.append(quote)

    return pd.concat(quotes)


cs_quantile=0.2
RES = {}
for name, (func, windows_) in FUNCS.items():
    sharpe = pd.DataFrame()
    calmar = pd.DataFrame()

    if name in RES:
        daily_ret = RES[name]

    else:
        daily_ret = {}

        quote_factor = get_factor(quote_raw, name, func, windows_)

        for window1 in windows_:
            feat_ = get_basis_mom_factor(quote_factor, window1, mom=name)

            for window in windows:
                feature = feat_.rolling(window, min_periods=1).mean()

                open_signal = trade_by_cs(feature, cs_interval=1, cs_quantile=cs_quantile)

                _, open_signal = weight.align(open_signal, join='left')

                daily_base = backtest_factor(open_signal, weight, ret)

                daily_ret[(window1, window)] = daily_base

        daily_ret = pd.DataFrame(daily_ret)
        RES[name] = daily_ret
        # daily_ret.cumsum().plot(figsize=(16, 9))

    for a, b in daily_ret.columns:
        sharpe.loc[a, b] = calc_sharpe(daily_ret[(a, b)].loc[dt.date(2016, 1, 1):])
        calmar.loc[a, b] = calc_calmar(daily_ret[(a, b)].loc[dt.date(2016, 1, 1):])

    print(name)
    plot_perf(sharpe, calmar)

