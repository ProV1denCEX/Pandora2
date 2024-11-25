import numpy as np
import pandas as pd
import talib


def get_mks_factor(quote, param):
    feat = {}
    for code, group in quote.groupby('symbol'):
        A = group['close_price']
        # A = (group['close_price'] + group['low_price'] + group['high_price']) / 3
        # A = (group['close_price'] + group['low_price'] + group['high_price']) / 3 * group['Amount']
        # f = pd.Series(mks(A, param), index=group.index)

        # A = (group['Amount'] / group['Volume']).fillna(method='ffill')

        f = pd.Series(mks(A, param), index=group.index)
        # f = pd.Series(mks_test(A, param), index=group.index)
        feat[code] = f

    return pd.DataFrame(feat)


def mks(A: np.ndarray, n: int, imax: int = 0):
    df = pd.Series(A)

    f = pd.Series(0, index=df.index)
    denom = 0
    for i in range(1, n):

        if imax and i > imax:
            break

        chg = np.sign(df.pct_change(i))
        f += chg.fillna(0).rolling(n - i).sum()

        denom += n - i

    f = f / denom

    return f


def get_natr_factor(quote, param):
    feat = {}

    for code, group in quote.groupby('symbol'):
        f = talib.NATR(group['high_price'], group['low_price'], group['close_price'], param)

        feat[code] = f

    return pd.DataFrame(feat)


def get_std_factor(quote, param):
    feat = {}

    for code, group in quote.groupby('symbol'):

        # alpha = 2 / (param + 1)
        f = group['close_price'].rolling(param).std()

        feat[code] = f

    return pd.DataFrame(feat)


def get_ema_factor(quote, param):
    feat = {}

    for code, group in quote.groupby('symbol'):
        ma1 = group['close_price'].ewm(span=param).mean()

        feat[code] = ma1

    return pd.DataFrame(feat)


def get_stm_factor(quote, param):
    feat = {}
    for code, group in quote.groupby('symbol'):
        hh = group.loc[:, 'high_price'].rolling(param, min_periods=1).max()
        ll = group.loc[:, 'low_price'].rolling(param, min_periods=1).min()
        feat[code] = ((group.loc[:, 'close_price'] * 2 - (hh + ll)).ewm(span=5).mean()) / ((hh - ll).ewm(span=5).mean())

    return pd.DataFrame(feat)


def get_rsi_factor(quote, param):
    feat = {}
    for code, group in quote.groupby('symbol'):
        ret = group['close_price'].diff()

        loc = ret > 0
        rv = np.abs(ret)

        good_rv = pd.Series(0, index=ret.index)
        good_rv[loc] = rv[loc]

        bad_rv = pd.Series(0, index=ret.index)
        bad_rv[~loc] = rv[~loc]

        good_rv = good_rv.ewm(span=param).mean()
        bad_rv = bad_rv.ewm(span=param).mean()

        f = good_rv / (good_rv + bad_rv)

        f = f * 2 - 1

        feat[code] = f

    return pd.DataFrame(feat)


def load_factor(quote, param, factor='mks'):
    factor = pd.read_csv(f'./{factor}_{param}.csv', index_col=0, parse_dates=True)
    return factor
