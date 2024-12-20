import datetime as dt

import bottleneck as bn
import numpy as np
import pandas as pd

from Pandora.helper import TDays

COMMISSION = 2e-4

CODES_MM = set(  # 25 in total
    "rb;i;j;hc;jm;"  # 黑色 5
    "ni;al;zn;"  # 金属 3
    "CF;AP;SR;y;m;c;p;"  # 农产 7
    "bu;MA;TA;ru;sp;fu;SA;v;FG;pp"  # 能化 10
    .split(';')
)

CODES_INTER = {  # 22
    "bu", "fu", "v", "TA", "eb", "MA", "eg", "ru",  # 能化 8
    "CF", "AP", "p", "RM", "jd",  # 农产 5
    "cu", "zn", "al",  # 有色 3
    "hc", "i", "j", "rb", "jm"  # 黑色 5
                          "ag",  # 贵金属 1
}

CODES_SHORT = {  # 25 in total
    'ag',  # 贵金属
    'p', 'AP', 'lh', 'PK', 'CJ',  # 农产 5
    'UR', 'SA', 'eb', 'eg', 'PF', 'pg', 'v', 'FG', 'TA', 'MA', 'sc',  # 能化 'MA', 'OI', 'Y'  11   5个与mm一致
    'rb', 'hc', 'i', 'j', 'jm',  # 黑色 5
    'al', 'ni', 'cu',  # 金属  3  'CU'
    'lc', 'ec', 'si', 'ao',  # new 4
}

CODES_TRADABLE = {
    'a', 'ag', 'al', 'AP', 'au', 'b', 'bu', 'c', 'CF', 'CJ', 'cs', 'cu', 'eb', 'eg', 'FG', 'hc', 'fu',
    'i', 'j', 'jd', 'jm', 'l', 'lh', 'm', 'MA', 'ni', 'OI', 'p', 'pb', 'PF', 'pg', 'PK', 'pp', 'rb', 'RM',
    'ru', 'SA', 'sc', 'SF', 'SM', 'sn', 'sp', 'SR', 'ss', 'TA', 'UR', 'v', 'y', 'zn'
}

CODES_TRADABLE_SL = {
    'a', 'ag', 'al', 'AP', 'ao', 'au',
    'b', 'bu', 'bc', 'br',
    'c', 'CF', 'CJ', 'cs', 'cu',
    'eb', 'ec', 'eg',
    'FG', 'fu', 'hc', 'i', 'j', 'jd', 'jm',
    'l', 'lc', 'lh', 'm', 'MA', 'ni', 'nr', 'OI',
    'p', 'pb', 'PF', 'pg', 'PK', 'pp', 'PX',
    'rb', 'RM', 'ru', 'SA', 'sc', 'SF', 'SM', 'sn', 'sp', 'SR', 'ss', 'si', 'SH',
    'TA', 'UR', 'v', 'y', 'zn',
}

CODES_FORBIDDEN_SL = {'jd', 'bc', 'CJ', 'br'}

CODES_EQUITY_INDEX = {"IF", "IH", "IC", "IM"}
CODES_TREASURY = {"T", "TL", "TF", "TS"}

CODES_NAME_MAP = {
    "MM": CODES_MM,
    "INTER": CODES_INTER,
    "SHORT": CODES_SHORT,
    "TRADABLE": CODES_TRADABLE,
    "TRADABLE_SL": CODES_TRADABLE_SL,
    "FORBIDDEN_SL": CODES_FORBIDDEN_SL,
    "EQUITY_INDEX": CODES_EQUITY_INDEX,
    "TREASURY": CODES_TREASURY,
}

SectorInfo = {
    "Agriculture": {"PK", "RM", "CJ", "PM", "OI", "CY", "RI", "AP", "JR", "P", "RS", "LH", "LR", "JD",
                    "A", "Y", "RR", "B", "CS", "CF", "SR", "M", "C", "WH", "RO", "WS"},

    "Chemical": {"FU", "EG", "SA", "PP", "EB", "SC", "L", "BU", "MA", "PF", "SP", "PG",
                 "TA", "V", "RU", "UR", "LU", "NR", "FG", "FB", "BB"},

    "NonFerrous": {"AU", "CU", "PB", "BC", "SN", "AL", "AG", "NI", "ZN"},

    "Ferrous": {"RB", "I", "HC", "J", "SF", "JM", "SS", "SM", "WR", "ZC"},

    "EquityIndex": {"IC", "IF", "IH", "IM"},

    "Treasury": {"T", "TS", "TF", "TL"},
}


def get_quote(codes, start=dt.datetime(2015, 1, 1), end=dt.datetime.now(), **kwargs):
    from Pandora.data_manager import FutureDataAPI

    api = FutureDataAPI()

    quote_bt = api.get_future_quote_main_adj(codes, start, end, **kwargs)

    quote_bt.set_index('datetime', inplace=True)
    quote_bt.sort_index(inplace=True)

    ret = get_bar_ret(quote_bt)

    return quote_bt, ret


def get_bar_ret(quote_bt: pd.DataFrame):
    ret = {}
    for code, group in quote_bt.groupby('symbol'):
        f = pd.Series(group['close_price'].pct_change().shift(-1), index=group.index)
        ret[code] = f

    ret = pd.DataFrame(ret)

    if 'fu00' in ret.columns:
        ret.loc[:dt.datetime(2018, 8, 1), 'fu00'] = np.nan

    if 'ni00' in ret.columns:
        ret.loc[dt.datetime(2022, 3, 7): dt.datetime(2022, 3, 31), 'ni00'] = 0

    if 'bu00' in ret.columns:
        ret.loc[:dt.datetime(2015, 10, 1), 'bu00'] = np.nan

    return ret


def trade_by_quantile_imba(feature, window, quantile_upper_long, quantile_lower_long,
                           window_short=None,
                           quantile_upper_short=None, quantile_lower_short=None):
    quantile_lower_short = quantile_lower_short or 1 - quantile_upper_long
    quantile_upper_short = quantile_upper_short or 1 - quantile_lower_long
    window_short = window_short or window

    open_signal_df = pd.DataFrame(np.nan, index=feature.index, columns=feature.columns)
    for col in range(feature.shape[1]):
        ft = feature.iloc[:, col].dropna()
        upper_long = ft.rolling(window=window, min_periods=min(window, 100)).quantile(quantile_upper_long)
        lower_long = ft.rolling(window=window, min_periods=min(window, 100)).quantile(quantile_lower_long)

        # buggy when upper_short > lower_long
        upper_short = ft.rolling(window=window_short, min_periods=min(window_short, 100)).quantile(quantile_upper_short)
        lower_short = ft.rolling(window=window_short, min_periods=min(window_short, 100)).quantile(quantile_lower_short)

        sig = pd.Series(np.nan, index=ft.index)
        loc = (ft >= upper_long)
        # loc = (ft >= upper_long) & (ft.diff() > 0)
        # loc = (ft >= upper_long) & (ft.shift() < upper_long)
        # loc = (ft >= upper_long) & (upper_long - lower_short > 0.25)
        sig.loc[loc] = 1
        # sig.loc[loc] = 0

        loc = (ft <= lower_long) & (ft.shift() > lower_long.shift())
        sig.loc[loc] = 0
        # sig.loc[loc] = 0

        loc = (ft >= upper_short) & (ft.shift() < upper_short.shift())
        sig.loc[loc] = 0

        loc = (ft <= lower_short)
        # loc = (ft <= lower_short) & (ft.diff() < 0)
        # loc = (ft <= lower_short) & (ft.shift() > lower_short)
        # loc = (ft <= lower_short) & (upper_long - lower_short > 0.25)
        sig.loc[loc] = -1

        # open_signal_df[open_signal_df.columns[col]] = sig

        sig_ = np.sign(sig.ffill().diff().fillna(0))
        sig_.loc[sig_ == 0] = np.nan
        sig_.loc[sig == 0] = 0
        open_signal_df[open_signal_df.columns[col]] = sig_

    open_signal = open_signal_df.values
    return open_signal


def trade_by_quantile(feature, window, quantile_upper_long, one_shot=True):
    quantile_lower_short = 1 - quantile_upper_long

    open_signal_df = pd.DataFrame(np.nan, index=feature.index, columns=feature.columns)
    for col in range(feature.shape[1]):
        ft = feature.iloc[:, col].dropna()

        rank = (bn.move_rank(ft, window, min_count=min(100, window), axis=0) + 1) / 2

        rank_shift = np.roll(rank, 1)
        rank_shift[0] = np.nan

        sig = pd.Series(np.nan, index=ft.index)
        loc = (rank >= quantile_upper_long)
        sig.loc[loc] = 1

        loc = (rank <= 0.5) & (rank_shift > 0.5)
        sig.loc[loc] = 0

        loc = (rank >= 0.5) & (rank_shift < 0.5)
        sig.loc[loc] = 0

        loc = (rank <= quantile_lower_short)
        sig.loc[loc] = -1

        if one_shot:
            sig_ = np.sign(sig.ffill().diff().fillna(0))
            sig_.loc[sig_ == 0] = np.nan
            sig_.loc[sig == 0] = 0
            open_signal_df[open_signal_df.columns[col]] = sig_

        else:
            open_signal_df[open_signal_df.columns[col]] = sig

    open_signal = open_signal_df.values
    return open_signal


def trade_by_thres_imba(feature, thres_open_long, thres_open_short, thres_close_long, thres_close_short):
    open_signal_df = pd.DataFrame(np.nan, index=feature.index, columns=feature.columns)
    for col in range(feature.shape[1]):
        ft = feature.iloc[:, col].dropna()

        sig = pd.Series(np.nan, index=ft.index)
        loc = (ft >= thres_open_long)
        sig.loc[loc] = 1
        # sig.loc[loc] = 0

        loc = (ft <= thres_close_long) & (ft.shift() > thres_close_long)
        sig.loc[loc] = 0
        # sig.loc[loc] = 0

        loc = (ft >= thres_close_short) & (ft.shift() < thres_close_short)
        sig.loc[loc] = 0

        loc = (ft <= thres_open_short)
        sig.loc[loc] = -1

        # open_signal_df[open_signal_df.columns[col]] = sig

        sig_ = np.sign(sig.ffill().diff().fillna(0))
        sig_.loc[sig_ == 0] = np.nan
        sig_.loc[sig == 0] = 0
        open_signal_df[open_signal_df.columns[col]] = sig_

    open_signal = open_signal_df.values
    return open_signal


def trade_by_cross(feature):
    open_signal = np.empty_like(feature)
    open_signal[:] = np.nan

    loc = (feature > 0) & (feature.shift() <= 0)
    open_signal[loc] = 1
    # open_signal[loc] = 0

    loc = (feature < 0) & (feature.shift() >= 0)
    open_signal[loc] = -1

    return open_signal


def trade_by_cross_ma(feat, window):
    open_signal_df = pd.DataFrame(np.nan, index=feat.index, columns=feat.columns)
    for col in range(feat.shape[1]):
        ft = feat.iloc[:, col].dropna()
        MA = bn.move_mean(ft.values, window, 1, axis=0)

        cross = ft - MA

        sig = pd.Series(np.nan, index=cross.index)
        loc = (cross > 0) & (cross.shift() <= 0)
        sig.loc[loc] = 1

        loc = (cross < 0) & (cross.shift() >= 0)
        sig.loc[loc] = -1

        open_signal_df[open_signal_df.columns[col]] = sig

    open_signal = open_signal_df.values
    return open_signal


def trade_by_norm(feat, std_multiplier=1):
    open_signal_df = pd.DataFrame(np.nan, index=feat.index, columns=feat.columns)
    for col in range(feat.shape[1]):
        ft = feat.iloc[:, col].dropna()

        mean = 0
        # std = ft.rolling(window=window).std()
        std = 1

        sig = pd.Series(np.nan, index=ft.index)
        loc = ft > (mean + std_multiplier * std)
        sig.loc[loc] = 1
        # sig.loc[loc] = 0

        loc = (ft < mean) & (ft.shift() > mean)
        sig.loc[loc] = 0

        loc = ft < (mean - std_multiplier * std)
        sig.loc[loc] = -1
        # sig.loc[loc] = 0

        loc = (ft > mean) & (ft.shift() < mean)
        sig.loc[loc] = 0

        sig_ = np.sign(sig.ffill().diff().fillna(0))
        sig_.loc[sig_ == 0] = np.nan
        sig_.loc[sig == 0] = 0

        open_signal_df[open_signal_df.columns[col]] = sig_

        # open_signal_df[open_signal_df.columns[col]] = sig

    open_signal = open_signal_df.values
    return open_signal


def trade_by_ts_rank(feat, window, quantile_lower=0.1, quantile_upper=0.9):
    open_signal = np.empty_like(feat)
    open_signal[:] = np.nan

    rank = (bn.move_rank(feat, window, min_count=min(100, window), axis=0) + 1) / 2

    rank_shift = np.empty_like(rank)
    rank_shift[:] = np.nan
    rank_shift[1:, :] = rank[:-1, :]

    loc = (rank > quantile_upper) & (rank_shift <= quantile_upper)
    open_signal[loc] = 1

    loc = (rank < quantile_lower) & (rank_shift >= quantile_lower)
    open_signal[loc] = -1

    return open_signal


def trade_by_bband(feat, window, std_multiplier=1):
    open_signal = np.empty_like(feat)
    open_signal[:] = np.nan

    MA = bn.move_mean(feat, window, axis=0)
    std = bn.move_std(feat, window, axis=0)

    upper = MA + std * std_multiplier
    lower = MA - std * std_multiplier

    loc = (feat > upper) & (feat.shift() <= upper)
    open_signal[loc] = 1
    # open_signal[loc] = 0

    loc = (feat < lower) & (feat.shift() >= lower)
    open_signal[loc] = -1
    # open_signal[loc] = 0

    sig = pd.DataFrame(open_signal, index=feat.index, columns=feat.columns)

    sig_ = np.sign(sig.ffill().diff().fillna(0))
    sig_[sig_ == 0] = np.nan
    sig_[sig == 0] = 0

    open_signal = sig_.values

    return open_signal


def trade_by_cs(feature, cs_interval, cs_quantile):
    idx = np.arange(0, len(feature), cs_interval)
    feature_ = feature.iloc[idx, :]
    # feature_ = feature.iloc[idx, :]

    lower = feature_.quantile(cs_quantile, axis=1)
    loc_lower = feature_.le(lower, axis=0)

    upper = feature_.quantile(1 - cs_quantile, axis=1)
    loc_upper = feature_.ge(upper, axis=0)

    signal_ = loc_upper.div(loc_upper.sum(axis=1), axis=0) - loc_lower.div(loc_lower.sum(axis=1), axis=0)

    # loc = signal_ < 0
    # signal_[loc] = 0

    open_signal = pd.DataFrame(np.nan, index=feature.index, columns=feature.columns)
    open_signal.iloc[idx, :] = signal_

    return open_signal


def trade_by_std_w_0(feat, window, std_multiplier=1):
    open_signal_df = pd.DataFrame(np.nan, index=feat.index, columns=feat.columns)
    for col in range(feat.shape[1]):
        ft = feat.iloc[:, col].dropna()

        mean = ft.rolling(window=window).mean()
        std = ft.rolling(window=window).std()

        sig = pd.Series(np.nan, index=ft.index)
        loc = ft > (mean + std_multiplier * std)
        sig.loc[loc] = 1
        # sig.loc[loc] = 0

        loc = (ft < mean) & (ft.shift() > mean.shift())
        sig.loc[loc] = 0

        loc = ft < (mean - std_multiplier * std)
        sig.loc[loc] = -1
        # sig.loc[loc] = 0

        loc = (ft > mean) & (ft.shift() < mean.shift())
        sig.loc[loc] = 0

        sig_ = np.sign(sig.ffill().diff().fillna(0))
        sig_.loc[sig_ == 0] = np.nan
        sig_.loc[sig == 0] = 0

        open_signal_df[open_signal_df.columns[col]] = sig_

    open_signal = open_signal_df.values
    return open_signal


def trade_by_std(feat, window, std_multiplier=1):
    open_signal_df = pd.DataFrame(np.nan, index=feat.index, columns=feat.columns)
    for col in range(feat.shape[1]):
        ft = feat.iloc[:, col].dropna()

        mean = ft.rolling(window=window).mean()
        std = ft.rolling(window=window).std()

        sig = pd.Series(np.nan, index=ft.index)
        loc = ft > (mean + std_multiplier * std)
        sig.loc[loc] = 1

        loc = ft < (mean - std_multiplier * std)
        sig.loc[loc] = -1

        sig_ = np.sign(sig.ffill().diff().fillna(0))
        sig_.loc[sig_ == 0] = np.nan
        sig_.loc[sig == 0] = 0

        open_signal_df[open_signal_df.columns[col]] = sig_

    open_signal = open_signal_df.values
    return open_signal


def get_weight_by_3d(quote, param=500, day_count=23, n=3, thres_min=0.25, thres_max=0.65):
    std = {}
    r_mat = {}
    stm_mat = {}
    for code, group in quote.groupby('symbol'):
        f = np.log(1 + group['close_price'].pct_change()).rolling(param, min_periods=min(100, param)).std()
        std[code] = f * np.sqrt(252 * day_count)
        r_mat[code] = np.log(1 + group['close_price'].pct_change())

        hh = group.loc[:, 'high_price'].rolling(param, min_periods=1).max()
        ll = group.loc[:, 'low_price'].rolling(param, min_periods=1).min()
        stm_mat[code] = ((group.loc[:, 'close_price'] * 2 - (hh + ll)).ewm(span=5).mean()) / ((hh - ll).ewm(span=5).mean())

    std = pd.DataFrame(std)
    r_mat = pd.DataFrame(r_mat)
    stm_mat = pd.DataFrame(stm_mat)

    corr = r_mat.rolling(param, min_periods=100).corr().abs().mean(axis=1).reset_index()
    corr.columns = ['datetime', 'symbol', 'corr']
    corr = corr.pivot(index='datetime', columns='symbol', values='corr')

    weight = std + corr
    weight = (thres_max - weight) / (thres_max - thres_min)
    weight = weight * 2 / 3 + stm_mat.abs() / 3

    if n:
        weight = (weight * (n - 1) + 1) / n

        loc = weight > 1
        weight[loc] = 1

        loc = weight < 1 / n
        weight[loc] = 1 / n

    close = quote.pivot(columns='symbol', values='close_price')
    trading_contract = (~pd.isna(close.ffill())).sum(axis=1)

    weight = weight.div(trading_contract, axis=0)

    return weight.ffill()


def get_weight_by_std_corr(quote, param=500, day_count=23, n=3, thres_min=0.25, thres_max=0.65):
    std = {}
    r_mat = {}
    for code, group in quote.groupby('symbol'):
        f = np.log(1 + group['close_price'].pct_change()).rolling(param, min_periods=min(100, param)).std()
        std[code] = f * np.sqrt(252 * day_count)
        r_mat[code] = np.log(1 + group['close_price'].pct_change())

    std = pd.DataFrame(std)

    r_mat = pd.DataFrame(r_mat)
    corr = r_mat.rolling(param, min_periods=100).corr().abs().mean(axis=1).reset_index()
    corr.columns = ['datetime', 'symbol', 'corr']
    corr = corr.pivot(index='datetime', columns='symbol', values='corr')

    weight = std + corr
    weight = ((thres_max - weight) / (thres_max - thres_min) * (n - 1) + 1) / n

    loc = weight > 1
    weight[loc] = 1

    loc = weight < 1 / n
    weight[loc] = 1 / n

    close = quote.pivot(columns='symbol', values='close_price')
    trading_contract = (~pd.isna(close.ffill())).sum(axis=1)

    weight = weight.div(trading_contract, axis=0)

    return weight.ffill()


def get_weight_by_std_minus(quote, param=500, day_count=23, n=3, std_min=0.1, std_max=0.45):
    weight = {}
    for code, group in quote.groupby('symbol'):
        f = np.log(1 + group['close_price'].pct_change()).rolling(param, min_periods=min(100, param)).std()
        weight[code] = f * np.sqrt(252 * day_count)

    weight = pd.DataFrame(weight)

    weight = ((std_max - weight) / (std_max - std_min) * (n - 1) + 1) / n

    loc = weight > 1
    weight[loc] = 1

    loc = weight < 1 / n
    weight[loc] = 1 / n

    close = quote.pivot(columns='symbol', values='close_price')
    trading_contract = (~pd.isna(close.ffill())).sum(axis=1)

    weight = weight.div(trading_contract, axis=0)

    return weight.ffill()


def get_weight_by_std_ratio(quote, param, target, day_count, n=3):
    vol = {}
    for code, group in quote.groupby('symbol'):
        f = np.log(1 + group['close_price'].pct_change()).rolling(param).std()
        vol[code] = f * np.sqrt(252 * day_count)

    vol = pd.DataFrame(vol)

    weight = target / vol

    loc = weight > 1
    weight[loc] = 1

    loc = weight < 1 / n
    weight[loc] = 1 / n

    close = quote.pivot(columns='symbol', values='close_price')
    trading_contract = (~pd.isna(close.ffill())).sum(axis=1)

    weight = weight.div(trading_contract, axis=0)

    return weight.ffill()


def get_weight_by_ew(quote):
    close = quote.pivot(columns='symbol', values='close_price')
    trading_contract = (~pd.isna(close.ffill())).sum(axis=1)

    weight = np.sign(close).div(trading_contract, axis=0)

    return weight.ffill()


def backtest_factor(open_signal, weight, ret, comm=COMMISSION):
    signal = bn.push(open_signal * weight, axis=0)
    tmp = bn.replace(signal, np.nan, 0)
    commission = np.abs(np.diff(tmp, prepend=0, axis=0)) * comm
    returns = (ret * signal - commission).sum(axis=1)

    return returns.groupby(returns.index.date).sum()


def backtest_and_summary(open_signal, ret, day_count=23):
    summary = {}

    signal = bn.push(open_signal, axis=0)
    tmp = bn.replace(signal, np.nan, 0)
    commission = np.abs(np.diff(tmp, prepend=0, axis=0)) * COMMISSION
    returns = (ret * signal).fillna(0)

    _daily_base = (returns - commission).sum(axis=1).groupby(returns.index.date).sum()
    _daily_base_0_comm = returns.sum(axis=1).groupby(returns.index.date).sum()

    info, trade_info_col, _ = get_trade_info(returns, signal, day_count=day_count)

    summary['sharpe'] = calc_sharpe(ret=_daily_base)
    summary['sharpe_0_comm'] = calc_sharpe(ret=_daily_base_0_comm)
    summary['calmar'] = calc_calmar(_daily_base)
    summary['avg_hp'] = info.loc['overall', 'avg_hp']
    summary['mid_hp'] = info.loc['overall', 'mid_hp']
    summary['contracts'] = (trade_info_col['trade_count'] * trade_info_col['avg_pnl']).to_dict()
    summary['counts'] = info.loc['overall', 'trade_count']
    summary['avg_pnl'] = info.loc['overall', 'avg_pnl']

    return _daily_base, summary, returns


def get_long_short_return(returns, signal):
    _signal = pd.DataFrame(signal, index=returns.index, columns=returns.columns)
    ret_ls = pd.DataFrame(index=returns.index)
    loc = _signal > 0
    tmp1 = returns[loc].sum(axis=1)
    ret_ls.loc[tmp1.index, 'long'] = tmp1

    loc = _signal < 0
    tmp2 = returns[loc].sum(axis=1)
    ret_ls.loc[tmp2.index, 'short'] = tmp2

    return ret_ls


def get_shoot_info(open_signal, day_count=23):
    info = pd.DataFrame()
    if isinstance(open_signal, pd.DataFrame):
        open_signal = open_signal.to_numpy()

    loc = (open_signal > 0) | (open_signal < 0)
    overall_shoot = loc.sum()
    info.loc['overall_shoot', 'shoot'] = (overall_shoot)
    info.loc['overall_shoot', 'daily_shoot_rate'] = (overall_shoot) / (open_signal.shape[0] / day_count)

    loc = open_signal > 0
    long_shoot = loc.sum()
    info.loc['long', 'shoot'] = (long_shoot)
    info.loc['long', 'shoot_rate'] = (long_shoot) / (overall_shoot)
    info.loc['long', 'daily_shoot_rate'] = (long_shoot) / (open_signal.shape[0] / day_count)

    loc = open_signal < 0
    short_shoot = loc.sum()
    info.loc['short', 'shoot'] = (short_shoot)
    info.loc['short', 'shoot_rate'] = (short_shoot) / (overall_shoot)
    info.loc['short', 'daily_shoot_rate'] = (short_shoot) / (open_signal.shape[0] / day_count)

    return info


def get_trade_info(returns, signal, day_count=23, comm=3e-4):
    sig = pd.DataFrame(signal, index=returns.index, columns=returns.columns)
    trade = sig.diff().fillna(0).abs().cumsum() * np.sign(sig)
    trade[sig == 0] = 0

    trade_detail = []
    trade_info_col = {}
    for col in trade.columns:
        trade_col = trade[col]
        returns_col = returns[col] / sig[col].abs()

        trade_ret = returns_col.groupby(trade_col).sum() - comm * 2
        trade_weight = sig[col].groupby(trade_col).mean()

        trade_hp = trade_col.value_counts()
        trade_hp = trade_hp[trade_hp.index != 0] / day_count

        trade_ = trade_col.reset_index()
        trade_detail_col = pd.DataFrame(columns=['symbol', 'HP', 'PnL'])
        trade_detail_col['EnterTime'] = trade_.groupby(col)['datetime'].first()
        trade_detail_col['ExitTime'] = trade_.groupby(col)['datetime'].last()

        trade_detail_col['Weight'] = trade_weight
        trade_detail_col['PnL'] = trade_ret
        trade_detail_col['HP'] = trade_hp
        trade_detail_col = trade_detail_col.reset_index().rename(columns={col: 'TradeID'})
        trade_detail_col['Direction'] = np.sign(trade_detail_col['TradeID'])
        trade_detail_col['symbol'] = col

        loc = trade_detail_col['TradeID'] != 0
        trade_detail_col = trade_detail_col[loc]

        trade_detail.append(trade_detail_col)

        trade_describe = describe_trade(trade_detail_col)
        trade_info_col[col] = trade_describe

    trade_info_col = pd.DataFrame(trade_info_col)
    trade_detail = pd.concat(trade_detail).reset_index(drop=True)

    trade_describe = describe_trade(trade_detail)
    cols_n = len([i for i in trade_describe.index if (not i.startswith('long') and not i.startswith('short'))])
    trade_info = pd.DataFrame(
        {
            'overall': trade_describe.iloc[: cols_n],
            'long': trade_describe.iloc[cols_n: cols_n * 2].values,
            'short': trade_describe.iloc[cols_n * 2:].values,
        }
    )

    return trade_info.T, trade_info_col.T, trade_detail


def describe_trade(trade_detail):
    desc = pd.Series(dtype=float)
    desc.loc['trade_count'] = len(trade_detail)
    desc.loc['trade_win_rate'] = (trade_detail['PnL'] > 0).mean()
    desc.loc['avg_pnl'] = trade_detail['PnL'].mean()
    desc.loc['mid_pnl'] = trade_detail['PnL'].median()
    desc.loc['avg_hp'] = trade_detail['HP'].mean()
    desc.loc['mid_hp'] = trade_detail['HP'].median()

    loc = trade_detail['Direction'] == 1
    tmp = trade_detail[loc]
    desc.loc['long_trade_count'] = len(tmp)
    desc.loc['long_trade_win_rate'] = (tmp['PnL'] > 0).mean()
    desc.loc['long_avg_pnl'] = tmp['PnL'].mean()
    desc.loc['long_mid_pnl'] = tmp['PnL'].median()
    desc.loc['long_avg_hp'] = tmp['HP'].mean()
    desc.loc['long_mid_hp'] = tmp['HP'].median()

    loc = trade_detail['Direction'] == -1
    tmp = trade_detail[loc]
    desc.loc['short_trade_count'] = len(tmp)
    desc.loc['short_trade_win_rate'] = (tmp['PnL'] > 0).mean()
    desc.loc['short_avg_pnl'] = tmp['PnL'].mean()
    desc.loc['short_mid_pnl'] = tmp['PnL'].median()
    desc.loc['short_avg_hp'] = tmp['HP'].mean()
    desc.loc['short_mid_hp'] = tmp['HP'].median()

    return desc


def signal_to_opensignal(signal_):
    open_signal_ = pd.DataFrame(np.nan, index=signal_.index, columns=signal_.columns)

    tmp = signal_.diff()
    loc = tmp != 0
    open_signal_[loc] = tmp[loc]

    loc = signal_ == 0
    open_signal_[loc] = 0

    return open_signal_


def limit_trade_hp(open_signal, min_hp=0, max_hp=1000000):
    open_signal_np = open_signal.values
    signal_np = np.empty_like(open_signal_np)
    signal_np[:] = np.nan

    for col in range(signal_np.shape[1]):
        sig_ = 0
        hp_now = 0

        for i in range(signal_np.shape[0]):
            sig_now = open_signal_np[i, col]

            if sig_ == 0:
                hp_now = 0
                if sig_now > 0 or sig_now < 0:
                    signal_np[i, col] = sig_ = sig_now

            else:
                hp_now += 1

                if hp_now < min_hp:
                    signal_np[i, col] = sig_

                elif hp_now > max_hp:
                    if sig_ * sig_now < 0:
                        signal_np[i, col] = sig_ = sig_now

                    else:
                        signal_np[i, col] = sig_ = 0

                    hp_now = 0

                else:
                    if sig_ * sig_now <= 0:
                        signal_np[i, col] = sig_ = sig_now
                        hp_now = 0

                    else:
                        signal_np[i, col] = sig_

    signal = pd.DataFrame(signal_np, index=open_signal.index, columns=open_signal.columns).fillna(0)

    return signal


def infer_day_count(quote):
    if "trade_date" not in quote:
        quote = TDays.wrap_tdays(quote.reset_index(), "datetime", "trade_date")

    return quote.groupby(['symbol', 'trade_date'])['close_price'].count().mode().iat[0]


def exit_w_trace_exit(open_signal, close, stoploss, max_hp):
    # Exit with fix holding period ONLY
    # 有头寸时，会忽略任何信号，直至当前头寸成功平仓

    os_np = open_signal.values
    os_exit_np = np.empty_like(os_np)
    os_exit_np[:] = np.nan

    close_np = close.values

    sequence = 1 - np.arange(len(os_np)) * (1 / max_hp)
    sequence[sequence < 0] = 0

    for j in range(os_exit_np.shape[1]):
        signal_ = os_np[:, j]
        close_ = close_np[:, j]

        indices = np.where(~np.isnan(signal_))
        next_idx = 0
        for i in indices[0]:
            current_signal = signal_[i]

            # if current_signal == 0:
            #     # close by signal
            #     open_signal_exit.iat[i, j] = 0
            #     next_idx = i + 1
            #     continue

            # if current_signal * last_signal >= 0 and i < next_idx:
            #     continue

            # 当存在反手信号时，会忽略反手，直至当前头寸成功平仓
            if i < next_idx:
                continue

            if current_signal > 0:
                c = bn.push(close_[i:])
                move_max_ = np.maximum.accumulate(c)

                loc = np.isnan(close_[i:])
                move_max_[loc] = np.nan

                # move_max_ = pd.Series(close_[i:]).expanding().max()

                close_idx = np.argmax((move_max_ - close_[i:]) > sequence[:len(move_max_)] * stoploss * close_[i])

                # last_signal = current_signal
                os_exit_np[i, j] = current_signal

            elif current_signal < 0:
                c = bn.push(close_[i:])
                move_min_ = np.minimum.accumulate(c)

                loc = np.isnan(close_[i:])
                move_min_[loc] = np.nan
                # move_min_pd = pd.Series(close_[i:]).expanding().min()

                close_idx = np.argmax((close_[i:] - move_min_) > sequence[:len(move_min_)] * stoploss * close_[i])

                # last_signal = current_signal
                os_exit_np[i, j] = current_signal

            else:
                # nan sig
                close_idx = 0

            if close_idx:
                os_exit_np[i + close_idx, j] = 0
                next_idx = i + close_idx + 1

    open_signal_exit = pd.DataFrame(os_exit_np, index=open_signal.index, columns=open_signal.columns)

    return open_signal_exit


def exit_w_trace_atr_exit(open_signal, close, atr, atr_multiplier, max_hp):
    # Exit with fix holding period ONLY
    # 有头寸时，会忽略任何信号，直至当前头寸成功平仓

    os_np = open_signal.values
    os_exit_np = np.empty_like(os_np)
    os_exit_np[:] = np.nan

    close_np = close.values
    atr_np = atr.values

    sequence = 1 - np.arange(len(os_np)) * (1 / max_hp)
    sequence[sequence < 0] = 0

    for j in range(os_exit_np.shape[1]):
        signal_ = os_np[:, j]
        close_ = close_np[:, j]
        atr_ = atr_np[:, j]

        indices = np.where(~np.isnan(signal_))
        next_idx = 0
        for i in indices[0]:
            current_signal = signal_[i]

            # if current_signal == 0:
            #     # close by signal
            #     open_signal_exit.iat[i, j] = 0
            #     next_idx = i + 1
            #     continue

            # if current_signal * last_signal >= 0 and i < next_idx:
            #     continue

            # 当存在反手信号时，会忽略反手，直至当前头寸成功平仓
            if i < next_idx:
                continue

            if current_signal > 0:
                c = bn.push(close_[i:])
                move_max_ = np.maximum.accumulate(c)

                loc = np.isnan(close_[i:])
                move_max_[loc] = np.nan

                # move_max_ = pd.Series(close_[i:]).expanding().max()

                close_idx = np.argmax((move_max_ - close_[i:]) > sequence[:len(move_max_)] * atr_multiplier * atr_[i:])

                # last_signal = current_signal
                os_exit_np[i, j] = current_signal

            elif current_signal < 0:
                c = bn.push(close_[i:])
                move_min_ = np.minimum.accumulate(c)

                loc = np.isnan(close_[i:])
                move_min_[loc] = np.nan
                # move_min_pd = pd.Series(close_[i:]).expanding().min()

                close_idx = np.argmax((close_[i:] - move_min_) > sequence[:len(move_min_)] * atr_multiplier * atr_[i:])

                # last_signal = current_signal
                os_exit_np[i, j] = current_signal

            else:
                # nan sig
                close_idx = 0

            if close_idx:
                os_exit_np[i + close_idx, j] = 0
                next_idx = i + close_idx + 1

    open_signal_exit = pd.DataFrame(os_exit_np, index=open_signal.index, columns=open_signal.columns)

    return open_signal_exit


def exit_w_atr_exit(open_signal, close, atr, atr_multiplier, max_hp=None):
    return exit_w_atr_barrier(open_signal, close, atr, None, atr_multiplier, max_hp)


def exit_w_atr_barrier(
        open_signal: pd.DataFrame,
        close: pd.DataFrame,
        atr: pd.DataFrame,
        takeprofit_multiplier=None,
        stoploss_multiplier=None,
        max_hp=None
):
    # 有头寸时，会忽略任何信号，直至当前头寸成功平仓

    os_np = open_signal.values
    os_exit_np = np.empty_like(os_np)
    os_exit_np[:] = np.nan

    close_np = close.values
    atr_np = atr.values

    for j in range(os_exit_np.shape[1]):
        signal_ = os_np[:, j]
        close_ = close_np[:, j]
        atr_ = atr_np[:, j]

        indices = np.where(~np.isnan(signal_))
        next_idx = 0
        try:
            for i in indices[0]:
                current_signal = signal_[i]

                # if current_signal == 0:
                #     # close by signal
                #     open_signal_exit.iat[i, j] = 0
                #     next_idx = i + 1
                #     continue

                # if current_signal * last_signal >= 0 and i < next_idx:
                #     continue

                # 当存在反手信号时，会忽略反手，直至当前头寸成功平仓
                if i < next_idx:
                    continue

                if current_signal > 0:
                    os_exit_np[i, j] = current_signal

                    c = bn.push(close_[i:])
                    move_max_ = np.maximum.accumulate(c)

                    loc = np.isnan(close_[i:])
                    move_max_[loc] = np.nan

                    # move_max_ = pd.Series(close_[i:]).expanding().max()
                    loc = np.full_like(move_max_, False, dtype=bool)
                    if takeprofit_multiplier is not None:
                        loc |= (close_[i:] - c[0]) > takeprofit_multiplier * atr_[i:]

                    if stoploss_multiplier is not None:
                        loc |= (move_max_ - close_[i:]) > stoploss_multiplier * atr_[i:]

                    if max_hp:
                        non_nan_indices = np.where(~np.isnan(close_[i:]))[0]
                        idx_max_hp = non_nan_indices[max_hp]

                        loc[idx_max_hp] = True  # max hp in bar

                        # loc[max_hp] = True  # max hp in day

                    close_idx = np.argmax(loc)

                elif current_signal < 0:
                    os_exit_np[i, j] = current_signal

                    c = bn.push(close_[i:])
                    move_min_ = np.minimum.accumulate(c)

                    loc = np.isnan(close_[i:])
                    move_min_[loc] = np.nan
                    # move_min_pd = pd.Series(close_[i:]).expanding().min()

                    loc = np.full_like(move_min_, False, dtype=bool)
                    if takeprofit_multiplier is not None:
                        loc |= (c[0] - close_[i:]) > takeprofit_multiplier * atr_[i:]

                    if stoploss_multiplier is not None:
                        loc |= (close_[i:] - move_min_) > stoploss_multiplier * atr_[i:]

                    if max_hp:
                        non_nan_indices = np.where(~np.isnan(close_[i:]))[0]
                        idx_max_hp = non_nan_indices[max_hp]

                        loc[idx_max_hp] = True  # max hp in bar

                        # loc[max_hp] = True  # max hp in day

                    close_idx = np.argmax(loc)

                else:
                    # nan sig
                    close_idx = 0

                if close_idx:
                    os_exit_np[i + close_idx, j] = 0
                    next_idx = i + close_idx + 1

        except IndexError:
            continue

    open_signal_exit = pd.DataFrame(os_exit_np, index=open_signal.index, columns=open_signal.columns)

    return open_signal_exit


def exit_w_loss_barrier(
        open_signal: pd.DataFrame,
        close: pd.DataFrame,
        takeprofit=None,
        stoploss=None,
        max_hp=None
):
    # 有头寸时，会忽略任何信号，直至当前头寸成功平仓

    os_np = open_signal.values
    os_exit_np = np.empty_like(os_np)
    os_exit_np[:] = np.nan

    close_np = close.values

    for j in range(os_exit_np.shape[1]):
        signal_ = os_np[:, j]
        close_ = close_np[:, j]

        indices = np.where(~np.isnan(signal_))
        next_idx = 0
        try:
            for i in indices[0]:
                current_signal = signal_[i]

                # if current_signal == 0:
                #     # close by signal
                #     open_signal_exit.iat[i, j] = 0
                #     next_idx = i + 1
                #     continue

                # if current_signal * last_signal >= 0 and i < next_idx:
                #     continue

                # 当存在反手信号时，会忽略反手，直至当前头寸成功平仓
                if i < next_idx:
                    continue

                if current_signal > 0:
                    os_exit_np[i, j] = current_signal

                    c = bn.push(close_[i:])
                    move_max_ = np.maximum.accumulate(c)

                    loc = np.isnan(close_[i:])
                    move_max_[loc] = np.nan

                    # move_max_ = pd.Series(close_[i:]).expanding().max()
                    loc = np.full_like(move_max_, False, dtype=bool)
                    if takeprofit is not None:
                        loc |= (close_[i:] / c[0] - 1) > takeprofit

                    if stoploss is not None:
                        loc |= (close_[i:] / move_max_ - 1) < -stoploss

                    if max_hp:
                        loc[max_hp] = True

                    close_idx = np.argmax(loc)

                elif current_signal < 0:
                    os_exit_np[i, j] = current_signal

                    c = bn.push(close_[i:])
                    move_min_ = np.minimum.accumulate(c)

                    loc = np.isnan(close_[i:])
                    move_min_[loc] = np.nan
                    # move_min_pd = pd.Series(close_[i:]).expanding().min()

                    loc = np.full_like(move_min_, False, dtype=bool)
                    if takeprofit is not None:
                        loc |= (close_[i:] / c[0] - 1) < -takeprofit

                    if stoploss is not None:
                        loc |= (close_[i:] / move_min_ - 1) > stoploss

                    if max_hp:
                        loc[max_hp] = True

                    close_idx = np.argmax(loc)

                else:
                    # nan sig
                    close_idx = 0

                if close_idx:
                    os_exit_np[i + close_idx, j] = 0
                    next_idx = i + close_idx + 1

        except IndexError:
            continue

    open_signal_exit = pd.DataFrame(os_exit_np, index=open_signal.index, columns=open_signal.columns)

    return open_signal_exit


def exit_w_loss_exit(
        open_signal,
        close,
        stoploss,
        max_hp=None
):
    return exit_w_loss_barrier(open_signal, close, None, stoploss, max_hp)


def exit_w_max_hp(open_signal, max_hp):
    open_signal_exit = open_signal.copy()

    for j in range(open_signal_exit.shape[1]):
        signal_ = open_signal.iloc[:, j]

        indices = np.where(~np.isnan(signal_))

        indices = indices[0]

        hp = np.diff(indices, append=0)
        if len(hp):
            hp[-1] = max_hp + 1
            loc = hp > max_hp

            loc_2_close = indices[loc] + max_hp
            loc = loc_2_close < len(signal_)
            loc_2_close = loc_2_close[loc]

            open_signal_exit.iloc[loc_2_close, j] = 0

    return open_signal_exit


def exit_w_fix_hp(open_signal, fix_hp):
    # Exit with fix holding period ONLY
    # 有头寸时，会忽略任何信号，直至当前头寸成功平仓
    open_signal_exit = pd.DataFrame(np.nan, index=open_signal.index, columns=open_signal.columns)

    for j in range(open_signal_exit.shape[1]):
        signal_ = open_signal.iloc[:, j]

        indices = np.where(~np.isnan(signal_))
        next_idx = 0
        for i in indices[0]:
            current_signal = signal_.iat[i]

            if i < next_idx:
                continue

            if current_signal != 0:
                open_signal_exit.iat[i, j] = current_signal

                if i + fix_hp >= len(open_signal_exit):
                    break

                open_signal_exit.iat[i + fix_hp, j] = 0
                next_idx = i + fix_hp + 1

    return open_signal_exit


def calc_sharpe(ret=None, nav=None):
    if ret is not None:
        return np.sqrt(252) * ret.mean() / ret.std()

    if nav is not None:
        return np.sqrt(252) * nav.diff().mean() / nav.diff().std()


def calc_calmar(returns):
    arr = returns.mean() * 252
    nv = np.cumsum(returns) + 1
    maxdd = np.max(np.maximum.accumulate(nv) - nv) + 1e-8
    return arr / maxdd


def calc_maxdd(returns):
    nv = np.cumsum(returns) + 1
    return np.max(np.maximum.accumulate(nv) - nv)
