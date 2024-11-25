import numpy as np
import pandas as pd


def check_index(df, col_dt, col_symbol):
    assert (
            isinstance(df, pd.DataFrame)
            and isinstance(df.index, pd.MultiIndex)
            and col_dt in df.index.names
            and col_symbol in df.index.names
    )


def rolling_rank(series: pd.Series, window: int, min_periods: int = None, pct=True, n_group :int = None):
    tmp = series.dropna()
    count = sum(tmp.shift(n).le(tmp) for n in range(window))

    if min_periods:
        count.iloc[:min_periods] = np.nan

    else:
        count.iloc[:window] = np.nan

    if pct:
        return count / window

    if n_group:
        return np.ceil(count / window * n_group)

    return count
