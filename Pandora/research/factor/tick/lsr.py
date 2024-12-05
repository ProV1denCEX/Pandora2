import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from Pandora.constant import Frequency
from Pandora.research.factor.template import TickFeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def get_factor(tick, volume_window, quantile, freq):
    return LSR(volume_window, quantile, freq).set_output(transform="pandas").transform(tick)


def get_multi_factors(tick, volume_windows, quantile, freq, n_job):
    results = Parallel(n_jobs=n_job, verbose=10)(  # n_jobs=-1 表示使用所有CPU核心
        delayed(get_factor)(tick, volume_window, quantile, freq)
        for volume_window in volume_windows
    )

    factors = pd.concat(results, axis=1)

    return factors


class LSR(TickFeatureTemplate):
    col_required = [
        TickFeatureTemplate.col_close,
        TickFeatureTemplate.col_volume,
        TickFeatureTemplate.col_bid_price,
        TickFeatureTemplate.col_ask_price,
    ]

    def __init__(self, volume_window, volume_quantile, freq: Frequency, agg_method: str = "sum"):
        self.volume_window = volume_window
        self.volume_quantile = volume_quantile
        self.freq = freq

        self.agg_method = agg_method

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")
        check_multi_index(X, self.col_datetime, self.col_symbol)

        volume_window = int(self.volume_window * self.window_multiplier)
        qtl = self.volume_quantile

        feat = []
        for symbol, data in X.groupby(level=self.col_symbol):
            assert data.index.is_monotonic_increasing

            loc1 = (data[self.col_close] > data[self.col_bid_price].shift()) & (data[self.col_volume] > data[self.col_volume].rolling(volume_window, min_periods=1).quantile(qtl))
            loc2 = (data[self.col_close] < data[self.col_ask_price].shift()) & (data[self.col_volume] > data[self.col_volume].rolling(volume_window, min_periods=1).quantile(qtl))

            data['long_vol'] = loc1 * data[self.col_volume]
            data['short_vol'] = loc2 * data[self.col_volume]

            f_agg = data.resample(self.freq.to_str(), level=self.col_datetime, closed='right', label='left').agg(
                {
                    'long_vol': self.agg_method,
                    'short_vol': self.agg_method,
                    self.col_volume: self.agg_method,
                    self.col_close: 'count'
                }
            )

            f_agg[self.col_symbol] = symbol

            loc = f_agg[self.col_close] > 1
            f_agg = f_agg[loc]
            f_agg = f_agg.set_index(self.col_symbol, append=True)

            f = (f_agg['long_vol'] - f_agg['short_vol']) / f_agg[self.col_volume]

            feat.append(f)

        feat_name = self.get_feature_names_out()[0]

        return pd.DataFrame({feat_name: pd.concat(feat)})

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.volume_window}D'])
