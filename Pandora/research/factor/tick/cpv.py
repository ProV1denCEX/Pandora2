import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from Pandora.constant import Frequency
from Pandora.research.factor.template import TickFeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def get_factor(tick, window, freq):
    return CPV(window, freq).set_output(transform="pandas").transform(tick)


def get_multi_factors(tick, windows, freq, n_job):
    results = Parallel(n_jobs=n_job, verbose=10)(  # n_jobs=-1 表示使用所有CPU核心
        delayed(get_factor)(tick, window, freq)
        for window in windows
    )

    factors = pd.concat(results, axis=1)

    return factors


class CPV(TickFeatureTemplate):
    col_required = [
        TickFeatureTemplate.col_close,
        TickFeatureTemplate.col_volume,
    ]

    def __init__(self, window, freq: Frequency, agg_method: str = "mean"):
        self.window = window
        self.freq = freq

        self.agg_method = agg_method

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")
        check_multi_index(X, self.col_datetime, self.col_symbol)

        window = int(self.window * 23 * 3)

        feat = []
        for symbol, data in X.groupby(level=self.col_symbol):
            assert data.index.is_monotonic_increasing

            data['counter'] = 1

            f_agg = data.resample(self.freq.to_str(), level=self.col_datetime, closed='right', label='left').agg(
                {
                    self.col_close: self.agg_method,
                    self.col_volume: self.agg_method,
                    'counter': 'count'
                }
            )
            f_agg[self.col_symbol] = symbol

            loc = f_agg['counter'] > 1
            f_agg = f_agg[loc]
            f_agg = f_agg.set_index(self.col_symbol, append=True)

            f = f_agg[self.col_close].rolling(window).corr(f_agg[self.col_volume])

            feat.append(f.replace([np.inf, -np.inf], 0))

        feat_name = self.get_feature_names_out()[0]

        return pd.DataFrame({feat_name: pd.concat(feat)})

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}D'])
