import numpy as np
import pandas as pd

from Pandora.constant import Frequency
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def sum_(array):
    return np.sum(array) if len(array) else np.nan


class PVM(FeatureTemplate):
    def __init__(self, freq: Frequency):
        self.freq = freq

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")
        check_multi_index(X, self.col_datetime, self.col_symbol)

        feat = []
        for symbol, group in X.groupby(level=self.col_symbol):
            assert group.index.is_monotonic_increasing

            mid_price = (group['bid_price_1'] + group['ask_price_1']) / 2
            pvm = np.log(group['last_price'] / mid_price)

            loc = (group['ask_price_1'] == 0) | (group['bid_price_1'] == 0)
            pvm[loc] = 0

            f_agg = pvm.resample(self.freq.to_str(), level=self.col_datetime, closed='right', label='left').agg(
                ['sum', 'count']
            )
            f_agg['symbol'] = symbol

            loc = f_agg['count'] > 1
            f_agg = f_agg[loc]
            f = f_agg.set_index('symbol', append=True)['sum']

            feat.append(f)

        feat_name = self.get_feature_names_out()[0]

        return pd.DataFrame({feat_name: pd.concat(feat)})

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}'])
