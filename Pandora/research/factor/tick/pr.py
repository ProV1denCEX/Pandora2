import numpy as np
import pandas as pd

from Pandora.constant import Frequency
from Pandora.research.factor.template import TickFeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class PR(TickFeatureTemplate):
    col_required = [
        TickFeatureTemplate.col_close,
        TickFeatureTemplate.col_bid_price,
        TickFeatureTemplate.col_ask_price
    ]

    def __init__(self, freq: Frequency, agg_method: str = "mean"):
        self.freq = freq
        self.agg_method = agg_method

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")
        check_multi_index(X, self.col_datetime, self.col_symbol)

        feat = []
        for symbol, group in X.groupby(level=self.col_symbol):
            assert group.index.is_monotonic_increasing

            pr = (group[self.col_close] > group[self.col_ask_price].shift()).astype(int)

            loc = (group[self.col_ask_price] == 0) | (group[self.col_bid_price] == 0)
            pr[loc] = 0

            f_agg = pr.resample(self.freq.to_str(), level=self.col_datetime, closed='right', label='left').agg(
                [self.agg_method, 'count']
            )
            f_agg[self.col_symbol] = symbol

            loc = f_agg['count'] > 1
            f_agg = f_agg[loc]
            f = f_agg.set_index(self.col_symbol, append=True)[self.agg_method]

            feat.append(f)

        feat_name = self.get_feature_names_out()[0]

        return pd.DataFrame({feat_name: pd.concat(feat)})

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}'])


if __name__ == '__main__':
    pvm = PR(Frequency.Min_5)

    aaa = 1