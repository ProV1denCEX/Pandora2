import numpy as np
import pandas as pd

from Pandora.constant import Frequency
from Pandora.research.factor.template import TickFeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class TVI(TickFeatureTemplate):
    col_required = [
        TickFeatureTemplate.col_bid_price,
        TickFeatureTemplate.col_ask_price,
        TickFeatureTemplate.col_bid_volume,
        TickFeatureTemplate.col_ask_volume
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

            tvi = (group[self.col_bid_volume] - group[self.col_ask_volume]) / (group[self.col_bid_volume] + group[self.col_ask_volume])

            loc = (group[self.col_ask_price] == 0) | (group[self.col_bid_price] == 0)
            tvi[loc] = 0

            f_agg = tvi.resample(self.freq.to_str(), level=self.col_datetime, closed='right', label='left').agg(
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
    pvm = TVI(Frequency.Min_5)

    aaa = 1