import numpy as np
import pandas as pd

from Pandora.constant import Frequency
from Pandora.research.factor.template import TickFeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class OFI(TickFeatureTemplate):
    col_required = [
        TickFeatureTemplate.col_close,
        TickFeatureTemplate.col_bid_price,
        TickFeatureTemplate.col_ask_price,
        TickFeatureTemplate.col_bid_volume,
        TickFeatureTemplate.col_ask_volume,
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

            bid_p_previous = group[self.col_bid_price].shift(1)
            bid_p_current = group[self.col_bid_price]
            # 当前bid price大于上一刻的bid price,增量为当前的挂单量也就是bid_v
            delta_v1 = (bid_p_current > bid_p_previous) * group[self.col_bid_volume]
            # 当前bid price小于上一刻的bid price,增量为前一刻被成交的量取负数
            delta_v2 = (bid_p_current < bid_p_previous) * group[self.col_bid_volume].shift(1) * -1.
            # 当前bid price等于上一刻的bid price,增量为当前的挂单量减去前一刻的挂单量
            delta_v3 = (bid_p_current == bid_p_previous) * (group[self.col_bid_volume] - group[self.col_bid_volume].shift(1))
            # 三者相加，得到最终的delta_v
            adelta_bid_v = delta_v1 + delta_v2 + delta_v3

            ask_p_previous = group[self.col_ask_price].shift(1)
            ask_p_current = group[self.col_ask_price]
            delta_v1 = (ask_p_current > ask_p_previous) * group[self.col_ask_volume].shift(1) * -1.
            delta_v2 = (ask_p_current < ask_p_previous) * group[self.col_ask_volume]
            delta_v3 = (ask_p_current == ask_p_previous) * (group[self.col_ask_volume] - group[self.col_ask_volume].shift(1))
            adelta_ask_v = delta_v1 + delta_v2 + delta_v3

            ofi = adelta_bid_v - adelta_ask_v

            loc = (group[self.col_ask_price] == 0) | (group[self.col_bid_price] == 0)
            ofi[loc] = 0

            f_agg = ofi.resample(self.freq.to_str(), level=self.col_datetime, closed='right', label='left').agg(
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
    pvm = OFI(Frequency.Min_5)

    aaa = 1