import numpy as np
import pandas as pd

from Pandora.constant import SymbolSuffix
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class BasisMom(FeatureTemplate):
    def __init__(self, liquidity: float, t2: int, col_product_id='product_id', col_ptm='ptm_day', col_name='name'):
        self.liquidity = liquidity
        self.t2 = t2

        self.col_product_id = col_product_id
        self.col_mom = 'roc'
        self.col_ptm = col_ptm
        self.col_name = col_name

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")

        check_multi_index(X, self.col_datetime, self.col_symbol)

        X_ = X.copy()

        def get_mom(group_):
            assert group_.index.is_monotonic_increasing

            return np.log(group_[self.col_close] / group_[self.col_close].shift())

        for symbol, group in X_.groupby(self.col_name):
            mom = get_mom(group)
            X_.loc[group.index, self.col_mom] = mom

        loc = X_[self.col_turnover] > self.liquidity * 1e8
        X_ = X_[loc]

        feat = []
        feat_name = self.get_feature_names_out()[0]
        t1 = 0
        for (product_id, dt_), group in X_.groupby([self.col_product_id, self.col_datetime]):

            group = group.sort_values(self.col_ptm)

            if self.t2:
                if len(group) <= abs(self.t2):
                    continue

                value = group[self.col_mom].iat[t1] - group[self.col_mom].iat[self.t2]

            else:
                if len(group) <= 1:
                    continue

                value = group[self.col_mom].iat[t1] - group[self.col_mom].iloc[t1 + 1:].mean()

            feat.append(
                {
                    self.col_datetime: dt_,
                    self.col_symbol: product_id + SymbolSuffix.MC,
                    feat_name: value
                }
            )

        feat = pd.DataFrame(feat).set_index([self.col_datetime, self.col_symbol]).sort_index()
        return feat

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.liquidity}_{self.t2}'])
