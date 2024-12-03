import datetime as dt
import numpy as np
import pandas as pd

from sklearn import linear_model

from Pandora.constant import SymbolSuffix
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class Mask(FeatureTemplate):
    def __init__(
            self,
            liquidity: int,
            turnover_window: int = 1,
            col_product_id='product_id',
            col_ptm='ptm_day',
            col_mc='mc'
    ):
        self.liquidity = liquidity
        self.turnover_window = turnover_window

        self.col_product_id = col_product_id
        self.col_ptm = col_ptm
        self.col_mc = col_mc

        self.col_turnover_check = 'turnover_check'

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")

        check_multi_index(X, self.col_datetime, self.col_symbol)

        X_ = X.copy()
        for symbol, group in X_.groupby(level=self.col_symbol):
            liquidity = group[self.col_turnover].rolling(self.turnover_window, min_periods=1).sum()
            X_.loc[group.index, self.col_turnover_check] = liquidity

        X_['ptm'] = X_['ptm_day'].dt.total_seconds() / dt.timedelta(days=1).total_seconds()
        X_['ptm_rev'] = 1 / X_['ptm']

        loc = X_['ptm'] != 0
        X_ = X_[loc]

        feat = []
        feat_name = self.get_feature_names_out()[0]

        for (product_id, dt_), group in X_.groupby([self.col_product_id, self.col_datetime]):
            if len(group) <= 1:
                continue

            group = group.sort_values([self.col_turnover_check, 'ptm_rev'], ascending=False).iloc[:self.liquidity, :]

            if not group[self.col_mc].any():
                continue

            value = group.sort_values('ptm')[self.col_mc].iat[0] or group.sort_values('ptm')[self.col_mc].iat[-1]

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
        return np.asarray([f'{self.prefix}{estimator_name}_{self.liquidity}'])

