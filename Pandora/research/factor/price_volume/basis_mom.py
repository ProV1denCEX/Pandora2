import numpy as np
import pandas as pd

from Pandora.constant import SymbolSuffix
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index


class BasisMom(FeatureTemplate):
    def __init__(self, window: int, liquidity: float, t2: int):
        self.window = window
        self.liquidity = liquidity
        self.t2 = t2

        self.col_product_id = 'product_id'
        self.col_mom = 'roc'

    def transform(self, X: pd.DataFrame) -> pd.Series:
        self.set_output(transform="pandas")

        check_index(X, self.col_datetime, self.col_symbol)

        for symbol, group in X.groupby(self.col_symbol):
            assert group.index.is_monotonic_increasing

            mom = np.log(group[self.col_close] / group[self.col_close].shift())


        feat = pd.Series(dtype=float)

        t1 = 0
        for (product_id, dt_), group in X.groupby([self.col_product_id, self.col_datetime]):

            loc = group[self.col_turnover] > self.liquidity * 1e8
            tmp = group[loc]

            if self.t2:
                if len(tmp) <= abs(self.t2):
                    continue

                feat.at[(dt_, product_id + SymbolSuffix.MC)] = tmp[self.col_mom].iat[t1] - tmp[self.col_mom].iat[self.t2]

            else:
                if len(tmp) <= abs(1):
                    continue

                feat.at[(dt_, product_id + SymbolSuffix.MC)] = tmp[self.col_mom].iat[t1] - tmp[self.col_mom].iloc[t1 + 1:].mean()

        return feat.sort_index()

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
