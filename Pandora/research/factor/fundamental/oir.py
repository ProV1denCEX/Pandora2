import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def stm(series: pd.Series, n: int = 1):
    hh = series.rolling(n, min_periods=1).max()
    ll = series.rolling(n, min_periods=1).min()
    return (series * 2 - (hh + ll)) / (hh - ll)


class OIR(FeatureTemplate):
    def __init__(self, window: int = 1):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        col_base_factor = "factor_value"

        feat = X[self.col_open_interest] / (X[col_base_factor] + 1)

        if self.window:
            window = int(self.window * 70)
            feat = feat.groupby(self.col_symbol).transform(lambda x: x.rolling(window, min_periods=1).rank(pct=True))

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
