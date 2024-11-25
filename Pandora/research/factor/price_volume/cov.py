import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index


class Cov(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_index(X, self.col_datetime, self.col_symbol)

        r_mat = {}
        for code, group in X.groupby(self.col_symbol):
            r_mat[code] = np.log(1 + group[self.col_close].pct_change()).reset_index(self.col_symbol, drop=True)

        r_mat = pd.DataFrame(r_mat)
        f = r_mat.rolling(self.window, min_periods=10).cov().abs().mean(axis=1)

        feat = f.loc[X.index]

        feat = feat.replace([np.inf, -np.inf], 0)

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
