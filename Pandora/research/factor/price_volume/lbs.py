import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index


class LBS(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_index(X, self.col_datetime, self.col_symbol)

        feat = pd.Series(index=X.index)
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            c = group[self.col_close]
            beta = c.diff(self.window) / self.window

            res = {}
            for i in range(self.window + 1):
                res[i] = beta * i + c.shift(self.window) - c.shift(self.window - i)

            sigma = pd.DataFrame(res).std(axis=1)
            f = beta / sigma

            feat.loc[group.index] = f

        feat = feat.replace([np.inf, -np.inf], 0)

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
