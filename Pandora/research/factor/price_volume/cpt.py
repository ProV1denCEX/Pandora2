import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class CPT(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        feat = pd.Series(index=X.index)
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            p = group.loc[:, self.col_close]
            v = group.loc[:, self.col_open_interest]

            cpv = p.rolling(self.window).corr(v)
            f = cpv.rolling(self.window, min_periods=1).mean()
            feat.loc[group.index] = f

        feat = feat.replace([np.inf, -np.inf], 0)

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])

