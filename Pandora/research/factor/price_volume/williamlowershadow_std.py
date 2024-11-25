import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index


class WilliamLowerShadowStd(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_index(X, self.col_datetime, self.col_symbol)

        param = self.window
        feat = pd.Series(index=X.index)
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            ll = group[self.col_low].rolling(param).min()
            f = (ll - group[self.col_close])

            fma = f.rolling(param).std()

            f = f / fma

            loc = fma == 0
            f[loc] = 0

            feat.loc[group.index] = f * -1

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])

