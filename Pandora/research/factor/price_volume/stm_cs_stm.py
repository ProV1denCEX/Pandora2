import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index


class STMCsSTM(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_index(X, self.col_datetime, self.col_symbol)

        ret = pd.Series(index=X.index)
        feat = {}
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            hh = group.loc[:, self.col_high].rolling(self.window).max()
            ll = group.loc[:, self.col_low].rolling(self.window).min()
            f = ((group.loc[:, self.col_close] * 2 - (hh + ll)).ewm(span=5).mean()) / ((hh - ll).ewm(span=5).mean())

            feat[code] = f.reset_index(self.col_symbol, drop=True)

        feat = pd.DataFrame(feat)

        hh = feat.ffill().max(axis=1)
        ll = feat.ffill().min(axis=1)

        feat = ((feat * 2).sub(hh + ll, axis=0).div(hh - ll, axis=0) + 1) / 2

        f = feat.melt(ignore_index=False, var_name=self.col_symbol, value_name='value').set_index(self.col_symbol, append=True)['value']
        ret.loc[X.index] = f.loc[X.index]

        return ret.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
