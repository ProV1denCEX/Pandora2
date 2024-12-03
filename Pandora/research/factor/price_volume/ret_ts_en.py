import numpy as np
import pandas as pd
from scipy.stats import differential_entropy

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class RetTSEntropy(FeatureTemplate):
    def __init__(self, windows):
        self.windows = windows

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        feat = pd.Series(index=X.index)

        for symbol, group in X.groupby(self.col_symbol):

            feat_symbol = {}
            for window in self.windows:
                feat_symbol[window] = np.log(1 + group[self.col_close].pct_change()).rolling(window, min_periods=10).sum()

            feat_symbol = pd.DataFrame(feat_symbol)

            cs_ma = feat_symbol.mean(axis=1)

            entro = feat_symbol.apply(differential_entropy, axis=1).replace([np.inf, -np.inf], 0)
            f = (1 - entro) * cs_ma

            feat.loc[f.index] = f

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}'])
