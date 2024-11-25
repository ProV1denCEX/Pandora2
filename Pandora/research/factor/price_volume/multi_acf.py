import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index


class MultiACF(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_index(X, self.col_datetime, self.col_symbol)

        feat = pd.Series(index=X.index)
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            r = np.log(group[self.col_close] / group[self.col_close].shift())

            acfs = {}
            for i in range(1, self.window):
                acf_ = r.rolling(window=self.window).corr(r.shift(i))
                # acf_ = r.rolling(window=i).corr(r.shift(i))

                acfs[i] = acf_ * (self.window - i) / self.window

            acfs = pd.DataFrame(acfs)

            acf = acfs.dropna().sum(axis=1)

            feat.loc[group.index] = acf

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
