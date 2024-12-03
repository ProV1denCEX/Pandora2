import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class MKS(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        feat = pd.Series(index=X.index)
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            A = group[self.col_close]
            f = pd.Series(mks(A, self.window), index=group.index)

            feat.loc[group.index] = f

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])


def mks(A: np.ndarray, n: int, imax: int = 0):
    df = pd.Series(A)

    f = pd.Series(0, index=df.index)
    denom = 0
    for i in range(1, n):

        if imax and i > imax:
            break

        chg = np.sign(df.pct_change(i))
        f += chg.fillna(0).rolling(n - i).sum()

        denom += n - i

    f = f / denom

    return f
