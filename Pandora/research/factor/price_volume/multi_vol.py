import numpy as np
import pandas as pd

from joblib import Parallel, delayed

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def _get_factor(group, param, col_close):
    assert group.index.is_monotonic_increasing

    v_1 = None

    f = pd.Series(0, index=group.index)

    for q in range(1, param):
        r2 = np.square(np.log(group[col_close] / group[col_close].shift(q)))

        idx = range(param - 1, -1, -q)
        n = param / q - 1

        v_q = r2.rolling(window=param).apply(lambda x: np.mean(x[idx]) * n, raw=True)

        if q == 1:
            v_1 = v_q

        f += v_q

    f = f / v_1 / param

    f = f - 1

    return f


class MultiVol(FeatureTemplate):
    def __init__(self, window: int, n_job=-1):
        self.window = window
        self.n_job = n_job

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        param = self.window
        feat = pd.Series(index=X.index)

        results = Parallel(n_jobs=self.n_job)(  # n_jobs=-1 表示使用所有CPU核心
            delayed(_get_factor)(group, param, self.col_close)
            for code, group in X.groupby(self.col_symbol)
        )

        for f in results:
            feat.loc[f.index] = f

        feat = feat.replace([np.inf, -np.inf], 0)

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
