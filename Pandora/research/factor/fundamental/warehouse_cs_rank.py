import numpy as np
import pandas as pd

from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class WarehouseCsRank(FeatureTemplate):
    def __init__(self, window: int = 1, fill_na = None):
        self.window = window
        self.fill_na = fill_na

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        col_base_factor = "factor_value"

        daily_factor_sl = X[col_base_factor].unstack()
        chg = daily_factor_sl.pct_change()

        chg = chg.replace([np.inf], 1)
        chg = chg.replace([-np.inf], -1)

        # handle 0 / 0
        if self.fill_na:
            loc = pd.isna(chg) & (daily_factor_sl == 0)
            chg[loc] = self.fill_na

        zs_factor = chg.rolling(self.window, min_periods=1).mean()
        feat = 1 - zs_factor.rank(axis=1, pct=True)

        feat_name = self.get_feature_names_out()[0]
        f = feat.melt(ignore_index=False, var_name=self.col_symbol, value_name=feat_name).set_index(self.col_symbol, append=True)[feat_name]

        ret = pd.Series(index=X.index)
        ret.loc[X.index] = f.loc[X.index]

        return ret.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
