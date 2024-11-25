import numpy as np
import pandas as pd
import talib
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_index, rolling_rank


class TSCorr(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_index(X, self.col_datetime, self.col_symbol)

        feat = pd.Series(index=X.index)
        for code, data in X.groupby(self.col_symbol):

            assert data.index.is_monotonic_increasing

            rolling_ret = self.window
            rolling_vol = rolling_ret * 4
            rank_window = rolling_ret
            relative_close = data.loc[:, self.col_close] / data.loc[:, self.col_close].rolling(rolling_ret, min_periods=1).mean()
            relative_vol = (data.loc[:, self.col_volume] / data.loc[:, self.col_volume].rolling(rolling_vol, min_periods=1).mean()).fillna(0)
        
            corr = talib.CORREL(relative_close, relative_vol, timeperiod=rank_window)
        
            rank = rolling_rank(data.loc[:, self.col_volume], rank_window, pct=True)
            # rank = data.loc[:, self.col_volume].rolling(rank_window, min_periods=1).apply(lambda x: x.rank(pct=True).iat[-1])
            f = corr * rank

            feat.loc[data.index] = f

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
