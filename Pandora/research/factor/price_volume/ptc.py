import numpy as np
import pandas as pd
import talib
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


class PTC(FeatureTemplate):
    def __init__(self, window: int):
        self.window = window

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_multi_index(X, self.col_datetime, self.col_symbol)

        fast = self.window
        slow = int(fast * 26 / 12)
        signal = int(fast * 9 / 12)

        feat = pd.Series(index=X.index)
        for code, group in X.groupby(self.col_symbol):

            assert group.index.is_monotonic_increasing

            hh = group.loc[:, self.col_high].rolling(self.window).max()
            ll = group.loc[:, self.col_low].rolling(self.window).min()
            a = ((group.loc[:, self.col_close] * 2 - (hh + ll)).ewm(span=5).mean()) / ((hh - ll).ewm(span=5).mean())

            (dif,
             dea,
             bar) = talib.MACD(group.loc[:, self.col_close],
                               fastperiod=fast,
                               slowperiod=slow,
                               signalperiod=signal
                               )

            std = group.loc[:, self.col_close].rolling(window=fast).std()

            bar = bar / std

            loc = std == 0
            bar[loc] = 0

            feat.loc[group.index] = a - bar

        return feat.values

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}'])
