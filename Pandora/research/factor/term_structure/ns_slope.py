import datetime as dt
import numpy as np
import pandas as pd

import statsmodels.api as sm
from joblib import Parallel, delayed

from Pandora.constant import SymbolSuffix
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def get_factor(
        product_id, dt_, group,
        col_turnover_check, liquidity,
        mask,
        col_mc,
        col_datetime, col_symbol, feat_name
):
    if len(group) <= 1:
        return

    if not group[col_mc].any():
        return

    group = group.sort_values([col_turnover_check, 'ptm_rev'], ascending=False).iloc[:liquidity, :]

    if mask and (group.sort_values('ptm')[col_mc].iat[0] or group.sort_values('ptm')[col_mc].iat[-1]):
        value = 0

    else:
        tau_over_lambda = group['delay']
        exp_term = np.exp(-tau_over_lambda)

        group['b1'] = (1 - exp_term) / tau_over_lambda
        group['b2'] = (1 - exp_term) / tau_over_lambda - exp_term

        model = sm.OLS(group['log_price'], sm.add_constant(group[['b1', 'b2']]))
        res = model.fit()

        value = res.params['b1']

    return {
        col_datetime: dt_,
        col_symbol: product_id + SymbolSuffix.MC,
        feat_name: value
    }


class NsSlope(FeatureTemplate):
    def __init__(
            self,
            liquidity: int,
            turnover_window: int = 1,
            mask: bool = False,
            col_product_id='product_id',
            col_ptm='ptm_day',
            col_mc='mc',
            col_name='name',
            n_job=-1
    ):
        self.liquidity = liquidity
        self.turnover_window = turnover_window

        self.mask = mask

        self.col_product_id = col_product_id
        self.col_ptm = col_ptm
        self.col_mc = col_mc
        self.col_name = col_name

        self.col_turnover_check = 'turnover_check'
        self.n_job = n_job

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")

        check_multi_index(X, self.col_datetime, self.col_symbol)

        results = []
        for symbol, group in X.groupby(self.col_name):
            assert group.index.is_monotonic_increasing

            liquidity = group[self.col_turnover].rolling(self.turnover_window, min_periods=1).sum()
            results.append(pd.DataFrame({self.col_turnover_check: liquidity}))

        X_ = X.join(pd.concat(results, axis=0, ignore_index=False))

        X_['ptm'] = X_['ptm_day'].dt.total_seconds() / dt.timedelta(days=1).total_seconds()
        X_['ptm_rev'] = 1 / X_['ptm']
        X_['delay'] = X_['ptm'] / 1000
        X_['log_price'] = np.log(X_[self.col_close])

        loc = X_['ptm'] != 0
        X_ = X_[loc]

        feat_name = self.get_feature_names_out()[0]

        results = Parallel(n_jobs=self.n_job)(  # n_jobs=-1 表示使用所有CPU核心
            delayed(get_factor)(
                product_id, dt_, group,
                self.col_turnover_check, self.liquidity,
                self.mask,
                self.col_mc,
                self.col_datetime, self.col_symbol, feat_name
            )
            for (product_id, dt_), group in X_.groupby([self.col_product_id, self.col_datetime])
        )

        feat = pd.DataFrame([i for i in results if i is not None]).set_index([self.col_datetime, self.col_symbol]).sort_index()

        return feat

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__

        name = f'{self.prefix}{estimator_name}_{self.liquidity}'
        if self.mask:
            name += '_MASK'

        if self.turnover_window > 1:
            name += f'_{self.turnover_window}'

        return np.asarray([name])
