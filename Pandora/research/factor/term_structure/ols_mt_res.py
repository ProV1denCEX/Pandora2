import datetime as dt
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from sklearn import linear_model

from Pandora.constant import SymbolSuffix
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def get_factor(
        product_id, dt_, group,
        model,
        col_turnover_check, liquidity,
        sign, mask,
        col_mc,
        col_datetime, col_symbol, feat_name
):
    if len(group) <= 1:
        return

    group = group.sort_values([col_turnover_check, 'ptm_rev'], ascending=False).iloc[:liquidity, :]

    if not group[col_mc].any():
        return

    if mask and (group.sort_values('ptm')[col_mc].iat[0] or group.sort_values('ptm')[col_mc].iat[-1]):
        value = 0

    else:
        x = group['ptm']
        y = group['log_price']

        x = x.values.reshape(-1, 1)
        y = y.values.reshape(-1, 1)

        model.fit(y=y, X=x)

        x_ = group.loc[group[col_mc], 'ptm']
        x_ = x_.values.reshape(-1, 1)
        y_ = model.predict(x_)

        y_true = group.loc[group[col_mc], 'log_price']
        y_true = y_true.values.reshape(-1, 1)

        res = y_true - y_
        value = res[0, 0]

        if sign:
            value = np.sign(value)

    return {
        col_datetime: dt_,
        col_symbol: product_id + SymbolSuffix.MC,
        feat_name: value
    }


class OlsMtRes(FeatureTemplate):
    def __init__(
            self,
            liquidity: int,
            turnover_window: int = 1,
            mask: bool = False,
            sign: bool = False,
            col_product_id='product_id',
            col_ptm='ptm_day',
            col_mc='mc',
            col_name='name',
            n_jobs=-1
    ):
        self.liquidity = liquidity
        self.turnover_window = turnover_window

        self.mask = mask
        self.sign = sign

        self.col_name = col_name
        self.col_product_id = col_product_id
        self.col_ptm = col_ptm
        self.col_mc = col_mc

        self.col_turnover_check = 'turnover_check'

        self.n_jobs = n_jobs

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

        loc = X_['ptm'] != 0
        X_ = X_[loc]

        X_['ptm_rev'] = 1 / X_['ptm']
        X_['log_price'] = np.log(X_[self.col_close])

        feat_name = self.get_feature_names_out()[0]
        model = linear_model.LinearRegression(fit_intercept=True)

        results = Parallel(n_jobs=self.n_jobs)(  # n_jobs=-1 表示使用所有CPU核心
            delayed(get_factor)(
                product_id, dt_, group,
                model,
                self.col_turnover_check, self.liquidity,
                self.sign, self.mask,
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

        if self.sign:
            name += '_SIGN'

        return np.asarray([name])
