import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from Pandora.constant import SymbolSuffix
from Pandora.research.factor.template import FeatureTemplate
from Pandora.research.factor.utils import check_multi_index


def get_stm(group_, window, col_close):
    hh = group_[col_close].rolling(window, min_periods=1).max()
    ll = group_[col_close].rolling(window, min_periods=1).min()
    mom = (group_[col_close] - (hh + ll) / 2) / (hh - ll + 1e-8)

    return mom


def get_estm(group_, window, col_close):
    hh = group_[col_close].rolling(window, min_periods=1).max()
    ll = group_[col_close].rolling(window, min_periods=1).min()
    mom = ((group_[col_close] * 2 - (hh + ll)).ewm(span=5).mean()) / ((hh - ll).ewm(span=5).mean())

    return mom


def get_roc(group_, window, col_close):
    return np.log(group_[col_close] / group_[col_close].shift(window))


def get_basis_mom(product_id, dt_, group, t1, t2, col_ptm, col_mom, col_datetime, col_symbol, feat_name):
    group = group.sort_values(col_ptm)

    if t2:
        if len(group) <= abs(t2):
            return

        value = group[col_mom].iat[t1] - group[col_mom].iat[t2]

    else:
        if len(group) <= 1:
            return

        value = group[col_mom].iat[t1] - group[col_mom].iloc[t1 + 1:].mean()

    return {
        col_datetime: dt_,
        col_symbol: product_id + SymbolSuffix.MC,
        feat_name: value
    }


class BasisSTM(FeatureTemplate):
    def __init__(
            self,
            window: int,
            liquidity: float,
            t2: int,
            turnover_window: int = 1,
            mom_type: str = "STM",
            col_product_id='product_id', col_ptm='ptm_day', col_name='name',
            n_job=-1
    ):
        self.liquidity = liquidity
        self.t2 = t2
        self.window = window
        self.turnover_window = turnover_window

        self.col_name = col_name
        self.col_product_id = col_product_id
        self.col_mom = mom_type
        self.col_ptm = col_ptm
        self.col_turnover_check = 'turnover_check'

        self.n_job = n_job

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        self.set_output(transform="pandas")

        check_multi_index(X, self.col_datetime, self.col_symbol)

        if self.col_mom == "STM":
            get_mom_ = get_stm

        elif self.col_mom == "ESTM":
            get_mom_ = get_estm

        elif self.col_mom == "ROC":
            get_mom_ = get_roc

        else:
            raise NotImplementedError("mom_type must be stm, estm or roc")

        def get_mom_info(group_, window, turnover_window, col_close, col_turnover, col_mom, col_turnover_check):
            assert group_.index.is_monotonic_increasing

            mom = get_mom_(group_, window, col_close)

            liquidity = group_[col_turnover].rolling(turnover_window, min_periods=1).sum()

            return pd.DataFrame({col_mom: mom, col_turnover_check: liquidity})

        # 这里用parallel 是为了in case mom 的算法复杂，算起来慢；实际上耗时主要在join的地方
        results = Parallel(n_jobs=self.n_job)(  # n_jobs=-1 表示使用所有CPU核心
            delayed(get_mom_info)(group, self.window, self.turnover_window,
                                  self.col_close, self.col_turnover, self.col_mom, self.col_turnover_check)
            for code, group in X.groupby(self.col_name)
        )

        X_ = X.join(pd.concat(results, axis=0, ignore_index=False))

        loc = X_[self.col_turnover_check] > self.liquidity * 1e8
        X_ = X_[loc]

        feat_name = self.get_feature_names_out()[0]
        t1 = 0

        results = Parallel(n_jobs=self.n_job)(  # n_jobs=-1 表示使用所有CPU核心
            delayed(get_basis_mom)(
                product_id, dt_, group,
                t1, self.t2,
                self.col_ptm, self.col_mom, self.col_datetime, self.col_symbol, feat_name
            )
            for (product_id, dt_), group in X_.groupby([self.col_product_id, self.col_datetime])
        )

        feat = pd.DataFrame([i for i in results if i is not None]).set_index([self.col_datetime, self.col_symbol]).sort_index()

        return feat

    def get_feature_names_out(self, input_features=None):
        estimator_name = self.__class__.__name__
        return np.asarray([f'{self.prefix}{estimator_name}_{self.window}_{self.liquidity}_{self.t2}'])
