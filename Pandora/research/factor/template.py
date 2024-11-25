import pandas as pd

from sklearn.base import BaseEstimator
from sklearn.base import TransformerMixin


class FeatureTemplate(BaseEstimator, TransformerMixin):
    col_datetime = "datetime"
    col_symbol = "symbol"
    col_open = "open_price"
    col_high = "high_price"
    col_low = "low_price"
    col_close = "close_price"
    col_volume = "volume"
    col_turnover = "turnover"
    col_open_interest = "open_interest"

    prefix = "FutureFactor_"

    def fit(self, X, y=None, **fit_params):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
            如果需要输出不同于X的index，需要以DataFrame 形式返回
            以series 或ndarray 形式返回的值都会被X.index 来构造DataFrame，
                此时要求返回的series 或ndarray 的index 与X 对齐 或被X包含（在返回series 的前提下）
                    (pd.DataFrame(index=xxx, columns=xxx) 执行的是选择动作而不是rename 动作)

        """
        raise NotImplementedError()
