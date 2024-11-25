from sklearn.pipeline import FeatureUnion

from Pandora.constant import Frequency
from Pandora.research import *
from Pandora.research.factor.price_volume.mks import MKS
from Pandora.research.factor.price_volume.stm import STM

codes = CODES_TRADABLE

start = dt.datetime(2020, 1, 1)
quote_bt, ret = get_quote(codes, start=start, end=dt.datetime.now(), freq=Frequency.Min_15)

# to_check = get_stm_factor(quote_bt, 500)

quote_bt = quote_bt.reset_index().set_index(['datetime', 'symbol'])

fff = STM(500).transform(quote_bt)

union = FeatureUnion(
    [
        ('stm_3', MKS(300)),
        ('stm_5', MKS(500)),
        ('stm_7', MKS(700)),
    ],
    # n_jobs=-1,
    verbose=True,
    verbose_feature_names_out=False
)
union.set_output(transform="pandas")
unioned = union.fit_transform(quote_bt)

check = unioned.iloc[:, 1].reset_index().pivot(index='datetime', columns='symbol', values=unioned.iloc[:, 1].name)

aaa = 1
