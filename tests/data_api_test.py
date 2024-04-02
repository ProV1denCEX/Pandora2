# -*- coding:utf-8 -*-
import pytest

from Pandora.data_manager.data_api import FutureDataAPI
from Pandora.constant import Frequency


@pytest.mark.parametrize("col,st,et,res",
                         [('Date', '2021-1-1', None, "Date >= '2021-1-1'"),
                          ('Date', '2021-1-1', '2021-12-1', "Date BETWEEN '2021-1-1' AND '2021-12-1'"),
                          ('Date', None, '2021-1-1', "Date <= '2021-1-1'")])
def test_feed_pair_dates(col, st, et, res):
    part = FutureDataAPI.pair_dates(col, st, et)
    assert part == res


@pytest.mark.parametrize("codes,res", [('A', 1), ('A,AG,B2210', 3), (('A', 'B', 'EG2210', 'B2210'), 3)])
def test_split_codes(codes, res):
    contracts, tickers = FutureDataAPI.split_codes(codes)
    print(contracts, tickers)
    assert len(contracts) + len(tickers) == res


def test_tsdb_query():
    sql = "select contract,ticker from future_quote_60min limit 10"
    td = FutureDataAPI()
    data = td.tsdb.query(sql)
    assert data.shape[0] == 10


def test_mssql_query():
    sql = "select TOP 10 contract,ticker from dbo.FutureInfo_Basic"
    td = FutureDataAPI()
    data = td.mssql_65.query(sql)
    assert data.shape[0] == 10


@pytest.mark.parametrize("params", [None, 'A', 'A,AG,B2210', ('A', 'B', 'EG2210', 'B2210')])
def test_query_future_basic(params):
    td = FutureDataAPI()
    data = td.get_future_basic(params)
    print(f"{params} => ", data.shape)
    assert data.shape[0] > 1


@pytest.mark.parametrize("codes,start_dt,end_dt",
                         [('A', "20200107", "20200107"), ('A,AG,B2012', "20200105", "20200108"),
                          (('A', 'B', 'EG2010', 'B2010'), "20200105", "20200108")])
def test_query_future_quote(codes, start_dt, end_dt):
    td = FutureDataAPI()
    data = td.get_future_quote(codes, start_dt, end_dt, freq=Frequency.Min_60)
    print(f"{codes} => ", data.shape)
    assert data.shape[0] > 0


@pytest.mark.parametrize("codes,start_dt,end_dt,res",
                         [('', "", "20200107", 111488), ('A,AG,B2012', "20200105", "20200108", 6),
                          (('A', 'B', 'EG2010', 'B2010'), "20200105", "20200108", 6)])
def test_query_future_main_ticker(codes, start_dt, end_dt, res):
    td = FutureDataAPI()
    data = td.get_future_main_ticker(codes, start_dt, end_dt)
    print(f"{codes} => ", data.shape)
    assert data.shape[0] == res


@pytest.mark.parametrize("codes,start_dt,end_dt,res",
                         [('', "", "20200107", 111488), ('A,AG,B2012', "20200105", "20200108", 6),
                          (('A', 'B', 'EG2010', 'B2010'), "20200105", "20200108", 6)])
def test_query_future_coefadj(codes, start_dt, end_dt, res):
    td = FutureDataAPI()
    data = td.get_future_coefadj(codes, start_dt, end_dt)
    print(f"{codes} => ", data.shape)
    assert data.shape[0] == res


@pytest.mark.parametrize("ids,tags,start_dt,end_dt,res",
                         [("EMA_V0_252,ATR_V0_14,OBV_V0", "", "20220101", "", 0),
                          ('', 'Mom,Lag,MA-Based', "", "20211231", 0),
                          (("EMA_V0_252", "ATR_V0_14", "OBV_V0"), "", "20200105", "20210108", 0),
                          ('', ("Mom", "Lag", "MA-Based"), "20200105", "20210108", 0)])
def test_query_future_factor(ids, tags, start_dt, end_dt, res):
    td = FutureDataAPI()
    data = td.get_future_factor(ids, tags, Frequency.Daily, start_dt, end_dt)
    print(f"id[{ids}] => {data.shape}, tags[{tags}] => {data.shape}")
    assert data.shape[0] == res


@pytest.mark.parametrize("ids,start_dt,end_dt,res", [("", "20200101", "20200105", 208),
                                                     ("NHA,NHAG", "20200101", "20200105", 4),
                                                     (("南华苹果", "南华豆二", "南华白银"), "20200101", "20200105", 6)])
def test_future_quote_index(ids, start_dt, end_dt, res):
    td = FutureDataAPI()
    data = td.get_future_index_quote(ids, start_dt, end_dt)
    assert data.shape[0] == res


if __name__ == '__main__':
    pytest.main()
