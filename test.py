from Pandora.data_manager.data_api import FutureDataAPI

sql = "select contract,ticker from future_quote_60min limit 10"
td = FutureDataAPI()
data = td.tsdb.query(sql)