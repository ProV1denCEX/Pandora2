from Pandora.data_manager.data_api import FutureDataAPI
# from Pandora.data_manager.edbdata_api import EdbDataApi


api = None


def get_api(real_trade=False):
    global api
    if api:
        return api

    api = FutureDataAPI(real_trade)
    return api
