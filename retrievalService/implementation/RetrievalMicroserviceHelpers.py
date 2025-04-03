from exceptions.InvalidDataKey import InvalidDataKey
from datetime import datetime
from pytz import timezone


def getKeyToTableNameMap():
    return {
        "finance": "seng3011-omega-25t1-testing-bucket",
        "news": "seng3011-omega-news-data"
    }


def getKeyToDataSourceMap():
    return {
        "finance": "yahoo_finance",
        "news": "yahoo_news"
    }


def getKeyToDatasetTypeMap():
    return {
        "finance": "Daily stock data",
        "news": "Financial news"
    }


def getTableNameFromKey(key: str):

    keyToTableNameMap = getKeyToTableNameMap()

    tableName = keyToTableNameMap.get(key, None)
    if tableName is None:
        print("That is very bad")
        raise InvalidDataKey(f"data type {key} is not valid - valid types are {keyToTableNameMap.keys()}")

    return tableName


def adageFormatter(s3BucketName: str, stockName: str, content: str, data_type: str):


    dataSrc = getKeyToDataSourceMap().get(data_type, None)
    datasetType = getKeyToDatasetTypeMap().get(data_type, None)

    if dataSrc is None or datasetType is None:
        raise InvalidDataKey(f"data type {data_type} is not valid - valid types are {getKeyToTableNameMap().keys()}")

    return {
        "data_source": f"{dataSrc}",
        "dataset_type": f"{datasetType}",
        "dataset_id": f"http://{s3BucketName}.s3-ap-southeast-2-amazonaws.com",
        "time_object": {
            "timestamp": f"{str(datetime.now(timezone('Australia/Sydney'))).split('+')[0]}",
            "timezone": "GMT+11",
        },
        "stock_name": stockName,
        "events": content,
    }

