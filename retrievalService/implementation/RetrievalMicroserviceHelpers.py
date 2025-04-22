from exceptions.InvalidDataKey import InvalidDataKey
from datetime import datetime
from pytz import timezone
import csv


def validateDataSrc(dataSrc):
    validDataSources = set(["finance", "news", "sport"])
    if dataSrc not in validDataSources:
        raise InvalidDataKey(
            f"data type {dataSrc} is not valid - valid types are {validDataSources}"
        )
    else:
        return True


def getKeyToTableNameMap():
    return {
        "finance": "seng3011-omega-25t1-testing-bucket",
        "news": "seng3011-omega-news-data",
        "sport": "seng3011-omega-sports-data",
    }


def getKeyToDataSourceMap():
    return {"finance": "yahoo_finance", "news": "yahoo_news", "sport": "yahoo_sport"}


def getKeyToDatasetTypeMap():
    return {
        "finance": "Daily stock data",
        "news": "Financial news",
        "sport": "Sports news",
    }


def getEventType(dataSrc):
    map = {"finance": "stock-ohlc", "news": "stock-news"}

    return map[dataSrc]


def getS3FileName(username, dataType, stockname, date):
    fileFormat = {
        "finance": f"{username}#{stockname}_stock_data.csv",
        "news": f"{username}_{stockname}_{date}_news.csv",
        "sport": f"{username}#{stockname}_{date}_sport.csv",  # TODO GET THIS CHECKED WITH RAKSHIL
    }

    return fileFormat[dataType]


def getTableNameFromKey(key: str):
    keyToTableNameMap = getKeyToTableNameMap()
    tableName = keyToTableNameMap.get(key, None)

    return tableName


def adageFormatter(s3BucketName: str, stockName: str, content: str, data_type: str):
    dataSrc = getKeyToDataSourceMap().get(data_type, None)
    datasetType = getKeyToDatasetTypeMap().get(data_type, None)

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


# private helper
def createDynamoDBAttributeMap(dataSrc, stockname, line):
    attributes = {
        "finance": [("close", "Close")],
        "news": [("url", "url"), ("sentiment_score", "sentiment_score")],
        "sport": None,  # TODO: Figure this out using an example csv file from Rakshil
    }

    attributeMap = {}

    requiredAttributes = attributes[dataSrc]
    for adageField, colName in requiredAttributes:
        attributeMap[adageField] = {"S": line.get(colName)}
    attributeMap["stock_name"] = {"S": stockname}
    return attributeMap


# private helper
def GettingCSVDateColName(dataSrc):
    keyToDateColumn = {
        "finance": "Date",
        "news": "published_at",
        "sport": None,  # TODO: Figure this out using an example csv file from
    }
    keyToDateColumn = {"finance": "Date", "news": "published_at"}

    return keyToDateColumn[dataSrc]


# public helper
def createDynamoDBContentList(dataSrc, stockname, fileContent):
    reader = csv.DictReader(fileContent.split("\n"), delimiter=",")
    contentList = []
    for line in list(reader):
        # if we have a blank line (especially at the end of a file)
        if line == "":
            continue
        date = line.get(GettingCSVDateColName(dataSrc))
        # closeVal = line.get("Close")

        contentList.append(
            {
                "M": {
                    "attribute": {
                        "M": createDynamoDBAttributeMap(dataSrc, stockname, line)
                    },
                    "event-type": {"S": f"{(getEventType(dataSrc))}"},
                    "time_object": {
                        "M": {
                            "duration": {"S": "0"},
                            "duration-unit": {"S": "days"},
                            "time-stamp": {"S": date},
                            "time-zone": {"S": "GMT+11"},
                        }
                    },
                }
            }
        )
    return contentList
