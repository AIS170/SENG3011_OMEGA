from flask import Flask, request
from botocore.exceptions import ClientError
from RetrievalInterface import RetrievalInterface

from RetrievalMicroserviceHelpers import getTableNameFromKey, adageFormatter

# import sys
from datetime import datetime
from pytz import timezone

import json

from exceptions.UserNotFound import UserNotFound
from exceptions.UserAlreadyExists import UserAlreadyExists
from exceptions.InvalidDataKey import InvalidDataKey


# from exceptions.UserHasFile import UserHasFile

app = Flask(__name__)

AWS_S3_BUCKET_NAME = "seng3011-omega-25t1-testing-bucket"
DYNAMO_DB_NAME = "seng3011-test-dynamodb"


@app.route("/v1/register/", methods=["POST"])
def register():
    username = request.get_json()["username"]
    retrievalInterface = RetrievalInterface()

    try:
        retrievalInterface.register(username, DYNAMO_DB_NAME)
        return json.dumps({"Success": f"User {username} registered successfully"}), 200
    except UserAlreadyExists:
        return json.dumps({"UserTakenError": "Username taken"}), 401
    except ClientError:
        return (
            json.dumps({"InternalError": "Something has gone wrong on our end. Please report"}),
            500,
        )


@app.route("/v1/retrieve/<username>/<stockname>/", methods=["GET"])
def retrieve(username: str, stockname: str):
    retrievalInterface = RetrievalInterface()
    filenameS3 = f"{username}#{stockname}_stock_data.csv"  # need to think about Rakshil's file formatting here
    try:
        found, content, index = retrievalInterface.getFileFromDynamo(stockname, username, DYNAMO_DB_NAME)

        if found:
            return (
                json.dumps(
                    {
                        "data_source": "yahoo_finance",
                        "dataset_type": "Daily stock data",
                        "dataset_id": "http://seng3011-omega-25t1-testing-bucket.s3-ap-southeast-2-amazonaws.com",
                        "time_object": {
                            "timestamp": f"{str(datetime.now(timezone('Australia/Sydney'))).split('+')[0]}",
                            "timezone": "GMT+11",
                        },
                        "stock_name": stockname,
                        "events": content,
                    }
                ),
                200,
            )
        else:
            content = retrievalInterface.pull(AWS_S3_BUCKET_NAME, f"{filenameS3}")
            # need to format Rakshil's S3 content format into DynamoDB content
            retrievalInterface.pushToDynamo(stockname, content, username, DYNAMO_DB_NAME)
            found, content, index = retrievalInterface.getFileFromDynamo(stockname, username, DYNAMO_DB_NAME)
            return (
                json.dumps(
                    {
                        "data_source": "yahoo_finance",
                        "dataset_type": "Daily stock data",
                        "dataset_id": "http://seng3011-omega-25t1-testing-bucket.s3-ap-southeast-2-amazonaws.com",
                        "time_object": {
                            "timestamp": f"{str(datetime.now(timezone('Australia/Sydney'))).split('+')[0]}",
                            "timezone": "GMT+11",
                        },
                        "stock_name": stockname,
                        "events": content,
                    }
                ),
                200,
            )
    except ClientError as e:

        if e.response["Error"]["Code"] == "NoSuchKey":
            return (
                json.dumps(
                    {
                        "StockNotFound": f"It appears that you have do not have access to stock {stockname}."
                        "Ensure you have collected the stock before attempting retrieval"
                    }
                ),
                400,
            )
        else:
            return json.dumps({"InternalError": f"Something unbelievable went wrong; please report - error = {e}"}), 500
    except UserNotFound:
        return json.dumps({"UserNotFound": "Username not found; ensure you have registered"}), 401
    except Exception as e:
        return json.dumps({"InternalError": f"Something went wrong; please report - error = {e}"}), 500


@app.route("/v1/delete/<username>/<filename>/", methods=["DELETE"])
def delete(username: str, filename: str):

    retrievalInterface = RetrievalInterface()
    try:
        # delete from dynamodb
        retrievalInterface.deleteFromDynamo(filename, username, DYNAMO_DB_NAME)
        return json.dumps({"Success": f"Deleted {filename}"})
    except FileNotFoundError:
        return json.dumps({"FileNotFound": f"No File for stock {filename} exists for deletion"}), 400
    except UserNotFound:
        return json.dumps({"UserNotFound": f"No user with username {username} exists, ensure you have registered"}), 401
    except Exception as e:
        return json.dumps({"InternalError": f"Something has gone wrong on our end, please report; e = {e}"}), 500


@app.route("/v1/list/<username>/", methods=["GET"])
def getAll(username: str):
    retrievalInterface = RetrievalInterface()
    try:
        return json.dumps({"Success": retrievalInterface.listUserFiles(username, DYNAMO_DB_NAME)})
    except UserNotFound:
        return json.dumps({"UserNotFound": "User does not appear to exist, ensure you have registered"}), 401
    except Exception:
        return json.dumps({"InternalError": "Something has gone wrong on our end, please report"}), 500


@app.route("/v2/retrieve/<username>/<data_type>/<stockname>/")
def retrieveV2(username, data_type, stockname):
    try:
        retrievalInterface = RetrievalInterface()
        s3BucketName = getTableNameFromKey(data_type)

        # need to think how this might change depending on the bucket that i am reaching into (collab with Rakshil here)
        filenameS3 = f"{username}#{stockname}_stock_data.csv"
        filenameDynamo = f"{data_type}_{stockname}"
        
        found, content, index = retrievalInterface.getFileFromDynamo(filenameDynamo, username, DYNAMO_DB_NAME)

        if found:
            return (
                json.dumps(
                    adageFormatter(s3BucketName, stockname, content, data_type)
                ),
                200,
            )
        else:
            content = retrievalInterface.pull(s3BucketName, f"{filenameS3}")
            retrievalInterface.pushToDynamoV2(data_type, stockname, content, username, DYNAMO_DB_NAME)

            found, content, index = retrievalInterface.getFileFromDynamo(stockname, username, DYNAMO_DB_NAME)
            return (
                json.dumps(
                    adageFormatter(s3BucketName, stockname, content, data_type)

                ),
                200,
            )
    except ClientError as e:

        if e.response["Error"]["Code"] == "NoSuchKey":
            return (
                json.dumps(
                    {
                        "StockNotFound": f"It appears that you have do not have access to stock {stockname}."
                        "Ensure you have collected the stock before attempting retrieval"
                    }
                ),
                400,
            )
        else:
            return json.dumps({"InternalError": f"Something unbelievable went wrong; please report - error = {e}"}), 500
    except UserNotFound:
        return json.dumps({"UserNotFound": "Username not found; ensure you have registered"}), 401
    except InvalidDataKey as e:
        print(f"Oogly boogly i am a chicken")
        return json.dumps({"InvalidDataKey": f"{e}"}), 400
    except Exception as e:
        return json.dumps({"InternalError": f"Something went wrong; please report - error = {e}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
