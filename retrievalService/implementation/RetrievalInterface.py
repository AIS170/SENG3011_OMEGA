import boto3
from boto3.dynamodb.types import TypeDeserializer
import csv

import sys
from botocore.exceptions import ClientError

# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html for dynamoDB

from exceptions.UserNotFound import UserNotFound
from exceptions.UserAlreadyExists import UserAlreadyExists
from exceptions.UserHasFile import UserHasFile


class RetrievalInterface:

    def register(self, username, tableName) -> str:

        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")
        try:
            response = dynamodb.get_item(TableName=tableName, Key={"username": {"S": username}})

            if response.get("Item"):
                raise UserAlreadyExists("User already exists, cannot register this username")

            response = dynamodb.put_item(
                TableName=tableName,
                Item={
                    "username": {"S": username},
                    "retrievedFiles": {"L": []},
                },
            )

        except ClientError as e:
            sys.stderr.write(
                f"""(Retrieval Interface.register) Client (DynamoDB)
                Error: {e.response['Error']['Code']}\n"""
            )
            raise

    def pull(self, bucketName: str, fileNameOnS3: str) -> str:
        """Pulls a specified file from the specified s3 bucket.
        Will return the content of that file as a string.
        This method should be called by a IAM user who has access
        to at least read from the s3 bucket."""
        s3_client = boto3.client(service_name="s3")

        try:
            response = s3_client.get_object(Bucket=bucketName, Key=fileNameOnS3)
            object_content = response["Body"].read().decode("utf-8")
            return object_content

        except ClientError as e:
            sys.stderr.write(
                f"""(Retrieval Interface.deleteFromDynamo) Client (DynamoDB)
                Error: {e.response['Error']['Code']}\n"""
            )
            raise

    def getFileFromDynamo(self, fileName: str, username: str, tableName: str):
        """Looks for a user's file in the DynamoDB structure. Returns a tuple of
        size three of the form (bool, str|None, int). If the bool value is true,
        this indicates that the desired fileName was found under the particular
        user's object. In this case, the second element will be the file's content
        and the thrid will be the index at which the file appears in the list of
        user's retrieved files. If the file is not found, then the boolean value
        will be false, the second value will be None and the integer will be -1."""

        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")

        try:
            response = dynamodb.get_item(TableName=tableName, Key={"username": {"S": username}})

            userInfo = response.get("Item")
            if not userInfo:
                raise UserNotFound("Username not found - ensure you have registered")

            deserializer = TypeDeserializer()
            unmarshalledItem = {k: deserializer.deserialize(v) for k, v in response["Item"].items()}

            files = unmarshalledItem.get("retrievedFiles")

            for i, f in enumerate(files):
                file = f.get("filename")
                if file == fileName:

                    return (True, f.get("content"), i)

            return (False, None, -1)

        except ClientError as e:
            sys.stderr.write(
                f"""(Retrieval Interface.deleteFromDynamo) Client (DynamoDB)
                Error: {e.response['Error']['Code']}\n"""
            )
            raise

    # Pushes a file and its content to dynamoDB
    def pushToDynamo(self, fileName: str, fileContent: str, username: str, tableName: str):
        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")

        reader = csv.DictReader(fileContent.split("\n"), delimiter=",")

        contentList = []
        for line in list(reader):
            # if we have a blank line (especially at the ned of a file)
            if line == "":
                continue
            date = line.get("Date")
            closeVal = line.get("Close")

            contentList.append(
                {
                    "M": {
                        "attribute": {"M": {"close": {"S": closeVal}, "stock_name": {"S": fileName}}},
                        "event-type": {"S": "stock-ohlc"},
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

        new_object = {
            "stockName": {"S": fileName.removesuffix("_stock_data.csv")},  # will change depending on what Rakshil did
            "content": {"L": contentList},
            "filename": {"S": fileName},
        }

        try:
            found, file, index = self.getFileFromDynamo(fileName, username, tableName)
            if found:
                raise UserHasFile("User already have a file with this name; refusing to push it again")

            dynamodb.update_item(
                TableName=tableName,
                Key={"username": {"S": username}},
                UpdateExpression="""SET retrievedFiles =
                    list_append(if_not_exists(retrievedFiles, :empty_list), :new_values)""",
                ExpressionAttributeValues={":new_values": {"L": [{"M": new_object}]}, ":empty_list": {"L": []}},
                ReturnValues="UPDATED_NEW",
            )

            return True
        except ClientError as e:
            sys.stderr.write(
                f"""(RetrievalInterface.pushToDynamo) Client (DynamoDB)
                Error: {e.response['Error']['Code']}\n"""
            )
            raise
        except Exception as e:
            sys.stderr.write(f"(RetrievalInterface.pushToDynamo) General Exception {e}\n")
            raise

    def deleteOne(self, bucketName: str, fileNameOnS3: str) -> bool:
        s3_client = boto3.client("s3")
        try:
            s3_client.delete_object(Bucket=bucketName, Key=fileNameOnS3)
            return True
        except Exception:
            raise

    def deleteFromDynamo(self, fileName: str, username: str, tableName):
        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")

        found, file, fileIndex = self.getFileFromDynamo(fileName, username, tableName)
        if not found:
            raise FileNotFoundError("Attempting to delete a file that you have never retrieved")

        try:
            dynamodb.update_item(
                TableName=tableName,
                Key={"username": {"S": username}},
                UpdateExpression=f"REMOVE retrievedFiles [{fileIndex}]",
                ReturnValues="UPDATED_NEW",
            )

            return True
        except ClientError as e:
            sys.stderr.write(
                f"""(Retrieval Interface.deleteFromDynamo) Client (DynamoDB)
                Error: {e.response['Error']['Code']}\n"""
            )
            raise
        except Exception:
            raise

    def listUserFiles(self, username: str, tableName: str):
        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")
        try:
            response = dynamodb.get_item(TableName=tableName, Key={"username": {"S": username}})

            if response.get("Item", None) is None:
                raise UserNotFound("User does not seem to exist, ensure you have registered")

            deserializer = TypeDeserializer()
            unmarshalledItem = {k: deserializer.deserialize(v) for k, v in response["Item"].items()}

            return [stock.get("filename") for stock in unmarshalledItem.get("retrievedFiles")]

        except Exception as e:
            sys.stderr.write(
                f"""(Retrieval Interface.register)
                Error: {e}\n"""
            )
            raise

    def pushToDynamoV2(self, data_src: str, stockName: str, fileContent: str, username: str, tableName: str):
        '''Redoing the push method to dynamodb to account for having different data types (finance and news at
        the moment). The key difference will be the filename that is associated with a pushed file will include
        the type of data. The format of the filename will be <data_type>_<stock_name>.'''

        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")

        reader = csv.DictReader(fileContent.split("\n"), delimiter=",")

        contentList = []
        for line in list(reader):
            # if we have a blank line (especially at the end of a file)
            if line == "":
                continue
            date = line.get("Date")
            closeVal = line.get("Close")

            contentList.append(
                {
                    "M": {
                        "attribute": {"M": {"close": {"S": closeVal}, "stock_name": {"S": stockName}}},
                        "event-type": {"S": "stock-ohlc"},
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

        fileName = f"{data_src}_{stockName}"

        new_object = {
            "stockName": {"S": stockName},
            "content": {"L": contentList},
            "filename": {"S": fileName},
        }

        try:
            found, file, index = self.getFileFromDynamo(fileName, username, tableName)
            if found:
                raise UserHasFile("User already have a file with this name; refusing to push it again")

            dynamodb.update_item(
                TableName=tableName,
                Key={"username": {"S": username}},
                UpdateExpression="""SET retrievedFiles =
                    list_append(if_not_exists(retrievedFiles, :empty_list), :new_values)""",
                ExpressionAttributeValues={":new_values": {"L": [{"M": new_object}]}, ":empty_list": {"L": []}},
                ReturnValues="UPDATED_NEW",
            )

            return True
        except ClientError as e:
            sys.stderr.write(
                f"""(RetrievalInterface.pushToDynamo) Client (DynamoDB)
                Error: {e.response['Error']['Code']}\n"""
            )
            raise
        except Exception as e:
            sys.stderr.write(f"(RetrievalInterface.pushToDynamo) General Exception {e}\n")
            raise
