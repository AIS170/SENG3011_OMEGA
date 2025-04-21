# https://pypi.org/project/moto/
# a mock for s3 to let me check that I am using boto3 correctly
import pytest
import os
from moto import mock_aws
from botocore.exceptions import ClientError

from ..implementation.RetrievalInterface import RetrievalInterface


# moto uses depreacted datetime.datetime.utcnow which causes a Deprecation Warning
# Therefore, I am choosing to hide this warning
@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestPushToDynamo:
    @mock_aws
    def test_push_file(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        username = "user1"
        tableName = "seng3011-test-dynamodb"

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        retrievedFiles = (
            test_table.get_item(TableName=tableName, Key={"username": {"S": username}})
            .get("Item")
            .get("retrievedFiles")
            .get("L")
        )

        assert len(retrievedFiles) == 1

        assert retrievedFiles[0].get("M").get("filename").get("S") == stockName

    @mock_aws
    def test_table_not_exist(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        username = "user1"

        retrievalInterface = RetrievalInterface()
        with pytest.raises(ClientError) as errorInfo:
            retrievalInterface.pushToDynamo(
                stockName, fileContent, username, "fakeTableName"
            )
        assert errorInfo.value.response["Error"]["Code"] == "ResourceNotFoundException"

    @mock_aws
    def test_user_does_not_exist(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        tableName = "seng3011-test-dynamodb"

        retrievalInterface = RetrievalInterface()
        # with pytest.raises(botocore.errorfactory.ResourceNotFoundException):
        with pytest.raises(Exception):
            retrievalInterface.pushToDynamo(
                stockName, fileContent, "fake-user", tableName
            )

    @mock_aws
    def test_user_double_pushes(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        tableName = "seng3011-test-dynamodb"
        username = "user1"

        retrievalInterface = RetrievalInterface()

        response = retrievalInterface.pushToDynamo(
            stockName, fileContent, username, tableName
        )
        assert response is True

        with pytest.raises(Exception):
            retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)
