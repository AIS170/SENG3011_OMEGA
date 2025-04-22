import pytest
import os
from moto import mock_aws

from ..implementation.RetrievalInterface import RetrievalInterface


@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestGetFileFromDynamo:
    @mock_aws
    def test_get_file(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"

        with open(fileName, "r") as f:
            fileContent = f.read()

        username = "user1"
        tableName = "seng3011-test-dynamodb"

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        found, retrievedFile, index = retrievalInterface.getFileFromDynamo(
            stockName, username, tableName
        )

        assert retrievedFile[0].get("attribute").get("stock_name") == stockName

    @mock_aws
    def test_get_file_from_wrong_user(self, test_table_two_users, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        username = "user1"
        username2 = "user2"
        tableName = "seng3011-test-dynamodb"

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        found, retrievedFile, index = retrievalInterface.getFileFromDynamo(
            stockName, username2, tableName
        )
        assert not found

    @mock_aws
    def test_get_file_wrong_file_name(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"

        with open(fileName, "r") as f:
            fileContent = f.read()

        username = "user1"
        tableName = "seng3011-test-dynamodb"

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        found, retrievedFile, index = retrievalInterface.getFileFromDynamo(
            "wrongFileName", username, tableName
        )
        assert not found

    @mock_aws
    def test_get_file_wrong_username(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"

        with open(fileName, "r") as f:
            fileContent = f.read()

        username = "user1"
        tableName = "seng3011-test-dynamodb"

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        with pytest.raises(Exception):
            retrievalInterface.getFileFromDynamo(stockName, "wrongUsername", tableName)
