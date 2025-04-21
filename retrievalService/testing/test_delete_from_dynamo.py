import pytest
import os
from moto import mock_aws
from botocore.exceptions import ClientError
from ..implementation.RetrievalInterface import RetrievalInterface


@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestDeleteFromDynamo:
    @mock_aws
    def test_delete_file(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        username = "user1"
        tableName = "seng3011-test-dynamodb"
        with open(fileName, "r") as f:
            fileContent = f.read()

        retrievalInterface = RetrievalInterface()

        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        retrievedFiles = (
            test_table.get_item(TableName=tableName, Key={"username": {"S": username}})
            .get("Item")
            .get("retrievedFiles")
            .get("L")
        )

        # retrievedFiles = test_table.get_item(Key={'username': username}).get('Item').get('retrievedFiles')
        assert len(retrievedFiles) == 1

        response = retrievalInterface.deleteFromDynamo(stockName, username, tableName)
        assert response is True

        # retrievedFiles = test_table.get_item(Key={'username': username}).get('Item').get('retrievedFiles')
        retrievedFiles = (
            test_table.get_item(TableName=tableName, Key={"username": {"S": username}})
            .get("Item")
            .get("retrievedFiles")
            .get("L")
        )
        assert len(retrievedFiles) == 0

    @mock_aws
    def test_delete_non_existent_file(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        username = "user1"
        tableName = "seng3011-test-dynamodb"
        with open(fileName, "r") as f:
            fileContent = f.read()

        retrievalInterface = RetrievalInterface()

        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)
        retrievedFiles = (
            test_table.get_item(TableName=tableName, Key={"username": {"S": username}})
            .get("Item")
            .get("retrievedFiles")
            .get("L")
        )

        assert len(retrievedFiles) == 1

        with pytest.raises(FileNotFoundError):
            retrievalInterface.deleteFromDynamo("fakeFile", username, tableName)

    @mock_aws
    def test_delete_from_non_existent_table(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        username = "user1"
        tableName = "seng3011-test-dynamodb"
        with open(fileName, "r") as f:
            fileContent = f.read()

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        with pytest.raises(ClientError) as errorInfo:
            retrievalInterface.deleteFromDynamo(stockName, username, "nonExistentTable")
        assert errorInfo.value.response["Error"]["Code"] == "ResourceNotFoundException"

    @mock_aws
    def test_delete_user_not_file_owner(self, test_table_two_users, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        username = "user1"
        username2 = "user2"
        tableName = "seng3011-test-dynamodb"
        with open(fileName, "r") as f:
            fileContent = f.read()

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        with pytest.raises(FileNotFoundError):
            retrievalInterface.deleteFromDynamo(stockName, username2, tableName)

    @mock_aws
    def test_invalid_double_delete(self, test_table, rootdir):
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        stockName = "apple"
        username = "user1"
        tableName = "seng3011-test-dynamodb"
        with open(fileName, "r") as f:
            fileContent = f.read()

        retrievalInterface = RetrievalInterface()
        retrievalInterface.pushToDynamo(stockName, fileContent, username, tableName)

        response = retrievalInterface.deleteFromDynamo(stockName, username, tableName)
        assert response is True

        with pytest.raises(FileNotFoundError):
            retrievalInterface.deleteFromDynamo(stockName, username, tableName)
