import pytest
import boto3
from moto import mock_aws
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../implementation"))
)
from ..implementation.RetrievalMicroservice import app as flask_app  # noqa: E402


@pytest.fixture
def rootdir():
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="function")
def s3_mock(rootdir):
    bucket_name = "seng3011-omega-25t1-testing-bucket"
    fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
    with open(fileName) as f:
        fileContent = f.read()

    with mock_aws():
        # Create a mock S3 bucket and upload a test file.
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(
            Bucket=bucket_name,
            Key="user1#apple_stock_data.csv",
            Body=fileContent.encode("utf-8"),
        )

        yield s3


@pytest.fixture(scope="function")
def s3_news_table(rootdir):
    bucket_name = "seng3011-omega-news-data"
    fileName = os.path.join(rootdir, "user1_honda_2025-04-09_news.csv")
    with open(fileName) as f:
        fileContent = f.read()

    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )
        s3.put_object(
            Bucket=bucket_name,
            Key="user1_honda_2025-04-09_news.csv",
            Body=fileContent.encode("utf-8"),
        )

        yield s3


@pytest.fixture(scope="function")
def dynamodb_mock():
    """Sets up a mock DynamoDB table before each test using mock_aws."""
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="ap-southeast-2")
        yield dynamodb


@pytest.fixture(scope="function")
def test_table(dynamodb_mock):
    """Creates a test DynamoDB table before each test."""

    username = "user1"

    with mock_aws():
        tableName = "seng3011-test-dynamodb"

        dynamodb_mock.create_table(
            TableName=tableName,
            KeySchema=[{"AttributeName": "username", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "username", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        item = {
            "username": {"S": username},
            "analysis": {"L": []},
            "retrievedFiles": {"L": []},
        }

        dynamodb_mock.put_item(TableName=tableName, Item=item)

        yield dynamodb_mock

        dynamodb_mock.delete_table(TableName=tableName)


# database with two users
@pytest.fixture(scope="function")
def test_table_two_users(dynamodb_mock):
    """Creates a test DynamoDB table before each test."""

    username = "user1"
    username2 = "user2"

    with mock_aws():
        tableName = "seng3011-test-dynamodb"

        dynamodb_mock.create_table(
            TableName=tableName,
            KeySchema=[{"AttributeName": "username", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "username", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        item = {
            "username": {"S": username},
            "analysis": {"L": []},
            "retrievedFiles": {"L": []},
        }

        item2 = {
            "username": {"S": username2},
            "analysis": {"L": []},
            "retrievedFiles": {"L": []},
        }

        dynamodb_mock.put_item(TableName=tableName, Item=item)

        dynamodb_mock.put_item(TableName=tableName, Item=item2)

        yield dynamodb_mock
        dynamodb_mock.delete_table(TableName=tableName)


@pytest.fixture
def app():
    yield flask_app


@pytest.fixture
def client(app):
    return app.test_client()
