# https://pypi.org/project/moto/
# a mock for s3 to let me check that I am using boto3 correctly
import pytest
import os
from moto import mock_aws
import boto3

from ..implementation.RetrievalInterface import RetrievalInterface


# moto uses depreacted datetime.datetime.utcnow which causes a Deprecation Warning
# Therefore, I am choosing to hide this warning
@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestDeleteFromS3:
    # successfully delete a file
    @mock_aws
    def test_delete_file(self, rootdir):
        bucketName = "seng3011-omega-25t1-testing-bucket"
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        s3FileName = "user1#apple_stock_data.csv"
        # stockName = "apple"

        with open(fileName, "r") as f:
            fileContent = f.read()

        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=bucketName,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(
            Bucket=bucketName, Key=s3FileName, Body=fileContent.encode("utf-8")
        )

        # Call the function to download and read the file.
        retrievalInterface = RetrievalInterface()
        result = retrievalInterface.deleteOne(bucketName, s3FileName)
        assert result is True

        with pytest.raises(s3.exceptions.NoSuchKey):
            retrievalInterface.pull(bucketName, fileName)

    @mock_aws
    def test_delete_non_existent_file(self, rootdir):
        bucketName = "seng3011-omega-25t1-testing-bucket"
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        s3FileName = "user1#apple_stock_data.csv"
        # stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=bucketName,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(
            Bucket=bucketName, Key=s3FileName, Body=fileContent.encode("utf-8")
        )

        retrievalInterface = RetrievalInterface()
        result = retrievalInterface.deleteOne(bucketName, s3FileName)

        # even though the file never existed, boto3 does not throw an error
        assert result is True

    @mock_aws
    def test_delete_non_existent_bucket(self, rootdir):
        bucketName = "seng3011-omega-25t1-testing-bucket"
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        s3FileName = "user1#apple_stock_data.csv"
        # stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=bucketName,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(
            Bucket=bucketName, Key=s3FileName, Body=fileContent.encode("utf-8")
        )
        retrievalInterface = RetrievalInterface()
        with pytest.raises(s3.exceptions.NoSuchBucket):
            retrievalInterface.deleteOne("non-existent-bucket", s3FileName)

    @mock_aws
    def test_double_delete(self, rootdir):
        bucketName = "seng3011-omega-25t1-testing-bucket"
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        s3FileName = "user1#apple_stock_data.csv"
        # stockName = "apple"
        with open(fileName, "r") as f:
            fileContent = f.read()

        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=bucketName,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"},
        )

        s3.put_object(
            Bucket=bucketName, Key=s3FileName, Body=fileContent.encode("utf-8")
        )
        retrievalInterface = RetrievalInterface()

        retrievalInterface.deleteOne(bucketName, s3FileName)

        with pytest.raises(Exception):
            retrievalInterface.deleteOne("non-existent-bucket", s3FileName)
