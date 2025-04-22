# https://pypi.org/project/moto/
# a mock for s3 to let me check that I am using boto3 correctly
import pytest
import os
from moto import mock_aws

# import boto3
from ..implementation.RetrievalInterface import RetrievalInterface


# moto uses depreacted datetime.datetime.utcnow which causes a Deprecation Warning
# Therefore, I am choosing to hide this warning
@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestPullFromS3:
    @mock_aws
    # successfully pull a file
    def test_pull_file(self, s3_mock, rootdir):
        bucket_name = "seng3011-omega-25t1-testing-bucket"
        s3FileName = "user1#apple_stock_data.csv"
        fileName = os.path.join(rootdir, "user1#apple_stock_data.csv")
        with open(fileName) as f:
            fileContent = f.read()

        # Call the function to download and read the file.
        retrievalInterface = RetrievalInterface()
        downloaded_content = retrievalInterface.pull(bucket_name, s3FileName)

        # Assert that the downloaded content matches the original content.
        assert downloaded_content == fileContent

    @mock_aws
    # try to pull a file that does not exist
    def test_pull_nonexistent_file(self, s3_mock):
        # bucket_name = "seng3011-omega-25t1-testing-bucket"

        retrievalInterface = RetrievalInterface()
        with pytest.raises(s3_mock.exceptions.NoSuchKey):
            retrievalInterface.pull(
                "seng3011-omega-25t1-testing-bucket", "non-existent-file"
            )

    @mock_aws
    def test_pull_nonexistent_bucket(self, s3_mock):
        retrievalInterface = RetrievalInterface()

        with pytest.raises(s3_mock.exceptions.NoSuchBucket):
            retrievalInterface.pull("fake-bucket", "non-existent-file")
