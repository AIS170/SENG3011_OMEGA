import pytest
from moto import mock_aws

import json

# from ..implementation.RetrievalMicroservice import app


@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestRetrieveRoute:
    @mock_aws
    def test_retrieve(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"
        username = "user1"

        # pull for the first time - need to look at s3
        res = client.get(f"/v1/retrieve/{username}/{stockName}/")

        assert res.status_code == 200
        assert json.loads(res.data)["stock_name"] == stockName

        # pull again - no need to look at s3 this time
        res = client.get(f"/v1/retrieve/{username}/{stockName}/")
        assert res.status_code == 200
        assert json.loads(res.data)["stock_name"] == stockName

    @mock_aws
    def test_uncollected_stock(self, rootdir, client, s3_mock, test_table):
        username = "user1"

        res = client.get(f"/v1/retrieve/{username}/fakestock/")
        assert res.status_code == 400
        assert json.loads(res.data)["StockNotFound"] is not None

    @mock_aws
    def test_invalid_username(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"

        res = client.get(f"/v1/retrieve/fakeUser/{stockName}/")
        assert res.status_code == 401
        assert json.loads(res.data)["UserNotFound"] is not None
