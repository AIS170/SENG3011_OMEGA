import pytest
from moto import mock_aws
from pprint import pprint
import json

# from ..implementation.RetrievalMicroservice import app


@pytest.mark.filterwarnings(r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning")
class TestRetrieveRoute:
    @mock_aws
    def test_retrieve(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"
        username = "user1"

        # pull for the first time - need to look at s3
        res = client.get(f"/v2/retrieve/{username}/finance/{stockName}/")

        assert res.status_code == 200
        assert json.loads(res.data)["stock_name"] == stockName

        # pull again - no need to look at s3 this time
        res = client.get(f"/v2/retrieve/{username}/finance/{stockName}/")
        assert res.status_code == 200
        assert json.loads(res.data)["stock_name"] == stockName

    @mock_aws
    def test_uncollected_stock(self, rootdir, client, s3_mock, test_table):
        username = "user1"

        res = client.get(f"/v2/retrieve/{username}/finance/fakestock/")
        pprint(res.data)
        assert res.status_code == 400
        assert json.loads(res.data)["StockNotFound"] is not None

    @mock_aws
    def test_invalid_data_type(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"
        username = "user1"

        res = client.get(f"/v2/retrieve/{username}/fake_data_type/{stockName}/")
        assert res.status_code == 400
        assert json.loads(res.data)["InvalidDataKey"] is not None

    @mock_aws
    def test_invalid_username(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"

        res = client.get(f"/v2/retrieve/fakeUser/finance/{stockName}/")
        assert res.status_code == 401
        assert json.loads(res.data)["UserNotFound"] is not None
