import pytest
from moto import mock_aws
import json

# from ..implementation.RetrievalMicroservice import app


@pytest.mark.filterwarnings(r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning")
class TestDeleteRoute:
    @mock_aws
    def test_delete(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"
        username = "user1"

        client.get(f"/v1/retrieve/{username}/{stockName}/")
        res = client.delete(f"/v1/delete/{username}/{stockName}/")

        assert res.status_code == 200

        res = client.delete(f"/v1/delete/{username}/{stockName}/")
        assert res.status_code == 400
        assert json.loads(res.data)["FileNotFound"] is not None

    @mock_aws
    def test_invalid_username(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"
        username = "user1"

        client.get(f"/v1/retrieve/{username}/{stockName}/")
        res = client.delete(f"/v1/delete/wrong-user/{stockName}/")

        assert res.status_code == 401
        assert json.loads(res.data)["UserNotFound"] is not None
