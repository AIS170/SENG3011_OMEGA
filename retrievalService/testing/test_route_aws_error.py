import pytest
from moto import mock_aws
import json

# from ..implementation.RetrievalMicroservice import app


@pytest.mark.filterwarnings(r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning")
class TestInternalError:
    @mock_aws
    def test_broken_registration(self, rootdir, client, s3_mock):
        username = "user1"

        res = client.post("/v1/register/", json={"username": username})
        print(json.loads(res.data))
        assert res.status_code == 500

        assert json.loads(res.data)["InternalError"] is not None

    @mock_aws
    def test_broken_retrieval(self, client, s3_mock):
        username = "user1"
        stockName = "apple"

        res = client.get(f"/v1/retrieve/{username}/{stockName}/")
        print(json.loads(res.data))
        assert res.status_code == 500

        assert json.loads(res.data)["InternalError"] is not None

    @mock_aws
    def test_broken_delete(self, client, s3_mock):
        username = "user1"
        stockName = "apple"

        res = client.delete(f"/v1/delete/{username}/{stockName}/")
        print(json.loads(res.data))
        assert res.status_code == 500

        assert json.loads(res.data)["InternalError"] is not None

    @mock_aws
    def test_broken_list(self, client, s3_mock):
        username = "user1"

        res = client.get(f"/v1/list/{username}/")
        print(json.loads(res.data))
        assert res.status_code == 500
        assert json.loads(res.data)["InternalError"] is not None
    
    @mock_aws
    def test_broken_retrieveV2(self, client, s3_mock):
        username = "user1"
        res = client.get(f"/v2/retrieve/{username}/finance/apple/")
        assert res.status_code == 500
        assert json.loads(res.data)["InternalError"] is not None

