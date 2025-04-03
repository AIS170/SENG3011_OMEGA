import pytest
from moto import mock_aws
import json


@pytest.mark.filterwarnings(r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning")
class TestListRoute:
    @mock_aws
    def test_list(self, rootdir, client, s3_mock, test_table):
        stockName = "apple"
        username = "user1"

        res = client.get(f"/v1/list/{username}/")
        assert res.status_code == 200
        assert len(json.loads(res.data)["Success"]) == 0

        client.get(f"/v1/retrieve/{username}/{stockName}/")

        res = client.get(f"/v1/list/{username}/")
        assert res.status_code == 200
        assert len(json.loads(res.data)["Success"]) == 1

        client.delete(f"/v1/delete/{username}/{stockName}/")

        res = client.get(f"/v1/list/{username}/")
        assert res.status_code == 200
        assert len(json.loads(res.data)["Success"]) == 0

    @mock_aws
    def test_wrong_username(self, rootdir, client, s3_mock, test_table):
        res = client.get("/v1/list/wrongusername/")
        assert res.status_code == 401
        assert json.loads(res.data)["UserNotFound"] is not None
