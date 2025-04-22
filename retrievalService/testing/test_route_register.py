import pytest
from moto import mock_aws
import json


@pytest.mark.filterwarnings(
    r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning"
)
class TestRegisterRoute:
    @mock_aws
    def test_successful_register(self, rootdir, client, test_table):
        username = "user2"

        res = client.post("/v1/register/", json={"username": username})
        assert res.status_code == 200
        assert json.loads(res.data)["Success"] is not None

    @mock_aws
    def test_duplicate_username(self, rootdir, client, test_table):
        username = "user1"

        res = client.post("/v1/register/", json={"username": username})
        assert res.status_code == 401
        assert json.loads(res.data)["UserTakenError"] is not None
