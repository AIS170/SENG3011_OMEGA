import pytest
import json
from moto import mock_aws


@pytest.mark.filterwarnings(r"ignore:datetime.datetime.utcnow\(\) is deprecated:DeprecationWarning")
class TestNewsRetrieval:
    @mock_aws
    def test_retrieve_news(self, rootdir, client, s3_mock, s3_news_table, test_table):
        username = "user1"
        stockname1 = "honda"
        date = "2025-04-09"
        res = client.get(f"/v2/retrieve/{username}/news/{stockname1}/", query_string={"date": date})
        assert res.status_code == 200
        stockname2 = "apple"
        res = client.get(f"/v2/retrieve/{username}/finance/{stockname2}/")

        assert res.status_code == 200

        res = client.get(f"/v1/list/{username}/")

        assert json.loads(res.data)["Success"] == ["news_honda", "finance_apple"]
