import os
import sys
import pytest
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import src.dataCol as dataCol
from src.dataCol import (
    CLIENT_BUCKET_NAME1,
    CLIENT_BUCKET_NAME2,
    CLIENT_BUCKET_NAME3,
    app,
    CLIENT_ROLE_ARN,
)


def create_s3_client():
    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )["Credentials"]
    return boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="ap-southeast-2",
    )


@pytest.fixture(scope="session", autouse=True)
def simulate_registration():
    username = "testuser"
    s3 = create_s3_client()
    profile_key = f"{username}/profile.txt"
    try:
        s3.put_object(
            Bucket=CLIENT_BUCKET_NAME3,
            Key=profile_key,
            Body=f"User: {username}\nCreated at: {datetime.now().isoformat()}",
            ContentType="text/plain",
        )
    except Exception:
        pass
    with open("active_user.txt", "w") as f:
        f.write(username)


@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client


# ------------------ REGISTER ------------------


def test_register_success(client):
    s3 = create_s3_client()
    username = "newtestuser"
    profile_key = f"{username}/profile.txt"
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME3, Key=profile_key)
    except Exception:
        pass
    res = client.post("/register", json={"username": username})
    assert res.status_code in [201, 409]
    data = res.get_json()
    if res.status_code == 201:
        assert "registered and set as current session user" in data["message"]
    else:
        assert "already registered" in data["error"]


def test_register_missing_username(client):
    res = client.post("/register", json={})
    assert res.status_code == 400
    assert "Username is required" in res.get_json()["error"]


def test_register_duplicate_username(client):
    res = client.post("/register", json={"username": "testuser"})
    assert res.status_code == 409
    assert "already registered" in res.get_json()["error"]


def test_register_no_payload(client):
    res = client.post("/register")
    assert res.status_code in [400, 415]


def test_register_nonjson_payload(client):
    res = client.post("/register", data="notjson", content_type="text/plain")
    assert res.status_code in [400, 415]


# ------------------ HOME ------------------


def test_home_route(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "Welcome to the Stock Data API" in res.get_data(as_text=True)


# ------------------ STOCK INFO ------------------


def test_stock_info_missing_company_param(client):
    res = client.get("/stockInfo")
    assert res.status_code == 400


def test_stock_info_invalid_company_name(client):
    res = client.get("/stockInfo?company=zzzzzzzzz")
    assert res.status_code == 404


def test_stock_info_stock_data_none(client, monkeypatch):
    monkeypatch.setattr(dataCol, "get_stock_data", lambda *a, **k: (None, None))
    res = client.get("/stockInfo?company=apple")
    assert res.status_code == 404


def test_stock_info_exception_handling(client, monkeypatch):
    monkeypatch.setattr(
        dataCol,
        "get_stock_data",
        lambda *a, **k: (_ for _ in ()).throw(Exception("boom")),
    )
    res = client.get("/stockInfo?company=apple")
    assert res.status_code == 500


def test_stock_info_real_s3(client):
    res = client.get("/stockInfo?company=apple")
    assert res.status_code == 200
    data = res.get_json()
    assert "ticker" in data
    assert "data" in data
    key = data["file"]
    s3 = create_s3_client()
    meta = s3.head_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)
    assert meta["ResponseMetadata"]["HTTPStatusCode"] == 200
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)


# ------------------ CHECK STOCK ------------------


def test_check_stock_exists_route_real_s3(client):
    file_key = "testuser#apple_stock_data.csv"
    s3 = create_s3_client()
    with open("active_user.txt", "w") as f:
        f.write("testuser")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key, Body="1,2\n3,4")
    res = client.get("/check_stock?company=apple")
    assert res.status_code == 200
    assert res.get_json()["exists"] is True
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key)


def test_check_stock_not_exists_route_real_s3(client):
    with open("active_user.txt", "w") as f:
        f.write("testuser")
    key = "testuser#nonexistent_stock_data.csv"
    s3 = create_s3_client()
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)
    except Exception:
        pass
    res = client.get("/check_stock?company=nonexistent")
    assert res.status_code == 200
    assert res.get_json()["exists"] is False


def test_check_stock_missing_params(client):
    res = client.get("/check_stock")
    assert res.status_code == 400
    res = client.get("/check_stock?company=apple")
    assert res.status_code in [200, 404]


def test_check_stock_unexpected_s3_error(client, monkeypatch):
    class MockS3:
        def head_object(self, *a, **k):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")

    real_boto3_client = boto3.client

    def mock_client(service, *a, **k):
        if service == "s3":
            return MockS3()
        return real_boto3_client(service, *a, **k)

    monkeypatch.setattr(dataCol.boto3, "client", mock_client)

    res = client.get("/check_stock?company=apple")
    assert res.status_code == 500


# ------------------ NEWS ------------------


def test_news_getallCompanyNews_route(client):
    s3 = create_s3_client()
    key = "testuser#testco_stock_data.csv"
    today = datetime.now().strftime("%Y-%m-%d")
    news_key = f"testuser_testco_{today}_news.csv"
    with open("active_user.txt", "w") as f:
        f.write("testuser")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=key, Body="test,data\n1,2")
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    except Exception:
        pass
    res = client.get("/news")
    assert res.status_code == 200
    assert isinstance(res.get_json()["files_added"], int)
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)


def test_news_file_uploaded(client):
    s3 = create_s3_client()
    today = datetime.now().strftime("%Y-%m-%d")
    file_key = "testuser#microsoft_stock_data.csv"
    news_key = f"testuser_microsoft_{today}_news.csv"
    with open("active_user.txt", "w") as f:
        f.write("testuser")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key, Body="dummy,data\n1,2")
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    except Exception:
        pass
    res = client.get("/news")
    assert res.status_code == 200
    assert res.get_json()["files_added"] >= 0
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key)


def test_news_skips_if_recent_exists(client):
    s3 = create_s3_client()
    today = datetime.now().strftime("%Y-%m-%d")
    stock_key = "testuser#tesla_stock_data.csv"
    news_key = f"testuser_tesla_{today}_news.csv"
    with open("active_user.txt", "w") as f:
        f.write("testuser")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=stock_key, Body="1,2")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key, Body="already exists")
    res = client.get("/news")
    assert res.status_code == 200
    assert res.get_json()["files_added"] == 0
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=stock_key)
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)


def test_news_handles_exception_gracefully(client, monkeypatch):
    monkeypatch.setattr(
        dataCol,
        "get_latest_news_date_from_s3",
        lambda *a, **k: (_ for _ in ()).throw(Exception("boom")),
    )
    s3 = create_s3_client()
    file_key = "testuser#fakeco_stock_data.csv"
    with open("active_user.txt", "w") as f:
        f.write("testuser")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key, Body="x,y\n1,2")
    res = client.get("/news")
    assert res.status_code == 200
    assert isinstance(res.get_json()["files_added"], int)
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key)
