import os
import sys
import json
import pytest
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# Setup import path and app
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import src.dataCol as dataCol
from src.dataCol import (
    CLIENT_BUCKET_NAME1,
    CLIENT_BUCKET_NAME2,
    CLIENT_BUCKET_NAME3,
    CLIENT_ROLE_ARN,
    app
)

def create_s3_client():
    sts = boto3.client('sts')
    creds = sts.assume_role(RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1")['Credentials']
    return boto3.client(
        's3',
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
        region_name="ap-southeast-2"
    )

@pytest.fixture(autouse=True)
def clear_active_user():
    if os.path.exists("active_user.txt"):
        os.remove("active_user.txt")

@pytest.fixture
def client():
    app.testing = True
    return app.test_client()

# ----------------- Register Tests ------------------

def test_register_success(client):
    username = "newtestuser"
    s3 = create_s3_client()
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME3, Key=f"{username}/profile.txt")
    except:
        pass
    res = client.post("/register", json={"username": username})
    assert res.status_code in [201, 409]

def test_register_missing_username(client):
    res = client.post("/register", json={})
    assert res.status_code == 400

def test_register_duplicate_username(client):
    client.post("/register", json={"username": "testuser"})
    res = client.post("/register", json={"username": "testuser"})
    assert res.status_code == 409

def test_register_no_payload(client):
    res = client.post("/register")
    assert res.status_code in [400, 415]

def test_register_nonjson_payload(client):
    res = client.post("/register", data="bad", content_type="text/plain")
    assert res.status_code in [400, 415]

# ----------------- Home ------------------

def test_home_route(client):
    res = client.get("/")
    assert res.status_code == 200

# ----------------- StockInfo ------------------

def test_stock_info_missing_company_param(client):
    client.post("/register", json={"username": "testuser"})
    res = client.get("/stockInfo")
    assert res.status_code == 400

def test_stock_info_invalid_company_name(client):
    client.post("/register", json={"username": "testuser"})
    res = client.get("/stockInfo?company=zzzzzzzzz")
    assert res.status_code == 404

def test_stock_info_stock_data_none(client, monkeypatch):
    monkeypatch.setattr(dataCol, "get_stock_data", lambda *a, **k: (None, None))
    client.post("/register", json={"username": "testuser"})
    res = client.get("/stockInfo?company=apple")
    assert res.status_code == 404

def test_stock_info_exception_handling(client, monkeypatch):
    def boom(*a, **k): raise Exception("boom")
    monkeypatch.setattr(dataCol, "get_stock_data", boom)
    client.post("/register", json={"username": "testuser"})
    res = client.get("/stockInfo?company=apple")
    assert res.status_code == 500

def test_stock_info_route_real_s3(client):
    client.post("/register", json={"username": "testuser"})
    res = client.get("/stockInfo?company=apple")
    assert res.status_code == 200
    key = res.get_json()["file"]
    s3 = create_s3_client()
    meta = s3.head_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)
    assert meta["ResponseMetadata"]["HTTPStatusCode"] == 200

# ----------------- Check Stock ------------------

def test_check_stock_exists(client):
    client.post("/register", json={"username": "testuser"})
    key = "testuser#apple_stock_data.csv"
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=key, Body="1,2\n3,4")
    res = client.get("/check_stock?company=apple")
    assert res.status_code == 200
    assert res.get_json()["exists"] is True
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)

def test_check_stock_not_exists(client):
    client.post("/register", json={"username": "testuser"})
    key = "testuser#nonexistent_stock_data.csv"
    s3 = create_s3_client()
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=key)
    except:
        pass
    res = client.get("/check_stock?company=nonexistent")
    assert res.status_code == 200
    assert res.get_json()["exists"] is False

def test_check_stock_missing_params(client):
    client.post("/register", json={"username": "testuser"})
    res = client.get("/check_stock")
    assert res.status_code == 400

def test_check_stock_no_user(client):
    res = client.get("/check_stock?company=apple")
    assert res.status_code == 403

def test_check_stock_s3_error(client, monkeypatch):
    class BrokenS3:
        def head_object(self, *a, **k):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
    def broken_client(service, *a, **k):
        if service == "s3":
            return BrokenS3()
        return boto3.client(service, *a, **k)
    monkeypatch.setattr(dataCol.boto3, "client", broken_client)
    client.post("/register", json={"username": "testuser"})
    res = client.get("/check_stock?company=apple")
    assert res.status_code == 500

# ----------------- News ------------------

def test_getallCompanyNews_route(client):
    client.post("/register", json={"username": "e2etestuser"})
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="e2etestuser#testco_stock_data.csv", Body="test,data\n1,2")
    today = datetime.now().strftime("%Y-%m-%d")
    news_key = f"e2etestuser_testco_{today}_news.csv"
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    except:
        pass
    res = client.get("/news")
    assert res.status_code == 200
    assert "files_added" in res.get_json()

def test_news_file_uploaded(client):
    client.post("/register", json={"username": "newsuser"})
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="newsuser#microsoft_stock_data.csv", Body="dummy,data\n1,2")
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"newsuser_microsoft_{today}_news.csv"
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)
    except:
        pass
    res = client.get("/news")
    assert res.status_code == 200
    assert res.get_json()["files_added"] >= 0
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="newsuser#microsoft_stock_data.csv")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)

def test_news_skips_if_recent_exists(client):
    client.post("/register", json={"username": "skipuser"})
    today = datetime.now().strftime("%Y-%m-%d")
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="skipuser#tesla_stock_data.csv", Body="1,2")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key=f"skipuser_tesla_{today}_news.csv", Body="dummy")
    res = client.get("/news")
    assert res.status_code == 200
    assert res.get_json()["files_added"] == 0

def test_news_handles_exception_gracefully(client, monkeypatch):
    client.post("/register", json={"username": "erruser"})
    monkeypatch.setattr(dataCol, "get_latest_news_date_from_s3", lambda *a, **k: (_ for _ in ()).throw(Exception("boom")))
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="erruser#fakeco_stock_data.csv", Body="x,y\n1,2")
    res = client.get("/news")
    assert res.status_code == 200
    assert isinstance(res.get_json()["files_added"], int)

def test_news_missing_user(client):
    res = client.get("/news")
    assert res.status_code == 403
