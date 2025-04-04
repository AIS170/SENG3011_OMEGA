import os
import sys
import json
import pytest
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

# Ensure src is in the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import src.dataCol as dataCol  # Use module alias
from src.dataCol import (
    CLIENT_BUCKET_NAME1,
    CLIENT_BUCKET_NAME2,
    app,
    CLIENT_ROLE_ARN,
)

def create_s3_client():
    sts_client = boto3.client('sts')
    assumed = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN,
        RoleSessionName="AssumeRoleSession1"
    )
    creds = assumed['Credentials']
    return boto3.client(
        's3',
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
        region_name="ap-southeast-2"
    )

@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client

def test_home_route(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "Welcome to the Stock Data API" in res.get_data(as_text=True)

def test_stock_info_missing_company_param(client):
    res = client.get("/stockInfo")
    assert res.status_code == 400
    assert "Please provide a company name." in res.get_json()["error"]

def test_stock_info_invalid_company_name(client):
    res = client.get("/stockInfo?company=zzzzzzzzz&name=testuser")
    assert res.status_code == 404
    assert "Could not find a stock ticker" in res.get_json()["error"]

def test_stock_info_stock_data_none(client, monkeypatch):
    def fake_get_stock_data(*args, **kwargs):
        return None, None

    monkeypatch.setattr(dataCol, "get_stock_data", fake_get_stock_data)

    res = client.get("/stockInfo?company=apple&name=anyuser")
    assert res.status_code == 404
    assert "not found or invalid" in res.get_json()["error"]

def test_stock_info_exception_handling(client, monkeypatch):
    def boom(*args, **kwargs):
        raise Exception("boom")

    monkeypatch.setattr(dataCol, "get_stock_data", boom)

    res = client.get("/stockInfo?company=apple&name=testuser")
    assert res.status_code == 500
    assert "Unexpected error" in res.get_json()["error"]

def test_stock_info_route_real_s3(client):
    res = client.get("/stockInfo?company=apple&name=testuser")
    assert res.status_code == 200
    json_data = res.get_json()
    assert "ticker" in json_data
    assert "file" in json_data
    assert "data" in json_data

    s3 = create_s3_client()
    file_key = json_data["file"]
    response = s3.head_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

def test_check_stock_exists_route_real_s3(client):
    file_key = "testuser#apple_stock_data.csv"
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key, Body="column1,column2\n1,A\n2,B")

    res = client.get("/check_stock?company=apple&name=testuser")
    assert res.status_code == 200
    assert res.get_json()["exists"] is True

    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key)

def test_check_stock_not_exists_route_real_s3(client):
    file_key = "testuser#nonexistentcompany_stock_data.csv"
    s3 = create_s3_client()
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_key)
    except:
        pass

    res = client.get("/check_stock?company=nonexistentcompany&name=testuser")
    assert res.status_code == 200
    assert res.get_json()["exists"] is False

def test_check_stock_missing_params(client):
    assert client.get("/check_stock").status_code == 400
    assert client.get("/check_stock?company=apple").status_code == 400
    assert client.get("/check_stock?name=testuser").status_code == 400

def test_check_stock_unexpected_s3_error(client, monkeypatch):
    def mock_head_object(*args, **kwargs):
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "Access denied"}}, "HeadObject")

    class MockS3:
        def head_object(self, *args, **kwargs):
            return mock_head_object()

    # Save the original boto3.client before monkeypatching
    original_boto3_client = boto3.client

    def mock_boto3_client(service, *args, **kwargs):
        if service == "s3":
            return MockS3()
        return original_boto3_client(service, *args, **kwargs)

    monkeypatch.setattr(dataCol.boto3, "client", mock_boto3_client)

    res = client.get("/check_stock?company=apple&name=testuser")
    assert res.status_code == 500
    assert "Error checking S3" in res.get_json()["error"]


def test_getallCompanyNews_route(client):
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="e2etestuser#testco_stock_data.csv", Body="test,data\n1,2")

    today = datetime.now().strftime("%Y-%m-%d")
    news_key = f"testco_{today}_news.csv"
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    except:
        pass

    response = client.get("/news?name=e2etestuser")
    assert response.status_code == 200
    assert "files_added" in response.get_json()

    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    except:
        pass

def test_news_missing_name_param(client):
    res = client.get("/news")
    assert res.status_code == 400
    assert "Please provide 'name'." in res.get_json()["error"]

def test_news_file_uploaded(client):
    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="newsuser#microsoft_stock_data.csv", Body="dummy,data\n1,2")

    today = datetime.now().strftime("%Y-%m-%d")
    key = f"microsoft_{today}_news.csv"
    try:
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)
    except:
        pass

    res = client.get("/news?name=newsuser")
    assert res.status_code == 200
    assert res.get_json()["files_added"] >= 1

    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="newsuser#microsoft_stock_data.csv")

def test_news_skips_if_recent_exists(client):
    s3 = create_s3_client()
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"tesla_{today}_news.csv"
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="skipuser#tesla_stock_data.csv", Body="1,2")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key=key, Body="dummy content")

    res = client.get("/news?name=skipuser")
    assert res.status_code == 200
    assert res.get_json()["files_added"] == 0

    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="skipuser#tesla_stock_data.csv")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)

def test_news_handles_exception_gracefully(client, monkeypatch):
    def broken(*args, **kwargs):
        raise Exception("oops")

    monkeypatch.setattr(dataCol, "get_latest_news_date_from_s3", broken)

    s3 = create_s3_client()
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="erruser#fakeco_stock_data.csv", Body="x,y\n1,2")

    res = client.get("/news?name=erruser")
    assert res.status_code == 200
    assert isinstance(res.get_json()["files_added"], int)

    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="erruser#fakeco_stock_data.csv")
