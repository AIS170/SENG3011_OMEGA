import pytest
import requests
import time
import boto3
from datetime import datetime

#takes 12 seconds to pass

CLIENT_ROLE_ARN = "arn:aws:iam::339712883212:role/sharing-s3-bucket"
CLIENT_BUCKET_NAME1 = "seng3011-omega-25t1-testing-bucket"
CLIENT_BUCKET_NAME2 = "seng3011-omega-news-data"
BASE_URL = "http://localhost:5001"

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

@pytest.mark.integration
def test_contract_end_to_end():
    name = "contractuser"
    company = "honda"

    # Step 1: Call /stockInfo to upload data
    print("[1] Requesting /stockInfo to fetch and upload stock data")
    r1 = requests.get(f"{BASE_URL}/stockInfo", params={"company": company, "name": name})
    assert r1.status_code == 200
    file_key = r1.json()["file"]
    assert file_key.startswith(f"{name}#{company}")
    print("✓ Stock info file created:", file_key)

    # Step 2: Verify stock file exists
    print("[2] Verifying stock file exists via /check_stock")
    r2 = requests.get(f"{BASE_URL}/check_stock", params={"company": company, "name": name})
    assert r2.status_code == 200
    assert r2.json()["exists"] is True
    print("✓ Stock file found in S3")

    # Step 3: Trigger news fetch
    print("[3] Calling /news to fetch latest news")
    r3 = requests.get(f"{BASE_URL}/news", params={"name": name})
    assert r3.status_code == 200
    files_added = r3.json()["files_added"]
    assert isinstance(files_added, int)
    print(f"✓ News fetch completed, {files_added} files added")

    # Step 4: Confirm news file exists in S3
    print("[4] Verifying news file in S3")
    s3 = create_s3_client()
    today = datetime.now().strftime("%Y-%m-%d")
    news_key = f"{company.lower()}_{today}_news.csv"
    head = s3.head_object(Bucket=CLIENT_BUCKET_NAME2, Key=news_key)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    print("✓ News file found in S3:", news_key)

    print("\n Contract test passed: All systems integrated and functioning.")
