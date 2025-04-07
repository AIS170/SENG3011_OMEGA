import pytest
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta, timezone
import boto3
import yfinance as yf
from src.dataCol import (
    search_ticker,
    get_stock_data,
    write_to_client_s3,
    fetch_company_news_df,
    get_stocks_for_news,
    get_latest_news_date_from_s3,
    upload_csv_to_s3,
    CLIENT_BUCKET_NAME1,
    CLIENT_BUCKET_NAME2,
    CLIENT_ROLE_ARN
)


# -------------------- COVERAGE-FOCUSED FUNCTION TESTS --------------------

def test_search_ticker_cases():
    # Valid ticker
    assert search_ticker("apple") == "AAPL"

    # Invalid tickers
    assert search_ticker("nonexistentcompanyxyz") is None
    assert search_ticker("$%^&*") is None
    assert search_ticker("") is None

    # Exception case
    class BadCompany:
        def __str__(self):
            raise Exception("forced failure")

    result = search_ticker(BadCompany())
    assert result is None

def test_write_to_client_s3_cases():
    # Case 1: Valid file and bucket
    with open("upload_test.csv", "w") as f:
        f.write("header,value\nA,1")
    assert write_to_client_s3("upload_test.csv", CLIENT_BUCKET_NAME1) is True
    os.remove("upload_test.csv")

    # Case 2: Non-existent file
    assert write_to_client_s3("missing.csv", CLIENT_BUCKET_NAME1) is False

    # Case 3: Empty file
    with open("empty_upload.csv", "w") as f:
        f.write("")
    assert write_to_client_s3("empty_upload.csv", CLIENT_BUCKET_NAME1) is True
    os.remove("empty_upload.csv")

    # Case 4: Invalid bucket
    with open("bad_bucket.csv", "w") as f:
        f.write("col1,col2\nval1,val2")
    assert write_to_client_s3("bad_bucket.csv", "nonexistent-bucket-12345") is False
    os.remove("bad_bucket.csv")

    # Case 5: Large file
    with open("large_file.csv", "w") as f:
        f.write("col\n")
        for i in range(10000):
            f.write(f"{i}\n")
    assert write_to_client_s3("large_file.csv", CLIENT_BUCKET_NAME1) is True
    os.remove("large_file.csv")

def test_get_stock_data_cases():
    # Valid ticker
    path, data = get_stock_data("AAPL", "apple", "user")
    assert path and os.path.exists(path)
    assert isinstance(data, list)
    os.remove(path)

    # Invalid ticker
    path, data = get_stock_data("INVALIDTICKER", "badco", "user")
    assert path is None and data is None

    # Exception case
    class BadTicker:
        def __str__(self):
            raise Exception("fail Ticker init")

    result = get_stock_data(BadTicker(), "co", "user")
    assert result == (None, None)

def test_fetch_company_news_df_valid():
    # We just call the function and check if we get a DataFrame
    df = fetch_company_news_df("apple")
    print(f"DataFrame check for valid company: {df}")  # Proof print
    assert isinstance(df, pd.DataFrame)

def test_fetch_company_news_df_empty():
    # Checking what happens when we try to fetch news for a non-existing company. It should return an empty DataFrame.
    df_empty = fetch_company_news_df("companythatdoesnotexistxyz")
    print(f"Empty DataFrame for non-existing company: {df_empty}")  # Proof print
    assert isinstance(df_empty, pd.DataFrame)
    assert df_empty.empty

def test_fetch_company_news_df_sentiment_and_timestamp():
    # We get the news for a valid company (apple) and verify if sentiment_score is a float and the timestamp is valid.
    df_check = fetch_company_news_df("apple")
    print(f"Sentiment and timestamp check for apple: {df_check}")  # Proof print
    if not df_check.empty:
        assert df_check["sentiment_score"].dtype == float
        expected_cols = ["company_name", "article_title", "article_content", "source", "url", "published_at", "sentiment_score"]
        for col in expected_cols:
            assert col in df_check.columns
        for val in df_check["published_at"]:
            dt = datetime.fromisoformat(val)
            assert isinstance(dt, datetime)
            assert dt >= datetime.now(dt.tzinfo) - timedelta(days=30)

def test_fetch_company_news_df_old_articles():
    # Simulate news that's older than 30 days and check that it gets skipped.
    class OldNews:
        @property
        def news(self):
            return [{
                "providerPublishTime": (datetime.now() - timedelta(days=31)).timestamp(),  # older than 30 days
                "title": "Old News",
                "content": "Some content",
                "publisher": "Some Source",
                "link": "http://example.com"
            }]
    
    # Test old article should be skipped
    original_ticker = yf.Ticker
    try:
        yf.Ticker = lambda _: OldNews()  # Simulate old news
        df_old = fetch_company_news_df("apple")
        print(f"Old news check (should be empty): {df_old}")  # Proof print
        assert df_old.empty  # It should be skipped, so the DataFrame should be empty
    finally:
    # Restoring the original `yf.Ticker` to ensure subsequent tests use the real Yahoo Finance API.
        yf.Ticker = original_ticker


def test_fetch_company_news_df_recent_articles():
    from datetime import datetime, timedelta
    import yfinance as yf
    import pandas as pd
    from src.dataCol import fetch_company_news_df

    # Simulate recent news and check if it’s included in the DataFrame
    class MockTicker:
        def __init__(self, *_):
            self.news = [{
                "content": {
                    "pubDate": (datetime.now() - timedelta(days=5)).isoformat(),
                    "title": "Recent News",
                    "summary": "Some recent content",
                    "provider": {"displayName": "Some Source"},
                    "canonicalUrl": {"url": "http://example.com"}
                }
            }]
    
    original_ticker = yf.Ticker
    try:
        # Patch yf.Ticker to return our mock object
        yf.Ticker = MockTicker
        df_recent = fetch_company_news_df("apple")
        print(f"Recent news check (should include article): {df_recent}")
        assert not df_recent.empty  # ✅ This should now pass
        assert "article_title" in df_recent.columns
        assert df_recent["article_title"].iloc[0] == "Recent News"
    finally:
        yf.Ticker = original_ticker  # Restore original

def test_fetch_company_news_df_broken_news_parsing():
    # Force error in parsing logic by returning an invalid timestamp
    class BrokenTicker:
        @property
        def news(self):
            return [{"providerPublishTime": "not-a-timestamp"}]  # invalid

    orig_search = search_ticker
    orig_ticker = yf.Ticker
    try:
        globals()['search_ticker'] = lambda _: "BROKEN"
        yf.Ticker = lambda _: BrokenTicker()
        df_broken = fetch_company_news_df("apple")
        print(f"Broken news parsing check: {df_broken}")  # Proof print
        assert isinstance(df_broken, pd.DataFrame)
    finally:
        globals()['search_ticker'] = orig_search
        yf.Ticker = orig_ticker


def test_fetch_company_news_df_exception_handling():
    # Force an exception during the parsing loop to test exception handling
    class BrokenNewsItem:
        @property
        def news(self):
            # This will cause an exception when accessed
            raise ValueError("Forced error during news item parsing")

    original_ticker = yf.Ticker

    try:
        # Simulate the broken news scenario
        yf.Ticker = lambda _: BrokenNewsItem()
        
        # Call the function — it should catch and log the exception
        df = fetch_company_news_df("apple")
        
        # Ensure the result is still a DataFrame, even though an exception was raised
        assert isinstance(df, pd.DataFrame)
        assert df.empty  # Since we simulated the exception, it should be empty
        
    finally:
        # Restore the original Ticker function after the test
        yf.Ticker = original_ticker


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
# The actual test function
def test_get_stocks_for_news_with_real_s3():
    # Create an S3 client using the assumed role credentials
    s3 = create_s3_client()

    # Ensure the bucket exists
    try:
        s3.head_bucket(Bucket=CLIENT_BUCKET_NAME1)
    except s3.exceptions.NoSuchBucket:
        print(f"The bucket {CLIENT_BUCKET_NAME1} does not exist.")
        return

    # Create test files in the S3 bucket
    print("Uploading test files to the S3 bucket...")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="testuser#apple_stock_data.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="testuser#banana_stock_data.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="otheruser#orange_stock_data.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="testuser#grape_stock_data.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME1, Key="otheruser#ignore.csv", Body="dummy data")

    # Now call the function you want to test
    print("Calling get_stocks_for_news('testuser')...")
    result = get_stocks_for_news("testuser")
    
    # Check if the result is a list
    print(f"Result for 'testuser': {result}")
    assert isinstance(result, list)
    assert "apple" in result
    assert "banana" in result
    assert "grape" in result
    assert "orange" not in result  # This should not be included as it's for a different user

    # Test edge cases for other usernames
    print("Testing with empty username...")
    result_empty = get_stocks_for_news("")  # Should return an empty list
    print(f"Result for empty username: {result_empty}")
    assert isinstance(result_empty, list)

    print("Testing with special characters...")
    result_special = get_stocks_for_news("!@#%")  # Should return an empty list
    print(f"Result for special characters: {result_special}")
    assert isinstance(result_special, list)

    print("Testing case-insensitivity...")
    result_case = get_stocks_for_news("TestUser")  # Case-insensitive check
    print(f"Result for case-insensitive username: {result_case}")
    assert isinstance(result_case, list)

    # Clean up the files in the S3 bucket after testing
    print("Cleaning up test files from S3...")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="testuser#apple_stock_data.csv")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="testuser#banana_stock_data.csv")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="testuser#grape_stock_data.csv")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="otheruser#orange_stock_data.csv")
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME1, Key="otheruser#ignore.csv")

    print(f"Test completed. Files created and deleted from the {CLIENT_BUCKET_NAME1} bucket.")
    

def test_get_latest_news_date_from_s3_cases():
    # Create the S3 client using assumed role credentials
    s3 = create_s3_client()

    # Ensure the bucket exists
    try:
        s3.head_bucket(Bucket=CLIENT_BUCKET_NAME2)
    except s3.exceptions.NoSuchBucket:
        print(f"The bucket {CLIENT_BUCKET_NAME2} does not exist.")
        return

    # Clean up old test files to ensure the test is accurate
    print("Cleaning up old test files in the S3 bucket...")
    response = s3.list_objects_v2(Bucket=CLIENT_BUCKET_NAME2, Prefix="apple_")
    for obj in response.get("Contents", []):
        key = obj["Key"]
        print(f"Deleting: {key}")
        s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)

    # Upload test files to S3 with known dates
    print("Uploading test files to the S3 bucket...")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key="apple_2023-01-01_news.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key="apple_2023-05-10_news.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key="apple_2023-04-15_news.csv", Body="dummy data")
    s3.put_object(Bucket=CLIENT_BUCKET_NAME2, Key="banana_2023-01-01_news.csv", Body="dummy data")

    # Call the function to get the latest news date for 'apple'
    print("Calling get_latest_news_date_from_s3('apple')...")
    latest_date = get_latest_news_date_from_s3("apple")
    print(f"Latest date for apple: {latest_date}")

    # Expecting 2023-05-10
    expected_date = datetime(2023, 5, 10, tzinfo=timezone.utc)
    print(f"Expected date: {expected_date}")

    # Remove tz for fair comparison
    assert latest_date.replace(tzinfo=None) == expected_date.replace(tzinfo=None) 
    
def test_upload_csv_to_s3_real():
    # Create dummy DataFrame
    data = {
        "column1": [1, 2, 3],
        "column2": ["A", "B", "C"]
    }
    df = pd.DataFrame(data)

    # Capture the date string BEFORE uploading
    upload_date_str = datetime.now().strftime("%Y-%m-%d")
    key = f"testcompany_{upload_date_str}_news.csv"

    # Upload to S3
    upload_csv_to_s3("testcompany", df)

    # Recreate S3 client
    s3 = create_s3_client()

    # Check if the uploaded file exists
    try:
        print(f"Checking if s3://{CLIENT_BUCKET_NAME2}/{key} exists...")
        s3.head_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)
        print("File upload verified in S3.")
    except s3.exceptions.ClientError as e:
        print(f"File not found in S3: {e}")
        assert False

    obj = s3.get_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)
    content = obj['Body'].read().decode('utf-8')
    print(f"\n S3 File Content:\n{content}")
    assert "column1" in content
    assert "A" in content

    # Clean up after test
    s3.delete_object(Bucket=CLIENT_BUCKET_NAME2, Key=key)
    print("Cleaned up test file.")


