import pytest
from dataCol import search_ticker, get_stock_data, fetch_company_news_df, get_latest_news_date_from_s3, get_stocks_for_news, upload_csv_to_s3
import pandas as pd
import os

def test_search_ticker_success():
    ticker = search_ticker("Apple")
    assert ticker == "AAPL"

def test_get_stock_data():
    path, records = get_stock_data("AAPL", "apple", "unituser")
    assert path is not None and os.path.exists(path)
    assert isinstance(records, list)
    os.remove(path)

def test_fetch_company_news_df():
    df = fetch_company_news_df("apple")
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert "article_title" in df.columns

def test_get_latest_news_date_from_s3():
    latest = get_latest_news_date_from_s3("apple")
    assert latest is None or isinstance(latest, pd.Timestamp)

def test_upload_csv_to_s3():
    df = pd.DataFrame([{
        "company_name": "apple",
        "article_title": "Sample title",
        "article_content": "sample content",
        "source": "source",
        "url": "https://apple.com",
        "published_at": "2024-01-01T00:00:00",
        "sentiment_score": 0.0
    }])
    upload_csv_to_s3("apple", df)  # just test no crash