from flask import Flask, jsonify, request
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from flask_cors import CORS
from gnews import GNews
import yfinance as yf
import pandas as pd
import requests
import boto3
from botocore.exceptions import ClientError
import re
from datetime import datetime, timedelta, timezone
import io
from dateutil import parser
import os
import nltk

nltk.download("vader_lexicon")

# meeee bombbbaclarttttt


app = Flask(__name__)
CORS(app)

CLIENT_ROLE_ARN = "arn:aws:iam::339712883212:role/sharing-s3-bucket"
CLIENT_BUCKET_NAME1 = "seng3011-omega-25t1-testing-bucket"
CLIENT_BUCKET_NAME2 = "seng3011-omega-news-data"
CLIENT_BUCKET_NAME3 = "seng3011-collection-usernames"
ONE_MONTH_AGO = datetime.now(timezone.utc) - timedelta(days=30)
TODAY_STR = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ACTIVE_USER_FILE = "active_user.txt"
sia = SentimentIntensityAnalyzer()


def set_current_user(username):
    with open(ACTIVE_USER_FILE, "w") as f:
        f.write(username.strip().lower())


def get_current_user():
    if os.path.exists(ACTIVE_USER_FILE):
        with open(ACTIVE_USER_FILE, "r") as f:
            return f.read().strip()
    return None


class UserAlreadyExists(Exception):
    pass


@app.route("/register", methods=["POST"])
def register_user():
    data = request.get_json()
    if not data or "username" not in data:
        return jsonify({"error": "Username is required."}), 400

    username = data["username"].strip().lower()
    profile_key = f"{username}/profile.txt"

    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    try:
        s3.head_object(Bucket=CLIENT_BUCKET_NAME3, Key=profile_key)
        raise UserAlreadyExists(f"User '{username}' is already registered.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            s3.put_object(
                Bucket=CLIENT_BUCKET_NAME3,
                Key=profile_key,
                Body=f"User: {username}\nCreated at: {datetime.now(timezone.utc).isoformat()}",
                ContentType="text/plain",
            )
            set_current_user(username)
            return jsonify(
                {
                    "message": f"User '{username}' registered and set as current session user."
                }
            ), 201
        else:
            return jsonify({"error": f"S3 access error: {str(e)}"}), 500
    except UserAlreadyExists as ue:
        return jsonify({"error": str(ue)}), 409


def write_to_client_s3(filename, bucketname):
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    try:
        s3.upload_file(filename, bucketname, filename)
        return True
    except Exception as e:
        print(f"Error writing to S3: {e}")
        return False


def search_ticker(company_name):
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={company_name}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for quote in data.get("quotes", []):
                if quote.get("isYahooFinance") and "symbol" in quote:
                    return quote["symbol"]
        return None
    except Exception:
        return None


def get_stock_data(stock_ticker, company, name, period="1mo"):
    try:
        stock = yf.Ticker(stock_ticker)
        hist = stock.history(period=period)
        if hist.empty:
            return None, None
        hist = hist[
            ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]
        ]
        hist.reset_index(inplace=True)
        hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")
        file_path = f"{name}#{company}_stock_data.csv"
        hist.to_csv(file_path, index=False)
        write_to_client_s3(file_path, CLIENT_BUCKET_NAME1)
        return file_path, hist.to_dict(orient="records")
    except Exception as e:
        print(f"ERROR in get_stock_data: {e}")
        return None, None


@app.route("/")
def home():
    return "Welcome to the Stock Data API! Use /stockInfo?company=COMPANY_NAME to fetch stock details."


@app.route("/stockInfo")
def stock_info():
    try:
        company_name = request.args.get("company")
        if not company_name:
            return jsonify({"error": "Please provide a company name."}), 400
        name = get_current_user()
        if not name:
            return jsonify(
                {"error": "No active user found. Please register first."}
            ), 403
        stock_ticker = search_ticker(company_name.strip().lower())
        if not stock_ticker:
            return jsonify(
                {"error": f"Could not find a stock ticker for '{company_name}'."}
            ), 404
        file_path, stock_data = get_stock_data(stock_ticker, company_name, name)
        if stock_data is None:
            return jsonify(
                {"error": f"Stock data for '{stock_ticker}' not found or invalid."}
            ), 404
        return jsonify(
            {
                "message": "Stock data retrieved successfully",
                "ticker": stock_ticker,
                "file": file_path,
                "data": stock_data,
            }
        )
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


@app.route("/check_stock")
def check_stock():
    company_name = request.args.get("company")
    name = get_current_user()
    if not company_name:
        return jsonify({"error": "Please provide a company name."}), 400
    if not name:
        return jsonify({"error": "No active user found. Please register first."}), 403
    file_path = f"{name}#{company_name.strip().lower()}_stock_data.csv"
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    try:
        s3.head_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_path)
        return jsonify(
            {"exists": True, "message": "Stock data exists.", "file": file_path}
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return jsonify(
                {"exists": False, "message": "No stock data found.", "file": file_path}
            )
        return jsonify({"error": f"Error checking S3: {e}"}), 500


def get_stocks_for_news(username):
    name = username.strip().lower()
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )
    creds = assumed_role["Credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=CLIENT_BUCKET_NAME1)

    companies = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            if filename.startswith(f"{name}#") and filename.endswith("_stock_data.csv"):
                company = filename.split("#")[1].replace("_stock_data.csv", "")
                companies.append(company)
    return companies


def get_latest_news_date_from_s3(company_name, username):
    prefix = f"{username}_{company_name}_"
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )
    creds = assumed_role["Credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="ap-southeast-2",
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=CLIENT_BUCKET_NAME2, Prefix=prefix)

    latest_date = None
    pattern = re.compile(
        f"{username}_{company_name}_(\\d{{4}}-\\d{{2}}-\\d{{2}})_news\\.csv"
    )

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"].split("/")[-1]
            match = pattern.match(key)
            if match:
                date_str = match.group(1)
                try:
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                except ValueError:
                    continue
    return latest_date


def fetch_company_news_df(company_name):
    ticker = search_ticker(company_name)
    if not ticker:
        return pd.DataFrame()

    ticker_obj = yf.Ticker(ticker)
    records = []
    try:
        raw_news = ticker_obj.news
        for item in raw_news:
            try:
                content = item.get("content", {})
                pub_date_str = content.get("pubDate")
                if not pub_date_str:
                    continue
                pub_time = parser.isoparse(pub_date_str).astimezone(timezone.utc)
                if pub_time < ONE_MONTH_AGO:
                    continue
                title = content.get("title", "")
                summary = content.get("summary", "")
                combined_text = f"{title}. {summary}"
                sentiment = sia.polarity_scores(combined_text)["compound"]

                records.append(
                    {
                        "company_name": company_name,
                        "article_title": title,
                        "url": content.get("canonicalUrl", {}).get("url", ""),
                        "published_at": pub_time.isoformat(),
                        "sentiment_score": sentiment,
                    }
                )
            except Exception:
                continue
    except Exception:
        pass
    return pd.DataFrame(records)


def upload_csv_to_s3(username, company_name, df, date_str=None):
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    key = f"{username}_{company_name}_{date_str}_news.csv"
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    sts_client = boto3.client("sts")
    assumed = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN, RoleSessionName="AssumeRoleSession1"
    )
    creds = assumed["Credentials"]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )

    s3.put_object(
        Bucket=CLIENT_BUCKET_NAME2,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )


@app.route("/news")
def getallCompanyNews():
    name = get_current_user()
    if not name:
        return jsonify({"error": "No active user found. Please register first."}), 403

    companies = get_stocks_for_news(name)
    files_added = 0

    for company in companies:
        try:
            latest = get_latest_news_date_from_s3(company, name)
            if not latest or latest < ONE_MONTH_AGO:
                df = fetch_company_news_df(company)
                if not df.empty:
                    upload_csv_to_s3(name, company, df)
                    files_added += 1
        except Exception as e:
            print(f"Error with {company}: {e}")

    return jsonify({"status": "complete", "files_added": files_added}), 200


gn = GNews(language="en", max_results=100)


@app.route("/sportsNews", methods=["GET"])
def get_sports_news():
    try:
        # Search for latest sports news
        news_items = gn.get_news("sports")
        stories = []
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=48)

        for item in news_items:
            try:
                # Parse and standardize the publication time
                pub_time = item["published date"]
                if isinstance(pub_time, datetime):
                    pub_time = pub_time.astimezone(timezone.utc)
                else:
                    pub_time = datetime.strptime(
                        pub_time, "%a, %d %b %Y %H:%M:%S %Z"
                    ).replace(tzinfo=timezone.utc)

                # Filter by time window
                if pub_time >= cutoff_time:
                    stories.append(
                        {
                            "title": item["title"],
                            "link": item["url"],
                            "published": pub_time.isoformat(),
                        }
                    )
            except Exception:
                continue  # Skip any bad entries

        return jsonify(
            {"status": "success", "article_count": len(stories), "articles": stories}
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
