from flask import Flask, jsonify, request
import yfinance as yf
import pandas as pd
import requests
import boto3
from botocore.exceptions import ClientError
import re
from datetime import datetime, timedelta, timezone
import io
from dateutil import parser
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from flask_cors import CORS
from gnews import GNews
import pytz


SYDNEY_TZ = pytz.timezone("Australia/Sydney")

app = Flask(__name__)
CORS(app)

CLIENT_ROLE_ARN = "arn:aws:iam::339712883212:role/sharing-s3-bucket"
CLIENT_BUCKET_NAME1 = "seng3011-omega-25t1-testing-bucket"
CLIENT_BUCKET_NAME2 = "seng3011-omega-news-data"
CLIENT_BUCKET_NAME3 = "seng3011-collection-usernames"
ONE_MONTH_AGO = datetime.now(timezone.utc) - timedelta(days=30)
TODAY_STR = datetime.now(timezone.utc).strftime("%Y-%m-%d")
sia = SentimentIntensityAnalyzer()

# removed the current user stuff


class UserAlreadyExists(Exception):
    pass


@app.route("/register", methods=["POST"])
def register_user():
    username = request.args.get("name")
    if not username:
        return jsonify({"error": "Username is required as query param `name`."}), 400

    username = username.strip().lower()
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
            return jsonify(
                {"message": f"User '{username}' registered successfully in S3."}
            ), 201
        else:
            return jsonify({"error": f"S3 access error: {str(e)}"}), 500
    except UserAlreadyExists as ue:
        return jsonify({"error": str(ue)}), 409


def is_registered_user(username):
    username = username.strip().lower()
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
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            return False
        elif code == "AccessDenied":
            # Optional: log or handle this separately
            raise Exception("S3 access denied during user registration check.")
        raise e


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
                    symbol = quote["symbol"]
                    # Strip .MX only, keep all other suffixes (e.g. .KS, .NS, etc.)
                    return symbol.split(".")[0] if symbol.endswith(".MX") else symbol
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
        file_path = f"{name.strip().lower()}#{company.strip().lower()}_stock_data.csv"
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
        company_name = company_name.strip().lower()
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "Please provide your username as `name`."}), 400

        if not is_registered_user(name):
            return jsonify({"error": f"User '{name}' is not registered."}), 403
        name = name.strip().lower()

        stock_ticker = search_ticker(company_name)
        if not stock_ticker:
            return jsonify(
                {"error": f"Could not find a stock ticker for '{company_name}'."}
            ), 404
        file_path, stock_data = get_stock_data(stock_ticker, company_name, name)
        if stock_data is None:
            return jsonify(
                {"error": f"Stock data for '{company_name}' not found or invalid."}
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
    try:
        company_name = request.args.get("company")
        name = request.args.get("name")

        if not company_name:
            return jsonify(
                {"error": "Please provide a company name as `company`."}
            ), 400
        if not name:
            return jsonify({"error": "Please provide your username as `name`."}), 400

        if not is_registered_user(name):
            return jsonify({"error": f"User '{name}' is not registered."}), 403

        company_name = company_name.strip().lower()
        name = name.strip().lower()
        file_path = f"{name}#{company_name}_stock_data.csv"

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

        s3.head_object(Bucket=CLIENT_BUCKET_NAME1, Key=file_path)
        return jsonify(
            {"exists": True, "message": "Stock data exists.", "file": file_path}
        )

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return jsonify(
                {"exists": False, "message": "No stock data found.", "file": file_path}
            )
        return jsonify({"error": f"Error checking S3: {str(e)}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_stocks_for_news(username):
    name = username
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
    name = request.args.get("name")
    if not name:
        return jsonify({"error": "Please provide your username as `name`."}), 400

    if not is_registered_user(name):
        return jsonify({"error": f"User '{name}' is not registered."}), 403
    name = name.strip().lower()

    companies = get_stocks_for_news(name)
    files_added = 0

    for company in companies:
        try:
            latest = get_latest_news_date_from_s3(company.strip().lower(), name)
            if not latest or latest < ONE_MONTH_AGO:
                df = fetch_company_news_df(company.strip().lower())
                if not df.empty:
                    upload_csv_to_s3(name, company.strip().lower(), df)
                    files_added += 1
        except Exception as e:
            print(f"Error with {company.strip().lower()}: {e}")

    return jsonify({"status": "complete", "files_added": files_added}), 200


gn = GNews(language="en", max_results=100)


@app.route("/sportsNews", methods=["GET"])
def get_sports_news():
    try:
        news_items = gn.get_news("nba")
        stories = []
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=48)

        for item in news_items:
            try:
                pub_time = item["published date"]
                if isinstance(pub_time, datetime):
                    pub_time = pub_time.astimezone(timezone.utc)
                else:
                    pub_time = datetime.strptime(
                        pub_time, "%a, %d %b %Y %H:%M:%S %Z"
                    ).replace(tzinfo=timezone.utc)

                if pub_time >= cutoff_time:
                    stories.append(
                        {
                            "title": item["title"],
                            "link": item["url"],
                            "published": pub_time,  # still in UTC here
                        }
                    )
            except Exception:
                continue

        # Sort stories by time
        stories.sort(key=lambda x: x["published"], reverse=True)

        # Convert to NSW time for display
        for s in stories:
            s["published"] = s["published"].astimezone(SYDNEY_TZ).isoformat()

        return jsonify(
            {"status": "success", "article_count": len(stories), "articles": stories}
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
