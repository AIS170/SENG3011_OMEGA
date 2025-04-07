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


app = Flask(__name__)


CLIENT_ROLE_ARN = "arn:aws:iam::339712883212:role/sharing-s3-bucket"
CLIENT_BUCKET_NAME1 = "seng3011-omega-25t1-testing-bucket"
CLIENT_BUCKET_NAME2 = "seng3011-omega-news-data"
ONE_MONTH_AGO = datetime.now(timezone.utc) - timedelta(days=30)
TODAY_STR = datetime.now(timezone.utc).strftime("%Y-%m-%d")
#ik we not in utc but we not really displaying this info for time rather date so should be fine for little discrepancies
#but i can fix this later

def write_to_client_s3(filename, bucketname):
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN,
        RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object['Credentials']
    s3 = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    try:
        s3.upload_file(filename, bucketname , filename)
        var = s3.get_object(Bucket=bucketname, Key=filename)
        object_content = var['Body'].read().decode('utf-8')
        print(object_content)
        return True
    except Exception as e:
        print(f"Error writing to S3: {e}")
        return False
    
    
def search_ticker(company_name):
    """
    Searches for a stock ticker based on a company name using Yahoo Finance's search API.
    """
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={company_name}"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if "quotes" in data and len(data["quotes"]) > 0:
                for quote in data["quotes"]:
                    if "symbol" in quote and "isYahooFinance" in quote and quote["isYahooFinance"]:
                        return quote["symbol"] 
        
        return None

    except Exception as e:
        return None
    
def get_stock_data(stock_ticker, company, name, period="1mo"):
    try:
        stock = yf.Ticker(stock_ticker)
        hist = stock.history(period=period)

        if hist.empty:
            return None, None

        hist = hist[["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]]
        hist.reset_index(inplace=True)
        hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")

        if hist.empty:
            return None, None  # Double check after formatting

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
        name = request.args.get("name")

        if not company_name:
            return jsonify({"error": "Please provide a company name."}), 400

        company_name = company_name.strip().lower()
        if name:
            name = name.strip().lower()

        stock_ticker = search_ticker(company_name)

        if not stock_ticker:
            return jsonify({"error": f"Could not find a stock ticker for '{company_name}'."}), 404

        file_path, stock_data = get_stock_data(stock_ticker,company_name, name)

        if stock_data is None:
            return jsonify({"error": f"Stock data for '{stock_ticker}' not found or invalid."}), 404

        return jsonify({
            "message": "Stock data retrieved successfully",
            "ticker": stock_ticker,
            "file": file_path,
            "data": stock_data
        })
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500

@app.route("/check_stock")
def check_stock():
    company_name = request.args.get("company")
    name = request.args.get("name")

    if not company_name or not name:
        return jsonify({"error": "Please provide both 'company' and 'name'."}), 400

    company_name = company_name.strip().lower()
    name = name.strip().lower()

    # Construct file path (same as in `get_stock_data`)
    file_path = f"{name}#{company_name}_stock_data.csv"

    # Assume Role for S3 access
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN,
        RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object['Credentials']
    s3 = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    try:
        # Check if file exists in S3 bucket
        s3.head_object(Bucket='seng3011-omega-25t1-testing-bucket', Key= f"{name}#{company_name}_stock_data.csv")
        return jsonify({
            "exists": True,
            "message": f"Stock data for {name} for '{company_name}' exists in S3.",
            "file": file_path
        })
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return jsonify({
                "exists": False,
                "message": f"No stock data found for '{company_name}' in S3.",
                "file": file_path
            })
        else:
            return jsonify({"error": f"Error checking S3: {e}"}), 500
        
def get_stocks_for_news(username):
    name = username.strip().lower()
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN,
        RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object['Credentials']
    s3 = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=CLIENT_BUCKET_NAME1)

    companies = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            filename = key.split("/")[-1]  # Just in case folders exist later

            if filename.startswith(f"{name}#") and filename.endswith("_stock_data.csv"):
                company = filename.split("#")[1].replace("_stock_data.csv", "")
                companies.append(company)
    
    print(companies)
    return companies

#getting the latest data cause we might have a lot of news data for a stock but we need to get the latest one
#why not delete the outdated ones but what if we design a more better model that works with data for a year

def get_latest_news_date_from_s3(company_name):
    prefix = f"{company_name}_"
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN,
        RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object['Credentials']
    s3 = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name='ap-southeast-2'  # Sydney region
    )
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=CLIENT_BUCKET_NAME2, Prefix=prefix)

    latest_date = None  # Start with no latest date, so the first valid date found will be used
    pattern = re.compile(f"{company_name}_(\\d{{4}}-\\d{{2}}-\\d{{2}})_news\\.csv")

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key'].split('/')[-1]
            match = pattern.match(key)
            if match:
                file_date_str = match.group(1)
                
                try:
                    # Parse the date from the filename and ensure it is timezone-aware (UTC)
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)  # Ensure timezone-aware date

                    # Debugging: print the current file's name and date
                    print(f"Processing file: {key} with date: {file_date}")

                    # Only set latest_date if it's the first date found or a newer date
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date

                except ValueError as e:
                    print(f"Error parsing date for file {key}: {e}")

    print(f"Latest date found: {latest_date}")  # Debug print to track the latest date
    return latest_date

def fetch_company_news_df(company_name):
    ticker = search_ticker(company_name)
    
    if not ticker:
        return pd.DataFrame()  # Return empty DataFrame if no ticker is found

    print(f"Ticker for {company_name} : {ticker}")
    
    ticker_obj = yf.Ticker(ticker)
    records = []

    try:
        raw_news = ticker_obj.news
        print(f"Found {len(raw_news)} articles for {ticker}")

        for item in raw_news:
            try:
                # Use dateutil to parse the string-based pubDate safely
                pub_date_str = item.get("content", {}).get("pubDate")
                if not pub_date_str:
                    continue

                pub_time = parser.isoparse(pub_date_str).astimezone(timezone.utc)
                if pub_time < ONE_MONTH_AGO:
                    continue  # Skip old articles

                records.append({
                    "company_name": company_name,
                    "article_title": item.get("content", {}).get("title", ""),
                    "article_content": item.get("content", {}).get("summary", ""),
                    "source": item.get("content", {}).get("provider", {}).get("displayName", ""),
                    "url": item.get("content", {}).get("canonicalUrl", {}).get("url", ""),
                    "published_at": pub_time.isoformat(),
                    "sentiment_score": 0.0  # Placeholder
                })
            except Exception as e:
                print(f"Error parsing news item: {e}")
                continue
    except Exception as e:
        print(f"Error fetching news for {company_name}: {e}")
    
    df = pd.DataFrame(records)
    print(df)
    return df

#upload it to s3 and ensure in csv format so did the buffering of the df which is records dataframe like an excel or sql table
#and with this to_csv is very easy as already in tabular format
# upload it to s3 and ensure in csv format so did the buffering of the df which is records dataframe like an excel or sql table
# and with this to_csv is very easy as already in tabular format
def upload_csv_to_s3(company_name, df, date_str=None):
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    key = f"{company_name}_{date_str}_news.csv"
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=CLIENT_ROLE_ARN,
        RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object['Credentials']
    s3 = boto3.client(
        's3',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken']
    )
    s3.put_object(
        Bucket=CLIENT_BUCKET_NAME2,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f"Uploaded: s3://{CLIENT_BUCKET_NAME2}/{key}")



@app.route("/news")
def getallCompanyNews():
    name = request.args.get("name")
    
    if not name:
        return jsonify({"error": "Please provide 'name'."}), 400
    
    name = name.strip().lower()
    
    companies = get_stocks_for_news(name)
    files_added = 0

    for company in companies:
        try:
            latest_date = get_latest_news_date_from_s3(company)
            if not latest_date or latest_date < ONE_MONTH_AGO:
                print(f"Fetching news for {company}...")
                df = fetch_company_news_df(company)
                
                if not df.empty:
                    upload_csv_to_s3(company, df)
                    files_added += 1
                else:
                    print(f"No recent news for {company}")
            else:
                print(f"Skipping {company} — recent file already exists.")
        except Exception as e:
            print(f"Error with {company}: {e}")

    return jsonify({
        "status": "complete",
        "files_added": files_added
    }), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)


