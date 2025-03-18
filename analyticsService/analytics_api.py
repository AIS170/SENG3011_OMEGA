from flask import Flask, request, jsonify
import pandas as pd
from prophet import Prophet
import requests
import numpy as np
import json

#plotly








import boto3
from decimal import Decimal

# Initialize the DynamoDB resource
dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-2")

# Reference your existing table (replace with your table name)
TABLE_NAME = "StockAnalytics"
table = dynamodb.Table(TABLE_NAME)




def save_stock_data_to_dynamodb(user_name, stock_symbol, forecast_df):
    """
    Save stock analysis data to DynamoDB.
    """
    for _, row in forecast_df.iterrows():
        item = {
            "user_name": user_name,
            "stock_symbol#date": f"{stock_symbol}#{row['ds'].strftime('%Y-%m-%d')}",  # Composite Key
            "stock_symbol": stock_symbol,
            "date": row["ds"].strftime("%Y-%m-%d"),
            "yhat": Decimal(str(row["yhat"])),
            "yhat_lower": Decimal(str(row["yhat_lower"])),
            "yhat_upper": Decimal(str(row["yhat_upper"])),
            "Rolling_Max": Decimal(str(row["Rolling_Max"])),
            "Rolling_Min": Decimal(str(row["Rolling_Min"])),
            "Sell_Signal": bool(row["Sell_Signal"]),
            "Buy_Signal": bool(row["Buy_Signal"]),
            "Price_Change": Decimal(str(row["Price_Change"])),
        }

        # Insert item into DynamoDB
        table.put_item(Item=item)
    
    print(f"Stock data for {stock_symbol} saved successfully in DynamoDB.")









#Functions 



def preprocess_data_prophet(data, years=5):
    # Convert the 'data' dictionary list to a Pandas DataFrame
    df = pd.DataFrame(data)

    # Ensure 'Date' is converted to datetime format
    df['Date'] = pd.to_datetime(df['Date'])

    # Rename columns for Prophet model
    df = df[['Date', 'Close']].rename(columns={'Date': 'ds', 'Close': 'y'})

    # Remove timezone information (if applicable)
    df['ds'] = df['ds'].dt.tz_localize(None)

    # Filter data for the last 'years' years
    cutoff_date = df['ds'].max() - pd.DateOffset(years=years)
    df = df[df['ds'] >= cutoff_date]

    return df






def analyze_stock(df, forecast_days=30, sell_threshold=0.02, buy_threshold=-0.02):
    """
    Train Prophet model on stock data and provide buy/sell recommendations.
    """
    model = Prophet(daily_seasonality=True)
    model.fit(df)
    
    future = model.make_future_dataframe(periods=forecast_days)
    forecast = model.predict(future)

    # Extract relevant columns
    forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    
    # Identify buy/sell signals
    forecast_df['Rolling_Max'] = forecast_df['yhat'].rolling(window=5).max()
    forecast_df['Rolling_Min'] = forecast_df['yhat'].rolling(window=5).min()
    
    forecast_df['Sell_Signal'] = (forecast_df['yhat'] >= forecast_df['Rolling_Max'].shift(1))
    forecast_df['Buy_Signal'] = (forecast_df['yhat'] <= forecast_df['Rolling_Min'].shift(1))

    forecast_df['Price_Change'] = forecast_df['yhat'].pct_change()
    forecast_df['Sell_Signal'] |= forecast_df['Price_Change'] > sell_threshold
    forecast_df['Buy_Signal'] |= forecast_df['Price_Change'] < buy_threshold

    forecast_df = forecast_df.fillna(0)  # Replace NaN with 0


    return forecast_df,model











def send_results_to_server(callback_url, stock_name, forecast_df, user_name):
    """
    Sends the forecasted stock analysis back to the originating server.
    """
    print(forecast_df)
    
    try:
        data_to_send = {
            "user_name": user_name,
            "stock_name": stock_name,
            "forecast_data": json.loads(forecast_df.to_json(orient="records"))  # Convert DataFrame to list of dictionaries
        }
        
        
        response = requests.post(callback_url, json=data_to_send,timeout=10)
        

        if response.status_code == 200:
            
            
            return {"message": "Data successfully sent", "server_response": response.json()}
        else:
            
            return {"error": f"Failed to send data. Status code: {response.status_code}", "server_response": response.text}
    
    except Exception as e:
        return {"error": str(e)}














#API




app = Flask(__name__)
@app.route("/analyze", methods=["POST"])
def analyze():

    try:

        request_data = request.get_json()
        
        if not request_data:
            return jsonify({"error": "No data received"}), 400

        

        stock_name = request_data.get("stock_name")
        stock_data = request_data.get("data")  
        years = request_data.get("years", 5)  
        forecast_days = request_data.get("forecast_days", 30)  
        sell_threshold = request_data.get("sell_threshold", 0.02)  
        buy_threshold = request_data.get("buy_threshold", -0.02)

        user_name = request_data.get("user_name")
        
        

        df = preprocess_data_prophet(stock_data,years)

        
        df_a,model_a = analyze_stock(df,forecast_days,sell_threshold,buy_threshold)
        

        save_stock_data_to_dynamodb(user_name, stock_name, df_a)

        #callback_url = request_data.get("callback_url")  # URL to send results back
        #send_result = send_results_to_server(callback_url, stock_name, df_a,user_name)


        
       
        return jsonify(df_a.to_json())


        
    
    except Exception as e:
        print({"123 error": str(e)})
        return jsonify({"error": str(e)}), 500
        






@app.route("/retrieve_analysis", methods=["POST"])
def retrieve_analysis():

    try:

        request_details = request.get_json()

        user_name = request_details.get("user_name")
        stock_name = request_details.get("stock_name")

        

        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('user_name').eq(user_name) & 
                                  boto3.dynamodb.conditions.Key('stock_symbol#date').begins_with(stock_name)
        )


        

        if 'Items' in response and response['Items']:
            print("hello")
            return jsonify(response['Items'])  #return jsonify(df_a.to_json())
        else:
            return jsonify({"message": "No data found for the given user and stock."}), 404








    except Exception as e:
        print({"123 error": str(e)})
        return jsonify({"error": str(e)}), 500




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)