import requests
import pandas as pd
import json

url = "http://127.0.0.1:5002/analyze"
data = {
    "stock_name": "AAPL",
    "data": [
            {"Date": "2023-01-01", "Close": 145.32},
            {"Date": "2023-01-02", "Close": 146.50},
            {"Date": "2023-01-03", "Close": 147.20},
            {"Date": "2023-01-04", "Close": 144.80},
            {"Date": "2023-01-05", "Close": 143.75},
            {"Date": "2023-01-06", "Close": 145.10},
            {"Date": "2023-01-07", "Close": 146.95},
            {"Date": "2023-01-08", "Close": 148.00},
            {"Date": "2023-01-09", "Close": 149.20},
            {"Date": "2023-01-10", "Close": 150.30}
        ]
    ,
    "years": 5,
    "forecast_days": 30,
    "sell_threshold": 0.02,
    "buy_threshold": -0.02,
    "user_name": "man12"
}



response = requests.post(url, json=data)

data_dict = json.loads(response.json())

# Convert dictionary to DataFrame
df = pd.DataFrame.from_dict(data_dict)

print(df)