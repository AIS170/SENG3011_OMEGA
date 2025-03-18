import requests
import pandas as pd
import json

url = "http://127.0.0.1:5002/retrieve_analysis"

data = {"stock_name": "AAPL",
        "user_name": "man12"
        }


response = requests.post(url, json=data)

data_dict = response.json()


# Convert dictionary to DataFrame
df = pd.DataFrame.from_dict(data_dict)
print(df.columns)  # Shows all column names


print(df)