import requests
import pandas as pd
from datetime import datetime
import time

def price_df_from_response(res):
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()
    result = res.json()
    df = df.append({
         'status': result['status'],
         'date': result['from'],
         'open': result['open'],
         'high': result['low'],
         'close': result['close'],
         'volume': result['volume'],
         'afterHours': result['afterHours'],
         'preMarket': result['preMarket']}, ignore_index=True)
    return df

            
key = "FkJ0c2CuFs7HDQoJTSBaPXJdh_Mc90tU"
prices = pd.DataFrame()

for month in ["01","02","03","04","05","06"]:
    if month in ["01","03","05",]:
        last = 31
    if month == "02":
        last = 28
    else:
        last = 30
    for day in range(1,last+1):
        time.sleep(60)
        if day < 10:
            day_str = "0" + str(day)
        else:
            day_str = str(day)
    
        res = requests.get(f"https://api.polygon.io/v1/open-close/GME/2021-{month}-{day_str}?adjusted=true&apiKey={key}")
        
        
        if res.status_code == 404:
            # market was closed
            continue
        
        else: 
            temp_df = price_df_from_response(res)
            print(f"finished {temp_df['date']}")
            prices = prices.append(temp_df)

prices.to_csv(r"C:\Users\Daniel\Documents\NLP Class\Project\gme_prices.csv")