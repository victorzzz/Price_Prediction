import datetime as dt
from polygon import RESTClient
import pandas as pd
from os.path import exists
import time
import constants as cnts
import secrets as secrets

today_date = dt.datetime.now().date()
two_years_days = 365*2

def do_step():

    client = RESTClient(api_key=secrets.polygon_api_key_secret)

    def download_stock_events(ticker:str):
        try:
            events_response = client.get_ticker_events(ticker)
            
            file_name = f"{cnts.stock_events_folder}/{ticker}--events.csv"

            print(events_response.name)
            print(events_response.events)
            
            events_df = pd.DataFrame(events_response.events)
            events_df.to_csv(file_name, index=False)

        except Exception as ex:
            print(f"Downloading error {ex}") 

        time.sleep(12)

    for ticker in cnts.get_all_tickers_list():
        download_stock_events(ticker)
