import datetime as dt
from polygon import RESTClient
import pandas as pd
from os.path import exists
import time
import constants as cnts
import secrets as secrets

from_date = dt.datetime.now().date()

fb_from_date = dt.datetime(2022, 6, 8).date()

two_years_days = 365*2

meta_to_day = dt.datetime(2022, 6, 9).date()

def download_stock_bars(date, max_days_history, client, ticker, multiplier, timespan, limit_date = None, save_as = None):
    
    minute_timespan_iteration_time_delta = dt.timedelta(days=45*multiplier)
    day_timespan_iteration_time_delta = dt.timedelta(days=40000*multiplier)

    iteration_time_delta = minute_timespan_iteration_time_delta if (timespan == "minute") else day_timespan_iteration_time_delta

    if (limit_date is None):
        maximum_time_delta = dt.timedelta(days=max_days_history)
        limit_date = date - maximum_time_delta

    # read previously downloaded and merged data
    merged_data_file_name = f"{cnts.merged_data_folder}/{ticker}--price-candle--{multiplier}--{timespan}.csv"

    if exists(merged_data_file_name):

        merged_df = pd.read_csv(merged_data_file_name)

        last_record = merged_df.tail(1)
        las_time_stamp = last_record['timestamp'].values[0]

        last_date = cnts.nyse_msec_timestamp_to_date_time(las_time_stamp).date()

        limit_date = max(limit_date, last_date)

    while date > limit_date:
        date_from = date - iteration_time_delta
        if (date_from < limit_date):
            date_from = limit_date

        date_from_str = date_from.strftime('%Y-%m-%d')

        date_to = date
        date_to_str = date_to.strftime('%Y-%m-%d')

        tiker_to_save = ticker if (save_as is None) else save_as

        file_name = f"{cnts.data_folder}/{tiker_to_save}--price-candle--{multiplier}--{timespan}--{date_from_str}--{date_to_str}.csv"
        if exists(file_name):
            print(f"File {file_name} exists")
        else:
            print(f"Call {ticker}--price-candle--{multiplier}--{timespan}--{date_from_str}--{date_to_str}")

            try:
                bars = client.get_aggs(
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=date_from_str,
                    to=date_to_str,
                    limit=50000)
                
                df = pd.DataFrame(bars)
                df.to_csv(file_name, index=False)
                print(f"Saving file {file_name}. {len(bars)}") # type: ignore

            except Exception as ex:
                print(f"Downloading error {ex}") 

            print(" ")
            time.sleep(12)
        
        date = date - iteration_time_delta - dt.timedelta(days=1)

def do_step():
    client = RESTClient(api_key=secrets.polygon_api_key_secret)

    # all except META
    for ticker in cnts.get_all_tickers_list():
        if (ticker == "META"):
            continue

        for multiplier in cnts.minute_multipliers:
            download_stock_bars(from_date, two_years_days, client, ticker, multiplier, "minute")
        for multiplier in cnts.day_multipliers:
            download_stock_bars(from_date, two_years_days, client, ticker, multiplier, "day")

    # META / FB

    two_years_from_today = dt.datetime.now().date() - dt.timedelta(days=two_years_days)

    for ticker in ["FB"]:
        for multiplier in cnts.minute_multipliers:
            download_stock_bars(fb_from_date, two_years_days, client, ticker, multiplier, "minute", two_years_from_today, "META")
        for multiplier in cnts.day_multipliers:
            download_stock_bars(fb_from_date, two_years_days, client, ticker, multiplier, "day", two_years_from_today, "META")

    for ticker in ["META"]:
        for multiplier in cnts.minute_multipliers:
            download_stock_bars(from_date, two_years_days, client, ticker, multiplier, "minute", meta_to_day)
        for multiplier in cnts.day_multipliers:
            download_stock_bars(from_date, two_years_days, client, ticker, multiplier, "day", meta_to_day)


