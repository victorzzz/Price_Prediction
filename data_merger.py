import multiprocessing
import constants as cnts
import pandas as pd
from os.path import exists
import numpy as np
from typing import List

def merge_csv_files(
        tickers:List[str],
        ignoreAlreadyMerged:bool = False,
        data_folder:str = cnts.data_folder,
        merged_data_folder:str = cnts.merged_data_folder):

    data_frames = {}

    # read and concat all the files
    for file_name in cnts.iterate_files_name_only(data_folder):

        file_name_parts = file_name.split("--")
        symbol = file_name_parts[0]

        if (not symbol in tickers):
            continue

        multiplier = int(file_name_parts[2])
        time_interval = file_name_parts[3]

        if (ignoreAlreadyMerged):
            merged_file_name = f"{merged_data_folder}/{symbol}--price-candle--{multiplier}--{time_interval}.csv"
            if exists(merged_file_name):
                print(f"Merged file exists {file_name} exists. Ignoring ...")
                continue

        print(f"Reading {file_name} ...")
        df = pd.read_csv(f"{data_folder}/{file_name}")

        if (time_interval == "minute"):
            df['normalized_trading_time'] = df['timestamp'].apply(cnts.nyse_msec_timestamp_to_float_with_round_6)
            df = df[(df['normalized_trading_time'] >= 0.0) & (df['normalized_trading_time'] <= 1.0)]

        df['transactions'].fillna(0.0, inplace=True)
        df['vwap'].fillna( np.round((df['open'] + df['close']) / 2.0, 6), inplace=True)

        df['normalized_week'] = df['timestamp'].apply(cnts.nyse_msec_timestamp_to_normalized_week_with_round_3)
        df['normalized_day_of_week'] = df['timestamp'].apply(cnts.nyse_msec_timestamp_to_normalized_day_of_week_with_round_2)
        df['day_of_week'] = df['timestamp'].apply(cnts.nyse_msec_timestamp_to_day_of_week)

        df = df.copy()

        target_df = None

        print(f"Processing {file_name} ...")
        key = (symbol, multiplier, time_interval)
        if (key in data_frames):
            target_df = data_frames[key]
            target_df = pd.concat([target_df, df], axis=0)
        else:
            target_df = df

        data_frames[key] = target_df

    #save merged
    for key, df_dict_value in data_frames.items():

        df:pd.DataFrame = df_dict_value

        df.sort_values(by='timestamp', inplace=True)
        df.drop_duplicates(inplace=True)

        symbol, multiplier, time_interval = key
        file_name = f"{merged_data_folder}/{symbol}--price-candle--{multiplier}--{time_interval}.csv" 

        print(f"Saving {file_name} ...")
        df.to_csv(file_name, index=False)   

# ----------------------------

def do_step():
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(cnts.complex_processing_batch_size):
        print("-------------------------------------")
        print(f"Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        process = multiprocessing.Process(target=merge_csv_files, args=(tikers_batch,))
        processes.append(process)
        process.start()

    # Wait for all processes to finish
    print(f"Waiting for '{', '.join(tikers_batch)}' ...")
    for process in processes:
        process.join()

# ----------------------------

if __name__ == "__main__":
    do_step()

    

