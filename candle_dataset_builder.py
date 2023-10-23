from math import isnan
import constants as cnts
import pandas as pd
import multiprocessing
import simple_price_analizer as spa
import numpy as np
from typing import Iterable
from typing import Dict
from typing import Tuple

def add_fields(tiker:str, df:pd.DataFrame):
    df['symbol'] = tiker

def read_merged_csv_files(tickers:Iterable[str]):

    data_frames_result = {}

    for tiker in tickers:
        
        minute_multiplier = 1
        
        file_name = f"{cnts.merged_data_folder}/{tiker}--price-candle--{minute_multiplier}--minute.csv"
        df = pd.read_csv(f"{file_name}")
        key = (tiker, minute_multiplier, "minute")
        add_fields(tiker, df)
        df = df.copy()
        data_frames_result[key] = df

    return data_frames_result

def prepare_dataset(in_symbol:str, in_data_frames:Dict[Tuple[str, int, str], pd.DataFrame]):

    print(f"Preparing dataset {in_symbol} ...")

    multiplier = 1
    time_interval = "minute"

    key = (in_symbol, multiplier, time_interval)
    df:pd.DataFrame = in_data_frames[key]

    number_of_rows = df.shape[0]

    print(f"{in_symbol}: Numbers of rows {number_of_rows}")

    prices = df['vwap'].values
    days = df['day_of_week'].values

    inc, dec = spa.calculateDayPriceChangeWithTrailingStopLoss(prices, days, cnts.loss_levels / 100.0)

    for level_index, level in enumerate(cnts.loss_levels):
        strLevel = str(level).replace('.', '_')

        df[f'up_{strLevel}'] = np.round(inc[level_index] * 100.0, 6)
        df[f'down_{strLevel}'] = np.round(dec[level_index] * 100.0, 6)

    df = df.copy()  

    data_set_file_name = f"{cnts.data_sets_folder}/{in_symbol}.csv"
    print(f"{in_symbol}: Saving file {data_set_file_name}")

    df.to_csv(data_set_file_name, index=False)

# ----------------------------

def build_dataset_for_ticker(tiker:str):
    tiker_data_frames = read_merged_csv_files((tiker,))
    prepare_dataset(tiker, tiker_data_frames)

# ----------------------------

def do_step():
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(14):
        print("-------------------------------------")
        print(f"Date tradig dataset. Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        for tiker in tikers_batch:
            process = multiprocessing.Process(target=build_dataset_for_ticker, args=(tiker,))
            processes.append(process)
            process.start()

        # Wait for all processes to finish
        print(f"Waiting for '{', '.join(tikers_batch)}' ...")
        for process in processes:
            process.join()

# ----------------------------

if __name__ == "__main__":
    do_step()


