from typing import List
from typing import Dict
from typing import Tuple

import constants as cnts
import pandas as pd
import multiprocessing
import sys
import matplotlib.pyplot as plt

def add_long_term_fields(tiker:str, df:pd.DataFrame):
    df['symbol'] = tiker

    for key, value in cnts.long_term_prediction_time_frames.items():
        multiplier, interval = key
        for distance in value:
            df[f'max_up_{distance}_{multiplier}_{interval}'] = cnts.float_nan
            df[f'min_dpwn_{distance}_{multiplier}_{interval}'] = cnts.float_nan
            df[f'perc_max_up_{distance}_{multiplier}_{interval}'] = cnts.float_nan
            df[f'perc_min_down_{distance}_{multiplier}_{interval}'] = cnts.float_nan

            df[f'buy_{distance}_{multiplier}_{interval}'] = 0
            df[f'sell_{distance}_{multiplier}_{interval}'] = 0

def read_merged_csv_files(tickers:List[str]):

    data_frames_result = {}

    for tiker in tickers:
        
        for minute_multiplier in cnts.minute_multipliers:
            file_name = f"{cnts.merged_data_folder}/{tiker}--price-candle--{minute_multiplier}--minute.csv"
            df = pd.read_csv(f"{file_name}")
            key = (tiker, minute_multiplier, "minute")

            if (minute_multiplier in (10,30)):
                add_long_term_fields(tiker, df)
                df = df.copy()
            
            data_frames_result[key] = df
        
        for day_multiplier in cnts.day_multipliers:
            file_name = f"{cnts.merged_data_folder}/{tiker}--price-candle--{day_multiplier}--day.csv"
            df = pd.read_csv(f"{file_name}")
            key = (tiker, minute_multiplier, "day")
            add_long_term_fields(tiker, df)
            df = df.copy()
            data_frames_result[key] = df

    return data_frames_result

def prepare_long_term_dataset(in_symbol:str, in_data_frames:Dict[Tuple[str, int, str], pd.DataFrame]):

    print(f"Preparing long term dataset {in_symbol}")

    for key, value in cnts.long_term_prediction_time_frames.items():
        multiplier, interval = key
        key = (in_symbol, multiplier, interval)
        df = in_data_frames[key]

        for distance in value:
            for i in range(len(df)):
                if i + distance < len(df):
                    current_vwap = df.loc[i, 'vwap']
                    seq =  df.loc[i+1:i+distance, 'vwap']
                    max_for_distance = seq.max()
                    min_for_distance = seq.min()
                    df.at[i, f'max_up_{distance}_{multiplier}_{interval}'] = max_for_distance
                    df.at[i, f'min_down_{distance}_{multiplier}_{interval}'] = min_for_distance
                    df.at[i, f'perc_max_up_{distance}_{multiplier}_{interval}'] = round((max_for_distance - current_vwap) / current_vwap, 6)
                    df.at[i, f'perc_min_down_{distance}_{multiplier}_{interval}'] = round((current_vwap - min_for_distance) / current_vwap, 6)

        # ---- 3 
        df.loc[
            (df[f'perc_max_up_{value[0]}_{multiplier}_{interval}'] > cnts.long_term_buy_thresholds[0])
            &
            (df[f'perc_min_down_{value[0]}_{multiplier}_{interval}'] < cnts.long_term_buy_thresholds[0] / 2)
            , f'buy_{value[0]}_{multiplier}_{interval}'] = 1

        df.loc[
            (df[f'perc_min_down_{value[0]}_{multiplier}_{interval}'] > cnts.long_term_buy_thresholds[0])
            &
            (df[f'perc_max_up_{value[0]}_{multiplier}_{interval}'] < cnts.long_term_buy_thresholds[0] / 2)
            , f'sell_{value[0]}_{multiplier}_{interval}'] = 1

        # ---- 5 
        df.loc[
            (df[f'perc_max_up_{value[1]}_{multiplier}_{interval}'] > cnts.long_term_buy_thresholds[1])
            &
            (df[f'perc_min_down_{value[1]}_{multiplier}_{interval}'] < cnts.long_term_buy_thresholds[1] / 2)          
            , f'buy_{value[1]}_{multiplier}_{interval}'] = 1

        df.loc[
            (df[f'perc_min_down_{value[1]}_{multiplier}_{interval}'] > cnts.long_term_buy_thresholds[1])
            &
            (df[f'perc_max_up_{value[1]}_{multiplier}_{interval}'] < cnts.long_term_buy_thresholds[1] / 2)           
            , f'sell_{value[1]}_{multiplier}_{interval}'] = 1
        
        # ---- 7
        df.loc[
            (df[f'perc_max_up_{value[2]}_{multiplier}_{interval}'] > cnts.long_term_buy_thresholds[2])
            &
            (df[f'perc_min_down_{value[2]}_{multiplier}_{interval}'] < cnts.long_term_buy_thresholds[2] / 2)          
            , f'buy_{value[2]}_{multiplier}_{interval}'] = 1

        df.loc[
            (df[f'perc_min_down_{value[2]}_{multiplier}_{interval}'] > cnts.long_term_buy_thresholds[2])
            &
            (df[f'perc_max_up_{value[2]}_{multiplier}_{interval}'] < cnts.long_term_buy_thresholds[2] / 2)
            , f'sell_{value[2]}_{multiplier}_{interval}'] = 1      

    data_set_file_name = f"{cnts.data_long_term_sets_folder}/{in_symbol}.csv"
    print(f"{in_symbol}: Saving file {data_set_file_name}")

    df.to_csv(data_set_file_name, index=False)

# ----------------------------

def build_long_term_dataset_for_ticker(tiker:str):
    tiker_data_frames = read_merged_csv_files([tiker])
    prepare_long_term_dataset(tiker, tiker_data_frames)

# ----------------------------

def do_step():
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(14):
        print("-------------------------------------")
        print(f"Long term trading dataset. Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        for tiker in tikers_batch:
            process = multiprocessing.Process(target=build_long_term_dataset_for_ticker, args=(tiker,))
            processes.append(process)
            process.start()

        # Wait for all processes to finish
        print(f"Waiting for '{', '.join(tikers_batch)}' ...")
        for process in processes:
            process.join()

# ----------------------------

if __name__ == "__main__":
    do_step()


