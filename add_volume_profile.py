import multiprocessing
import sys
import time
import constants as cnts
import pandas as pd
import numpy as np

def calculate_volume_profile(df:pd.DataFrame, multiplier:int, calculate_for_last_records:int = -1, print_time:bool = False) -> pd.DataFrame:
    vwap = df['vwap'].to_numpy(copy=True)
    volume = df['volume'].to_numpy(copy=True)

    for depth in cnts.volume_profile_depths:

        print(f"VP depth:{depth}")
        print(" ")

        print("Adding new fields")

        start_adding_fileds_time = time.time()
        num_bins = int(depth / 4)
        df[f'vp_{multiplier}_{depth}_width'] = 0
        df = df.copy()

        for bin in range(num_bins):
            df[f'vp_{multiplier}_{depth}_{bin}_price'] = 0
            df[f'vp_{multiplier}_{depth}_{bin}_volume'] = 0
            df = df.copy()

        end_adding_fileds_time = time.time()

        print(f"New field were added, time {end_adding_fileds_time - start_adding_fileds_time}")

        n = 0
        total_getting_time = 0
        total_calcualtion_time = 0
        total_adding_time = 0

        avg_getting_time = 0
        avg_calcualtion_time = 0
        avg_adding_time = 0

        print(" ")
        print(" ")

        for index, row in df.iterrows():
          if (index < depth):
              continue

          start_getting_time = time.time()
          
          vwap_for_volume_profile = vwap[index - depth:index]
          volume_for_volume_profile = volume[index - depth:index]

          end_getting_time = time.time()

          n += 1
          total_getting_time += end_getting_time - start_getting_time
          avg_getting_time = total_getting_time / n


          start_calculation_time = time.time()

          hist, bins = np.histogram(vwap_for_volume_profile, bins=num_bins, weights=volume_for_volume_profile)

          hist = hist / sum(hist)

          sorted_indices = np.argsort(hist)[::-1]
          sorted_hist = hist[sorted_indices]
          sorted_bins_start = bins[:-1][sorted_indices]

          end_calculation_time = time.time()

          total_calcualtion_time += end_calculation_time - start_calculation_time
          avg_calcualtion_time = total_calcualtion_time / n

          start_field_adding_time = time.time()

          df.loc[index, f'vp_{multiplier}_{depth}_width'] = round(bins[1] - bins[0], 4)

          for histogram_index, item in enumerate(zip(sorted_bins_start, sorted_hist)):
              bin_start, histogram_volume = item
              df.loc[index, f'vp_{multiplier}_{depth}_{histogram_index}_price'] = round(bin_start, 4)
              df.loc[index, f'vp_{multiplier}_{depth}_{histogram_index}_volume'] = round(histogram_volume, 6)
          
          end_field_adding_time = time.time()

          total_adding_time += end_field_adding_time - start_field_adding_time
          avg_adding_time = total_adding_time / n

          if print_time:
            if index % 100 == 0:
                sys.stdout.write('\033[F')  # Move cursor to the beginning of current line
                sys.stdout.flush() 

                print(f"Index {index}  avg_getting_time {avg_getting_time:.6f},  avg_calcualtion_time {avg_calcualtion_time:.6f},  avg_adding_time {avg_adding_time:.6f}")
        
    return df

def add_volume_profiles(ticker:str):
    merged_data_file_name = f"{cnts.merged_data_folder}/{ticker}--price-candle--1--minute.csv"
    print(f"Reading {merged_data_file_name}")    
    merged_data_df = pd.read_csv(f"{merged_data_file_name}")

    records_in_merged_data = merged_data_df.shape[0]

    merged_data_with_vp_file_name = f"{cnts.merged_data_with_vp_folder}/{ticker}--volume_profile--1--minute.csv"

    # get last date of merged data with volume profile
    if cnts.is_file_exists(merged_data_with_vp_file_name):
        merged_data_with_vp_df = pd.read_csv(merged_data_with_vp_file_name)
        
        records_with_vp = merged_data_with_vp_df.shape[0]

        # return if there is no new data
        if merged_data_df.shape[0] <= records_with_vp: 
            print(f"No new data for {ticker}. Number of records in merged data {records_in_merged_data}, number of records with volume profile {records_with_vp}")
            return
        
        records_to_calculate_vp = records_in_merged_data - records_with_vp

        print(f"for {ticker}: ")
    
    # dropping unnecesary fields
    merged_data_df.drop('open', axis=1)
    merged_data_df.drop('high', axis=1)
    merged_data_df.drop('low', axis=1)
    merged_data_df.drop('close', axis=1)
    merged_data_df.drop('transactions', axis=1)
    merged_data_df.drop('otc', axis=1)

    merged_data_df = merged_data_df.copy()

    print(f"Creating valume profiles")

    merged_data_df = calculate_volume_profile(merged_data_df, 1, calculate_for_last_records=records_to_calculate_vp)

    merged_data_df = pd.concat([merged_data_with_vp_df, merged_data_df.tail(records_to_calculate_vp)], axis=0, ignore_index=True)

    print(f"Saving {merged_data_with_vp_file_name}")
    merged_data_df.to_csv(merged_data_with_vp_file_name)

# ----------------------------

def do_step():

    """
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(cnts.complex_processing_batch_size):
        print("-------------------------------------")
        print(f"Volume Profile. Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        for tiker in tikers_batch:
            process = multiprocessing.Process(target=add_volume_profiles, args=(tiker,))
            processes.append(process)
            process.start()

        # Wait for all processes to finish
        print(f"Waiting for '{', '.join(tikers_batch)}' ...")
        for process in processes:
            process.join()
    """

    add_volume_profiles("RY")

if __name__ == "__main__":
    do_step()