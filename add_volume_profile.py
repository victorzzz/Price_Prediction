import multiprocessing
import sys
import time
import constants as cnts
import pandas as pd
import numpy as np

def calculate_volume_profile(df:pd.DataFrame, multiplier:int, print_time:bool = False) -> pd.DataFrame:
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


def add_vcalculate_volume_profiles(ticker:str):
    file_name = f"{cnts.merged_data_folder}/{ticker}--price-candle--1--minute.csv"
    print(f"Reading {file_name}")    
    df = pd.read_csv(f"{file_name}")
    
    # dropping unnecesary fields
    df.drop('open', axis=1)
    df.drop('high', axis=1)
    df.drop('low', axis=1)
    df.drop('close', axis=1)
    df.drop('transactions', axis=1)
    df.drop('otc', axis=1)

    df = df.copy()

    print(f"Creating valume profiles")

    df = calculate_volume_profile(df, 1)

    file_name_to_save = f"{cnts.merged_data_with_vp_folder}/{ticker}--volume_profile--1--minute.csv"

    print(f"Saving {file_name_to_save}")
    df.to_csv(f"{file_name_to_save}")

# ----------------------------

def do_step():
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(14):
        print("-------------------------------------")
        print(f"Volume Profile. Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        for tiker in tikers_batch:
            process = multiprocessing.Process(target=add_vcalculate_volume_profiles, args=(tiker,))
            processes.append(process)
            process.start()

        # Wait for all processes to finish
        print(f"Waiting for '{', '.join(tikers_batch)}' ...")
        for process in processes:
            process.join()


if __name__ == "__main__":
    
    do_step()