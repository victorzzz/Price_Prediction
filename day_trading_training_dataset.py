import constants as cnts
import pandas as pd
import multiprocessing
import simple_price_analizer as spa
import numpy as np

def read_dataset_csv_file(ticker:str):    
    file_name = f"{cnts.data_sets_folder}/{ticker}.csv"
    df = pd.read_csv(file_name)

    return df

def read_indicators_csv_file(ticker:str):
    file_name = f"{cnts.merged_data_with_vp_folder}/{ticker}--volume_profile--1--minute.csv"
    df = pd.read_csv(file_name)

    return df

def build_training_dataset_for_ticker(tiker:str):
    df_dataset = read_dataset_csv_file(tiker)
    df_indicator = read_indicators_csv_file(tiker)
    

# ----------------------------

def do_step():
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(cnts.complex_processing_batch_size):
        print("-------------------------------------")
        print(f"Day tradig training dataset. Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        for tiker in tikers_batch:
            process = multiprocessing.Process(target=build_training_dataset_for_ticker, args=(tiker,))
            processes.append(process)
            process.start()

        # Wait for all processes to finish
        print(f"Waiting for '{', '.join(tikers_batch)}' ...")
        for process in processes:
            process.join()

# ----------------------------

if __name__ == "__main__":
    do_step()