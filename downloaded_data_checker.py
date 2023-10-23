import constants as cnts
import pandas as pd
from os.path import exists
import numpy as np
from typing import List

def check_downloaded_csv_files(
        data_folder:str = cnts.data_folder,
        merged_data_folder:str = cnts.merged_data_folder,
        data_check_folder:str = cnts.data_check_folder):
    
    result_df = pd.DataFrame()
    file_names = []
    num_of_records = []
    creation_dates = []

    # read and concat all the files
    for file_name in cnts.iterate_files_name_only(data_folder):

        file_name_parts = file_name.split("--")
        symbol = file_name_parts[0]

        multiplier = int(file_name_parts[2])
        time_interval = file_name_parts[3]

        df:pd.DataFrame = pd.read_csv(f"{data_folder}/{file_name}")

        number_of_rows = df.shape[0]

        creqation_date = cnts.get_file_creation_datetime(f"{data_folder}/{file_name}")

        file_names.append(file_name)
        num_of_records.append(number_of_rows)
        creation_dates.append(creqation_date)

    result_df["file_name"] = np.array(file_names)
    result_df["num_of_records"] = np.array(num_of_records)
    result_df["creation_date"] = np.array(creation_dates)

    result_df.sort_values("num_of_records", inplace=True)

    print(f"Saving result ...")
    result_df.to_csv(f"{data_check_folder}/num_of_records.csv", index=False)  

check_downloaded_csv_files()