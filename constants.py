import numpy as np
import os
import datetime
import pytz
from typing import List
from typing import Generator

minute_multipliers = (1,2,3,5,10,30)
day_multipliers = (1,2,3,5)

ema_spans = (7, 14, 28, 56, 112, 224)
rsi_spans = (7, 14, 28)

volume_profile_depths = (28, 56, 112, 224)

long_term_prediction_time_frames = {
    (10, "minute"): ( 42 * 3, 42 * 5, 42 * 7)#,
    #(30, "minute"): ( 14 * 3, 14 * 5)
}

long_term_buy_thresholds = (0.021, 0.035, 0.049)

loss_levels = np.array([0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0, 1.125, 1.25, 1.375, 1.5, 1.625, 1.75, 1.875, 2.0])
gain_levels = np.array([0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0, 1.125, 1.25, 1.375, 1.5, 1.625, 1.75, 1.875, 2.0, 2.125, 2.25, 2.375, 2.5, 2.625, 2.75, 2.875, 3.0])

ticker_groups = {
    "bank_financial": ("RY", "TD", "BNS", "BMO", "CM", "MFC", "SLF", "BAM"),
    "tech_software": ("SHOP", "OTEX", "CDAY", "TRI" ),
    "tech_data_processing": ("HUT", "BITF"),
    "transportation": ("TFII", "WCN", "CP", "CNI"),
    "oil_gas": ("TRP", "PBA", "ENB", "SU", "VET", "ERF", "CPG", "OVV", "TECK", "CVE", "CNQ"),
    "usa": ("TSLA", "AAPL", "NVDA", "AMZN", "META", "MSFT", "AMD", "NFLX", "GOOG", "GOOGL")
}

def get_all_tickers_list() -> List[str]:
    return list(get_all_tickers())

def get_all_tickers() -> Generator[str, None, None]:
    for values in ticker_groups.values():
        for value in values:
            yield value

def get_test_tickers() -> Generator[str, None, None]:
    yield "RY"
    yield "AAPL"

def get_test_tickets_batches(batch_size:int) -> Generator[List[str], None, None]:
    return batch_generator(get_test_tickers(), batch_size)

def get_all_tickets_batches_list(batch_size:int) -> List[List[str]]:
    return list(get_all_tickets_batches(batch_size))

def get_all_tickets_batches(batch_size:int) -> Generator[List[str], None, None]:
    return batch_generator(get_all_tickers(), batch_size)

data_folder = "Data"
merged_data_folder = "MergedData"
data_check_folder = "DataCheck"

test_data_folder = "TestData"
test_merged_data_folder = "TestMergedData"

data_sets_folder = "data_sets"
data_sets_summary_folder = "data_sets_summary"

data_long_term_sets_folder = "data_long_term_sets"
data_long_term_sets_summary_folder = "data_long_term_sets_summary"

data_sets_for_learning_folder = "data_sets_for_learning"

merged_data_with_indicators_folder = "merged_data_with_indicators"
merged_data_with_vp_folder = "merged_data_with_vp"

stock_events_folder = "StockEvents"

def get_file_creation_datetime(file_path:str) -> datetime:
    # Get file metadata
    file_time = os.path.getmtime(file_path)
    
    # Get creation time
    creation_time = datetime.datetime.fromtimestamp(file_time)

    return creation_time
    
def iterate_files(folder_path:str):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            yield file_path

def iterate_files_name_only(folder_path:str):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            yield file     

float_nan = float("nan")

def batch_generator(sequence:Generator[str, None, None], batch_size:int) -> Generator[List[str], None, None]:
    batch = []
    for item in sequence:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

# Define NYSE timezone
nyse_timezone = pytz.timezone("America/New_York")
total_seconds = (16 - 9.5) * 60 * 60
start_time = datetime.time(hour=9, minute=30)

def nyse_msec_timestamp_to_date_time(msec_timestamp:int) -> datetime:
    epoch_time = msec_timestamp / 1000

    # Convert epoch time to a datetime object in the specified timezone
    dt = datetime.datetime.fromtimestamp(epoch_time, nyse_timezone)

    return dt

def nyse_msec_timestamp_to_float_with_round_6(msec_timestamp:int) -> float:
    return round(nyse_msec_timestamp_to_float(msec_timestamp), 6)

def nyse_msec_timestamp_to_float(msec_timestamp:int) -> float:
    
    epoch_time = msec_timestamp / 1000

    # Convert epoch time to a datetime object in the specified timezone
    dt = datetime.datetime.fromtimestamp(epoch_time, nyse_timezone)
    
    # Extract the date from the datetime object
    date = dt.date()
    
    # Combine the extracted date with the desired start and end times
    start_datetime = datetime.datetime.combine(date, start_time)
    
    # Calculate the elapsed seconds from the start time to the given epoch time
    elapsed_seconds = epoch_time - start_datetime.timestamp()
    
    # Calculate the float value based on the elapsed seconds and total seconds
    float_value = elapsed_seconds / total_seconds
    
    return float_value

def nyse_msec_timestamp_to_normalized_week_with_round_3(msec_timestamp:int) -> float:
    
    week = nyse_msec_timestamp_to_week(msec_timestamp) - 1

    normalized_week = week / 51.0

    return round(normalized_week, 3)


def nyse_msec_timestamp_to_week(msec_timestamp:int) -> int:

    dt = nyse_msec_timestamp_to_date_time(msec_timestamp)

    week_number = dt.isocalendar()[1]

    return week_number


def nyse_msec_timestamp_to_normalized_day_of_week_with_round_2(msec_timestamp:int) -> float:

    day_of_week = nyse_msec_timestamp_to_day_of_week(msec_timestamp) - 1
    
    normalized_day_of_week = day_of_week / 6.0

    return round(normalized_day_of_week, 2)


def nyse_msec_timestamp_to_day_of_week(msec_timestamp:int) -> int:

    dt = nyse_msec_timestamp_to_date_time(msec_timestamp)

    day_of_week = dt.isocalendar()[2]

    return day_of_week

