import constants as cnts
import pandas as pd
import numpy as np
import multiprocessing

def calculate_rsi_diff_value(df:pd.DataFrame, diff_field:str, value_field:str, span:int):
    diff_column_data = df[diff_field]
    diff = diff_column_data.diff().dropna() 

    value_column_data = df[value_field]
    up = value_column_data.where(diff > 0, 0)
    down = value_column_data.where(diff < 0, 0)
    
    avg_gain = up.ewm(span=span, adjust=True).mean()
    avg_loss = down.ewm(span=span, adjust=True).mean() 

    avg_loss.replace(0.0, 0.00000001, inplace=True)

    relative_strength = avg_gain / avg_loss
    rsi  = 1.0 - (1.0 / (1.0 + relative_strength))

    df[f'rsi_diff-{diff_field}_value-{value_field}_{span}'] = rsi

def calculate_rsi(df:pd.DataFrame, diff_field:str, span:int):
    diff_column_data = df[diff_field]
    diff = diff_column_data.diff().dropna() 

    up = diff.where(diff > 0, 0)
    down = - diff.where(diff < 0, 0)

    avg_gain = up.ewm(span=span, adjust=True).mean()
    avg_loss = down.ewm(span=span, adjust=True).mean()  

    relative_strength = avg_gain / avg_loss

    rsi = 1.0 - (1.0 / (1.0 + relative_strength))

    df[f'rsi_{diff_field}_{span}'] = rsi

def calculate_ema(df:pd.DataFrame, field:str, span:int):
    df[f'ema_{field}_{span}'] = df[field].ewm(span=span, adjust=True).mean()

def calculate_sma(df:pd.DataFrame, field:str, span:int):
    df[f'sma_{field}_{span}'] = df[field].rolling(window=span).mean()

def calculate_indicators(df:pd.DataFrame):

    for index, span in enumerate(cnts.ema_spans):
        calculate_ema(df, 'close', span)
        calculate_ema(df, 'vwap', span)

        calculate_sma(df, 'close', span)
        calculate_sma(df, 'vwap', span)        

        if (index > 1):
            prev_span = cnts.ema_spans[index - 1]
            prev_prev_span = cnts.ema_spans[index - 2]
            
            df[f'diff_ema_close_{span}_{prev_span}'] = df[f'ema_close_{span}'] - df[f'ema_close_{prev_span}']
            df[f'diff_ema_vwap_{span}_{prev_span}'] = df[f'ema_vwap_{span}'] - df[f'ema_vwap_{prev_span}']

            calculate_ema(df, f'diff_ema_close_{span}_{prev_span}', prev_prev_span)
            calculate_ema(df, f'diff_ema_vwap_{span}_{prev_span}', prev_prev_span)
    
    for span in cnts.rsi_spans:
        calculate_rsi(df, 'vwap', span)
        calculate_rsi(df, 'close', span)
        calculate_rsi_diff_value(df, 'vwap', 'volume', span)
        calculate_rsi_diff_value(df, 'close', 'volume', span)

def calculate_indicatorsfor_ticker(ticker):

    for minute_multiplier in cnts.minute_multipliers:
        file_name = f"{cnts.merged_data_folder}/{ticker}--price-candle--{minute_multiplier}--minute.csv"
        print(f"Processing {file_name}")
        df = pd.read_csv(f"{file_name}")
        calculate_indicators(df)

        result_file_name = f"{cnts.merged_data_with_indicators_folder}/{ticker}--price-candle-indicators--{minute_multiplier}--minute.csv"
        print(f"Saving {result_file_name}")
        df.to_csv(result_file_name)

    for day_multiplier in cnts.day_multipliers:
        file_name = f"{cnts.merged_data_folder}/{ticker}--price-candle--{day_multiplier}--day.csv"
        print(f"Processing {file_name}")
        df = pd.read_csv(f"{file_name}")
        key = (ticker, day_multiplier, "day")
        calculate_indicators(df)

        result_file_name = f"{cnts.merged_data_with_indicators_folder}/{ticker}--price-candle-indicators--{day_multiplier}--day.csv"
        print(f"Saving {result_file_name}")
        df.to_csv(result_file_name)

# ----------------------------

def do_step():
    processes = []

    for tikers_batch in cnts.get_all_tickets_batches(14):
        print("-------------------------------------")
        print(f"Indicators. Processing group '{', '.join(tikers_batch)}' ...")
        print("-------------------------------------")

        for tiker in tikers_batch:
            process = multiprocessing.Process(target=calculate_indicatorsfor_ticker, args=(tiker,))
            processes.append(process)
            process.start()

        # Wait for all processes to finish
        print(f"Waiting for '{', '.join(tikers_batch)}' ...")
        for process in processes:
            process.join()

if __name__ == "__main__":
    do_step()
