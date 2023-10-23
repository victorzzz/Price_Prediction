import constants as cnts
import pandas as pd

stop_losses = (0.00125, 0.0025, 0.005, 0.0075, 0.010)

price_up_diff_fields = ('up_0125', 'up_025', 'up_050', 'up_075', 'up_1')
price_down_diff_fields = ('down_0125', 'down_025', 'down_050', 'down_075', 'down_1')

price_up_bool_fields = ('_0_5_up_0125', '_1_0_up_0125', '_1_5_up_0125', '_2_0_up_0125', '_2_5_up_0125',
                 '_0_5_up_025', '_1_0_up_025', '_1_5_up_025', '_2_0_up_025', '_2_5_up_025',
                 '_1_0_up_050', '_1_5_up_050', '_2_0_up_050', '_2_5_up_050',
                 '_1_0_up_075', '_1_5_up_075', '_2_0_up_075', '_2_5_up_075',
                 '_1_5_up_1', '_2_0_up_1', '_2_5_up_1')

price_down_bool_fields = ('down_0125', 'down_025', 'down_050', 'down_075', 'down_1',
                 '_0_5_down_0125', '_1_0_down_0125', '_1_5_down_0125', '_2_0_down_0125', '_2_5_down_0125',
                 '_0_5_down_025', '_1_0_down_025', '_1_5_down_025', '_2_0_down_025', '_2_5_down_025',
                 '_1_0_down_050', '_1_5_down_050', '_2_0_down_050', '_2_5_down_050',
                 '_1_0_down_075', '_1_5_down_075', '_2_0_down_075', '_2_5_down_075',
                 '_1_5_down_1', '_2_0_down_1', '_2_5_down_1')

target_stop_loss = 0.010
target_price_diff = 0.020

def prepare_learning_dataset(tickers):

    for ticker in tickers:

        print(f"Processing {ticker} ...")

        file_name = f"{cnts.data_set_folder}/{ticker}.csv"
        data_set_df = pd.read_csv(file_name)

        data_set_df.drop(price_up_bool_fields)
        data_set_df.drop(price_up_bool_fields)

        data_set_df['action'] = cnts.float_nan

        data_set_df = data_set_df.copy()


        
    
    
        
