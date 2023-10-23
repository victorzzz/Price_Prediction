import pandas as pd
from os.path import exists
import constants as cnts
import numpy as np
import matplotlib.pyplot as plt

num_bins = 16

def summaize_long_term_datasets(tickers):
    result_df = pd.DataFrame()

    fig, axs = plt.subplots(len(tickers))

    for ticker_index, ticker in enumerate(tickers):
        file_name = f"{cnts.data_long_term_sets_folder}/{ticker}.csv"
        df = pd.read_csv(file_name)

        new_row_dict = {
            "symbol": [ticker]
        }        

        for key, value in cnts.long_term_prediction_time_frames.items():
            multiplier, interval = key

            distance_index = 0

            axs2 = axs[ticker_index].twinx()

            axs[ticker_index].plot(df.index,  df[f'vwap'], label=f"vwap")
            axs[ticker_index].plot(df.index,  df[f'close'], label=f"close")
            axs[ticker_index].plot(df.index,  df[f'low'], label=f"low")
            axs[ticker_index].plot(df.index,  df[f'high'], label=f"high")
            axs[ticker_index].legend(loc='upper right')
            axs[ticker_index].set_title(ticker)

            axs2.scatter(df.index, df[f'buy_{value[0]}_{multiplier}_{interval}'], label=f"BUY 0")
            axs2.scatter(df.index, df[f'sell_{value[0]}_{multiplier}_{interval}'], label=f"SELL 0")

            axs2.scatter(df.index, df[f'buy_{value[1]}_{multiplier}_{interval}'] * 1.1, label=f"BUY 1")
            axs2.scatter(df.index, df[f'sell_{value[1]}_{multiplier}_{interval}'] * 1.1, label=f"SELL 1")

            axs2.scatter(df.index, df[f'buy_{value[2]}_{multiplier}_{interval}'] * 1.2, label=f"BUY 2")
            axs2.scatter(df.index, df[f'sell_{value[2]}_{multiplier}_{interval}'] * 1.2, label=f"SELL 2")

            # axs2.plot(df.index, df[f'normalized_trading_time'], label=f"NTT")


            axs2.set_ylabel(f"BUY SEL")
            axs2.set_ylim(-0.1, 1.4)
            axs2.legend(loc='upper left')

            for distance in value:
                perc_max_up = df[f'perc_max_up_{distance}_{multiplier}_{interval}'].to_numpy(copy=True)
                perc_min_down = df[f'perc_min_down_{distance}_{multiplier}_{interval}'].to_numpy(copy=True)

                perc_max_up = perc_max_up[~np.isnan(perc_max_up)]
                perc_min_down = perc_min_down[~np.isnan(perc_min_down)]

                # axs2 = axs[ticker_index, distance_index].twinx()

                # axs[ticker_index, distance_index].plot(df['timestamp'],  df[f'perc_max_up_{distance}_{multiplier}_{interval}'], label=f"MAX {distance}")
                # axs[ticker_index, distance_index].set_ylabel(f"% {distance}")

                # axs[ticker_index, distance_index].plot(df['timestamp'],  df[f'perc_min_down_{distance}_{multiplier}_{interval}'], label=f"MIN {distance}")

                # axs[ticker_index, distance_index].set_title(f"% {ticker} {distance}")
                # axs[ticker_index, distance_index].legend()

                # axs2.scatter(df['timestamp'], df[f'buy_{distance}_{multiplier}_{interval}'], label=f"BUY {distance}")
                # axs2.scatter(df['timestamp'], df[f'sell_{distance}_{multiplier}_{interval}'], label=f"SELL {distance}")
                # axs2.set_ylabel(f"BUY SEL")
                # axs2.set_ylim(0, 1)
                # axs2.legend()

                new_row_dict[f'buy_{distance}_{multiplier}_{interval}'] = df[f'buy_{distance}_{multiplier}_{interval}'].sum()
                new_row_dict[f'sell_{distance}_{multiplier}_{interval}'] = df[f'sell_{distance}_{multiplier}_{interval}'].sum()

                hist_up, bins_up = np.histogram(perc_max_up, bins=num_bins)
                hist_up = hist_up / sum(hist_up)

                sorted_indices_up = np.argsort(hist_up)[::-1]
                sorted_hist_up = hist_up[sorted_indices_up]
                sorted_bins_start_up = bins_up[:-1][sorted_indices_up]

                hist_down, bins_down = np.histogram(perc_min_down, bins=num_bins)
                hist_down = hist_down / sum(hist_down)

                sorted_indices_down = np.argsort(hist_down)[::-1]
                sorted_hist_down = hist_down[sorted_indices_down]
                sorted_bins_start_down = bins_down[:-1][sorted_indices_down]

                for hist_index in range(num_bins):
                    new_row_dict[f'hist_up_{hist_index}_perc_max_up_{distance}_{multiplier}_{interval}'] = sorted_hist_up[hist_index]
                    new_row_dict[f'hist_down_{hist_index}_perc_min_down_{distance}_{multiplier}_{interval}'] = sorted_hist_down[hist_index]
                    new_row_dict[f'price_up_{hist_index}_perc_max_up_{distance}_{multiplier}_{interval}'] = sorted_bins_start_up[hist_index]
                    new_row_dict[f'price_down_{hist_index}_perc_min_down_{distance}_{multiplier}_{interval}'] = sorted_bins_start_down[hist_index]

                new_row = pd.DataFrame(new_row_dict)

                distance_index += 1

        result_df = pd.concat([result_df, new_row])
    
    # Adjust the layout and spacing
    plt.tight_layout()

    # Display the figure
    plt.show()    

    result_df.to_csv(f"{cnts.data_long_term_sets_summary_folder}/long_term_summary.csv")


summaize_long_term_datasets(["RY", "TD"]) #cnts.get_all_tickers_list()[0:2]