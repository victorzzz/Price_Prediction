import matplotlib.pyplot as plt
import constants as cnts
import pandas as pd
import numpy as np

def drawPricesAndGains(prices, gains, losses):

    fig, ax1 = plt.subplots()

    # Plot prices with y-axis scale on the left side
    ax1.plot(prices, color='red', label="prices", linewidth=4)
    ax1.set_ylabel('prices', color='red')
    ax1.tick_params(axis='y', labelcolor='red')

    # Create a second y-axis
    ax2 = ax1.twinx()

    # Plot gains
    for loss_index, loss in enumerate(losses):
        #if loss_index >= 7:
            ax2.plot(gains[loss_index], label=loss)

    ax2.set_ylabel('losses', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    # Add legend
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2)

    # for i, val in enumerate(prices):
        # ax1.annotate(str(val), xy=(i, val), xytext=(5, 5), textcoords='offset points', color='red')

    # Add grid
    ax1.grid(True)

    # Show the plot
    plt.show()

def visualize_dataset(
        symbol:str,
        isUp:bool,
        data_sets_folder:str = cnts.data_sets_folder):
    
    data_set_file = f"{data_sets_folder}/{symbol}.csv"
    df = pd.read_csv(data_set_file)

    print(df)

    gain_arrays = []

    for level_index, level in enumerate(cnts.loss_levels):
        strLevel = str(level).replace('.', '_')

        if isUp:
            gain_arrays.append(df[f'up_{strLevel}'].values)
        else:
            gain_arrays.append(df[f'down_{strLevel}'].values)

    gains = np.vstack(gain_arrays)


    #drawPricesAndGains(df['vwap'].values[-1000:], gains[:,-1000:], cnts.loss_levels)
    drawPricesAndGains(df['vwap'].values, gains[-9:,:], cnts.loss_levels[-9:])

visualize_dataset("MSFT", True)