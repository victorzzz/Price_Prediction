import numpy as np
from typing import List
from typing import Tuple
from typing import Dict
from typing import Any
import matplotlib.pyplot as plt

def getArrayPairs(arr:np.ndarray) -> np.ndarray:
    pairs = np.column_stack((np.roll(arr, 1), arr))[1:]
    return pairs

def getCheckPointsIntervals(chek_points:np.ndarray) -> np.ndarray:
    # get indexes of chek-points
    check_points_indexes = np.where(chek_points == 1)[0]

    # get indexes of intervals between chekpoints
    intervals_between_chekpoints = getArrayPairs(check_points_indexes)

    return intervals_between_chekpoints

def drawResul(prices, gains, days, losses):

    fig, ax1 = plt.subplots()

    # Plot prices with y-axis scale on the left side
    ax1.plot(prices, color='red', label="prices")
    ax1.set_ylabel('prices', color='red')
    ax1.tick_params(axis='y', labelcolor='red')

    # Create a second y-axis
    ax2 = ax1.twinx()

    # Plot arr2 and arr3 with y-axis scale on the right side
    ax2.plot(gains[0], color='blue', label=losses[0])
    ax2.plot(gains[1], color='green', label=losses[1])
    ax2.plot(gains[2], color='black', label=losses[2])
    ax2.plot(gains[3], color='purple', label=losses[3])
    #ax2.plot(days, color='yellow', label="days")
    ax2.set_ylabel('losses', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    # Add legend
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2)

    for i, val in enumerate(prices):
        ax1.annotate(str(val), xy=(i, val), xytext=(5, 5), textcoords='offset points', color='red')

    # Add grid
    ax1.grid(True)

    # Show the plot
    plt.show()

# returns (increases, decreases)
def calculateDayPriceChangeWithTrailingStopLoss(
        prices:np.ndarray[float, Tuple[int]],
        days:np.ndarray[int, Tuple[int]],
        trailingStopLosses:np.ndarray[float, Tuple[int]]
    ) -> Tuple[np.ndarray[float, Tuple[int, int]], np.ndarray[float, Tuple[int, int]]]:
    
    # asserts
    assert len(prices) == len(days)

    l_prices = len(prices)
    l_losses = len(trailingStopLosses)

    # day_borders = np.where( ((np.diff(days, prepend=np.nan) != 0) | (np.diff(days, append=np.nan) != 0)), 1, 0)
    day_borders = np.where( (np.diff(days, append=np.nan) != 0), 1, 0)

    day_borders_intervals = getCheckPointsIntervals(day_borders)
    if (len(day_borders_intervals) > 0):
        first_interval = day_borders_intervals[0]
        first_interval_begin = first_interval[0]
        day_borders_intervals = np.vstack((np.array([-1, first_interval_begin]), day_borders_intervals))   

    inc_results_for_losses = np.zeros((l_losses, l_prices))
    dec_results_for_losses = np.zeros((l_losses, l_prices))

    for interval in day_borders_intervals:
        begin = interval[0]
        end = interval[1]

        price_end = prices[end]
        previous = price_end

        inc_current_max_arr = np.full_like(trailingStopLosses, price_end)
        inc_current_min = price_end

        dec_current_min_arr = np.full_like(trailingStopLosses, price_end)
        dec_current_max = price_end

        for i in range(end-1, begin, -1):
            i_price = prices[i]

            # inc
            inc_current_max_greater_price = inc_current_max_arr >= i_price
            inc_results_for_losses[inc_current_max_greater_price, i] = (inc_current_max_arr[inc_current_max_greater_price] - i_price) / i_price

            inc_current_max_less_price = inc_current_max_arr < i_price
            inc_current_max_arr[inc_current_max_less_price] = i_price

            # dec
            dec_current_min_less_price = dec_current_min_arr <= i_price
            dec_results_for_losses[dec_current_min_less_price, i] = (i_price - dec_current_min_arr[dec_current_min_less_price]) / i_price

            dec_current_min_greater_price = dec_current_min_arr > i_price
            dec_current_min_arr[dec_current_min_greater_price] = i_price

            # inc
            if (i_price > previous):
                    inc_dif_to_min = (i_price - inc_current_min) / i_price

                    inc_stopLoss_less_diff_to_min =  trailingStopLosses < inc_dif_to_min
                    inc_results_for_losses[inc_stopLoss_less_diff_to_min, i] = 0.0
                    inc_current_max_arr[inc_stopLoss_less_diff_to_min] = i_price

            if (i_price <= inc_current_min):
                inc_current_min = i_price 

            # dec
            if (i_price < previous):
                    dec_dif_to_max = (dec_current_max - i_price) / i_price

                    dec_stopLoss_less_diff_to_max =  trailingStopLosses < dec_dif_to_max
                    dec_results_for_losses[dec_stopLoss_less_diff_to_max, i] = 0.0
                    dec_current_min_arr[dec_stopLoss_less_diff_to_max] = i_price

            if (i_price >= dec_current_max):
                dec_current_max = i_price             

            previous = i_price

    return (inc_results_for_losses, dec_results_for_losses)

"""
losses = np.array([4.0, 3.0, 2.0, 1.0]) / 100.0 
prices = np.array([103, 102, 101.5, 101, 100, 99, 95, 98.5, 93, 91, 93, 95, 97.5, 94, 93.5,  95, 90, 89, 88, 91,  92,  93,  94], dtype=np.float32)
days = np.array([    1,   1,     1,   1,   1,  1,  1,    1,  1,  1,  2,   2,   2,  2,    2,   2,  2,  2,  2,  2,   2,   2,   2], dtype=np.int8)

inc, dec = calculateDayPriceChangeWithTrailingStopLoss(
    prices, 
    np.array(days), 
    losses)

for inc_element, loss_element in zip(inc, losses) :
    print(loss_element)
    print(inc_element)
    print(" ")

drawResul(prices, inc, days, losses)

drawResul(prices, dec, days, losses)

"""
