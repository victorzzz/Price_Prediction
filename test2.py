import pandas as pd
import numpy as np

from ta.momentum import RSIIndicator

df = pd.DataFrame({
    'close': [10, 12, 15, 16, 21, 25, 31, 42, 53, 55, 57, 58, 59, 61, 62, 63, 64, 65, 20, 10, 15, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12],
    'weights': [100, 120, 150, 1, 210, 250, 300, 400, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 2, 1, 1, 1, 1, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
})

df2 = pd.DataFrame({
    'close': [10]
})

# Calculate the histogram
hist, bins = np.histogram(df['close'], bins=10, weights=df['weights'])

hist_dencity, bins_dencity = np.histogram(df['close'], bins=10, weights=df['weights'], density=True, normed=True)

hist_sum = sum(hist)

normalized_hist = hist / hist_sum

sorted_indices = np.argsort(hist)[::-1]
sorted_hist = hist[sorted_indices]
sorted_bins_start = bins[:-1][sorted_indices]
sorted_bins_end = bins[1:][sorted_indices]

print("hist_dencity")
print(hist_dencity)

print("normalized_hist")
print(normalized_hist)

print("hist")
print(sorted_hist)

print("bins")
print(bins)

print("bins[:-1]")
print(sorted_bins_start)

print("bins[1:]")
print(sorted_bins_end)

# Print the histogram data
for bin_start, bin_end, freq in zip(bins[:-1], bins[1:], hist):
    print(f"Bin Range: {bin_start:.2f} - {bin_end:.2f}, Frequency: {freq}")
