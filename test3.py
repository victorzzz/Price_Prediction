import numpy as np
from typing import Dict, Any

# Create a sample NumPy array
arr = np.array([0, 1, 1, 0, 1, 0, 1])

# Find the indexes where the array items equal 1
indexes = np.where(arr == 1)[0]

where_result = np.where(arr == 1)

# Print the indexes
print("indexes")
print(indexes)

print("where_result")
print(where_result)

def array_to_dict(arr:np.ndarray) ->  Dict[Any, Any]:
    dictionary = {}
    for row in arr:
        key = row[0]
        value = row[1]
        dictionary[key] = value
    return dictionary

def getCheckPointsIntervals(chek_points:np.ndarray) -> np.ndarray:
    # get indexes of chek-points
    check_points_indexes = np.where(chek_points == 1)[0]

    # get indexes of intervals between chekpoints
    intervals_between_chekpoints = getArrayPairs(check_points_indexes)

    return intervals_between_chekpoints

def getArrayPairs(arr:np.ndarray) -> np.ndarray:
    pairs = np.column_stack((np.roll(arr, 1), arr))[1:]
    return pairs

def get_previous_next_pairs(arr):
    previous = np.roll(arr, 1)
    pairs = np.column_stack((previous, arr))
    return pairs

# Example usage
original_array = np.array([1, 2, 3, 4, -5, 6, 7, 8, 9, 0, 0, -3, 0])
pairs_array = getArrayPairs(original_array)

checkpoints = np.array(   [1, 0, 0, 0, 1,  0, 0, 1, 0, 0, 0, 1, 0])

checkpoints_intervals_as_dict = array_to_dict(getCheckPointsIntervals(checkpoints))

indexes_to_delete_checkpoints = np.where(original_array < 0)[0]
indexes_for_next_chekpoint = np.array([checkpoints_intervals_as_dict[key] for key in indexes_to_delete_checkpoints])

checkpoints[indexes_to_delete_checkpoints] = 0
checkpoints[indexes_for_next_chekpoint] = 0

print("indexes_to_delete_checkpoints")
print(indexes_to_delete_checkpoints)

print("indexes_for_next_chekpoint")
print(indexes_for_next_chekpoint)

print("checkpoints")
print(checkpoints)