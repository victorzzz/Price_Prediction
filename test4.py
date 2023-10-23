import numpy as np
import datetime

arr = np.zeros((5, 11))

b = np.array([2,4,5,7,3])

b_greater_4 = b > 4

arr[b_greater_4,2:6] = 777

# Create a datetime object for a specific date
date = datetime.datetime(2023, 1, 2)

# Get the week number and day within that week
isocalendar = date.isocalendar()
week_number = isocalendar[1]
day_of_week = isocalendar[2]  # Monday is considered the first day of the week

print("Week:", week_number)
print("Day in Week:", day_of_week)

print("isocalendar:", date.isocalendar())


