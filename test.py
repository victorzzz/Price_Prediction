import datetime
import pytz


# Define NYSE timezone
nyse_timezone = pytz.timezone("America/New_York")
total_seconds = (16 - 9.5) * 60 * 60
start_time = datetime.time(hour=9, minute=30)

def epoch_to_float(epoch_time):
    
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



epoch_time = 1621061873  # Replace this with your epoch time

# Convert epoch time to datetime
dt_utc = datetime.datetime.fromtimestamp(epoch_time, pytz.UTC)
dt_eastern = datetime.datetime.fromtimestamp(epoch_time, pytz.timezone("America/New_York"))

# Print the converted datetime in a specific format
print(dt_utc.strftime("%Y-%m-%d %H:%M:%S %Z%z"))
print(dt_eastern.strftime("%Y-%m-%d %H:%M:%S %Z%z"))

print(epoch_to_float(1684243800)) 
print(epoch_to_float(1684267200))

a = {1:2, 3:4, 5:6}