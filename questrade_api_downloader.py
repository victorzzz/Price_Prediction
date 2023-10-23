import datetime as dt
import pandas as pd
from os.path import exists
import time

import requests
from requests.exceptions import RequestException
import time

def make_http_request(url, headers=None, retries=3, backoff_factor=0.5):
    """
    Make an HTTP request with retries on transient errors.

    Args:
        url (str): The URL to make the request to.
        headers (dict): Dictionary of request headers. Default is None.
        retries (int): Number of retries on transient errors. Default is 3.
        backoff_factor (float): Backoff factor for retry delays. Default is 0.5.

    Returns:
        requests.Response: The response object.

    Raises:
        requests.exceptions.RequestException: If the request encounters a non-transient error.
    """
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise exception for non-2xx status codes
            return response
        except RequestException as e:
            if attempt == retries:
                raise  # If all retries failed, raise the exception
            if isinstance(e, (requests.Timeout, requests.ConnectionError)):
                # Retry on transient errors
                delay = backoff_factor * 2 ** attempt
                time.sleep(delay)
                print(f"Transient error occurred. Retrying in {delay} seconds...")
            else:
                raise  # Raise for non-transient errors

# Example usage:
url = 'https://api.example.com/some-endpoint'
headers = {
    'User-Agent': 'MyClient/1.0',
    'Authorization': 'Bearer my_token'
}
response = make_http_request(url, headers=headers)
print(response.text)

access_token = "oE_ZvbGTawOrWZ-_u0Vbi29nDJk7m4qN0"
symbols = (
    ("RY.TO", 34658),
    ("TD.TO", 38938),
    ("BNS.TO", 9339),
    ("BMO.TO", 9291),
    ("CM.TO", 12108),
    ("MFC.TO", 26585),
    ("SLF.TO", 1897767),
    ("NA.TO", 28523),
    ("BAM.TO", 45545434)
)

"""
https://api07.iq.questrade.com/v1/markets/candles/34658?startTime=2023-05-03T00:00:00-05:00&endTime=2023-05-08T00:00:00-05:00&interval=OneMinute
"""

intervals = {
    ((1, "minute"), "OneMinute"),
    ((2, "minute"), "TwoMinutes"),
    ((3, "minute"), "ThreeMinutes"),
    ((4, "minute"), "FourMinutes"),
    ((5, "minute"), "FiveMinutes"),
    ((10, "minute"), "TenMinutes"),
    ((15, "minute"), "FifteenMinutes"),
    ((20, "minute"), "TwentyMinutes"),
    ((30, "minute"), "HalfHour"),
    ((1, "day"), "OneDay")
}

api_server = "https://api07.iq.questrade.com/"


def download_stock_bars(date, max_days_history, ticker, multiplier, timespan):
    path = f"v1/markets/candles/{ticker[1]}?startTime=2023-05-03T00:00:00-05:00&endTime=2023-05-08T00:00:00-05:00&interval=OneMinute"