import time
import datetime
import pandas
import requests
import csv
import io

PROTOCOL = 'http://'
BASE_URL = 'www.google.com/finance/getprices'


def get_google_data(symbol, interval_s=60, lookback_d=15, end_time=time.time()):
    """
    Get intraday data for the symbol from google finance and
    return a pandas DataFrame
    :param symbol (str)
    :param interval_s (int)
    :param lookback_d (int)
    :param end_time (unix timestamp)
    :returns pandas.DataFrame
    """
    resource_url = PROTOCOL + BASE_URL
    payload = {
        'q': symbol,
        'i': str(interval_s),
        'p': str(lookback_d) + 'd',
        'ts': str(int(end_time * 1000)),
        'f': 'd,o,h,l,c,v'
    }

    r = requests.get(resource_url, params=payload)

    quotes = []

    with io.BytesIO(r.content) as csvfile:
        quote_reader = csv.reader(csvfile)
        timestamp_start = None
        timestamp_offset = None
        for row in quote_reader:
            if row[0][0] not in 'a1234567890':  # discard headers
                continue
            elif row[0][0] == 'a':  # 'a' prepended to the timestamp that starts each day
                timestamp_start = datetime.datetime.fromtimestamp(float(row[0][1:]))
                timestamp_offset = 0
            elif timestamp_start:
                timestamp_offset = int(row[0])

            if not timestamp_start and not timestamp_offset:
                continue

            timestamp = timestamp_start + datetime.timedelta(seconds=timestamp_offset * interval_s)
            close = float(row[1])
            high = float(row[2])
            low = float(row[3])
            open = float(row[4])
            volume = float(row[5])

            quotes.append((timestamp, close, high, low, open, volume))

    df = pandas.DataFrame(quotes, columns=['datetime', 'close', 'high', 'low', 'open', 'volume'])
    df = df.set_index('datetime')

    return df