from rrfs import rrfs
import pandas as pd

def make_time_windows(day_datetime, window_size):
    start = pd.Timestamp(year=day_datetime.year, month=day_datetime.month, day=day_datetime.day, hour=0)
    end = pd.Timestamp(year=day_datetime.year, month=day_datetime.month, day=day_datetime.day, hour=23)
    return pd.date_range(start, end, freq=f'{window_size}H')

r = rrfs.Rrfs()

start = pd.Timestamp(2023, 5, 9)
end = pd.Timestamp(2023, 6, 30)

for day in pd.date_range(start, end, freq='1D'):
    window_size = 3
    time_windows = make_time_windows(day, window_size)

    for tw in time_windows:
        forecast_hours_short = [3, 4, 5, 6]
        forecast_hours_long = [15, 16, 17, 18]

        #3 hour forecast
        init_time_short = tw - pd.Timedelta(hours=3)
        for hour in forecast_hours_short:

            r.download_outputs(init_time_short, hour)

        #15 hour forecast
        init_time_long = tw - pd.Timedelta(hours=15)
        for hour in forecast_hours_long:

            r.download_outputs(init_time_long, hour)
