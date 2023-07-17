from rrfs import rrfs
import pandas as pd

def make_time_windows(day_datetime, window_size):
    start = pd.Timestamp(year=day_datetime.year, month=day_datetime.month, day=day_datetime.day, hour=0)
    end = pd.Timestamp(year=day_datetime.year, month=day_datetime.month, day=day_datetime.day, hour=23)
    return pd.date_range(start, end, freq=f'{window_size}H')

r = rrfs.Rrfs()

start = pd.Timestamp(2023, 5, 8)
end = pd.Timestamp(2023, 6, 30)

print(pd.date_range(start, end, freq='1D')[0])
for day in pd.date_range(start, end, freq='1D'):

	window_size = 3
	time_windows = make_time_windows(day, window_size)


	for tw in time_windows:
		forecast_hour = tw.hour

# 		#3 hour forecast
		init_time = tw - pd.Timedelta(hours=3)
# 		r.download_outputs(init_time, forecast_hour)
# 		r.download_outputs(init_time, forecast_hour + 1)
# 		r.download_outputs(init_time, forecast_hour + 2)
        r.download_outputs(init_time, forecast_hour + 3)

# 		#15 hour forecast
		init_time = tw - pd.Timedelta(hours=15)
# 		r.download_outputs(init_time, forecast_hour)
# 		r.download_outputs(init_time, forecast_hour + 1)
# 		r.download_outputs(init_time, forecast_hour + 2)
		# print(f"downloaded forecasts for {tw}")
        r.download_outputs(init_time, forecast_hour + 3)




