import pandas as pd

#Adds time window column to the daily report csv
def add_time_window_column(daily_reports, day, window_length=3):
    
    window_hours = make_time_windows(day, window_length)
    windows = []
    
    #For each report, 
    daily_report_list = daily_reports.values.tolist()
    for row in daily_report_list:
        report_time_stamp = row[-1]
        windows.append(get_time_window(report_time_stamp, window_hours, window_length))
    daily_reports["window"] = windows
    return daily_reports

#Returns the correct bin for a given row 
def get_time_window(report_time_stamp, window_hours, window_length):
    for w in window_hours:
        if report_time_stamp.hour - w.hour < window_length:
            return w
    return 

#Helper functions

#Generates the time bins
def make_time_windows(day_datetime, window_size):
    start = pd.Timestamp(year=day_datetime.year, month=day_datetime.month, day=day_datetime.day, hour=0)
    end = pd.Timestamp(year=day_datetime.year, month=day_datetime.month, day=day_datetime.day, hour=23)
    return pd.date_range(start, end, freq=f'{window_size}H')
    # return 