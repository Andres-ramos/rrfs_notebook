import pandas as pd
import requests
#Returns pandas dataframe with wind reports for given date range
def get_weather_reports(start_date, end_date):    
    i = 0
    for date in generate_days(start_date, end_date):
        df = get_wind_reports(date)
        if i == 0:
            big_df = df
            i += 1
        else :
            big_df = pd.concat([big_df, df])
    big_df.reset_index(inplace=True)
    return big_df


#Gets wind report on given date as a pandas dataframe
def get_wind_reports(datetime):
    date = datetime.strftime("%y%m%d")
    df = request_wind_report_df(date)    
    df = add_datetime(df, datetime)
    return df

#Helper functions 
#Requests the wind report from the NOAA website and cleans up the data
def request_wind_report_df(date):
    #Actual request
    link = generate_link(date)
    data = requests.get(link)
    #Turns csv string into dataFrame
    csv_string = data.text
    data = [row.split(',') for row in csv_string.split('\n')]
    data.pop(-1)
    columns = data.pop(0)
    data.pop(0)
    #Returns dataframe 
    df = pd.DataFrame(data, columns=columns)
    df["Lat"] = df["Lat"].astype("float")
    df["Lon"] = df["Lon"].astype("float")
    return df

#Adds date time column to the dataframe
def add_datetime(df, datetime):
    time = df["Time"]
    #TODO: Rename foo function
    df["datetime"] = [foo(datetime, t) for t in time]
    df['datetime'] = pd.to_datetime(df["datetime"])
    #Removes Time column
    df.drop(columns=["Time"], inplace=True)
    return df 

#Corrects the date for a given report
#TODO: Rename this function
def foo(datetime, time):
    day = datetime.day if int(time[0] + time[1]) >= 12 else (datetime + pd.Timedelta(days = 1)).day
    dt = pd.Timestamp(year=datetime.year, month=datetime.month,
                                   day=day, hour=int(time[0] + time[1]),
                                  minute = int(time[2] + time[3]))
    return dt



#Generates link to csv download of wind report on given date. Date format "yymmdd"
def generate_link(date):
    return f'https://www.spc.noaa.gov/climo/reports/{date}_rpts_wind.csv'

#Generates dates in format "yymmdd"
def generate_days(start_date, end_date):
    
    days = pd.period_range(start=start_date, end=end_date)
    # days = [day.strftime("%y%m%d") for day in days]
    return days


start_date = pd.Timestamp(year=2023, month=5, day=9)
end_date = pd.Timestamp(year=2023, month=6, day=30)
reports = get_weather_reports(start_date, end_date)
reports.reset_index(drop=True)


reports.to_csv("./wind_reports.csv")