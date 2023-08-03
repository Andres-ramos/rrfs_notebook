import pandas as pd
import matplotlib.pyplot as plt
from geopy.distance import geodesic as GD
from sklearn.cluster import DBSCAN
import numpy as np
from rrfs import rrfs
import xarray as xr

import sys

from pipeline.storm_report import add_time_window_column 
from pipeline.cluster import get_clusters
from pipeline.time_window import time_window
from pipeline.datasets import make_wind_dt, make_max_wind_dt, make_25_UH_dt, make_03_UH_dt, make_downdraft_dt, make_gust_dt



def get_surrogate_storm_reports(window_datetime, window_size, thresholds, forecast_h):
    surrogate_report_list = []
    init_hour = pd.Timestamp(window_datetime) - pd.Timedelta(hours=forecast_h)
    
    variable_list = ['MAXDVV_P8_2L100_GLC0_max1h','LAND_P0_L1_GLC0',
                     'MAXUW_P8_L103_GLC0_max1h','MAXVW_P8_L103_GLC0_max1h',
                     'UGRD_P0_L103_GLC0','VGRD_P0_L103_GLC0','GUST_P0_L1_GLC0',
                     'MXUPHL_P8_2L103_GLC0_max1h'
                    ]

    r = rrfs.Rrfs()
    model_outputs = r.fetch_model_outputs(
        init_hour, 
        [forecast_h, forecast_h + 1, forecast_h + 2, forecast_h+3],
        variable_list=variable_list
    )
    
    #Variables
    #1) max 10m wind
    #2) max 2-5 updraft helicity
    #3) max 0-2 updraft helicity
    #4) max downdraft 
    #5) 10m wind
    #6) gust potential 
    
    surrogate_reports = {
        'max_wind': max_10_wind_surrogates(model_outputs, thresholds),
        'max_2_5_uh': max_2_5_uh_surrogates(model_outputs, thresholds),
        'max_0_3_uh': max_0_3_uh_surrogates(model_outputs, thresholds),
        'max_downdraft': max_downdraft(model_outputs, thresholds),
        'wind': wind_10_m_surrogates(model_outputs, thresholds),
        'gust': gust_surrogates(model_outputs, thresholds)
        
    }
    return surrogate_reports


def max_10_wind_surrogates(model_outputs, thresholds):
    surrogate_report_list = []
    outputs = model_outputs[1:-1] #Max so outputs are offset
    for output in outputs:
        wind_dt = make_max_wind_dt(output)
        #Filters out entries over the sea
        filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
        #Filters values under the threshold
        filtered_output = filtered_output.where(filtered_output.max_wind > thresholds["max_wind"], drop=True)
        #resets index
        filtered_output = filtered_output.to_dataframe().reset_index()
        t_df = pd.DataFrame(
            {'max_wind': filtered_output['max_wind'], 
             'Lat': filtered_output['gridlat_0'], 
             'Lon': filtered_output['gridlon_0']
            }
        )
        
        surrogate_report = t_df.dropna().reset_index(drop=True)
        #Appends surrogate reports from slice to surrogate list
        surrogate_report_list.append(surrogate_report)
    
    surrogate_df = pd.concat(surrogate_report_list)
    surrogate_df.index = pd.RangeIndex.from_range(range(len(surrogate_df.index)))
    return surrogate_df 

#Change this method 
def max_downdraft(model_outputs, thresholds):
    # print("getting max downdraft surrogates")
    surrogate_report_list = []
    outputs = model_outputs[1:-1] #Max so outputs are offset

    for output in outputs:
        # print("output land", output['LAND_P0_L1_GLC0'].data)
        # print("output downdraft", output['MAXDVV_P8_2L100_GLC0_max1h'])
        downdraft_dt = make_downdraft_dt(output)
        filtered_output = downdraft_dt.where(downdraft_dt.land == 1, drop=True)
        filtered_output = filtered_output.where(filtered_output.max_downdraft < thresholds["max_downdraft"], drop=True)
        filtered_output = filtered_output.to_dataframe().reset_index()
        t_df = pd.DataFrame(
            {'max_downdraft': filtered_output.max_downdraft, 
             'Lat': filtered_output['gridlat_0'], 
             'Lon': filtered_output['gridlon_0']
            }
        )
        
        surrogate_report = t_df.dropna().reset_index(drop=True)
        #Appends surrogate reports from slice to surrogate list
        surrogate_report_list.append(surrogate_report)
    surrogate_df = pd.concat(surrogate_report_list)
    surrogate_df.index = pd.RangeIndex.from_range(range(len(surrogate_df.index)))
    
    return surrogate_df

def wind_10_m_surrogates(model_outputs, thresholds):
    surrogate_report_list = []
    outputs = model_outputs[0:2] 
    for output in outputs:
        wind_dt = make_wind_dt(output)
        filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
        filtered_output = filtered_output.where(filtered_output.wind > thresholds["wind"], drop=True)
        filtered_output = filtered_output.to_dataframe().reset_index()
        t_df = pd.DataFrame(
            {'wind': filtered_output['wind'], 
             'Lat': filtered_output['gridlat_0'], 
             'Lon': filtered_output['gridlon_0']
            }
        )
        
        surrogate_report = t_df.dropna().reset_index(drop=True)
        #Appends surrogate reports from slice to surrogate list
        surrogate_report_list.append(surrogate_report)
    surrogate_df = pd.concat(surrogate_report_list)
    surrogate_df.index = pd.RangeIndex.from_range(range(len(surrogate_df.index)))
    
    return surrogate_df

#TODO: Change this method 
def gust_surrogates(model_outputs, thresholds):
    surrogate_report_list = []
    outputs = model_outputs[0:2]
    for output in outputs:
        gust_dt = make_gust_dt(output)
        filtered_output = gust_dt.where(gust_dt.land == 1, drop=True)
        filtered_output = filtered_output.where(filtered_output.gust > thresholds["gust"], drop=True)
        filtered_output = filtered_output.to_dataframe().reset_index()
        t_df = pd.DataFrame(
            {'gust': filtered_output['gust'], 
             'Lat': filtered_output['gridlat_0'], 
             'Lon': filtered_output['gridlon_0']
            }
        )
        
        surrogate_report = t_df.dropna().reset_index(drop=True)
        #Appends surrogate reports from slice to surrogate list
        surrogate_report_list.append(surrogate_report)
    surrogate_df = pd.concat(surrogate_report_list)
    surrogate_df.index = pd.RangeIndex.from_range(range(len(surrogate_df.index)))
    
    return surrogate_df

#Change this method
def max_0_3_uh_surrogates(model_outputs, thresholds):
    surrogate_report_list = []
    outputs = model_outputs[1:-1] #max so its offset
    for output in outputs:
        uh_03_dt = make_03_UH_dt(output)
        filtered_output =uh_03_dt.where(uh_03_dt.land == 1, drop=True)
        filtered_output = filtered_output.where(filtered_output.uh_03 > thresholds["uh_03"], drop=True)
        filtered_output = filtered_output.to_dataframe().reset_index()
        t_df = pd.DataFrame(
            {'uh_03': filtered_output['uh_03'], 
             'Lat': filtered_output['gridlat_0'], 
             'Lon': filtered_output['gridlon_0']
            }
        )
        
        surrogate_report = t_df.dropna().reset_index(drop=True)
        #Appends surrogate reports from slice to surrogate list
        surrogate_report_list.append(surrogate_report)
    surrogate_df = pd.concat(surrogate_report_list)
    surrogate_df.index = pd.RangeIndex.from_range(range(len(surrogate_df.index)))
    
    return surrogate_df

#Change this method
def max_2_5_uh_surrogates(model_outputs, thresholds):
    surrogate_report_list = []
    outputs = model_outputs[1:-1] #max so its offset
    for output in outputs:
        uh_25_dt = make_25_UH_dt(output)
        filtered_output = uh_25_dt.where(uh_25_dt.land == 1, drop=True)
        filtered_output = filtered_output.where(filtered_output.uh_25 > thresholds["uh_25"], drop=True)
        filtered_output = filtered_output.to_dataframe().reset_index()
        t_df = pd.DataFrame(
            {'uh_25': filtered_output['uh_25'], 
             'Lat': filtered_output['gridlat_0'], 
             'Lon': filtered_output['gridlon_0']
            }
        )
        
        surrogate_report = t_df.dropna().reset_index(drop=True)
        #Appends surrogate reports from slice to surrogate list
        surrogate_report_list.append(surrogate_report)
    surrogate_df = pd.concat(surrogate_report_list)
    surrogate_df.index = pd.RangeIndex.from_range(range(len(surrogate_df.index)))
    
    return surrogate_df



def run_analysis(thresholds, forecast_h, folder, grouped_reports):
    #
    day_analysis = {}
    #15 hour forecasts
    for sreports in grouped_reports:
        window_analysis = {}
        window_datetime = sreports[0]
        storm_reports = sreports[1]
        #Get the surrogate storm reports
        # print("time window", sreports[0])
        # print("number of reports", sreports[1].shape[0])
        # print("storm reports", sreports[1]["Lat"])
        surrogate_reports_df = get_surrogate_storm_reports(window_datetime=window_datetime, 
                                                    window_size=3,  
                                                    thresholds=thresholds,
                                                    forecast_h=forecast_h)

        
        #Create time window object 
        w = time_window(storm_report_df=storm_reports,rrfs_surrogate_reports_dict=surrogate_reports_df)

        analysis = w.analysis()
        
        for var in analysis.keys():
            day_analysis[var] = {"hits": 0, "misses": 0, "false_alarms": 0}
            day_analysis[var]["hits"] += analysis[var]["data"]["hits"]
            day_analysis[var]["misses"] += analysis[var]["data"]["misses"]
            day_analysis[var]["false_alarms"] += analysis[var]["data"]["false_alarms"]
            window_analysis[var] = analysis[var]["window_analysis"]
        #Return the results 
        # a = pd.DataFrame.from_dict(window_analysis, orient='index')
        break

    day_analysis_df = pd.DataFrame.from_dict(day_analysis, orient='index')
    day_analysis_df.to_csv(f"./{folder}/{day}.csv")    


def get_folder(thresholds, forecast_h):
    return f"{thresholds}-{forecast_h}"

def get_thresholds(thresholds):
    thresholds_nine_to_one = {'wind': 19.86455982, 'max_wind': 24.87193902, 'max_downdraft': -15.47703168, 'gust': 29.73619439, 'uh_03': 119.13697761, 'uh_25': 283.96103896}
    thresholds_three_to_one = {'wind': 21.18029763, 'max_wind': 26.75973054, 'max_downdraft': -18.11128219, 'gust': 31.61344086, 'uh_03': 150.07628866, 'uh_25': 360.64686152}
    if thresholds == "3t1":
        return thresholds_three_to_one
    elif thresholds == "9t1":
        return thresholds_nine_to_one
    else :
        raise Exception("no threshold")

#SCRIPT START -----------------------------------------------------------------------------------------------------------------------------------------------------------

#wind : (14, 36, 2)
# max wind :s same
# uh_25: (300,400,10)
# uh_03: (100, 200, 10)
# downdraft: (-10, -30, 2)
# gust:  (18,40,2)  

#Constants
# thresholds = {"wind":20, "max_wind": 25, "max_downdraft": -22, "gust": 32, "uh_25": 375, "uh_03": 150}


#Gets the datetime from commandline args
day_input = sys.argv[1] 
forecast_hour = int(sys.argv[2])
thresholds = sys.argv[3]

folder = get_folder(thresholds, forecast_hour)
t = get_thresholds(thresholds)


day = pd.Timestamp(day_input)
month = day.strftime('%m')
#Gets the wind reports for the given day
wind_reports_df = pd.read_csv("./wind_reports.csv")
wind_reports_df.index = wind_reports_df.datetime
wind_reports = wind_reports_df[(wind_reports_df['datetime'] >=f'2023-{month}-{day.day} 00:00:00') & (wind_reports_df['datetime'] < f'2023-{month}-{day.day + 1} 00:00:00')]   
wind_reports["datetime"] = [pd.Timestamp(date) for date in wind_reports["datetime"]]

#Prepares the dataframe to filter by time window
wind_reports = add_time_window_column(wind_reports, day)
#Groups reports by window
grouped_reports = wind_reports.groupby('window')

#Get the rrfs stuff for the correct day

run_analysis(t, forecast_hour, folder, grouped_reports)


