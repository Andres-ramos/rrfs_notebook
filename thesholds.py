import sys
import pandas as pd
from pipeline.storm_report import make_time_windows
from rrfs import rrfs
import xarray as xr
import json
from pipeline.datasets import make_wind_dt, make_max_wind_dt, make_25_UH_dt, make_03_UH_dt, make_downdraft_dt, make_gust_dt


def get_surrogate_numbers(window_datetime, window_size, thresholds, forecast_h):
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


    surrogate_reports = {
        'max_wind': max_10_wind_surrogate_numbers(model_outputs, thresholds['max_wind']),
        'uh_25': max_2_5_uh_surrogate_numbers(model_outputs, thresholds["uh_25"]),
        'uh_03': max_0_3_uh_surrogate_numbers(model_outputs, thresholds["uh_03"]),
        'max_downdraft': max_downdraft_surrogate_numbers(model_outputs, thresholds["max_downdraft"]),
        'wind': wind_10_m_surrogate_numbers(model_outputs, thresholds["wind"]),
        'gust': gust_surrogates_surrogate_numbers(model_outputs, thresholds["gust"])
        
    }
    return surrogate_reports

def max_10_wind_surrogate_numbers(model_outputs, threshold_list):
    outputs = model_outputs[1:-1] #Max so outputs are offset
    surrogate_numbers = {}
    for t in threshold_list:
        # print(f"testing thresholg {t}")
        num_surrogates = 0
        for output in outputs:
            wind_dt = make_max_wind_dt(output)
            #Filters out entries over the sea
            filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
            #Filters values under the threshold
            filtered_output = filtered_output.where(filtered_output.max_wind > t, drop=True)
            #resets index
            filtered_output = filtered_output.to_dataframe().reset_index()
            t_df = pd.DataFrame(
                {'max_wind': filtered_output['max_wind'], 
                'Lat': filtered_output['gridlat_0'], 
                'Lon': filtered_output['gridlon_0']
                }
            )
        num_surrogates += t_df.shape[0]
        # print(f"threshold {t} number of surrogates {num_surrogates}")
        surrogate_numbers[t] = num_surrogates
    
    return surrogate_numbers

def wind_10_m_surrogate_numbers(model_outputs, threshold_list):
    outputs = model_outputs[0:2] #Max so outputs are offset
    surrogate_numbers = {}
    for t in threshold_list:
        # print(f"testing thresholg {t}")
        num_surrogates = 0
        for output in outputs:
            wind_dt = make_wind_dt(output)
            #Filters out entries over the sea
            filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
            #Filters values under the threshold
            filtered_output = filtered_output.where(filtered_output.wind > t, drop=True)
            #resets index
            filtered_output = filtered_output.to_dataframe().reset_index()
            t_df = pd.DataFrame(
                {'wind': filtered_output['wind'], 
                'Lat': filtered_output['gridlat_0'], 
                'Lon': filtered_output['gridlon_0']
                }
            )
        num_surrogates += t_df.shape[0]
        # print(f"threshold {t} number of surrogates {num_surrogates}")
        surrogate_numbers[t] = num_surrogates
    
    return surrogate_numbers

def max_2_5_uh_surrogate_numbers(model_outputs, threshold_list):
    outputs = model_outputs[1:-1] #Max so outputs are offset
    surrogate_numbers = {}
    for t in threshold_list:
        # print(f"testing thresholg {t}")
        num_surrogates = 0
        for output in outputs:
            wind_dt = make_25_UH_dt(output)
            #Filters out entries over the sea
            filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
            #Filters values under the threshold
            filtered_output = filtered_output.where(filtered_output.uh_25 > t, drop=True)
            #resets index
            filtered_output = filtered_output.to_dataframe().reset_index()
            t_df = pd.DataFrame(
                {'uh_25': filtered_output['uh_25'], 
                'Lat': filtered_output['gridlat_0'], 
                'Lon': filtered_output['gridlon_0']
                }
            )
        num_surrogates += t_df.shape[0]
        # print(f"threshold {t} number of surrogates {num_surrogates}")
        surrogate_numbers[t] = num_surrogates
    
    return surrogate_numbers


def max_0_3_uh_surrogate_numbers(model_outputs, threshold_list):
    outputs = model_outputs[1:-1] #Max so outputs are offset
    surrogate_numbers = {}
    for t in threshold_list:
        # print(f"testing thresholg {t}")
        num_surrogates = 0
        for output in outputs:
            wind_dt = make_03_UH_dt(output)
            #Filters out entries over the sea
            filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
            #Filters values under the threshold
            filtered_output = filtered_output.where(filtered_output.uh_03 > t, drop=True)
            #resets index
            filtered_output = filtered_output.to_dataframe().reset_index()
            t_df = pd.DataFrame(
                {'uh_03': filtered_output['uh_03'], 
                'Lat': filtered_output['gridlat_0'], 
                'Lon': filtered_output['gridlon_0']
                }
            )
        num_surrogates += t_df.shape[0]
        # print(f"threshold {t} number of surrogates {num_surrogates}")
        surrogate_numbers[t] = num_surrogates
    
    return surrogate_numbers

def max_downdraft_surrogate_numbers(model_outputs, threshold_list):
    outputs = model_outputs[1:-1] #Max so outputs are offset
    surrogate_numbers = {}
    for t in threshold_list:
        num_surrogates = 0
        for output in outputs:
            wind_dt = make_downdraft_dt(output)
            #Filters out entries over the sea
            filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
            #Filters values under the threshold
            filtered_output = filtered_output.where(filtered_output.max_downdraft < t, drop=True)
            #resets index
            filtered_output = filtered_output.to_dataframe().reset_index()
            t_df = pd.DataFrame(
                {'max_downdraft': filtered_output['max_downdraft'], 
                'Lat': filtered_output['gridlat_0'], 
                'Lon': filtered_output['gridlon_0']
                }
            )
        num_surrogates += t_df.shape[0]

        surrogate_numbers[t] = num_surrogates
    
    return surrogate_numbers

def gust_surrogates_surrogate_numbers(model_outputs, threshold_list):
    outputs = model_outputs[0:2] #Max so outputs are offset
    surrogate_numbers = {}
    for t in threshold_list:
        # print(f"testing thresholg {t}")
        num_surrogates = 0
        for output in outputs:
            wind_dt = make_gust_dt(output)
            #Filters out entries over the sea
            filtered_output = wind_dt.where(wind_dt.land == 1, drop=True)
            #Filters values under the threshold
            filtered_output = filtered_output.where(filtered_output.gust > t, drop=True)
            #resets index
            filtered_output = filtered_output.to_dataframe().reset_index()
            t_df = pd.DataFrame(
                {'gust': filtered_output['gust'], 
                'Lat': filtered_output['gridlat_0'], 
                'Lon': filtered_output['gridlon_0']
                }
            )
        num_surrogates += t_df.shape[0]
        # print(f"threshold {t} number of surrogates {num_surrogates}")
        surrogate_numbers[t] = num_surrogates
    
    return surrogate_numbers


def aggregate(data, variable_numbers):

    for var in variable_numbers.keys():
        for number in variable_numbers[var]:
            data[var][number] += variable_numbers[var][number]
    return data

def threshold_ds(threshold):
    data = {}
    for var in threshold.keys():
        data[var] = {}
        for values in threshold[var]:

            data[var][values] = 0
    return data

##################################################################################3
## script start ################################################################3#
day_input = sys.argv[1]
day = pd.Timestamp(day_input)


threshold_ranges = {
    "wind": range(18, 24, 0.25),
    "max_wind": range(22, 30,0.25),
    "max_downdraft": range(-14, -32, .25),
    "gust": range(28,34,.25),
    "uh_25": range(250,450, 10),
    "uh_03": range(100, 230, 5)
}

time_windows = make_time_windows(day, 3)

results = threshold_ds(threshold_ranges)

for window in time_windows:
    numbers = get_surrogate_numbers(window_datetime=window,
                                    window_size=3,
                                    thresholds=threshold_ranges,
                                    forecast_h=3)
    
    results = aggregate(results, numbers)
  

day_file = day.strftime("%Y-%m-%d")

with open(f"./threshold_json/{day_file}.json", "w") as outfile:
    json.dump(results, outfile)


