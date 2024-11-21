import os
import subprocess
import numpy as np
import pandas as pd
import datetime as dt
from ecmwfapi import ECMWFService
from dateutil.relativedelta import relativedelta
from itertools import product

def create_ifs_forecast_folders(
    target_directory,
    start = '2016-01-01',
    end = '2023-12-31',
    params = ['t2m'],
    init_times = ["00", "12"],
    lead_times = ["0", "6", "12", "24", "48", "72", "96", "120", "168", "240"]
):
    '''
    Creates directories needed to store ECMWF IFS forecast data
    '''
    dates = pd.date_range(start = start, end = end, freq = 'MS')

    for param in params:
        for init_time in init_times:
            for lead_time in lead_times:
                for date in dates:
                    valid_time = date + relativedelta(hours=int(init_time)) + relativedelta(hours=int(lead_time))
                    year = valid_time.strftime("%Y")
                    month = 
                    path = '/'.join([target_directory, param, init_time, lead_time, year, month])
                    os.makedirs(path, exist_ok=True)

def create_ifs_analysis_folders(
    target_directory,
    start = '2016-01-01',
    end = '2023-12-31',
    params = ['t2m'],
    init_times = ["00", "06", "12", "18"]
):
    '''
    Creates directories needed to store ECMWF IFS analysis data
    '''
    dates = pd.date_range(start = start, end = end, freq = 'MS')
    for param in params:
        for year, month in product(np.unique(dates.strftime("%Y")), np.unique(dates.strftime("%m"))):
            for init_time in init_times:
                path = '/'.join([target_directory, param, init_time, year, month])
                os.makedirs(path, exist_ok=True)

def create_era_climatology_folders(target_directory, params = ['t2m'], times = ["00", "06", "12", "18"]):
    '''
    Creates directories needed to store ECMWF ERA5 daily climatology data
    '''
    dates = pd.date_range(start = "2000-01-01", end = "2000-12-31", freq = 'MS')
    for param in params:
        for month, day in product(np.unique(dates.strftime("%m")), np.unique(dates.strftime("%d"))):
            for time in times:
                path = '/'.join([target_directory, param, month, day, time])
                os.makedirs(path, exist_ok=True)
                        
    return

def retrieve_ifs_forecast(
    target_directory,
    params = {"t2m": "167.128"},
    start = '2016-01-01',
    end = '2023-12-31',
    init_times = ["00", "12"],
    lead_times = ["0", "6", "12", "24", "48", "72", "96", "120", "168", "240"],
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads IFS forecast data from ECMWF MARS archive
    '''
    # Ensure directories exist
    create_ifs_forecast_folders(
        target_directory = target_directory,
        start = start,
        params = params.keys(),
        end = end,
        init_times = init_times,
        lead_times = lead_times
    )
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for param in params.keys():
        for init_time in init_times:
            for lead_time in lead_times:
                for date in dates:
                    valid_time = date + relativedelta(hours=int(init_time)) + relativedelta(hours=int(lead_time))
                    year = valid_time.strftime("%Y")
                    month = valid_time.strftime("%m")
                    day = valid_time.strftime("%d")
                    date_str = valid_time.strftime("%Y-%m-%d")
                    path = '/'.join([target_directory, param, init_time, lead_time, year, month, f'ifs_fc_{param}_{year}_{month}_{day}.grib'])
                    server.execute({
                        'class': "od",
                        'type': "fc",
                        'stream': "oper",
                        'expver': "1",
                        'repres': "gg",
                        'levtype': "sfc",
                        'param': params[param],
                        'time': init_time,
                        'step': lead_time,
                        'domain': "g",
                        'resol': "auto",
                        'area': '/'.join(bounds),
                        'grid': "0.125/0.125",
                        'padding': "0",
                        'expect': "any",
                        'date': date_str
                    },
                        path)
    
                    # Convert to netcdf
                    subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
                
                    # Delete grib file
                    subprocess.run(f'rm {path}')
    return

def retrieve_ifs_analysis(
    target_directory,
    params = {"t2m": "167.128"},
    start = '2016-01-01',
    end = '2023-12-31',
    init_times = ["00", "06", "12", "18"],
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads IFS analysis data from ECMWF MARS archive
    '''
    # Ensure directories exist
    create_ifs_analysis_folders(
        target_directory = target_directory,
        params = params.keys(), 
        start = start,
        end = end,
        init_times = init_times
    )

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for param in params.keys():
        for init_time in init_times:
            for date in dates:
                year = date.strftime("%Y")
                month = date.strftime("%m")
                day = date.strftime("%d")
                date_str = date.strftime("%Y-%m-%d")
                path = '/'.join([target_directory, param, init_time, year, month, f'ifs_an_{param}_{year}_{month}_{day}.grib'])
                server.execute({
                    'class': "od",
                    'type': "an",
                    'stream': "oper",
                    'expver': "1",
                    'repres': "gg",
                    'levtype': "sfc",
                    'param': params[param],
                    'time': init_time,
                    'step': "0",
                    'domain': "g",
                    'resol': "auto",
                    'area': '/'.join(bounds),
                    'grid': "0.125/0.125",
                    'padding': "0",
                    'expect': "any",
                    'date': date_str
                },
                    path)
    
                # Convert to netcdf
                subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
            
                # Delete grib file
                subprocess.run(f'rm {path}')
    
    return

def retrieve_era5_climatology(
    target_directory,
    params = {"t2m": "167.128"},
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads daily climatology from ERA5 to the folder target_directory as a netcdf file
    '''
    # Ensure directories exist
    create_era_climatology_folders(
        target_directory=target_directory, params=params.keys(),
        start=start,
        end=end
    )

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")

    # Retrieve data
    dates = pd.date_range(start = "2000-01-01", end = "2000-12-31", freq = 'MS')
    for param in params.keys():
        for date in dates:
            month = date.strftime("%m")
            day = date.strftime("%d")
            date_str = date.strftime("2000-%m-%d")
            path = '/'.join([target_directory, param, year, month, f'era5_dacl_{param}_{month}_{day}.grib'])
            server.execute({
                'class': "ea",
                'type': "em",
                'stream': "dacl",
                'expver': "1",
                'levtype': "sfc",
                'param': "167.128",
                'time': "00/06/12/18",
                'area': '/'.join(bounds),
                'grid': "0.25/0.25",
                'padding': "0",
                'expect': "any",
                'date': date_str
            },
                path)

            # Convert to netcdf
            subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
        
            # Delete grib file
            subprocess.run(f'rm {path}')
    
    return

def calculate_forecast_anomalies():
    return

def calculate_analysis_anomalies():
    return

def calculate_RMSE():
    return

def calculate_ACC():
    return

if __name__ == '__main__':
    dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs'
    retrieve_ifs_forecast('/'.join([dir, "fc"])
    retrieve_ifs_analysis('/'.join([dir, "an"])
    retrieve_era5_climatology('/'.join([dir, "dacl"])