import os
import subprocess
import numpy as np
import pandas as pd
import datetime as dt
import netCDF4 as nc
import xarray as xr
from ecmwfapi import ECMWFService
from dateutil.relativedelta import relativedelta
from itertools import product

def create_ifs_forecast_folders(
    target_dir,
    start = '2016-01-01',
    end = '2024-01-01',
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
                    year = date.strftime("%Y")
                    month = date.strftime("%m")
                    path = '/'.join([target_dir, param, init_time, lead_time, year, month])
                    os.makedirs(path, exist_ok=True)

def create_ifs_analysis_folders(
    target_dir,
    start = '2016-01-01',
    end = '2024-01-01',
    params = ['t2m']
):
    '''
    Creates directories needed to store ECMWF IFS analysis data
    '''
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for param in params:
        for date in dates:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            path = '/'.join([target_dir, param, year, month, day])
            os.makedirs(path, exist_ok=True)

def create_era_climatology_folders(target_dir, params = ['t2m']):
    '''
    Creates directories needed to store ECMWF ERA5 daily climatology data
    '''
    dates = pd.date_range(start = "2000-01-01", end = "2000-12-31", freq = 'MS')
    for param in params:
        for date in dates:
            month = date.strftime("%m")
            day = date.strftime("%d")
            path = '/'.join([target_dir, param, month, day])
            os.makedirs(path, exist_ok=True)
                        
    return

def retrieve_ifs_forecast(
    target_dir,
    start = '2016-01-01',
    end = '2024-01-01',
    params = {"t2m": "167.128"},
    init_times = ["00", "12"],
    lead_times = ["0", "6", "12", "24", "48", "72", "96", "120", "168", "240"],
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads IFS forecast data from ECMWF MARS archive
    '''
    # Ensure directories exist
    create_ifs_forecast_folders(
        target_dir = target_dir,
        start = start,
        end = end,
        params = params.keys(),
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
                    hour = valid_time.strftime("%H")
                    date_str = valid_time.strftime("%Y-%m-%d")
                    path = '/'.join([
                        target_dir, param, init_time, lead_time, year, month, f'ifs_fc_{param}_{year}_{month}_{day}_{hour}z.grib'
                    ])
                    try:
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
                        
                    except Exception:
                        print(f'Unable to retrieve forecast data for {"_".join([init_time,lead_time,date.strftime("%Y-%m-%d")])}')
                        pass
    
                    # Convert to netcdf
                    subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
                
                    # Delete grib file
                    subprocess.run(f'rm {path}')
    return

def retrieve_ifs_analysis(
    target_dir,
    params = {"t2m": "167.128"},
    start = '2016-01-01',
    end = '2024-01-10',
    times = ["00", "06", "12", "18"],
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads IFS analysis data from ECMWF MARS archive
    '''
    # Ensure directories exist
    create_ifs_analysis_folders(
        target_dir = target_dir,
        params = params.keys(), 
        start = start,
        end = end
    )

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for param in params.keys():
        for date in dates:
            for time in times:
                year = date.strftime("%Y")
                month = date.strftime("%m")
                day = date.strftime("%d")
                date_str = date.strftime("%Y-%m-%d")
                path = '/'.join([target_dir, param, init_time, year, month, day, f'ifs_an_{param}_{year}_{month}_{day}_{time}z.grib'])
                try:
                    server.execute({
                        'class': "od",
                        'type': "an",
                        'stream': "oper",
                        'expver': "1",
                        'repres': "gg",
                        'levtype': "sfc",
                        'param': params[param],
                        'time': time,
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
                    
                except Exception:
                        print(f'Unable to retrieve analysis data for {date.strftime("%Y-%m-%d-")+time+"z"}')
                        pass
    
                # Convert to netcdf
                subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
            
                # Delete grib file
                subprocess.run(f'rm {path}')
    
    return

def retrieve_era5_climatology(
    target_dir,
    params = {"t2m": "167.128"},
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads daily climatology from ERA5 to the folder target_dir as a netcdf file
    '''
    # Ensure directories exist
    create_era_climatology_folders(
        target_dir = target_dir,
        params = params.keys(),
        start = start,
        end = end,
        times = ["00", "06", "12", "18"]
    )

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")

    # Retrieve data
    dates = pd.date_range(start = "2000-01-01", end = "2000-12-31", freq = 'MS')
    for param in params.keys():
        for date in dates:
            for time in times:
                month = date.strftime("%m")
                day = date.strftime("%d")
                date_str = date.strftime("2000-%m-%d")
                path = '/'.join([target_dir, param, month, day, f'era5_dacl_{param}_{month}_{day}_{time}z.grib'])
                try:
                    server.execute({
                        'class': "ea",
                        'type': "em",
                        'stream': "dacl",
                        'expver': "1",
                        'levtype': "sfc",
                        'param': "167.128",
                        'time': time,
                        'area': '/'.join(bounds),
                        'grid': "0.25/0.25",
                        'padding': "0",
                        'expect': "any",
                        'date': date_str
                    },
                        path)

                except Exception:
                        print(f'Unable to retrieve ERA data for {date.strftime("%m-%d-")+time+"z"}')
                        pass
    
                # Convert to netcdf
                subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
            
                # Delete grib file
                subprocess.run(f'rm {path}')
    
    return

def calculate_forecast_anomalies(
    target_dir,
    fc_dir,
    dacl_dir,
    start = ,
    end = ,
    params = ,
    init_times = ,
    lead_times = ,
    bounds = 
):
    '''
    Calculates the difference between forecast and climatology
    '''

    # Create anomaly directories
    create_ifs_forecast_folders(
        target_dir = target_dir,
        start = start,
        end = end,
        params = params.keys(),
        init_times = init_times,
        lead_times = lead_times
    )
    
    # Read in forecast and climatology
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for param in params.keys():
        init_time in init_times:
            for lead_time in lead_times:
                for date in dates:
                    valid_time = date + relativedelta(hours=int(init_time)) + relativedelta(hours=int(lead_time))
                    year = valid_time.strftime("%Y")
                    month = valid_time.strftime("%m")
                    day = valid_time.strftime("%d")
                    hour = valid_time.strftime("%H")
                    fc_path = '/'.join([
                            fc_dir, param, init_time, lead_time, year, month, f'ifs_fc_{param}_{year}_{month}_{day}_{hour}z.nc'
                        ])
                    dacl_path = '/'.join([dacl_dir, param, month, day, f'era5_dacl_{param}_{month}_{day}_{hour}z.nc'])

                    fc_ds = xr.open_dataset(fc_path)
                    dacl_ds = xr.open_dataset(dacl_path)

    # Ensure grid alignment

    # Subtract climatology

    # Save out
    
    return

def calculate_analysis_anomalies(
    target_dir,
    start = ,
    end = ,
    params = ,
    times = ,
    bounds = 
):
    '''
    Calculates the differencd between analysis and climatology
    '''

    # Create anomaly directories
    create_ifs_analysis_folders(
        target_dir = target_dir,
        params = params.keys(), 
        start = start,
        end = end
    )

    # Iterate over dates

    # Read in analysis and climatology

    # Ensure grid alignment

    # Subtract climatology

    # Save out
    
    return

def calculate_RMSE(
    target_dir,
    fc_anom_dir,
    an_anom_dir,
    start =,
    end = ,
    params = ,
    
):
    '''
    Calculates the RMSE between forecast anomalies and analysis anomalies
    '''

    # Create directories

    # Iterate over dates

    # Read in forecast and analysis anomalies

    # Ensure grid alignment

    # Calculate RMSE

    # Save out
    
    return

def calculate_ACC():
    '''
    Calculates the anomaly correlation coefficient,
    the correlation between forecast anomalies and analysis anomalies
    '''

    # Create directories

    # Iterate over dates

    # Read in forecast and analysis anomalies

    # Ensure grid alignment

    # Calculate ACC

    # Save out
    
    return

if __name__ == '__main__':
    dir = '/glade/derecho/scratch/dcalhoun/ecmwf_ifs'
    retrieve_ifs_forecast('/'.join([dir, "fc"])
    retrieve_ifs_analysis('/'.join([dir, "an"])
    retrieve_era5_climatology('/'.join([dir, "dacl"])
    calculate_forecast_anomalies()
    calculate_analysis_anomalies()
    calculate_RMSE()
    calculate_ACC()