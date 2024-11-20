import os
import subprocess
import numpy as np
import pandas as pd
import datetime as dt
from ecmwfapi import ECMWFService
from dateutil.relativedelta import relativedelta
from itertools import product

def create_ecmwf_folders(mode, target_directory, params = ['t2m'], start='2016-01-01', end='2023-12-31', init_times = None, lead_times = None):
    '''
    Creates directories needed to store ECMWF data
    '''
    dates = pd.date_range(start = start, end = end, freq = 'MS')

    if mode == 'fc':
        for param in params:
            for year, month in product(np.unique(dates.strftime("%Y")), np.unique(dates.strftime("%m"))):
                for init_time in init_times:
                    for lead_time in lead_times:
                        path = '/'.join([target_directory, param, init_time, lead_time, year, month])
                        os.makedirs(path, exist_ok=True)

    if mode == 'an':
        for param in params:
            for year, month in product(np.unique(dates.strftime("%Y")), np.unique(dates.strftime("%m"))):
                for init_time in init_times:
                    path = '/'.join([target_directory, param, init_time, year, month])
                    os.makedirs(path, exist_ok=True)

    if mode == 'era':
        for param in params:
            for year, month in product(np.unique(dates.strftime("%Y")), np.unique(dates.strftime("%m"))):
                path = '/'.join([target_directory, param, year, month])
                os.makedirs(path, exist_ok=True)
                        
    return

def retrieve_ifs_forecast(
    target_directory,
    params = {"t2m": "167.128"}
    start = '2016-01-01',
    end = '2023-12-31',
    init_times = ["00:00:00", "12:00:00"],
    lead_times = ["0", "6", "12", "24", "48", "72", "96", "120", "168", "240"],
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads IFS forecast data from ECMWF MARS archive
    '''
    # Ensure directories exist
    create_ecmwf_folders(mode='fc', target_directory=target_directory, params=params.keys(), start=start, end=end, init_times=init_times, lead_times=lead_times)
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'MS')
    for param in params.keys():
        for init_time in init_times:
            for lead_time in lead_times:
                for date in dates:
                    year = date.strftime("%Y")
                    month = date.strftime("%m")
                    first_day = date.strftime("%Y%m%d")
                    last_day = (date + relativedelta(months=+1, days=-1)).strftime("%Y%m%d")
                    path = '/'.join([target_directory, param, init_time, lead_time, year, month, f'ifs_fc_{param}_{year}_{month}.grib'])
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
                        'date': first_day+'/to/'+last_day
                    },
                        path)
    
    # Convert to netcdf
    subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)

    # Delete grib file
    subprocess.run(f'rm {path}')
    
    return

def retrieve_ifs_analysis(
    target_directory,
    params = {"t2m": "167.128"}
    start = '2016-01-01',
    end = '2023-12-31',
    init_times = ["00:00:00", "06:00:00", "12:00:00", "18:00:00"],
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads IFS analysis data from ECMWF MARS archive
    '''
    # Ensure directories exist
    create_ecmwf_folders(mode='an', target_directory=target_directory, params=params.keys(), start=start, end=end, init_times=init_times)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'MS')
    for param in params.keys():
        for init_time in init_times:
            for date in dates:
                year = date.strftime("%Y")
                month = date.strftime("%m")
                first_day = date.strftime("%Y%m%d")
                last_day = (date + relativedelta(months=+1, days=-1)).strftime("%Y%m%d")
                path = '/'.join([target_directory, param, init_time, year, month, f'ifs_an_{param}_{year}_{month}.grib'])
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
                    'date': first_day+'/to/'+last_day
                },
                    path)
    
    # Convert to netcdf
    subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)

    # Delete grib file
    subprocess.run(f'rm {path}')
    
    return

def retrieve_era5_climatology(
    target_directory,
    params = {"t2m": "167.128"}
    start = '2016-01-01',
    end = '2023-12-31',
    bounds = ["45","-85","35","-70"]
):
    '''
    Downloads daily climatology from ERA5 to the folder target_directory as a netcdf file
    '''
    # Ensure directories exist
    create_ecmwf_folders(mode='era', target_directory=target_directory, params=params.keys(), start=start, end=end)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")

    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'MS')
    for param in params.keys():
        for date in dates:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            first_day = date.strftime("%Y%m%d")
            last_day = (date + relativedelta(months=+1, days=-1)).strftime("%Y%m%d")
            path = '/'.join([target_directory, param, year, month, f'era5_dacl_{param}_{year}_{month}.grib'])
            server.execute({
                'class': "ea",
                'type': "em",
                'stream': "dacl",
                'expver': "1",
                'levtype': "sfc",
                'param': "167.128",
                'time': "00:00:00/06:00:00/12:00:00/18:00:00",
                'area': '/'.join(bounds),
                'grid': "0.25/0.25",
                'padding': "0",
                'expect': "any",
                'date': "2000-01-01/to/2000-12-31"
            },
                path)

    # Convert to netcdf
    subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)

    # Delete grib file
    subprocess.run(f'rm {path}')
    
    return
