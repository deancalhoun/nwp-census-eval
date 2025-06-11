import os
import logging
import subprocess
import glob
import numpy as np
import pandas as pd
from ecmwfapi import ECMWFService

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

## IFS FORECAST DATA ##
def retrieve_ifs_forecast(target_dir, start, end, param, init_times, lead_times, bounds, grid, filter_file):
    '''
    Downloads IFS forecast data from ECMWF MARS archive
    
    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        init_times: list of initalization times (list of str)
        lead_times: list of lead times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
        grid: grid resolution (str)
        filter_file: path to grib_filter file (str)
    Outputs:
        None
    '''
    # Create subdirectories
    for init_time in init_times:
        for lead_time in lead_times:
            path = os.path.join(target_dir, grid, param[0], init_time, lead_time)
            os.makedirs(path, exist_ok=True)
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    tempfile = os.path.join(target_dir, grid, param[0], f'ifs_fc_temp.grib')
    dates = pd.date_range(start=start, end=end, freq='D')
    for date in dates:
        paths_nc = [path for init_time in init_times for lead_time in lead_times for path in glob.glob(os.path.join(*[target_dir, grid, param[0], init_time, lead_time, '*', '*', f'*{date.strftime("%Y%m%d")}.nc']))]
        paths_grib = [path for init_time in init_times for lead_time in lead_times for path in glob.glob(os.path.join(*[target_dir, grid, param[0], init_time, lead_time, '*', '*', f'*{date.strftime("%Y%m%d")}.grib']))]
        if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (paths_grib and all(os.path.exists(path) for path in paths_grib)):
            # Skip already downloaded files
            logging.info(f'Skipping already downloaded forecast data for {date.strftime("%Y-%m-%d")}')
            continue
        try:
            server.execute(
                {
                    'class': "od",
                    'type': "fc",
                    'stream': "oper",
                    'expver': "1",
                    'repres': "gg",
                    'levtype': "sfc",
                    'param': param[1],
                    'time': "/".join(init_times),
                    'step': "/".join(lead_times),
                    'domain': "g",
                    'resol': "auto",
                    'area': "/".join(bounds),
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date.strftime("%Y%m%d"),
                },
                tempfile
            )
            
        except Exception as e:
            logging.error(f'Forecast retrieval failed for {date.strftime("%Y-%m-%d")}: {e}')
            continue

        # Split the grib file into individual files
        subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)

        # Delete temp file
        subprocess.run(['rm', tempfile])
    return

## IFS ANALYSIS DATA ##
def retrieve_ifs_analysis(target_dir, start, end, param, valid_times, bounds, grid, filter_file):
    '''
    Downloads IFS analysis data from ECMWF MARS archive

    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        valid_times: list of valid times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
        grid: grid resolution (str)
        filter_file: path to grib_filter file (str)
    Outputs:
        None
    '''
    # Create directory
    path = os.path.join(target_dir, grid, param[0])
    os.makedirs(path, exist_ok=True)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    tempfile = os.path.join(target_dir, grid, param[0], f'ifs_an_temp.grib')
    dates = pd.date_range(start=start, end=end, freq='D')
    for date in dates:
        paths_nc = glob.glob(os.path.join(target_dir, grid, param[0], '*', '*', f'*{date.strftime("%Y%m%d")}.nc'))
        paths_grib = glob.glob(os.path.join(target_dir, grid, param[0], '*', '*', f'*{date.strftime("%Y%m%d")}.grib'))
        if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (paths_grib and all(os.path.exists(path) for path in paths_grib)):
            # Skip already downloaded files
            logging.info(f'Skipping already downloaded analysis data for {date.strftime("%Y-%m-%d")}')
            continue
        try:
            server.execute(
                {
                    'class': "od",
                    'type': "an",
                    'stream': "oper",
                    'expver': "1",
                    'repres': "gg",
                    'levtype': "sfc",
                    'param': param[1],
                    'time': "/".join(valid_times),
                    'step': "0",
                    'domain': "g",
                    'resol': "auto",
                    'area': "/".join(bounds),
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date.strftime("%Y%m%d")
                },
                tempfile
            )
                
        except Exception as e:
            logging.error(f'Analysis retrieval failed for {date.strftime("%Y-%m-%d")}: {e}')
            continue

        # Split the grib file into individual files
        subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)
        
        # Delete temp file
        subprocess.run(['rm', tempfile])
    return