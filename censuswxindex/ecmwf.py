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
def retrieve_forecast_data(target_dir, date, param, init_times, lead_times, filter_file, grid="0.125", model="ifs", bounds=None):
    '''
    Downloads NWP model forecast data from ECMWF MARS archive for a given date.
    
    Inputs:
        target_dir: parent directory to save data within (str)
        date: date to retrieve data for (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        init_times: list of initalization times (list of str)
        lead_times: list of lead times (list of str)
        filter_file: path to grib_filter file (str)
        grid: grid resolution (str)
        model: NWP model name (str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str) or None for global
    Outputs:
        None
    '''
    assert model in ["ifs", "aifs"]
    assert bounds is None or len(bounds) == 4

    # Create subdirectories
    for init_time in init_times:
        for lead_time in lead_times:
            path = os.path.join(target_dir, grid, param[0], init_time, lead_time)
            os.makedirs(path, exist_ok=True)
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    tempfile = os.path.join(target_dir, grid, param[0], f'ifs_fc_temp.grib')
    paths_nc = [path for init_time in init_times for lead_time in lead_times for path in glob.glob(os.path.join(*[target_dir, grid, param[0], init_time, lead_time, '*', '*', f'*{date.strftime("%Y%m%d")}.nc']))]
    paths_grib = [path for init_time in init_times for lead_time in lead_times for path in glob.glob(os.path.join(*[target_dir, grid, param[0], init_time, lead_time, '*', '*', f'*{date.strftime("%Y%m%d")}.grib']))]
    if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (paths_grib and all(os.path.exists(path) for path in paths_grib)):
        # Skip already downloaded files
        logging.info(f'Skipping already downloaded forecast data for {date}')
        return
    try:
            server.execute(
                {
                    'class': "ai" if model == "aifs" else "od",
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
                    'area': "/".join(bounds) if bounds else ["90", "-180", "-90", "180"],
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date,
                },
                tempfile
            )
            
    except Exception as e:
        logging.error(f'Forecast retrieval failed for {date}: {e}')
        return

    # Split the grib file into individual files
    subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)

    # Delete temp file
    subprocess.run(['rm', tempfile])
    return

## IFS ANALYSIS DATA ##
def retrieve_analysis_data(target_dir, date, param, valid_times, filter_file, grid="0.125", bounds=None):
    '''
    Downloads IFS analysis data from ECMWF MARS archive

    Inputs:
        target_dir: parent directory to save data within (str)
        date: date to retrieve data for (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        valid_times: list of valid times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str) or None for global
        grid: grid resolution (str)
        filter_file: path to grib_filter file (str)
    Outputs:
        None
    '''
    assert bounds is None or len(bounds) == 4

    # Create directory
    path = os.path.join(target_dir, grid, param[0])
    os.makedirs(path, exist_ok=True)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    tempfile = os.path.join(target_dir, grid, param[0], f'ifs_an_temp.grib')
    paths_nc = glob.glob(os.path.join(target_dir, grid, param[0], '*', '*', f'*{date.strftime("%Y%m%d")}.nc'))
    paths_grib = glob.glob(os.path.join(target_dir, grid, param[0], '*', '*', f'*{date.strftime("%Y%m%d")}.grib'))
    if (paths_nc and all(os.path.exists(path) for path in paths_nc)) or (paths_grib and all(os.path.exists(path) for path in paths_grib)):
        # Skip already downloaded files
        logging.info(f'Skipping already downloaded analysis data for {date}')
        return
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
                    'area': "/".join(bounds) if bounds else ["90", "-180", "-90", "180"],
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date,
                },
                tempfile
            )
                
    except Exception as e:
        logging.error(f'Analysis retrieval failed for {date}: {e}')
        return

    # Split the grib file into individual files
    subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)
    
    # Delete temp file
    subprocess.run(['rm', tempfile])
    return

class ForecastData:
    def __init__(self, target_dir, start, end, param, init_times, lead_times, filter_file, grid="0.125", model="ifs", bounds=None):
        self.target_dir = target_dir
        self.start = start
        self.end = end
        self.param = param
        self.init_times = init_times
        self.lead_times = lead_times
        self.grid = grid
        self.filter_file = filter_file
        self.model = model
        self.bounds = bounds

    def retrieve_data(self):
        dates = pd.date_range(start=self.start, end=self.end, freq='D')
        for date in dates:
            date_str = date.strftime("%Y%m%d")
            retrieve_forecast_data(self.target_dir, date_str, self.param, self.init_times, self.lead_times, self.filter_file, self.grid, self.model, self.bounds)
        return

class AnalysisData:  
    def __init__(self, target_dir, start, end, param, valid_times, filter_file, grid="0.125", bounds=None):
        self.target_dir = target_dir
        self.start = start
        self.end = end
        self.param = param
        self.valid_times = valid_times
        self.filter_file = filter_file
        self.grid = grid
        self.bounds = bounds

    def retrieve_data(self):
        dates = pd.date_range(start=self.start, end=self.end, freq='D')
        for date in dates:
            date_str = date.strftime("%Y%m%d")
            retrieve_analysis_data(self.target_dir, date_str, self.param, self.valid_times, self.filter_file, self.grid, self.bounds)
        return