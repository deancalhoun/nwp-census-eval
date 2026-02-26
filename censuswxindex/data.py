import os
import logging
import subprocess
import glob
import requests
import us
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

class ForecastDataClient:
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

class AnalysisDataClient:  
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

def retrieve_census_data(target_dir, table_list, level, base_url):
    '''
    Downloads census data from the U.S. Census Bureau API.

    Inputs:
        target_dir: parent directory to save data within (str)
        table_list: list of census table codes to download (list of str)
        level: geographic level of data to download (str)
        base_url: base URL for the Census API (str)
    Outputs:
        None
    '''
    ## Setup
    os.makedirs(target_dir, exist_ok=True)
    api_key = os.environ.get('CENSUS_API_KEY')  # Set your API key in the environment variable CENSUS_API_KEY
    if not api_key:
        raise ValueError("Census API key not found. Please set the 'CENSUS_API_KEY' environment variable. See https://api.census.gov/data/key_signup.html")
    if level not in ['state', 'county', 'tract']:
        raise ValueError("Invalid level specified. Choose from 'state', 'county', or 'tract'.")
    if not isinstance(table_list, list) or not all(isinstance(table, str) for table in table_list):
        raise ValueError("table_list must be a list of strings representing table codes.")
    if not table_list:
        raise ValueError("table_list cannot be empty. Please provide at least one table code.")

    # Dictionary associating state FIPS codes and state abbreviations
    fips_state_names = {
        "01": 'AL',
        "02": 'AK',
        "04": 'AZ',
        "05": 'AR',
        "06": 'CA',
        "08": 'CO',
        "09": 'CT',
        "10": 'DE',
        "11": 'DC',
        "12": 'FL',
        "13": 'GA',
        "15": 'HI',
        "16": 'ID',
        "17": 'IL',
        "18": 'IN',
        "19": 'IA',
        "20": 'KS',
        "21": 'KY',
        "22": 'LA',
        "23": 'ME',
        "24": 'MD',
        "25": 'MA',
        "26": 'MI',
        "27": 'MN',
        "28": 'MS',
        "29": 'MO',
        "30": 'MT',
        "31": 'NE',
        "32": 'NV',
        "33": 'NH',
        "34": 'NJ',
        "35": 'NM',
        "36": 'NY',
        "37": 'NC',
        "38": 'ND',
        "39": 'OH',
        "40": 'OK',
        "41": 'OR',
        "42": 'PA',
        "44": 'RI',
        "45": 'SC',
        "46": 'SD',
        "47": 'TN',
        "48": 'TX',
        "49": 'UT',
        "50": 'VT',
        "51": 'VA',
        "53": 'WA',
        "54": 'WV',
        "55": 'WI',
        "56": 'WY'
    }

    ## Retrieve data
    for state_code in fips_state_names.keys():
        state_name = fips_state_names.get(state_code)
        save_dir = os.path.join(target_dir, level, state_name)
        os.makedirs(save_dir, exist_ok=True)
        for table in table_list:
            outfile = os.path.join(save_dir, f'acs_5yr_2023_{level}_{state_name}_{table}.csv')
            if os.path.exists(outfile):
                logging.info(f'Skipping already downloaded data for {state_name}, table {table}')
                continue
            params = {
                'get': f'group({table})',
                'for': f'{level}:*',
                'in': f'state:{state_code}',
                'key': api_key
            }
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data[1:], columns=data[0])
                df.to_csv(outfile, index=False)
            else:
                logging.error(f"Error: {state_name}, URL: {response.url}, Status Code: {response.status_code}")
    return

class CensusDataClient:
    pass