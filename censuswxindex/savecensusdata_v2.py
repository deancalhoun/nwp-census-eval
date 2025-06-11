################### SAVING CENSUS DATA ###########
################### VIA CENSUS API ###############
################### DEAN CALHOUN #################
################### JUN 5 2025 ###################

import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import os
from savecensusdata_helper import create_helper_file

## Setup
data_save_dir = 'census/data/acs_5yr_2023/'
os.makedirs(data_save_dir, exist_ok=True)
api_key = 'YOUR KEY HERE' # https://api.census.gov/data/key_signup.html
base_url = 'https://api.census.gov/data/2023/acs/acs5' # American Community Survey 5 year estimates released in 2023

# Create dictionary with state FIPS state and county codes
create_helper_file()
state_county_fips = pd.read_csv("state_county_fips.csv") # Read in helper file
state_county_fips_codes = {f"{i:02}": [[f"{j:03}" for j in state_county_fips[state_county_fips['State FIPS Code'] == i]['County FIPS Code'].values]] + [state_county_fips[state_county_fips['State FIPS Code'] == i]["State Name"].values[0]] for i in np.unique(state_county_fips["State FIPS Code"].values)}
    # {state FIPS code: [list of county FIPS codes, state name]}

# List of tables to download
acs_tables = [
    'B01001', 'B01002', 'B02001', 'B03001', 'B15002', 'B15003',
    'B17001', 'B19001', 'B19013', 'B23025', 'B25001', 'B25002',
    'B25003', 'B25064', 'B25077'
]

## Retrieve tract data
geo_levels = ['county', 'county subdivision', 'tract']
for state_code in list(state_county_fips_codes.keys()):
    state_name = state_county_fips_codes[state_code][1]
    save_dir = data_save_dir + '/' + 'tract' + '/' + state_name
    os.makedirs(save_dir, exist_ok=True)
    for table in acs_tables:
        outfile = save_dir + '/' + f'acs_5yr_2023_{state_name}_{table}_tract.csv'
        if os.path.exists(outfile):
            continue
        params = {
            'get': f'group({table})',
            'for': f'tract:*',
            'in': f'state:{state_code}',
            'key': api_key
        }
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data[1:], columns=data[0])
        else:
            print(f"Error: {response.status_code}, tract, {state_name}, {state_code}")
        df.to_csv(outfile, index=False)