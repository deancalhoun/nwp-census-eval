import requests
import numpy as np
import pandas as pd
import os

def create_helper_file():
    '''
    Creates a csv file with state and county FIPS codes and state names.
    '''
    # Dictionary associating state FIPS codes and state names
    fips_state_names = {
        "01": 'Alabama',
        "02": 'Alaska',
        "04": 'Arizona',
        "05": 'Arkansas',
        "06": 'California',
        "08": 'Colorado',
        "09": 'Connecticut',
        "10": 'Delaware',
        "11": 'District of Columbia',
        "12": 'Florida',
        "13": 'Georgia',
        "15": 'Hawaii',
        "16": 'Idaho',
        "17": 'Illinois',
        "18": 'Indiana',
        "19": 'Iowa',
        "20": 'Kansas',
        "21": 'Kentucky',
        "22": 'Louisiana',
        "23": 'Maine',
        "24": 'Maryland',
        "25": 'Massachusetts',
        "26": 'Michigan',
        "27": 'Minnesota',
        "28": 'Mississippi',
        "29": 'Missouri',
        "30": 'Montana',
        "31": 'Nebraska',
        "32": 'Nevada',
        "33": 'New Hampshire',
        "34": 'New Jersey',
        "35": 'New Mexico',
        "36": 'New York',
        "37": 'North Carolina',
        "38": 'North Dakota',
        "39": 'Ohio',
        "40": 'Oklahoma',
        "41": 'Oregon',
        "42": 'Pennsylvania',
        "44": 'Rhode Island',
        "45": 'South Carolina',
        "46": 'South Dakota',
        "47": 'Tennessee',
        "48": 'Texas',
        "49": 'Utah',
        "50": 'Vermont',
        "51": 'Virginia',
        "53": 'Washington',
        "54": 'West Virginia',
        "55": 'Wisconsin',
        "56": 'Wyoming'
    }
    geocodes = pd.read_csv('all-geocodes-v2023.csv', skiprows=4) # Read in geocode info from US Census Bureau
    geocodes = geocodes[geocodes["Summary Level"] == 50] # County level
    state_county_fips = geocodes[["State Code (FIPS)", "County Code (FIPS)"]]
    state_county_fips = state_county_fips.rename(columns={"State Code (FIPS)": "State FIPS Code", "County Code (FIPS)": "County FIPS Code"})
    state_county_fips = state_county_fips[state_county_fips["County FIPS Code"]!=0].reset_index(drop=True) # Remove null entries
    state_county_fips = state_county_fips[state_county_fips["State FIPS Code"]<72].reset_index(drop=True) # Remove territories
    state_county_fips["State FIPS Code"] = [f"{i:02}" for i in state_county_fips['State FIPS Code']]
    state_county_fips["County FIPS Code"] = [f"{i:03}" for i in state_county_fips['County FIPS Code']]
    state_county_fips["State Name"] = [fips_state_names[i] for i in state_county_fips["State FIPS Code"].values]
    state_county_fips.to_csv("state_county_fips.csv", index=False)
    return

## Setup
data_save_dir = '/glade/derecho/scratch/dcalhoun/census/data/acs_5yr_2023/'
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