import requests
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

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