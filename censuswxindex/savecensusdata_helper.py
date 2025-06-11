import pandas as pd

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
    geocodes = pd.read_csv('all-geocodes-v2025.csv', skiprows=4) # Read in geocode info from US Census Bureau
                                                                 # https://www.census.gov/geographies/reference-files/2020/demo/popest/2020-fips.html
                                                                 # Note: manually updated Connecticut to reflect county changes since 2020
    geocodes = geocodes[geocodes["Summary Level"] == 50]
    state_county_fips = geocodes[["State Code (FIPS)", "County Code (FIPS)"]]
    state_county_fips = state_county_fips.rename(columns={"State Code (FIPS)": "State FIPS Code", "County Code (FIPS)": "County FIPS Code"})
    state_county_fips = state_county_fips[state_county_fips["County FIPS Code"]!=0].reset_index(drop=True)
    state_county_fips = state_county_fips[state_county_fips["State FIPS Code"]<72].reset_index(drop=True)
    state_county_fips["State FIPS Code"] = [f"{i:02}" for i in state_county_fips['State FIPS Code']]
    state_county_fips["County FIPS Code"] = [f"{i:03}" for i in state_county_fips['County FIPS Code']]
    state_county_fips["State Name"] = [fips_state_names[i] for i in state_county_fips["State FIPS Code"].values]
    state_county_fips.to_csv("state_county_fips.csv", index=False)
    return