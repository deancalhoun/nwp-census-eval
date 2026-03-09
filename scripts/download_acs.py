import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from censuswxindex.data.census import retrieve_census_data

# Parameters
acs_dir = '/glade/derecho/scratch/dcalhoun/census/data/acs_5yr_2023'
acs_tables = [
    'B01001', 'B01002', 'B02001', 'B03001', 'B15002', 'B15003',
    'B17001', 'B19001', 'B19013', 'B23025', 'B25001', 'B25002',
    'B25003', 'B25064', 'B25077'
]
level = 'tract'  # Census tract level
acs_url = 'https://api.census.gov/data/2023/acs/acs5' # Base URL for American Community Survey (ACS) 5-Year Estimates

# Retrieve data
retrieve_census_data(
    target_dir = acs_dir,
    table_list = acs_tables,
    level=level,
    base_url=acs_url,
)