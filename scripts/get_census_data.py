import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from censuswxindex.censusdata import retrieve_census_data

# # List of tables to download
# acs_tables = [
#     'B01001', 'B01002', 'B02001', 'B03001', 'B15002', 'B15003',
#     'B17001', 'B19001', 'B19013', 'B23025', 'B25001', 'B25002',
#     'B25003', 'B25064', 'B25077'
# ]

# retrieve_census_data(
#     target_dir = '/glade/derecho/scratch/dcalhoun/census/data/acs_5yr_2023',
#     table_list = acs_tables
# )

print('Hello world!')