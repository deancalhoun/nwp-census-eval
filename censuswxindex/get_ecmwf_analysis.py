import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import censuswxindex as cwi

parser = argparse.ArgumentParser(description='Process data for a specific year.')
parser.add_argument('--year', type=int, required=True, help='The year to process data for')

# Parse the arguments
args = parser.parse_args()

# Define directory
ifs_an_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an'

# Define parameters
start = f'{args.years}-01-01'
end = f'{args.year}-12-31'
param = ('2t', '167.128')
valid_times = ['0000', '0600', '1200', '1800']
bounds = ['49.5','-125','24.5','-66.5'] # CONUS
grid = '0.125'
an_filter_file = '/glade/u/home/dcalhoun/CensusWxIndex/censuswxindex/split_an.txt'

cwi.retrieve_ifs_analysis(
    target_dir = ifs_an_dir,
    start = start,
    end = end,
    param = param,
    valid_times = valid_times,
    bounds = bounds,
    grid = grid,
    filter_file = an_filter_file
)