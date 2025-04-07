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
ifs_fc_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc'

# Define parameters
start = f'{args.year}-01-01'
end = f'{args.year}-12-31'
param = ('2t', '167.128')
init_times = ['0000', '1200']
lead_times = ['0', '6', '12', '18', '24', '48', '72', '96', '120', '168', '240']
bounds = ['49.5','-125','24.5','-66.5'] # CONUS
grid = '0.125'
fc_filter_file = '/glade/u/home/dcalhoun/CensusWxIndex/censuswxindex/split_fc.txt'

# Retrieve data
cwi.retrieve_ifs_forecast(
    target_dir = ifs_fc_dir,
    start = start,
    end = end,
    param = param,
    init_times = init_times,
    lead_times = lead_times,
    bounds = bounds,
    grid = grid,
    filter_file = fc_filter_file
)