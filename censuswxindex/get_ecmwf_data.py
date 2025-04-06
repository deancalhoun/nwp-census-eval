import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import censuswxindex as cwi

# Define directory
ifs_fc_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc'
ifs_an_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an'

# Define parameters
start = '2016-01-01'
end = '2024-12-31'
param = ('2t', '167.128')
init_times = ['0000', '1200']
lead_times = ['0', '6', '12', '18', '24', '48', '72', '96', '120', '168', '240']
valid_times = ['0000', '0600', '1200', '1800']
bounds = ['49.5','-125','24.5','-66.5'] # CONUS
grid = '0.125'
fc_filter_file = '/glade/u/home/dcalhoun/CensusWxIndex/censuswxindex/split_fc.txt'
an_filter_file = '/glade/u/home/dcalhoun/CensusWxIndex/censuswxindex/split_an.txt'

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