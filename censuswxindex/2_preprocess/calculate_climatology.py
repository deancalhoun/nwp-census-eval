import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import censuswxindex as cwi

## Define parameters
era_dir = '/glade/campaign/collections/rda/data/d633000/e5.oper.an.sfc/'
era_save_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/era5'
era_params = ['2t']
start_era = '1991-01-01'
end_era = '2020-12-31'

## Calculate ERA5 climatology
era_files = cwi.calculate_era5_climatology(
    era_dir = era_dir,
    save_dir = era_save_dir,
    params = era_params,
    start = start_era,
    end = end_era
)