import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
import glob as glob
from preprocessing import calculate_era5_climatology
from preprocessing import calculate_RMSE
from preprocessing import calculate_ACC

## Define parameters
era_path = '/glade/campaign/collections/rda/data/d633000/e5.oper.an.sfc/'
era_dir = '/glade/derecho/scratch/dcalhoun/era5'
params = ['t2m']
start_era = 
end_era = 
start_ifs = 
start_aifs = 
end = 
ifs_dir = 
aifs_dir = 
lead_times = 

## Calculate ERA5 climatology
calculate_era5_climatology(
    era_path = era_path,
    save_dir = era_dir,
    params = params,
    start = start_era,
    end = end_era
)

## Calculate RMSE
# IFS
calculate_RMSE(
    fc_dir = ,
    an_dir = ,
    clim_path = ,
    save_dir = ,
    model_name = ,
    start = ,
    end = ,
    lead_times = 
)

# AIFS
calculate_RMSE(
    fc_dir = ,
    an_dir = ,
    clim_path = ,
    save_dir = ,
    model_name = ,
    start = ,
    end = ,
    lead_times = 
)

## Calculate ACC
# IFS
calculate_ACC(
    fc_dir = ,
    an_dir = ,
    clim_path = ,
    save_dir = ,
    model_name = ,
    start = ,
    end = ,
    lead_times = 
)

# AIFS
calculate_ACC(
    fc_dir = ,
    an_dir = ,
    clim_path = ,
    save_dir = ,
    model_name = ,
    start = ,
    end = ,
    lead_times = 
)