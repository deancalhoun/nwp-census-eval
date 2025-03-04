import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
import glob as glob
from preprocessing import calculate_era5_climatology
from preprocessing import calculate_rmse
from preprocessing import calculate_acc

## Define parameters
# ERA
era_dir = '/glade/campaign/collections/rda/data/d633000/e5.oper.an.sfc/'
era_save_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/era5'
params = ['t2m']
start_era = '1991-01-01'
end_era = '2020-12-31'

# IFS/AIFS
start_ifs = '2024-03-01'
start_aifs = '2024-03-01'
end = '2024-12-31'
ifs_an_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an'
ifs_fc_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc'
aifs_fc_dir = 'glade/derecho/scratch/dcalhoun/ecmwf/aifs/fc'
lead_times = ['06', '12', '24']
save_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/scores'

## Calculate ERA5 climatology
era_files = calculate_era5_climatology(
    era_dir = era_dir,
    save_dir = era_save_dir,
    params = params,
    start = start_era,
    end = end_era
)

## Calculate RMSE
# IFS
ifs_rmse_files = calculate_rmse(
    fc_dir = ifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = save_dir+'/ifs/rmse',
    model_name = 'ifs',
    start = start_ifs,
    end = end,
    lead_times = lead_times
)

# AIFS
aifs_rmse_files = calculate_rmse(
    fc_dir = aifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = save_dir+'/aifs/rmse',
    model_name = 'aifs',
    start = start_aifs,
    end = end,
    lead_times = lead_times
)

## Calculate ACC
# IFS
ifs_acc_files = calculate_acc(
    fc_dir = ifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = save_dir+'/ifs/acc',
    model_name = 'ifs',
    start = start_ifs,
    end = end,
    lead_times = lead_times
)

# AIFS
aifs_acc_files = calculate_acc(
    fc_dir = aifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = save_dir+'/aifs/acc',
    model_name = 'aifs',
    start = start_aifs,
    end = end,
    lead_times = lead_times
)