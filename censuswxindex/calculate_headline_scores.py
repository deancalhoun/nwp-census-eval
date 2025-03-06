import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import censuswxindex as cwi

## Define parameters
# ERA
era_dir = '/glade/campaign/collections/rda/data/d633000/e5.oper.an.sfc/'
era_save_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/era5'
era_params = ['2t']
start_era = '1991-01-01'
end_era = '2020-12-31'

# IFS/AIFS
start_ifs = '2024-03-01'
start_aifs = '2024-03-01'
end = '2024-12-31'
ifs_an_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an/t2m'
ifs_fc_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc/t2m'
aifs_fc_dir = 'glade/derecho/scratch/dcalhoun/ecmwf/aifs/fc/t2m'
lead_times = ['00', '06', '12', '24', '48', '72', '96', '120', '168', '240'] 
save_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/scores'

## Calculate ERA5 climatology
era_files = cwi.calculate_era5_climatology(
    era_dir = era_dir,
    save_dir = era_save_dir,
    params = era_params,
    start = start_era,
    end = end_era
)

## Calculate RMSE
# IFS
ifs_rmse_files = cwi.calculate_rmse(
    fc_dir = ifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = os.path.join(save_dir, 'ifs', 'rmse'),
    model_name = 'ifs',
    start = start_ifs,
    end = end,
    lead_times = lead_times
)

# AIFS
aifs_rmse_files = cwi.calculate_rmse(
    fc_dir = aifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = os.path.join(save_dir, 'aifs', 'rmse'),
    model_name = 'aifs',
    start = start_aifs,
    end = end,
    lead_times = lead_times
)

## Calculate ACC
# IFS
ifs_acc_files = cwi.calculate_acc(
    fc_dir = ifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = os.path.join(save_dir, 'ifs', 'acc'),
    model_name = 'ifs',
    start = start_ifs,
    end = end,
    lead_times = lead_times
)

# AIFS
aifs_acc_files = cwi.calculate_acc(
    fc_dir = aifs_fc_dir,
    an_dir = ifs_an_dir,
    clim_path = era_files[0],
    save_dir = os.path.join(save_dir, 'aifs', 'acc'),
    model_name = 'aifs',
    start = start_aifs,
    end = end,
    lead_times = lead_times
)