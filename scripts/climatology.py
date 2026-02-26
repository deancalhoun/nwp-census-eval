import os
import logging
import gc
import glob
import numpy as np
import pandas as pd
import xarray as xr

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

## CLIMATOLOGY ##
def calculate_era5_climatology(era_dir, save_dir, params, start, end):
    '''
    Calculates climatology of a given variable from GLADE ERA5 data

    Inputs:
        era_dir: ERA5 data directory (str)
        save_dir: directory to save climatology in (str)
        params: list of short names of parameters (list of str)
        start: climatology period start date (str, YYYY-MM-DD)
        end: climatology period end date (str, YYYY-MM-DD)
    Outputs:
        None
    '''
    os.makedirs(save_dir, exist_ok=True)
    dates = pd.date_range(start=start, end=end, freq='D')
    
    clim = None
    for param in params:
        outfile = os.path.join(save_dir, f'era5_{param}_climatology_{"".join(start.split("-")[:1])}_{"".join(end.split("-")[:1])}.nc')
        if os.path.exists(outfile): # Skip already calculated climatology
            logging.info(f'Skipping already calculated {dates[0].strftime("%Y")}-{dates[-1].strftime("%Y")} climatology for {param}')
            continue
        logging.info(f'Starting calculation of {dates[0].strftime("%Y")}-{dates[-1].strftime("%Y")} climatology for parameter: {param}')
        for i, date in enumerate(dates):
            if date.dayofyear == 1:
                offset = 1
            
            if date.month == 2 and date.day == 29:
                offset = 2
                continue # No leap day
            
            if date.day == 1:
                # Read in ERA files
                path = os.path.join(era_dir, f'{date.strftime("%Y%m")}', f'*{param}*.nc')
                era_file = glob.glob(path)[0]
                ds_era = xr.open_dataset(era_file)
                ds_era = ds_era.assign_coords(time=pd.to_datetime(ds_era.time))
    
            if clim is None:
                var_name = list(ds_era.keys())[0]
                clim = np.zeros((len(np.unique(dates.year)), 365, ds_era[var_name].shape[1], ds_era[var_name].shape[2])) # [year, doy, lat, lon]
    
            # Calculate climatology
            clim[date.year - dates[0].year, date.dayofyear - offset, :, :] = ds_era.sel(time=date.strftime('%Y-%m-%d'))[var_name].mean(dim="time", skipna=True).values # daily average
            ds_era.close()

            if i % (len(dates)//100) == 0:
                logging.info(f'Processed {i} / {len(dates)} days [{i/len(dates)*100:.1f}%]')
            
        # Save out climatology
        logging.info(f'Saving {dates[0].strftime("%Y")}-{dates[-1].strftime("%Y")} climatology for parameter: {param}')
        climatology_dataset = xr.Dataset({
                             param: (['time','latitude','longitude'], np.nanmean(clim, axis=0)), # average across all years
                            },
                             coords =
                            {'time': (['time'], np.arange(1,365 + 1,1)),
                             'latitude' : (['latitude'], ds_era.latitude.values),
                             'longitude' : (['longitude'], (((ds_era.longitude.values + 180) % 360) - 180)) # transform longitude from [0, 360] to [-180, 180]
                            })
        del clim
        gc.collect()                                                                  
        climatology_dataset.to_netcdf(outfile)
        logging.info(f'Saved {dates[0].strftime("%Y")}-{dates[-1].strftime("%Y")} climatology for parameter: {param} to {outfile}')
    return