import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
import glob as glob

def calculate_era5_t2m_climatology(era_dir, save_dir, start='1991-01-01', end='2020-12-31'):
    
    dates = pd.date_range(start=start, end=end, freq='D')
    
    print("Calculating climatology...")
    
    for i, date in enumerate(dates):
        if date.dayofyear == 1:
            offset = 1
        
        if date.month == 2 and date.day == 29:
            offset = 2
            continue # No leap day
        
        if date.day == 1:
            # Read in ERA files
            path = era_dir+f'{date.strftime('%Y%m')}/*2t*.nc'
            era_file = glob.glob(path)[0]
            ds_era = xr.open_dataset(era_file)
            ds_era = ds_era.assign_coords(time=pd.to_datetime(ds_era.time))
            print("Reading ERA5 file: " + date.strftime('%Y-%m'))

            if i == 0:
                clim = np.zeros((len(np.unique(dates.year)), 365, ds_era.VAR_2T.shape[1], ds_era.VAR_2T.shape[2])) # [year, doy, lat, lon]

        # Calculate climatology
        daily_avg_t2m = ds_era.sel(time=date.strftime('%Y-%m-%d')).VAR_2T.mean(dim="time", skipna=True).values - 273.15
        clim[date.year-dates[0].year, date.dayofyear-offset, :, :] = daily_avg_t2m
    
    print("Done.")
    
    # Save out climatology
    print("Saving...")
    
    climatology_dataset = xr.Dataset({
                         't2m': (['time','latitude','longitude'], np.nanmean(clim, axis=0)), # average across all years
                        },
                         coords =
                        {'time': (['time'], np.arange(1,365 + 1,1)),
                         'latitude' : (['latitude'], ds_era.latitude.values),
                         'longitude' : (['longitude'], (((ds_era.longitude.values + 180) % 360) - 180)) # transform longitude from
                        })                                                                              # [0, 360] to [-180, 180]
    climatology_dataset.to_netcdf(f'{save_dir}era5_t2m_climatology.nc')
    
    print("Done.")
    return

if __name__ == "__main__":
    calculate_era5_t2m_climatology('/glade/campaign/collections/rda/data/d633000/e5.oper.an.sfc/',
                                   '/glade/u/home/dcalhoun/Code/Autoencoder-Forecast-Error/')