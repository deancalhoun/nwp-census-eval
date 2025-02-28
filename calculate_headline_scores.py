import numpy as np
import xarray as xr
import pandas as pd
import datetime as dt
import glob as glob

def calculate_RMSE(fc_dir,
                   an_path,
                   clim_path,
                   save_path,
                   model = 'IFS',
                   dates = ['2024-03-01', '2024-12-01'],
                   lead_times = ['0', '6', '12', '24', '48', '72', '96', '120', '168', '240']):
    '''
    fc_dir: directory of forecast files
    an_path: path for analysis files
    clim_path: path for climatology file
    save_path: directory to save in
    '''
    an_files = sorted(glob.glob(an_path))
    ds_an = xr.open_mfdataset(an_files)
    
    ds_clim = xr.open_dataset(clim_path)
    
    for lead_time in lead_times:
        print(f"Calculating RMSE for lead time {lead_time}...")
        fc_path = fc_dir + f'/*/{lead_time}/*/*/*.nc'
        fc_files = sorted(glob.glob(fc_path))
        ds_fc = xr.open_mfdataset(fc_files)
        ds_fc = ds_fc.sel(time=slice(dates[0], dates[1])) # restrict to dates of interest
        ds_fc = ds_fc.sel(time=~((ds_fc.time.dt.month == 2) & (ds_fc.time.dt.day == 29))) # remove leap year
        common_times = np.intersect1d(ds_fc['t2m'].time.values, ds_an['t2m'].time.values) # ensure all times present in both fc and an
        ds_fc = ds_fc.sel(time=common_times)
        t2m_fc = ds_fc.t2m.values
        t2m_an = ds_an.sel(time=common_times).t2m.values
        ds_fc = ds_fc.assign_coords(dayofyear = pd.to_datetime(ds_fc.time.dt.strftime('2017-%m-%d')).dayofyear) # get day of year
        t2m_clim = ds_clim.sel(time=ds_fc.dayofyear.values).t2m.values # align climatology to forecast data
        mse = ((t2m_fc - t2m_clim)**2).mean(axis=0) + ((t2m_an - t2m_clim)**2).mean(axis=0) - (2*(t2m_fc - t2m_clim)*(t2m_an - t2m_clim)).mean(axis=0)
        rmse = np.sqrt(mse)
        rmse_dataset = xr.Dataset({
                         'rmse': (['latitude','longitude'], rmse), # average across all years
                        },
                         coords =
                        {'latitude' : (['latitude'], ds_fc.latitude.values),
                         'longitude' : (['longitude'], ds_fc.longitude.values) # transform longitude from
                        })                                           
        rmse_dataset.to_netcdf(f'/glade/u/home/dcalhoun/Code/Autoencoder-Forecast-Error/{model}_RMSE_{lead_time}_2024.nc')
        print("Done.")
    return


def calculate_ACC(fc_dir, an_path, c_path, save_path, lead_times = ['0', '6', '12', '24', '48', '72', '96', '120', '168', '240'], model='IFS'):
    '''
    fc_dir: directory of forecast files = /glade/derecho/scratch/dcalhoun/ecmwf_ifs/fc/t2m
    an_path: path for analysis files
    clim_path: path for climatology file
    save_path: directory to save in
    '''
    return

if __name__ == "__main__":
    # IFS
    calculate_RMSE('/glade/derecho/scratch/dcalhoun/ecmwf_ifs/fc/t2m',
                   '/glade/derecho/scratch/dcalhoun/ecmwf_ifs/an/t2m/*/*/*/*.nc',
                   '/glade/u/home/dcalhoun/Code/Autoencoder-Forecast-Error/era5_t2m_climatology_filtered.nc',
                   '/glade/u/home/dcalhoun/Code/Autoencoder-Forecast-Error/', lead_times=['24'])
    # AIFS
    calculate_RMSE('/glade/derecho/scratch/dcalhoun/ecmwf_aifs/t2m',
                   '/glade/derecho/scratch/dcalhoun/ecmwf_ifs/an/t2m/*/*/*/*.nc',
                   '/glade/u/home/dcalhoun/Code/Autoencoder-Forecast-Error/era5_t2m_climatology_filtered.nc',
                   '/glade/u/home/dcalhoun/Code/Autoencoder-Forecast-Error/', model='AIFS', lead_times=['24'])