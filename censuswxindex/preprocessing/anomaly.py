import os
import glob
import xarray as xr
import numpy as np
import pandas as pd

def calculate_ifs_2t_fc_anomaly(fc_dir, clim_path, save_dir, year, month, init_time, lead_time):
    '''
    Calculate 2-meter temperature anomalies from IFS forecast data relative to climatology.
    
    This function reads IFS forecast data, aligns it with climatological data, and computes
    temperature anomalies by subtracting the climatology from the forecast values. The
    climatology is interpolated to match the forecast grid and temporal alignment.
    
    Parameters
    ----------
    fc_dir : str
        Path to the directory containing forecast data files.
    clim_path : str
        Path to the climatology NetCDF file.
    save_dir : str
        Directory where the output anomaly file will be saved.
    year : str
        Year of the forecast data (e.g., '2023').
    month : str
        Month of the forecast data (e.g., '01' for January).
    init_time : str
        Initialization time identifier for the forecast.
    lead_time : str
        Lead time identifier for the forecast.
    
    Returns
    -------
    None
        Saves the anomaly dataset as a NetCDF file to the specified directory.
    
    Notes
    -----
    - Leap days (February 29) are removed from the analysis.
    - Climatology is interpolated to match the forecast grid using nearest neighbor method.
    - Output file is named: 'ifs_fc_2t_anom_{init_time}_{lead_time}_{year}{month}.nc'
    '''
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    # Read in forecast data
    fc_path = os.path.join(fc_dir, init_time, lead_time, year, month, '*.nc')
    fc_files = sorted(glob.glob(fc_path))
    ds_fc = xr.open_mfdataset(fc_files)

    # Climatology
    ds_clim = xr.open_dataset(clim_path)

    # Remove leap day
    ds_fc = ds_fc.sel(time=~((ds_fc.time.dt.month == 2) & (ds_fc.time.dt.day == 29)))

    # Interpolate the climatology to the same grid as the forecast and analysis
    ds_clim = ds_clim.sel(latitude=slice(ds_fc.latitude.min(), ds_fc.latitude.max()), longitude=slice(ds_fc.longitude.min(), ds_fc.longitude.max()))
    ds_clim = ds_clim.interp(latitude=ds_fc.latitude.values, longitude=ds_fc.longitude.values, method='nearest')

    # Calculate anomalies
    ds_clim = ds_clim.sel(time=pd.to_datetime(ds_fc.time.dt.strftime('2017-%m-%d')).dayofyear)  # align climatology to forecast data
    ds_fc_anom = xr.Dataset({
        '2t_anom': (['time', 'latitude', 'longitude'], ds_fc['t2m'].values - ds_clim['2t'].values)
    },
    coords={
        'time': (['time'], ds_fc.time.values),
        'latitude': (['latitude'], ds_fc.latitude.values),
        'longitude': (['longitude'], ds_fc.longitude.values)
    })
    
    # Save the anomalies dataset
    save_path = os.path.join(save_dir, f'ifs_fc_2t_anom_{init_time}_{lead_time}_{year}{month}.nc')
    ds_fc_anom.to_netcdf(save_path)
    return

def calculate_ifs_2t_an_anomaly(an_dir, clim_path, save_dir, year, month):
    '''
    Calculate 2-meter temperature anomalies from IFS analysis data relative to climatology.
    
    This function reads IFS analysis data, aligns it with climatological data, and computes
    temperature anomalies by subtracting the climatology from the analysis values. The
    climatology is interpolated to match the analysis grid and temporal alignment.
    
    Parameters
    ----------
    an_dir : str
        Path to the directory containing analysis data files.
    clim_path : str
        Path to the climatology NetCDF file.
    save_dir : str
        Directory where the output anomaly file will be saved.
    year : str
        Year of the analysis data (e.g., '2023').
    month : str
        Month of the analysis data (e.g., '01' for January).
    
    Returns
    -------
    None
        Saves the anomaly dataset as a NetCDF file to the specified directory.
    
    Notes
    -----
    - Leap days (February 29) are removed from the analysis.
    - Climatology is interpolated to match the analysis grid using nearest neighbor method.
    - Output file is named: 'ifs_an_2t_anom_{year}{month}.nc'
    '''
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    # Read in analysis data
    an_path = os.path.join(an_dir, year, month, '*.nc')
    an_files = sorted(glob.glob(an_path))
    ds_an = xr.open_mfdataset(an_files)

    # Climatology
    ds_clim = xr.open_dataset(clim_path)

    # Remove leap day
    ds_an = ds_an.sel(time=~((ds_an.time.dt.month == 2) & (ds_an.time.dt.day == 29)))

    # Interpolate the climatology to the same grid as the forecast and analysis
    ds_clim = ds_clim.sel(latitude=slice(ds_an.latitude.min(), ds_an.latitude.max()), longitude=slice(ds_an.longitude.min(), ds_an.longitude.max()))
    ds_clim = ds_clim.interp(latitude=ds_an.latitude.values, longitude=ds_an.longitude.values, method='nearest')

    # Calculate anomalies
    ds_clim = ds_clim.sel(time=pd.to_datetime(ds_an.time.dt.strftime('2017-%m-%d')).dayofyear)  # align climatology to forecast data
    ds_an_anom = xr.Dataset({
        '2t_anom': (['time', 'latitude', 'longitude'], ds_an['t2m'].values - ds_clim['2t'].values)
    },
    coords={
        'time': (['time'], ds_an.time.values),
        'latitude': (['latitude'], ds_an.latitude.values),
        'longitude': (['longitude'], ds_an.longitude.values)
    })
    
    # Save the anomalies dataset
    save_path = os.path.join(save_dir, f'ifs_an_2t_anom_{year}{month}.nc')
    ds_an_anom.to_netcdf(save_path)
    return