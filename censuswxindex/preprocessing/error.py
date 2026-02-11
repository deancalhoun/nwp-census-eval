import os
import glob
import xarray as xr
import numpy as np
import pandas as pd
from scipy.signal import detrend

def calculate_ifs_2t_absolute_error(fc_dir, an_dir, save_dir, year, month, init_time, lead_time):
    '''
    Calculates the absolute 2m temperature error between IFS forecast and analysis data
    for a given year, month, initialization time, and lead time.

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        year: year to evaluate (str)
        month: month to evaluate (str)
        init_time: forecast initialization time (str)
        lead_time: forecast lead time to evaluate (str)
    Outputs:
        None
    '''
    # Create save directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    # Read in forecast data
    fc_path = os.path.join(fc_dir, init_time, lead_time, year, month, '*.nc')
    fc_files = sorted(glob.glob(fc_path))
    ds_fc = xr.open_mfdataset(fc_files)

    # Read in analysis data
    an_path = os.path.join(an_dir, year, month, '*.nc')
    an_files = sorted(glob.glob(an_path))
    ds_an = xr.open_mfdataset(an_files)

    # Ensure all times present in both datasets
    common_times = np.intersect1d(ds_fc['2t_anom'].time.values, ds_an['2t_anom'].time.values)
    ds_fc = ds_fc.sel(time=common_times)
    ds_an = ds_an.sel(time=common_times)

    # Calculate absolute error
    ds_abs_error = xr.Dataset({
                '2t_abs_error': (['time', 'latitude','longitude'], abs(ds_fc['2t_anom'] - ds_an['2t_anom']).values)
                },
                coords = {
                    'time':(['time'], ds_fc.time.values),
                    'latitude' : (['latitude'], ds_fc.latitude.values),
                    'longitude' : (['longitude'], ds_fc.longitude.values)
                })
    
    # Save the absolute error dataset
    save_path = os.path.join(save_dir, f'ifs_2t_abs_error_{init_time}_{lead_time}_{year}{month}.nc')
    ds_abs_error.to_netcdf(save_path)
    return

# def detrend_ifs_2t_absolute_error(abs_error_dir, save_dir, init_time, lead_time):
#     '''
#     Detrends the absolute 2m temperature error data for IFS forecasts for a given initialization time and lead time.

#     Inputs:
#         abs_error_dir: directory of absolute error files (str)
#         save_dir: directory to save detrended data (str)
#         year: year to evaluate (str)
#         month: month to evaluate (str)
#         init_time: forecast initialization time (str)
#         lead_time: forecast lead time to evaluate (str)
#     Outputs:
#         None
#     '''
#     # Create save directory if it doesn't exist
#     os.makedirs(save_dir, exist_ok=True)

#     # Read in absolute error data
#     abs_error_path = os.path.join(abs_error_dir, init_time, lead_time, '*', '*', '*.nc')
#     abs_error_files = sorted(glob.glob(abs_error_path))
#     ds_abs_error = xr.open_mfdataset(abs_error_files)

#     # Detrend the absolute error data
#     ds_abs_error_detrend = xr.Dataset({
#                 't2m_abs_error': (['time', 'latitude','longitude'], detrend(ds_abs_error.t2m_abs_error.values, axis=0))
#                 },
#                 coords = {
#                     'time':(['time'], ds_abs_error.time.values),
#                     'latitude' : (['latitude'], ds_abs_error.latitude.values),
#                     'longitude' : (['longitude'], ds_abs_error.longitude.values)
#                 })
    
#     # Calculate area-weighted mean absolute error
#     weights = np.cos(np.deg2rad(ds_abs_error_detrend.latitude))
#     weights.name = "weights"
#     ds_abs_error_weighted = ds_abs_error_detrend.weighted(weights)
#     MAE = xr.Dataset({
#                 'MAE': (['time'], ds_abs_error_weighted.mean(dim=['latitude', 'longitude']).t2m_abs_error.values)
#                 },
#                 coords = {
#                     'time':(['time'], ds_abs_error.time.values),
#                     'dayofyear':(['time'], pd.to_datetime(ds_abs_error.time.dt.strftime('2017-%m-%d')).dayofyear)
#                 })

#     # Calculate MAE climatology
#     MAE_clim = MAE.groupby('dayofyear').mean()

#     # Calculate absolute error anomaly
#     abs_error_anom = ds_abs_error_detrend.t2m_abs_error.values - MAE_clim.MAE.sel(dayofyear = pd.to_datetime(MAE.time.dt.strftime('2017-%m-%d')).dayofyear).values[:,np.newaxis,np.newaxis]
#     ds_abs_error_anom = xr.Dataset({
#                     't2m_abs_error_anom': (['time', 'latitude', 'longitude'], abs_error_anom)
#                     },
#                     coords = {
#                         'time':(['time'], ds_abs_error.time.values),
#                         'latitude' : (['latitude'], ds_abs_error.latitude.values),
#                         'longitude' : (['longitude'], ds_abs_error.longitude.values)
#                     })

#     # Save the absolute error anomaly dataset
#     save_path = os.path.join(save_dir, f'ifs_t2m_abs_error_anom_{init_time}_{lead_time}.nc')
#     ds_abs_error_anom.to_netcdf(save_path)
#     return

# if __name__ == "__main__":
#     pass