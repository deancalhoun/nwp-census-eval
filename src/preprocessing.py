import os
import subprocess
import numpy as np
import pandas as pd
import datetime as dt
import netCDF4 as nc
import xarray as xr
from ecmwfapi import ECMWFService
from dateutil.relativedelta import relativedelta

### DOWNLOADING DATA ###

## AIFS FORECAST DATA ##
def retrieve_aifs_forecast(target_dir, start, end, params, init_times, lead_times, bounds):
    '''
    Downloads AIFS forecast data from ECMWF MARS archive

    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        params: dictionary of parameter names and codes {str: str}
        init_times: list of initalization times (list of str)
        lead_times: list of lead times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
    Outputs:
        None
    '''
    # Create subdirectories
    dates = pd.date_range(start = start, end = end, freq = 'MS')
    for param in params.keys():
        for init_time in init_times:
            for lead_time in lead_times:
                for date in dates:
                    year = date.strftime("%Y")
                    month = date.strftime("%m")
                    path = '/'.join([target_dir, param, init_time, lead_time, year, month])
                    os.makedirs(path, exist_ok=True)
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for param in params.keys():
        for init_time in init_times:
            for lead_time in lead_times:
                for date in dates:
                    valid_time = date + relativedelta(hours=int(init_time)) + relativedelta(hours=int(lead_time))
                    year = valid_time.strftime("%Y")
                    month = valid_time.strftime("%m")
                    day = valid_time.strftime("%d")
                    hour = valid_time.strftime("%H")
                    date_str = valid_time.strftime("%Y-%m-%d")
                    path = '/'.join([
                        target_dir, param, init_time, lead_time, year, month, f'aifs_fc_{param}_{year}_{month}_{day}_{hour}z.grib'
                    ])
                    if os.path.exists(path[:-4]+"nc"): # Skip already downloaded data
                        continue
                    try:
                        server.execute({
                            'class': "ai",
                            'type': "fc",
                            'stream': "oper",
                            'expver': "1",
                            'repres': "gg",
                            'levtype': "sfc",
                            'param': params[param],
                            'time': init_time,
                            'step': lead_time,
                            'domain': "g",
                            'resol': "auto",
                            'area': '/'.join(bounds),
                            'grid': "0.25/0.25",
                            'padding': "0",
                            'expect': "any",
                            'date': date_str
                        },
                            path)
                        
                    except Exception:
                        print(f'Unable to retrieve AIFS forecast data for {"_".join([param, init_time, lead_time, date.strftime("%Y-%m-%d")])}')
                        continue
    
                    # Convert to netcdf
                    subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
                
                    # Delete grib file
                    subprocess.run(['rm', path])
    return

## IFS FORECAST DATA ##
def retrieve_ifs_forecast(target_dir, start, end, grids, params, init_times, lead_times, bounds):
    '''
    Downloads IFS forecast data from ECMWF MARS archive
    
    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        grids: list of grid resolutions (list of str)
        params: dictionary of parameter names and codes {str: str}
        init_times: list of initalization times (list of str)
        lead_times: list of lead times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
    Outputs:
        None
    '''
    # Create subdirectories
    dates = pd.date_range(start = start, end = end, freq = 'MS')
    for grid in grids:
        for param in params.keys():
            for init_time in init_times:
                for lead_time in lead_times:
                    for date in dates:
                        year = date.strftime("%Y")
                        month = date.strftime("%m")
                        path = '/'.join([target_dir, grid, param, init_time, lead_time, year, month])
                        os.makedirs(path, exist_ok=True)
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for grid in grids:
        for param in params.keys():
            for init_time in init_times:
                for lead_time in lead_times:
                    for date in dates:
                        valid_time = date + relativedelta(hours=int(init_time)) + relativedelta(hours=int(lead_time))
                        year = valid_time.strftime("%Y")
                        month = valid_time.strftime("%m")
                        day = valid_time.strftime("%d")
                        hour = valid_time.strftime("%H")
                        date_str = valid_time.strftime("%Y-%m-%d")
                        path = '/'.join([
                            target_dir, grid, param, init_time, lead_time, year, month, f'ifs_fc_{grid}_{param}_{year}_{month}_{day}_{hour}z.grib'
                        ])
                        if os.path.exists(path[:-4]+"nc"): # Skip already downloaded data
                            continue
                        try:
                            server.execute({
                                'class': "od",
                                'type': "fc",
                                'stream': "oper",
                                'expver': "1",
                                'repres': "gg",
                                'levtype': "sfc",
                                'param': params[param],
                                'time': init_time,
                                'step': lead_time,
                                'domain': "g",
                                'resol': "auto",
                                'area': "/".join(bounds),
                                'grid': "/".join([grid, grid]),
                                'padding': "0",
                                'expect': "any",
                                'date': date_str
                            },
                                path)
                            
                        except Exception:
                            print(f'Unable to retrieve forecast data for {"_".join([grid,param,init_time,lead_time,date.strftime("%Y-%m-%d")])}')
                            continue
        
                        # Convert to netcdf
                        subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
                    
                        # Delete grib file
                        subprocess.run(['rm', path])
    return

## IFS ANALYSIS DATA ##
def retrieve_ifs_analysis(target_dir, start, end, grids, params, times, bounds):
    '''
    Downloads IFS analysis data from ECMWF MARS archive

    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        grids: list of grid resolutions (list of str)
        params: dictionary of parameter names and codes {str: str}
        times: list of valid times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
    Outputs:
        None
    '''
    # Create subdirectories
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for grid in grids:
        for param in params.keys():
            for date in dates:
                year = date.strftime("%Y")
                month = date.strftime("%m")
                day = date.strftime("%d")
                path = '/'.join([target_dir, grid, param, year, month, day])
                os.makedirs(path, exist_ok=True)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    dates = pd.date_range(start = start, end = end, freq = 'D')
    for grid in grids:
        for param in params.keys():
            for date in dates:
                year = date.strftime("%Y")
                month = date.strftime("%m")
                day = date.strftime("%d")
                date_str = date.strftime("%Y-%m-%d")
                path = '/'.join([target_dir, grid, param, year, month, day, f'ifs_an_{grid}_{param}_{year}_{month}_{day}.grib'])
                if os.path.exists(path[:-4]+"nc"): # Skip already downloaded data
                            continue
                try:
                    server.execute({
                        'class': "od",
                        'type': "an",
                        'stream': "oper",
                        'expver': "1",
                        'repres': "gg",
                        'levtype': "sfc",
                        'param': params[param],
                        'time': "/".join(times),
                        'step': "0",
                        'domain': "g",
                        'resol': "auto",
                        'area': "/".join(bounds),
                        'grid': "/".join([grid, grid]),
                        'padding': "0",
                        'expect': "any",
                        'date': date_str
                    },
                        path)
                        
                except Exception:
                        print(f'Unable to retrieve analysis data for {date_str}')
                        continue

            # Convert to netcdf
            subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
        
            # Delete grib file
            subprocess.run(['rm', path])
    return

## LAND-SEA MASK ##
def retrieve_land_sea_mask(target_dir, grids, bounds):
    '''
    Downloads IFS land-sea mask data from ECMWF MARS archive
    
    Inputs:
        target_dir: parent directory to save data within (str)
        grids: list of grid resolutions (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
    Outputs:
        None
    '''
    # Create subdirectories
    for grid in grids:
        path = target_dir + '/' + grid
        os.makedirs(path, exist_ok=True)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    for grid in grids:
        path = target_dir + f"/{grid}/land_sea_mask_{grid}.grib"
        server.execute({
            'class': "od",
            'type': "an",
            'stream': "oper",
            'expver': "1",
            'repres': "gg",
            'levtype': "sfc",
            'param': "172.128",
            'time': "00:00:00",
            'step': "0",
            'domain': "g",
            'resol': "auto",
            'area': "/".join(bounds),
            'grid': "/".join([grid, grid]),
            'padding': "0",
            'expect': "any",
            'date': "2024-01-01"
        },
            path)
        
        # Convert to netcdf
        subprocess.run(f'module load eccodes && grib_to_netcdf -o {path[:-4]+"nc"} {path}', shell=True)
    
        # Delete grib file
        subprocess.run(['rm', path])
    return

### PROCESSING DATA ###

## CLIMATOLOGY ##
def calculate_era5_climatology(era_path, save_dir, params, start, end):
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
    dates = pd.date_range(start=start, end=end, freq='D')
    for param in params:
        for i, date in enumerate(dates):
            if date.dayofyear == 1:
                offset = 1
            
            if date.month == 2 and date.day == 29:
                offset = 2
                continue # No leap day
            
            if date.day == 1:
                # Read in ERA files
                path = era_dir + f'{date.strftime("%Y%m")}/*{param}*.nc'
                era_file = glob.glob(path)[0]
                ds_era = xr.open_dataset(era_file)
                ds_era = ds_era.assign_coords(time=pd.to_datetime(ds_era.time))
    
                if i == 0:
                    var_name = list(ds_era.keys())[0]
                    clim = np.zeros((len(np.unique(dates.year)), 365, ds_era[var_name].shape[1], ds_era[var_name].shape[2])) # [year, doy, lat, lon]
    
            # Calculate climatology
            daily_avg = ds_era.sel(time=date.strftime('%Y-%m-%d'))[var_name].mean(dim="time", skipna=True).values
            clim[date.year - dates[0].year, date.dayofyear - offset, :, :] = daily_avg
            
        # Save out climatology
        climatology_dataset = xr.Dataset({
                             param: (['time','latitude','longitude'], np.nanmean(clim, axis=0)), # average across all years
                            },
                             coords =
                            {'time': (['time'], np.arange(1,365 + 1,1)),
                             'latitude' : (['latitude'], ds_era.latitude.values),
                             'longitude' : (['longitude'], (((ds_era.longitude.values + 180) % 360) - 180)) # transform longitude from
                            })                                                                              # [0, 360] to [-180, 180]
        climatology_dataset.to_netcdf(f'{save_dir}/era5_{param}_climatology.nc')
    return

## MASKING ##

def apply_land_sea_mask(data_path, mask_path, threshhold):
    '''
    Applies the IFS land-sea mask to the given data field and saves as a new file

    Inputs:
        data_path: path to dataset of interest (str)
        mask_path: path to IFS land-sea mask (str)
    Outputs:
        None
    '''
    ds = xr.open_dataset(data_path)
    mask = xr.open_dataset(mask_path)
    mask = xr.where(mask >= threshhold, 1, np.nan).broadcast_like(ds)
    masked = mask[list(mask.keys())[0]] * ds
    return ds

## RMSE ##
def calculate_rmse(fc_dir, an_dir, clim_path, save_dir, model_name, start, end, lead_times):
    '''
    Calculates the root mean squared error between forecast and analysis data for a given model and lead times over the specified date range

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        model_name: name of forecast model (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        lead_times: list of forecast lead times to evaluate (list of str)
    Outputs:
        None
    '''
    an_path = an_dir + '/*/*/*/*.nc'
    an_files = sorted(glob.glob(an_path))
    ds_an = xr.open_mfdataset(an_files)
    ds_clim = xr.open_dataset(clim_path)
    
    for lead_time in lead_times:
        fc_path = fc_dir + f'/*/{lead_time}/*/*/*.nc'
        fc_files = sorted(glob.glob(fc_path))
        ds_fc = xr.open_mfdataset(fc_files)
        ds_fc = ds_fc.sel(time=slice(start, end)) # restrict to dates of interest
        ds_fc = ds_fc.sel(time=~((ds_fc.time.dt.month == 2) & (ds_fc.time.dt.day == 29))) # remove leap year
        var_names = list(ds_fc.keys())
        for var_name in var_names:
            common_times = np.intersect1d(ds_fc[var_name].time.values, ds_an[var_name].time.values) # ensure all times present in both fc and an
            ds_fc = ds_fc.sel(time=common_times)
            var_fc = ds_fc[var_name].values
            var_an = ds_an.sel(time=common_times)[var_name].values
            ds_fc = ds_fc.assign_coords(dayofyear = pd.to_datetime(ds_fc.time.dt.strftime('2017-%m-%d')).dayofyear) # get day of year
            var_clim = ds_clim.sel(time=ds_fc.dayofyear.values)[var_name].values # align climatology to forecast data
            mse = ((var_fc - var_clim)**2).mean(axis=0) + ((var_an - var_clim)**2).mean(axis=0) - (2*(var_fc - var_clim)*(var_an - var_clim)).mean(axis=0)
            rmse = np.sqrt(mse)
            rmse_dataset = xr.Dataset({
                            f'{var_name}_rmse': (['latitude','longitude'], rmse),
                            },
                            coords =
                            {'latitude' : (['latitude'], ds_fc.latitude.values),
                            'longitude' : (['longitude'], ds_fc.longitude.values)
                            })                                           
            rmse_dataset.to_netcdf(f'{save_dir}/{model_name}_{var_name}_rmse_{lead_time}_{"".join(start.split("-"))}_{"".join(end.split("-"))}.nc')
    return

## ACC ##
def calculate_acc(fc_dir, an_path, c_path, save_path, lead_times, model):
    '''
    Calculates the anomaly correlation coefficient between forecast and analysis data for a given model and lead times over the specified date range

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        model_name: name of forecast model (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        lead_times: list of forecast lead times to evaluate (list of str)
    Outputs:
        None
    '''
    an_path = an_dir + '/*/*/*/*.nc'
    an_files = sorted(glob.glob(an_path))
    ds_an = xr.open_mfdataset(an_files)
    ds_clim = xr.open_dataset(clim_path)
    
    for lead_time in lead_times:
        fc_path = fc_dir + f'/*/{lead_time}/*/*/*.nc'
        fc_files = sorted(glob.glob(fc_path))
        ds_fc = xr.open_mfdataset(fc_files)
        ds_fc = ds_fc.sel(time=slice(start, end)) # restrict to dates of interest
        ds_fc = ds_fc.sel(time=~((ds_fc.time.dt.month == 2) & (ds_fc.time.dt.day == 29))) # remove leap year
        var_names = list(ds_fc.keys())
        for var_name in var_names:
            common_times = np.intersect1d(ds_fc[var_name].time.values, ds_an[var_name].time.values) # ensure all times present in both fc and an
            ds_fc = ds_fc.sel(time=common_times)
            var_fc = ds_fc[var_name].values
            var_an = ds_an.sel(time=common_times)[var_name].values
            ds_fc = ds_fc.assign_coords(dayofyear = pd.to_datetime(ds_fc.time.dt.strftime('2017-%m-%d')).dayofyear) # get day of year
            var_clim = ds_clim.sel(time=ds_fc.dayofyear.values)[var_name].values # align climatology to forecast data
            acc = ((var_fc - var_clim) * (var_an - var_clim)).mean(axis=0) / np.sqrt(((var_fc - var_clim)**2).mean(axis=0) * ((var_an - var_clim)**2).mean(axis=0))
            acc_dataset = xr.Dataset({
                            f'{var_name}_acc': (['latitude','longitude'], acc),
                            },
                            coords =
                            {'latitude' : (['latitude'], ds_fc.latitude.values),
                            'longitude' : (['longitude'], ds_fc.longitude.values)
                            })                                           
            acc_dataset.to_netcdf(f'{save_dir}/{model_name}_{var_name}_acc_{lead_time}_{"".join(start.split("-"))}_{"".join(end.split("-"))}.nc')
    return