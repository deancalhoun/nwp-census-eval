import os
import logging
import gc
import warnings
import subprocess
import glob
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from ecmwfapi import ECMWFService

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

### DOWNLOADING DATA ###

## IFS FORECAST DATA ##
def retrieve_ifs_forecast(target_dir, start, end, param, init_times, lead_times, bounds, grid, filter_file):
    '''
    Downloads IFS forecast data from ECMWF MARS archive
    
    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        init_times: list of initalization times (list of str)
        lead_times: list of lead times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
        grid: grid resolution (str)
        filter_file: path to grib_filter file (str)
    Outputs:
        None
    '''
    # Create subdirectories
    for init_time in init_times:
        for lead_time in lead_times:
            path = os.path.join(target_dir, grid, param[0], init_time, lead_time)
            os.makedirs(path, exist_ok=True)
    
    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
    
    # Retrieve data
    tempfile = os.path.join(target_dir, grid, param[0], f'ifs_fc_temp.grib')
    dates = pd.date_range(start=start, end=end, freq='D')
    for date in dates:
        paths = [path for init_time in init_times for lead_time in lead_times for path in glob.glob(os.path.join(*[target_dir, grid, param[0], init_time, lead_time, f'*{date.strftime("%Y%m%d")}*']))]
        if paths and all(os.path.exists(path) for path in paths):
            # Skip already downloaded files
            logging.info(f'Skipping already downloaded forecast data for {date.strftime("%Y-%m-%d")}')
            continue
        try:
            server.execute(
                {
                    'class': "od",
                    'type': "fc",
                    'stream': "oper",
                    'expver': "1",
                    'repres': "gg",
                    'levtype': "sfc",
                    'param': param[1],
                    'time': "/".join(init_times),
                    'step': "/".join(lead_times),
                    'domain': "g",
                    'resol': "auto",
                    'area': "/".join(bounds),
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date.strftime("%Y%m%d"),
                },
                tempfile
            )
            
        except Exception as e:
            logging.error(f'Forecast retrieval failed for {date.strftime("%Y-%m-%d")}: {e}')
            continue

        # Split the grib file into individual files
        subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)

        # Delete temp file
        subprocess.run(['rm', tempfile])
    return

## IFS ANALYSIS DATA ##
def retrieve_ifs_analysis(target_dir, start, end, param, valid_times, bounds, grid, filter_file):
    '''
    Downloads IFS analysis data from ECMWF MARS archive

    Inputs:
        target_dir: parent directory to save data within (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        param: tuple of parameter short name and code (str, str)
        valid_times: list of valid times (list of str)
        bounds: latitude and longitude boundaries [deg N, deg W, deg S, deg E] (list of str)
        grid: grid resolution (str)
        filter_file: path to grib_filter file (str)
    Outputs:
        None
    '''
    # Create directory
    path = os.path.join(target_dir, grid, param[0])
    os.makedirs(path, exist_ok=True)

    # Establish API connection to ECMWF MARS archive
    server = ECMWFService("mars")
     
    # Retrieve data
    tempfile = os.path.join(target_dir, grid, param[0], f'ifs_an_temp.grib')
    dates = pd.date_range(start=start, end=end, freq='D')
    for date in dates:
        path = glob.glob(os.path.join(target_dir, grid, param[0], f'*{date.strftime("%Y%m%d")}*'))
        if os.path.exists(path):
            # Skip already downloaded files
            logging.info(f'Skipping already downloaded analysis data for {date.strftime("%Y-%m-%d")}')
            continue
        try:
            server.execute(
                {
                    'class': "od",
                    'type': "an",
                    'stream': "oper",
                    'expver': "1",
                    'repres': "gg",
                    'levtype': "sfc",
                    'param': param[1],
                    'time': "/".join(valid_times),
                    'step': "0",
                    'domain': "g",
                    'resol': "auto",
                    'area': "/".join(bounds),
                    'grid': "/".join([grid, grid]),
                    'padding': "0",
                    'expect': "any",
                    'date': date.strftime("%Y%m%d")
                },
                tempfile
            )
                
        except Exception as e:
            logging.error(f'Analysis retrieval failed for {date.strftime("%Y-%m-%d")}: {e}')
            continue

        # Split the grib file into individual files
        subprocess.run(f'grib_filter {filter_file} {tempfile}', shell=True)
        
        # Delete temp file
        subprocess.run(['rm', tempfile])
    return

### PROCESSING DATA ###

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

## ABSOLUTE ERROR ##
def calculate_absolute_error(fc_dir, an_dir, clim_path, save_dir, start, end, lead_times):
    '''
    Calculates the root mean squared error between forecast and analysis data for given lead times over the specified date range

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        lead_times: list of forecast lead times to evaluate (list of str)
    Outputs:
        None
    '''
    return

## RMSE ##
def calculate_rmse(fc_dir, an_dir, clim_path, save_dir, start, end, lead_times):
    '''
    Calculates the root mean squared error between forecast and analysis data for given lead times over the specified date range

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        lead_times: list of forecast lead times to evaluate (list of str)
    Outputs:
        None
    '''
    return None

## ACC ##
def calculate_acc(fc_dir, an_dir, clim_path, save_dir, start, end, lead_times):
    '''
    Calculates the anomaly correlation coefficient between forecast and analysis data for a given model and lead times over the specified date range

    Inputs:
        fc_dir: directory of forecast files (str)
        an_dir: directory of analysis files (str)
        clim_path: path for climatology file (str)
        save_dir: directory to save in (str)
        start: start date (str, YYYY-MM-DD)
        end: end date (str, YYYY-MM-DD)
        lead_times: list of forecast lead times to evaluate (list of str)
    Outputs:
        None
    '''
    return
    
## AGGREGATION ##
def aggregate_to_geography(data_files, shapefile, outfile):
    '''
    Aggregates gridded data to a specified geography.
    
    Inputs:
    	data_files: list of gridded data filenames to aggregate (list of str)
        shapefile: shapefile containing geography to aggregate to (str)
        outfile: path to save aggregated data (str)
    Outputs:
    	None
    '''
    return

### VISUALIZATION ###