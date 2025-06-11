import xarray as xr
import xagg
import geopandas as gpd
import numpy as np
import pandas as pd
import glob
import cfgrib
import os
import argparse

# ## Aggregating IFS 2m Temperature Forecast Absolute Error with Maryland Counties
# ## 00z initialization time, 24 hour lead time

# Parameters
parser = argparse.ArgumentParser(description='Process data for a specific year.')
parser.add_argument('--year', type=int, required=True, help='The year to process data for')
args = parser.parse_args()
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
year = str(args.year)

# Read in the shapefile
shapefile_path = '/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_tract_2023'
gdf = gpd.read_file(shapefile_path, columns=['GEOID', 'STATEFP', 'geometry']).to_crs('WGS84')

# Maryland only
gdf = gdf[gdf['STATEFP'] == '24'][['GEOID', 'geometry']].reset_index(drop=True)

for month in months:
    print(f'Starting aggregation for {year} {month}')
    
    an_path = f'/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an/0.125/2t/{year}/{month}/*.nc'
    clim_path = '/glade/derecho/scratch/dcalhoun/ecmwf/era5/era5_2t_climatology_1991_2020_sorted.nc'

    ## Read in forecast, analysis, and climatology data and compute anomalies
    # Analysis
    an_files = sorted(glob.glob(an_path))
    ds_an = xr.open_mfdataset(an_files)
    
    # Climatology
    ds_clim = xr.open_dataset(clim_path)

    # Remove leap day
    ds_an = ds_an.sel(time=~((ds_an.time.dt.month == 2) & (ds_an.time.dt.day == 29)))
    
    # Interpolate the climatology to the same grid as the forecast and analysis
    ds_clim = ds_clim.sel(
        latitude=slice(ds_an.latitude.min(), ds_an.latitude.max()),
        longitude=slice(ds_an.longitude.min(), ds_an.longitude.max())
    )
    ds_clim = ds_clim.interp(latitude = ds_an.latitude.values, longitude = ds_an.longitude.values, method='nearest')
    
    # Calculate anomalies
    ds_clim = ds_clim.sel(time=pd.to_datetime(ds_an.time.dt.strftime('2017-%m-%d')).dayofyear) # align climatology to analysis data
    ds_an['t2m'] = (['time', 'latitude', 'longitude'], ds_an['t2m'].values - ds_clim['2t'].values)
    
    ## Calculate absolute error
    ds_anom = xr.Dataset(
        {'t2m_anom': (['time', 'latitude','longitude'], ds_an.t2m.values)},
        coords = {
            'time':(['time'], ds_an.time.values),
            'latitude' : (['latitude'], ds_an.latitude.values),
            'longitude' : (['longitude'], ds_an.longitude.values)
        }
    )
    
    ## Aggregation
    dates = pd.to_datetime(ds_anom.time.values)
    for i, date in enumerate(dates):
        day = date.strftime('%d')
        hour = date.strftime('%H')
        dirname = f'/glade/derecho/scratch/dcalhoun/aggregated/tract/Maryland/ifs/an/0.125/t2m/{year}/{month}'
        os.makedirs(dirname, exist_ok=True)
        filename = os.path.join(dirname, f'aggregated_ifs_2t_anom_{year}_{month}_{day}_{hour}z_MD.csv')
        weightmap = xagg.pixel_overlaps(ds_anom.sel(time=date), gdf, silent=True)
        aggregated = xagg.aggregate(ds_anom.sel(time=date), weightmap, silent=True)
        df = aggregated.to_dataframe()
        df['time'] = date.strftime('%Y-%m-%d-%H')
        df.reset_index(inplace=True, drop=True)
        cols = df.columns.tolist()
        df = df[cols[0:1] + cols[-1:] + cols[1:-1]] # Reorder columns
        df.to_csv(filename) # Save aggregated data
        print(f'Saved {filename}')
    
    print(f'Finished aggregation for {year} {month} \n')
