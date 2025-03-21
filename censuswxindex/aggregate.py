import xarray as xr
import xagg
import geopandas as gpd
import numpy as np
import pandas as pd
import glob
import os

### Aggregating IFS 2m Temperature Forecast Absolute Error with Maryland Counties
### 00z initialization time, 24 hour lead time, 2016-2024

# Parameters
years = ['2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# Read in the shapefile
shapefile_path = '/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_county_2023'
gdf = gpd.read_file(shapefile_path, columns=['GEOID', 'STATEFP', 'geometry']).to_crs('WGS84')

# Maryland only
gdf = gdf[gdf['STATEFP'] == '24'][['GEOID', 'geometry']].reset_index(drop=True)

for year in years:
    for month in months:
        print(f'Starting aggregation for {year} {month}')
        
        fc_path = f'/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc/0.125/t2m/00/24/{year}/{month}/*.nc'
        an_path = f'/glade/derecho/scratch/dcalhoun/ecmwf/ifs/an/0.125/t2m/{year}/{month}/*/*.nc'
        clim_path = '/glade/derecho/scratch/dcalhoun/ecmwf/era5/era5_2t_climatology_1991_2020_sorted.nc'

        ## Read in forecast, analysis, and climatology data and compute anomalies
        # Forecast
        fc_files = sorted(glob.glob(fc_path))
        ds_fc = xr.open_mfdataset(fc_files)
        
        # Analysis
        an_files = sorted(glob.glob(an_path))
        ds_an = xr.open_mfdataset(an_files)
        
        # Climatology
        ds_clim = xr.open_dataset(clim_path)
        
        # Ensure all times present in both fc and an
        common_times = np.intersect1d(ds_fc['t2m'].time.values, ds_an['t2m'].time.values)
        ds_fc = ds_fc.sel(time=common_times)
        ds_an = ds_an.sel(time=common_times)

        # Remove leap day
        ds_fc = ds_fc.sel(time=~((ds_fc.time.dt.month == 2) & (ds_fc.time.dt.day == 29)))
        ds_an = ds_an.sel(time=~((ds_an.time.dt.month == 2) & (ds_an.time.dt.day == 29)))
        
        # Interpolate the climatology to the same grid as the forecast and analysis
        ds_clim = ds_clim.sel(
            latitude=slice(ds_fc.latitude.min(), ds_fc.latitude.max()),
            longitude=slice(ds_fc.longitude.min(), ds_fc.longitude.max())
        )
        ds_clim = ds_clim.interp(latitude = ds_fc.latitude.values, longitude = ds_fc.longitude.values, method='nearest')
        
        # Calculate anomalies
        ds_clim = ds_clim.sel(time=pd.to_datetime(ds_fc.time.dt.strftime('2017-%m-%d')).dayofyear) # align climatology to forecast data
        ds_fc['t2m'] = (['time', 'latitude', 'longitude'], ds_fc['t2m'].values - ds_clim['2t'].values)
        ds_an['t2m'] = (['time', 'latitude', 'longitude'], ds_an['t2m'].values - ds_clim['2t'].values)
        
        ## Calculate absolute error
        ds_abs_error = xr.Dataset(
            {'t2m_abs_error': (['time', 'latitude','longitude'], abs(ds_fc.t2m - ds_an.t2m).values)},
            coords = {
                'time':(['time'], ds_fc.time.values),
                'latitude' : (['latitude'], ds_fc.latitude.values),
                'longitude' : (['longitude'], ds_fc.longitude.values)
            }
        )
        
        ## Aggregation
        dates = pd.to_datetime(ds_abs_error.time.values)
        for i, date in enumerate(dates):
            day = date.strftime('%d')
            hour = date.strftime('%H')
            dirname = f'/glade/derecho/scratch/dcalhoun/aggregated/county/Maryland/ifs/0.125/t2m/00/24/{year}/{month}'
            os.makedirs(dirname, exist_ok=True)
            filename = os.path.join(dirname, f'aggregated_ifs_abs_error_t2m_{year}_{month}_{day}_{hour}z_MD.csv')
            weightmap = xagg.pixel_overlaps(ds_abs_error.sel(time=date), gdf, silent=True)
            aggregated = xagg.aggregate(ds_abs_error.sel(time=date), weightmap, silent=True)
            df = aggregated.to_dataframe()
            df['time'] = date.strftime('%Y-%m-%d-%H')
            df.reset_index(inplace=True, drop=True)
            cols = df.columns.tolist()
            df = df[cols[0:1] + cols[-1:] + cols[1:-1]] # Reorder columns
            df.to_csv(filename) # Save aggregated data
            print(f'Saved {filename}')
        
        print(f'Finished aggregation for {year} {month} \n')