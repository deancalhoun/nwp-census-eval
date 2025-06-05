import xarray as xr
import xagg
import geopandas as gpd
import numpy as np
import pandas as pd
import glob
import cfgrib
import os

### Aggregating IFS 2m Temperature Forecast Absolute Error with Maryland Counties ###
### 00z initialization time, 24 hour lead time, 2016-2024 ###

# Read in the shapefile
shapefile_path = '/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_tract_2023'
gdf = gpd.read_file(shapefile_path, columns=['GEOID', 'STATEFP', 'geometry']).to_crs('WGS84')

# Maryland only
gdf = gdf[gdf['STATEFP'] == '24'][['GEOID', 'geometry']].reset_index(drop=True)

# Read in cleaned abs error data
ds_abs_error = xr.open_dataset('/glade/derecho/scratch/dcalhoun/ecmwf/ifs/error/0.125/2t/0000/24/ifs_2t_abs_error_anom_0000_24_2016_2024.nc')

## Aggregation
dates = pd.to_datetime(ds_abs_error.time.values)
for i, date in enumerate(dates):
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')
    hour = date.strftime('%H')
    dirname = f'/glade/derecho/scratch/dcalhoun/aggregated/tract/Maryland/ifs/0.125/t2m/00/24/{year}/{month}'
    os.makedirs(dirname, exist_ok=True)
    filename = os.path.join(dirname, f'aggregated_ifs_abs_error_anom_t2m_{year}_{month}_{day}_{hour}z_MD.csv')
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
