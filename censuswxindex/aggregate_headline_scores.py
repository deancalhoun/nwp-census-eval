import os
import sys
import xarray as xr
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import censuswxindex as cwi

## Parameters
data_dir = ''
model_name = ''
param = ''
path = os.path.join(data_dir, '*.nc')
data_path_list = glob.glob(path)
shapefile_path='/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_county_2023/US_county_2023.shp'
target_dir='/glade/derecho/scratch/dcalhoun/aggregated'
outfile = os.path.join(target_dir, os.path.splitext(data_path)[1][:-3] + "_aggregated.shp")
bounds = [50, -130, 25, -40]

## Read shapefile
gdf = gpd.read_file(shapefile_path).to_crs('WGS84')
gdf["INTPTLAT"] = pd.to_numeric(gdf["INTPTLAT"])
gdf["INTPTLON"] = pd.to_numeric(gdf["INTPTLON"])

## Read data
for data_path in data_path_lis
ds = xr.open_dataset(data_path).isel(time=0)

if not os.path.exists(outfile):
    cwi.aggregate_to_geography(ds=ds, gdf=gdf, outfile=outfile, bounds=bounds)

aggregated = gpd.read_file(outfile)
aggregated.plot(column='t2m', legend=True)
plt.savefig('aggregated.png')
     