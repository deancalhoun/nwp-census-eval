import os
import sys
import xarray as xr
import geopandas as gpd
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import censuswxindex as cwi

## Parameters
data_dir = '/glade/derecho/scratch/dcalhoun/ecmwf/scores'
scores = ['rmse', 'acc']
model_names = ['ifs', 'aifs']
lead_times = ['00', '06', '12', '24', '48', '72', '96', '120', '168', '240']
shapefile_path='/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_county_2023/US_county_2023.shp'
target_dir='/glade/derecho/scratch/dcalhoun/aggregated'
bounds = [50, -130, 25, -40]

## Read shapefile
gdf = gpd.read_file(shapefile_path).to_crs('WGS84')
gdf["INTPTLAT"] = pd.to_numeric(gdf["INTPTLAT"])
gdf["INTPTLON"] = pd.to_numeric(gdf["INTPTLON"])

## Read data and aggregate
for score in scores:
    for model_name in model_names:
        for lead_time in lead_times:
            data_path_lis = [os.path.join(data_dir, model_name, score, f'{model_name}_{score}_{lead_time}.nc')]
            for data_path in data_path_lis
                ds = xr.open_dataset(data_path).isel(time=0)
                outfile = os.path.join(target_dir, os.path.splitext(data_path)[1][:-3] + "_aggregated.shp")
                if not os.path.exists(outfile):
                    cwi.aggregate_to_geography(ds=ds, gdf=gdf, outfile=outfile, bounds=bounds)
     