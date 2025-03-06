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
shapefile_path = '/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_county_2023/US_county_2023.shp'
save_dir = '/glade/derecho/scratch/dcalhoun/aggregated'
bounds = [50, -130, 25, -40]

## Read shapefile
gdf = gpd.read_file(shapefile_path).to_crs('WGS84')
gdf["INTPTLAT"] = pd.to_numeric(gdf["INTPTLAT"])
gdf["INTPTLON"] = pd.to_numeric(gdf["INTPTLON"])

## Read data and aggregate
for score in scores:
    for model_name in model_names:
        for lead_time in lead_times:
            path = os.path.join(data_dir, score, model_name, lead_time, '*.nc')
            data_path_list = glob.glob(path)
            for data_path in data_path_list
                ds = xr.open_dataset(data_path)
                outfile = os.path.join(save_dir, os.path.split(data_path)[-1][:-3] + "_aggregated.shp")
                if not os.path.exists(outfile):
                    cwi.aggregate_to_geography(ds=ds, gdf=gdf, outfile=outfile, bounds=bounds)
     