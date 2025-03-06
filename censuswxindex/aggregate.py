import censuswxindex as cwi
import xarray as xr
import geopandas as gpd
import pandas as pd
import os
import matplotlib.pyplot as plt

data_path='/glade/derecho/scratch/dcalhoun/ecmwf/ifs/fc/t2m/00/06/2016/01/ifs_fc_t2m_2016_01_01_06z.nc'
shapefile_path='/glade/derecho/scratch/dcalhoun/census/shapefiles/nhgis0001_shapefile_tl2023_us_county_2023/US_county_2023.shp'
target_dir='/glade/derecho/scratch/dcalhoun/aggregated'
outfile = os.path.join(target_dir, os.path.splitext(data_path)[1][:-3] + "_aggregated.shp")
bounds = [50, -130, 25, -40]

ds = xr.open_dataset(data_path).isel(time=0)
gdf = gpd.read_file(shapefile_path).to_crs('WGS84')
gdf["INTPTLAT"] = pd.to_numeric(gdf["INTPTLAT"])
gdf["INTPTLON"] = pd.to_numeric(gdf["INTPTLON"])

if not os.path.exists(outfile):
    cwi.aggregate_to_geography(ds=ds, gdf=gdf, outfile=outfile, bounds=bounds)

aggregated = gpd.read_file(outfile)
aggregated.plot(column='t2m', legend=True)
plt.savefig('aggregated.png')
     