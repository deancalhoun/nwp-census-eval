import xarray as xr
import geopandas as gpd
import numpy as np
import pandas as pd
import glob
import netCDF4
import os
import xagg

class GeoAggregator:
    def __init__(self, shapefile_path, datafile_paths, var_name, coords="WGS84", silent=True):
        '''
        Initializes the GeoAggregator class.
        Inputs:
            shapefile_path: path to the shapefile to aggregate to (str)
            datafile_paths: list of paths to the data files to aggregate (list of str)
            var_name: name of the variable to aggregate (str)
            coords: coordinate system of the data (str)
        '''
        self.coords = coords
        self.silent = silent
        self.shapefile = gpd.read_file(shapefile_path).to_crs(self.coords)
        self.sample_datafile = xr.open_dataset(datafile_paths[0])
        self.weightmap = xagg.pixel_overlaps(self.sample_datafile, self.shapefile, silent=self.silent)
        self.var_name = var_name

    def aggregate(self, datafile_path):
        '''
        Aggregates the data to the specified geography.
        Inputs:
            datafile_path: path to the data file to aggregate (str)
        Returns:
            aggregated: the aggregated data (xr.Dataset)
        '''
        ds = xr.open_dataset(datafile_path)
        aggregated = xagg.aggregate(ds, self.weightmap, silent=self.silent)
        ds_agg = aggregated.to_dataframe().dropna(subset=[self.var_name]).reset_index().drop(columns=['poly_idx'])
        return ds_agg

class ForecastAggregator(GeoAggregator):
    def __init__(self, shapefile_path, datafile_paths, var_name, lead_times, coords="WGS84", silent=True):
        super().__init__(shapefile_path, datafile_paths, var_name)
        self.lead_times = lead_times

    def aggregate_for_one_lead_time(self, datafile_path, lead_time):
        ds_agg = self.aggregate(datafile_path)
        ds_agg['lead_time'] = lead_time
        ds_agg['init_time'] = pd.to_datetime(ds_agg['valid_time']) - pd.to_timedelta(ds_agg['lead_time'], unit='h')
        ds_fc = ds_agg[['GEOID', 'valid_time', 'init_time', 'lead_time', self.var_name]]
        return ds_fc

    def build_data_table(self, start_date, end_date, init_times, lead_times):
        return

class AnalysisAggregator(GeoAggregator):
    def __init__(self, shapefile_path, sample_datafile_path, var_name):
        super().__init__(shapefile_path, sample_datafile_path, var_name)

    def build_data_table(self, start_date, end_date, init_times, lead_times):
        return

class ClimatologyAggregator(GeoAggregator):
    def __init__(self, shapefile_path, sample_datafile_path, var_name):
        super().__init__(shapefile_path, sample_datafile_path, var_name)

    def build_data_table(self, start_date, end_date, init_times, lead_times):
        return

class KoppenAggregator():
    pass