from typing import Any


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
        self.shapefile_path = shapefile_path
        self.datafile_paths = datafile_paths
        self.shapefile = gpd.read_file(self.shapefile_path).to_crs(coords)
        self.sample_datafile = xr.open_dataset(datafile_paths[0])
        self.weightmap = xagg.pixel_overlaps(self.sample_datafile, self.shapefile, silent=silent)
        self.var_name = var_name
        self.coords = coords
        self.silent = silent

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
        df_agg = aggregated.to_dataframe().dropna(subset=[self.var_name]).reset_index().drop(columns=['poly_idx'])
        return df_agg

class ForecastAggregator(GeoAggregator):
    def __init__(self, shapefile_path, forecast_files, var_name, coords="WGS84", silent=True):
        assert all(
            isinstance(item, tuple) and len(item) == 2
            for item in forecast_files
        ), "forecast_files must be [(file_path, lead_time), ...]"
        datafile_paths = [path for path, _ in forecast_files]
        super().__init__(shapefile_path, datafile_paths, var_name, coords, silent)
        self.forecast_files = forecast_files
        self.lead_times = [int(x) for x in np.unique([lead_time for _ , lead_time in forecast_files])]
        self.fc_data_table = None

    def aggregate_for_one_lead_time(self, datafile_path, lead_time):
        df_agg = self.aggregate(datafile_path).rename(columns={'time': 'valid_time', 'GEOID': 'geo_id'})
        df_agg['lead_time'] = lead_time
        df_agg['init_time'] = pd.to_datetime(df_agg['valid_time']) - pd.to_timedelta(df_agg['lead_time'], unit='h')
        df_fc = df_agg[['geo_id', 'valid_time', 'init_time', 'lead_time', self.var_name]]
        return df_fc

    def build_data_table(self):
        results = []
        for datafile_path, lead_time in self.forecast_files:
            df_fc = self.aggregate_for_one_lead_time(datafile_path, lead_time)
            results.append(df_fc)
        self.fc_data_table = pd.concat(results, ignore_index=True)
        return self.fc_data_table

    def save_data_table(self, save_path):
        assert self.fc_data_table is not None
        self.fc_data_table.to_csv(save_path)

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