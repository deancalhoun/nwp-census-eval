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
    def __init__(self, shapefile_path, grid_path, latname='latitude', lonname='longitude', coords="WGS84", silent=True):
        '''
        Initializes the GeoAggregator class.
        Inputs:
            shapefile_path: path to the shapefile to aggregate to (str)
            grid_path: path to a sample gridded data file to aggregate (str)
            coords: coordinate system of the data (str)
        '''
        sample_datafile = xr.open_dataset(grid_path)
        self.grid = sample_datafile[[latname, lonname]].coords
        self.shapefile = gpd.read_file(shapefile_path).to_crs(coords)
        self.weightmap = xagg.pixel_overlaps(sample_datafile, self.shapefile, silent=silent)

        self.shapefile_path = shapefile_path
        self.latname = latname
        self.lonname = lonname
        self.coords = coords
        self.silent = silent

    def __repr__(self):
        return f"GeoAggregator\nshapefile: {self.shapefile_path!r}\n{self.grid!r}\nCRS: {self.coords!r}"

    def aggregate(self, datafile_path, var_name):
        '''
        Aggregates the data to the specified geography.
        Inputs:
            datafile_path: path to the data file to aggregate (str)
        Returns:
            aggregated: the aggregated data (xr.Dataset)
        '''
        ds = xr.open_dataset(datafile_path)
        aggregated = xagg.aggregate(ds, self.weightmap, silent=self.silent)
        df_agg = aggregated.to_dataframe().dropna(subset=[var_name]).reset_index().drop(columns=['poly_idx'])
        return df_agg

class ForecastAggregator(GeoAggregator):
    def __init__(self, shapefile_path, forecast_files, var_name, latname='latitude', lonname='longitude', coords="WGS84", silent=True):
        assert all(
            isinstance(item, tuple) and len(item) == 3
            for item in forecast_files
        ), "forecast_files must be [(file_path, init_time, lead_time), ...]"
        grid_path = forecast_files[0][0]
        super().__init__(shapefile_path, grid_path, latname, lonname, coords, silent)
        self._set_forecast_state(forecast_files, var_name)
        self._assert_forecast_grids_match()

    def __repr__(self):
        n_files = len(self.forecast_files)
        if len(self.init_times) > 6:
            init_times = '\t'+'\n\t'.join(self.init_times[:3]+['...']+self.init_times[-3:])
        else:
            init_times = '\t'+'\n\t'.join(self.init_times)
        return f"ForecastAggregator\nshapefile: {self.shapefile_path!r}\nn_files: {n_files}\ninit_times:\n{init_times}\nlead_times: {self.lead_times!r}\nvar_name: {self.var_name!r}\n{self.grid!r}\nCRS: {self.coords!r}"

    @classmethod
    def from_GeoAggregator(cls, geo_aggregator, forecast_files, var_name):
        assert all(
            isinstance(item, tuple) and len(item) == 3
            for item in forecast_files
        ), "forecast_files must be [(file_path, init_time, lead_time), ...]"
        instance = cls.__new__(cls)
        # Copy parent state
        instance.shapefile_path = geo_aggregator.shapefile_path
        instance.grid = geo_aggregator.grid
        instance.shapefile = geo_aggregator.shapefile
        instance.weightmap = geo_aggregator.weightmap
        instance.latname = geo_aggregator.latname
        instance.lonname = geo_aggregator.lonname
        instance.coords = geo_aggregator.coords
        instance.silent = geo_aggregator.silent
        # Forecast-specific
        instance._set_forecast_state(forecast_files, var_name)
        instance._assert_forecast_grids_match()
        return instance

    def _set_forecast_state(self, forecast_files, var_name):
        self.var_name = var_name
        self.forecast_files = forecast_files
        self.init_times = sorted([str(x) for x in np.unique([t[1] for t in forecast_files])])
        self.lead_times = [int(x) for x in np.unique([t[2] for t in forecast_files])]
        self.fc_data_table = None

    def _assert_forecast_grids_match(self):
        """Assert all forecast files share the same lat/lon grid as self.grid."""
        ref_lat = self.grid[self.latname].values
        ref_lon = self.grid[self.lonname].values
        for path, _, _ in self.forecast_files:
            with xr.open_dataset(path) as ds:
                lat_ok = np.array_equal(ds[self.latname].values, ref_lat)
                lon_ok = np.array_equal(ds[self.lonname].values, ref_lon)
                if not (lat_ok and lon_ok):
                    raise ValueError(
                        f"Grid in {path} does not match expected grid. "
                        f"All forecast files (and the inherited grid if from GeoAggregator) must have identical lat/lon."
                    )

    def aggregate(self, datafile_path, init_time, lead_time):
        '''
        Aggregates for one (init_time, lead_time) combination.
        '''
        # Aggregate the data using the parent method with the current var_name
        df_agg = super().aggregate(datafile_path, self.var_name).rename(columns={'time': 'valid_time', 'GEOID': 'geo_id'})
        
        # Assign forecast metadata columns
        df_agg['lead_time'] = lead_time
        df_agg['init_time'] = pd.to_datetime(df_agg['valid_time']) - pd.to_timedelta(df_agg['lead_time'], unit='h')
        
        # Assert the reconstructed init_time matches the provided init_time
        unique_init_times = pd.to_datetime(df_agg['init_time'].unique())
        input_init_time = pd.to_datetime(init_time)
        assert len(unique_init_times) == 1, (
            f"Provided datafile {datafile_path} contains multiple init_times: {unique_init_times}"
        )
        assert (unique_init_times[0] == input_init_time), (
            f"Provided init_time {input_init_time} does not match reconstructed {unique_init_times[0]} "
            f"for lead_time {lead_time} from {datafile_path}"
        )

        df_fc = df_agg[['geo_id', 'valid_time', 'init_time', 'lead_time', self.var_name]]
        return df_fc

    def build_data_table(self):
        results = []
        for datafile_path, init_time, lead_time in self.forecast_files:
            df_fc = self.aggregate(datafile_path, init_time, lead_time)
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