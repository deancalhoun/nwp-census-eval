from typing import Any

import numpy as np
import xarray as xr

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def _maybe_progress(iterable, total, desc, silent, position=None):
    """Wrap iterable with tqdm when silent=False and tqdm available; else log at intervals.
    position: optional line index for tqdm (for multiple concurrent progress bars)."""
    if silent:
        return iterable
    if tqdm is not None:
        kwargs = dict(total=total, desc=desc, unit="file")
        if position is not None:
            kwargs["position"] = position
            kwargs["leave"] = True
        return tqdm(iterable, **kwargs)
    # Fallback: log every 10% or every 10 files, whichever is more frequent
    step = max(1, min(total // 10, 10))
    return _log_progress(iterable, total, desc, step)


def _log_progress(iterable, total, desc, step):
    """Yield items and print progress every step items."""
    for i, item in enumerate(iterable):
        if i > 0 and i % step == 0:
            print(f"  {desc}: {i}/{total}")
        yield item


def _add_writable_bnds(ds):
    """Add lat_bnds and lon_bnds so xagg skips get_bnds (avoids read-only error)."""
    for var in ['lat', 'lon']:
        coord = np.array(ds[var].values, copy=True)
        diff = np.diff(coord)
        if len(diff) == 0:
            bnds = np.array([[coord[0] - 0.5, coord[0] + 0.5]])
        else:
            edges = np.concatenate(
                [[coord[0] - diff[0] / 2], coord[:-1] + diff / 2, [coord[-1] + diff[-1] / 2]]
            )
            bnds = np.stack([edges[:-1], edges[1:]], axis=1)
        ds[var + '_bnds'] = xr.DataArray(bnds, dims=(var, 'bnds'), coords={var: ds[var]})

import geopandas as gpd
import pandas as pd
import glob
import netCDF4
import os
import xagg

class GeoAggregator:
    def __init__(self, shapefile_path, grid_path, coords="WGS84", silent=True):
        '''
        Initializes the GeoAggregator class.
        Inputs:
            shapefile_path: path to the shapefile to aggregate to (str)
            grid_path: path to a sample gridded data file to aggregate (str)
            coords: coordinate system of the data (str)
        '''
        with xr.open_dataset(grid_path) as ds:
            sample_datafile = ds.load().copy(deep=True)
        # Pre-process for xagg: convert lon 0-360 to -180-180, add bounds (avoids read-only error in xagg.get_bnds)
        from xagg.auxfuncs import fix_ds
        sample_datafile = fix_ds(sample_datafile)
        _add_writable_bnds(sample_datafile)
        self.grid = sample_datafile[['lat', 'lon']].coords
        self.shapefile = gpd.read_file(shapefile_path).to_crs(coords)
        if not silent:
            print("Building weightmap from grid and shapefile...")
        self.weightmap = xagg.pixel_overlaps(sample_datafile, self.shapefile, silent=silent)

        self.shapefile_path = shapefile_path
        self.latname = 'lat'  # fix_ds renames to lat/lon
        self.lonname = 'lon'
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
        from xagg.auxfuncs import fix_ds
        ds = fix_ds(ds)  # weightmap expects lat/lon
        aggregated = xagg.aggregate(ds, self.weightmap, silent=self.silent)
        df_agg = aggregated.to_dataframe().dropna(subset=[var_name]).reset_index().drop(columns=['poly_idx'])
        return df_agg

class ForecastAggregator(GeoAggregator):
    def __init__(self, shapefile_path, forecast_files, var_name, coords="WGS84", silent=True):
        assert all(
            isinstance(item, tuple) and len(item) == 3
            for item in forecast_files
        ), "forecast_files must be [(file_path, init_time, lead_time), ...]"
        grid_path = forecast_files[0][0]
        super().__init__(shapefile_path, grid_path, coords, silent)
        self._set_forecast_state(forecast_files, var_name)
        self._assert_forecast_grids_match()

    def __repr__(self):
        n_files = len(self.forecast_files)
        if len(self.init_times) > 6:
            init_times = '\t'+'\n\t'.join(self.init_times[:3]+['...']+self.init_times[-3:])
        else:
            init_times = '\t'+'\n\t'.join(self.init_times)
        return f"ForecastAggregator\nn_files: {n_files}\ninit_times:\n{init_times}\nlead_times: {self.lead_times!r}\nvar_name: {self.var_name!r}\nshapefile: {self.shapefile_path!r}\n{self.grid!r}\nCRS: {self.coords!r}"

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
        ref_lat = self.grid['lat'].values
        ref_lon = self.grid['lon'].values
        from xagg.auxfuncs import fix_ds
        for path, _, _ in self.forecast_files:
            with xr.open_dataset(path) as ds:
                ds = fix_ds(ds)  # files may use latitude/longitude; normalize to lat/lon
                lat_ok = np.array_equal(ds['lat'].values, ref_lat)
                lon_ok = np.array_equal(ds['lon'].values, ref_lon)
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
        n = len(self.forecast_files)
        for datafile_path, init_time, lead_time in _maybe_progress(
            self.forecast_files, n, "Forecast", self.silent
        ):
            df_fc = self.aggregate(datafile_path, init_time, lead_time)
            results.append(df_fc)
        self.fc_data_table = pd.concat(results, ignore_index=True)
        return self.fc_data_table

    def save_data_table(self, save_path):
        assert self.fc_data_table is not None
        self.fc_data_table.to_csv(save_path)

class AnalysisAggregator(GeoAggregator):
    def __init__(self, shapefile_path, analysis_files, var_name, coords="WGS84", silent=True):
        assert all(
            isinstance(item, tuple) and len(item) == 2
            for item in analysis_files
        ), "analysis_files must be [(file_path, time), ...]"
        grid_path = analysis_files[0][0]
        super().__init__(shapefile_path, grid_path, coords, silent)
        self._set_analysis_state(analysis_files, var_name)
        self._assert_analysis_grids_match()

    def __repr__(self):
        n_files = len(self.analysis_files)
        if len(self.times) > 6:
            times = '\t' + '\n\t'.join(self.times[:3] + ['...'] + self.times[-3:])
        else:
            times = '\t' + '\n\t'.join(self.times)
        return f"AnalysisAggregator\nn_files: {n_files}\ntimes:\n{times}\nvar_name: {self.var_name!r}\nshapefile: {self.shapefile_path!r}\n{self.grid!r}\nCRS: {self.coords!r}"

    @classmethod
    def from_GeoAggregator(cls, geo_aggregator, analysis_files, var_name):
        assert all(
            isinstance(item, tuple) and len(item) == 2
            for item in analysis_files
        ), "analysis_files must be [(file_path, time), ...]"
        instance = cls.__new__(cls)
        instance.shapefile_path = geo_aggregator.shapefile_path
        instance.grid = geo_aggregator.grid
        instance.shapefile = geo_aggregator.shapefile
        instance.weightmap = geo_aggregator.weightmap
        instance.latname = geo_aggregator.latname
        instance.lonname = geo_aggregator.lonname
        instance.coords = geo_aggregator.coords
        instance.silent = geo_aggregator.silent
        instance._set_analysis_state(analysis_files, var_name)
        instance._assert_analysis_grids_match()
        return instance

    def _set_analysis_state(self, analysis_files, var_name):
        self.var_name = var_name
        self.analysis_files = analysis_files
        self.times = sorted([str(x) for x in np.unique([t[1] for t in analysis_files])])
        self.an_data_table = None

    def _assert_analysis_grids_match(self):
        """Assert all analysis files share the same lat/lon grid as self.grid."""
        ref_lat = self.grid['lat'].values
        ref_lon = self.grid['lon'].values
        from xagg.auxfuncs import fix_ds
        for path, _ in self.analysis_files:
            with xr.open_dataset(path) as ds:
                ds = fix_ds(ds)  # files may use latitude/longitude; normalize to lat/lon
                lat_ok = np.array_equal(ds['lat'].values, ref_lat)
                lon_ok = np.array_equal(ds['lon'].values, ref_lon)
                if not (lat_ok and lon_ok):
                    raise ValueError(
                        f"Grid in {path} does not match expected grid. "
                        f"All analysis files (and the inherited grid if from GeoAggregator) must have identical lat/lon."
                    )

    def aggregate(self, datafile_path, time):
        """Aggregates an analysis file for a single time. Selects the time slice before aggregating."""
        from xagg.auxfuncs import fix_ds
        with xr.open_dataset(datafile_path) as ds:
            ds_slice = ds.sel(time=pd.to_datetime(time), method='nearest')
        ds_slice = fix_ds(ds_slice)  # weightmap expects lat/lon
        aggregated = xagg.aggregate(ds_slice, self.weightmap, silent=self.silent)
        df_agg = aggregated.to_dataframe().dropna(subset=[self.var_name]).reset_index().drop(columns=['poly_idx']).rename(columns={'GEOID': 'geo_id'})
        df_agg['time'] = pd.to_datetime(time)
        return df_agg[['geo_id', 'time', self.var_name]]

    def build_data_table(self):
        results = []
        n = len(self.analysis_files)
        for datafile_path, time in _maybe_progress(
            self.analysis_files, n, "Analysis", self.silent
        ):
            df_an = self.aggregate(datafile_path, time)
            results.append(df_an)
        self.an_data_table = pd.concat(results, ignore_index=True)
        return self.an_data_table

    def save_data_table(self, save_path):
        assert self.an_data_table is not None
        self.an_data_table.to_csv(save_path)

class CategoricalAggregator(GeoAggregator):
    pass