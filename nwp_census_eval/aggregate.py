import glob
import netCDF4
import os
import xagg
import numpy as np
import xarray as xr
import geopandas as gpd
import pandas as pd
from typing import Any
from xagg.auxfuncs import fix_ds
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

class GeoAggregator:
    def __init__(self, shapefile_path, grid_path, coords="WGS84", silent=True):
        '''
        Initializes the GeoAggregator class.
        Inputs:
            shapefile_path: path to the shapefile to aggregate to (str)
            grid_path: path to a sample gridded data file to aggregate (str)
            coords: coordinate system of the data (str)
        '''
        # Allow either NetCDF-like files or GeoTIFFs for the sample grid.
        ext = os.path.splitext(grid_path)[1].lower()
        if ext in (".tif", ".tiff"):
            da = xr.open_dataarray(grid_path)
            # Ensure there is at least one data variable name
            if da.name is None:
                da = da.rename("var")
            sample_datafile = da.to_dataset()
        else:
            with xr.open_dataset(grid_path) as ds:
                sample_datafile = ds.load().copy(deep=True)
        # Pre-process for xagg: convert lon 0-360 to -180-180, add bounds (avoids read-only error in xagg.get_bnds)
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
    """
    Aggregates a categorical gridded field to polygons by computing, for each
    polygon (and optional time), the top two categories and their percentages.

    The output is a `pandas.DataFrame` with one row per polygon (and time, if
    present) and four new columns:

    - `category_1`: most frequent category
    - `category_1_pct`: percentage of area covered by `category_1` (0–100)
    - `category_2`: second most frequent category (or NaN if none)
    - `category_2_pct`: percentage of area covered by `category_2` (0–100)
    """

    def __init__(
        self,
        shapefile_path: str,
        categorical_file: str,
        var_name: str,
        coords: str = "WGS84",
        silent: bool = True,
    ):
        """
        Initialize from a shapefile and a sample categorical gridded file.

        The categorical file is used to define the grid for building the
        weightmap; any other categorical files passed to `aggregate` must
        share this same grid.
        """
        super().__init__(shapefile_path, grid_path=categorical_file, coords=coords, silent=silent)
        self.var_name = var_name

    @classmethod
    def from_GeoAggregator(cls, geo_aggregator: GeoAggregator, var_name: str) -> "CategoricalAggregator":
        """
        Construct a CategoricalAggregator that reuses an existing GeoAggregator's
        grid, shapefile, and weightmap (no new weightmap computation).
        """
        instance = cls.__new__(cls)
        # Inherit spatial state from the provided GeoAggregator
        instance.shapefile_path = geo_aggregator.shapefile_path
        instance.grid = geo_aggregator.grid
        instance.shapefile = geo_aggregator.shapefile
        instance.weightmap = geo_aggregator.weightmap
        instance.latname = geo_aggregator.latname
        instance.lonname = geo_aggregator.lonname
        instance.coords = geo_aggregator.coords
        instance.silent = geo_aggregator.silent
        # Categorical-specific state
        instance.var_name = var_name
        return instance

    def aggregate(self, datafile_path: str) -> pd.DataFrame:
        """
        Aggregate a categorical field from a gridded file to the polygons.

        Parameters
        ----------
        datafile_path : str
            Path to the gridded categorical data file to aggregate (NetCDF or GeoTIFF).

        Returns
        -------
        pandas.DataFrame
            DataFrame with polygon identifiers (and time, if present) and
            four columns describing the top two categories and their
            corresponding percentages.
        """
        # Load the dataset from NetCDF or GeoTIFF and ensure consistent lat/lon naming
        ext = os.path.splitext(datafile_path)[1].lower()
        if ext in (".tif", ".tiff"):
            da_raw = xr.open_dataarray(datafile_path)
            # Ensure the DataArray has the expected name so it becomes a
            # variable with that name when converted to a Dataset.
            if da_raw.name is None or da_raw.name != self.var_name:
                da_raw = da_raw.rename(self.var_name)
            ds = da_raw.to_dataset()
        else:
            with xr.open_dataset(datafile_path) as ds_in:
                ds = ds_in.load().copy(deep=True)

        ds = fix_ds(ds)

        if self.var_name not in ds:
            raise KeyError(f"Variable {self.var_name!r} not found in {datafile_path!r}")

        da = ds[self.var_name]

        # Support either 2D (lat, lon) or 3D (time, lat, lon) categorical fields.
        # Any additional dimensions are not currently supported.
        allowed_dims = {self.latname, self.lonname}
        extra_dims = set(da.dims) - allowed_dims
        # Allow a single time-like dimension (named "time", case-insensitive)
        time_like_dims = {dim for dim in extra_dims if dim.lower() == "time"}
        extra_dims = extra_dims - time_like_dims
        if extra_dims:
            raise ValueError(
                f"CategoricalAggregator currently supports at most one extra "
                f"time-like dimension in addition to ({self.latname}, {self.lonname}). "
                f"Found unsupported extra dimensions: {sorted(extra_dims)}"
            )

        # Compute unique categories across the dataset, ignoring NaNs
        values = da.values
        flat = values.ravel()
        # Mask out NaNs for numeric dtypes
        if np.issubdtype(flat.dtype, np.number):
            flat = flat[~np.isnan(flat)]
        unique_cats = pd.unique(flat)
        if len(unique_cats) == 0:
            raise ValueError("No valid (non-NaN) categories found in the input data.")

        # Build an indicator-variable dataset: one float field per category.
        # Each indicator is 1 where the category is present, else 0.
        indicator_ds = xr.Dataset()
        for idx, cat in enumerate(unique_cats):
            var = f"cat_{idx}"
            indicator = (da == cat).astype(float)
            indicator_ds[var] = indicator

        # Aggregate the indicator variables using the precomputed weightmap.
        # The mean of each indicator over a polygon is the area-weighted
        # fraction of that polygon covered by the category.
        aggregated = xagg.aggregate(indicator_ds, self.weightmap, silent=self.silent)

        df = aggregated.to_dataframe().reset_index()

        # Determine ID and time columns, if present
        id_col = None
        if "GEOID" in df.columns:
            id_col = "GEOID"
        elif "geo_id" in df.columns:
            id_col = "geo_id"

        time_col = None
        for col in ("time", "valid_time"):
            if col in df.columns:
                time_col = col
                break

        cat_cols = [f"cat_{idx}" for idx in range(len(unique_cats))]

        def _top_two(row: pd.Series) -> pd.Series:
            vals = row[cat_cols].to_numpy(dtype=float)
            # Handle rows with all-NaN or all-zero coverage
            if np.all(np.isnan(vals)) or np.nansum(vals) == 0.0:
                return pd.Series(
                    {
                        "category_1": np.nan,
                        "category_1_pct": 0.0,
                        "category_2": np.nan,
                        "category_2_pct": 0.0,
                    }
                )

            order = np.argsort(vals)[::-1]
            first_idx = order[0]
            second_idx = order[1] if len(order) > 1 else None

            cat1 = unique_cats[first_idx]
            pct1 = float(vals[first_idx]) * 100.0

            if second_idx is not None and not np.isnan(vals[second_idx]) and vals[second_idx] > 0.0:
                cat2 = unique_cats[second_idx]
                pct2 = float(vals[second_idx]) * 100.0
            else:
                cat2 = np.nan
                pct2 = 0.0

            return pd.Series(
                {
                    "category_1": cat1,
                    "category_1_pct": pct1,
                    "category_2": cat2,
                    "category_2_pct": pct2,
                }
            )

        top_two_df = df.apply(_top_two, axis=1)

        # Assemble the final DataFrame with identifier (and time, if present)
        keep_cols = []
        if id_col is not None:
            keep_cols.append(id_col)
        if time_col is not None:
            keep_cols.append(time_col)

        out = pd.concat([df[keep_cols].reset_index(drop=True), top_two_df], axis=1)

        # Standardize geo identifier column name if possible
        if id_col == "GEOID":
            out = out.rename(columns={"GEOID": "geo_id"})

        return out