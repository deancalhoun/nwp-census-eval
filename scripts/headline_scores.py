# import xarray as xr
# import glob
# import pandas as pd
# from datetime import datetime
# from dateutil.relativedelta import relativedelta
# import os
# import numpy as np
# from tqdm import tqdm
# from dask.distributed import Client, as_completed
# import sys
# import duckdb

# SCRATCH = "/glade/derecho/scratch/dcalhoun/"
# IFS_FC_DIR = SCRATCH + "ecmwf/ifs/fc/0.125/2t/"
# AIFS_FC_DIR = SCRATCH + "ecmwf/aifs/fc/0.25/2t/"
# AN_DIR = SCRATCH + "ecmwf/ifs/an/0.125/2t/"
# ERA_DIR = SCRATCH + "ecmwf/era5/era5_2t/"
# LSM_PATH = SCRATCH + "ecmwf/ifs/land_sea_mask.nc"
# CLIM_PATH = SCRATCH + "ecmwf/era5/era5_2t_climatology_1991_2020.nc"
# IFS_COUNTY_PATH = SCRATCH + "aggregated/ifs_fc_bias_anom_2t_county.parquet"
# AIFS_COUNTY_PATH = SCRATCH + "aggregated/aifs_fc_bias_anom_2t_county.parquet"
# OUT_PATH = SCRATCH + "headline_scores.csv"
# NUM_WORKERS = 32

# MODELS = ["ifs", "aifs"]
# INITS = [0, 12]
# LEADS = [0, 6, 12, 18, 24, 36, 48, 60, 72, 84, 96, 108, 120, 168, 240]
# YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
# MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


# def read_fc_files(model, init, lead, year, month):
#     init = fix_init(init)
#     assert model in ("ifs", "aifs"), "Must specify a model (ifs, aifs)"
#     dir = {"ifs": IFS_FC_DIR, "aifs": AIFS_FC_DIR}
#     files = glob.glob(dir[model] + f"{init}/{lead}/{year}/{month:02}/*.nc")
#     if files:
#         ds = xr.open_mfdataset(files)
#         if model == "aifs":
#             ds = fix_longitude(ds)
#         return ds
#     return None


# def read_an_files(init, lead, year, month):
#     init = fix_init(init)
#     valid_step = relativedelta(hours=int(lead))
#     start = datetime.strptime(f"{year}-{month}-01-{init}", "%Y-%m-%d-%H%M")
#     end = start + relativedelta(months=1, days=-1)
#     valid_dates = pd.date_range(start + valid_step, end + valid_step, freq="D")
#     files = []
#     for date in valid_dates:
#         file = AN_DIR + f"{date.year}/{date.month:02}/ifs_an_2t_{date.year}{date.month:02}{date.day:02}.nc"
#         if os.path.exists(file):
#             files.append(file)
#     if files:
#         return xr.open_mfdataset(files)
#     return None


# def fc_data_exists(model, init, lead, year, month):
#     init = fix_init(init)
#     dir = {"ifs": IFS_FC_DIR, "aifs": AIFS_FC_DIR}
#     return bool(glob.glob(dir[model] + f"{init}/{lead}/{year}/{month:02}/*.nc"))


# def fix_init(init):
#     # Accept integers (0, 12) or already-formatted strings ("0000", "1200")
#     init_int = int(str(init).lstrip("0") or "0")
#     assert init_int in (0, 12), f"Invalid init '{init}' (must be 00z or 12z)"
#     return {0: "0000", 12: "1200"}[init_int]


# def fix_doy(ds):
#     mask = ~((ds.time.dt.month == 2) & (ds.time.dt.day == 29))
#     ds = ds.sel(time=mask)
#     times = pd.DatetimeIndex(ds.time.values)
#     ds["doy"] = ("time", pd.to_datetime(times.strftime("2001-%m-%d")).dayofyear)
#     return ds


# def fix_longitude(ds):
#     ds["longitude"] = (ds.longitude + 180) % 360 - 180
#     return ds.sortby("longitude")


# def regrid(ds, ds_coords):
#     return ds.interp(latitude=ds_coords.latitude, longitude=ds_coords.longitude)


# def get_common_times(ds1, ds2):
#     return np.intersect1d(ds1.time, ds2.time)


# def make_ds(name, var, coords, attrs):
#     return xr.Dataset(
#         {name: (list(coords.keys()), var)},
#         coords=coords,
#         attrs=attrs
#     )


# def get_lsm(ds_coords):
#     lsm = fix_longitude(xr.open_dataset(LSM_PATH))
#     lsm_val = lsm.lsm.interp(
#         latitude=ds_coords.latitude,
#         longitude=ds_coords.longitude
#     ).compute().values
#     if lsm_val.ndim == 3:
#         lsm_val = lsm_val[0]
#     return make_ds(
#         "lsm",
#         lsm_val,
#         {"latitude": ds_coords.latitude.values, "longitude": ds_coords.longitude.values},
#         {}
#     )


# def lsm_filter(ds, ds_lsm):
#     if ds is None:
#         return None
#     return ds.where(ds_lsm.lsm > 0)


# def aw_avg(da):
#     weights = np.cos(np.deg2rad(da.latitude))
#     return da.mean(dim="time").weighted(weights).mean().values


# def compute_headline_scores(model, init, lead, year, month, lsm):
#     ds_fc = read_fc_files(model, init, lead, year, month)
#     ds_an = read_an_files(init, lead, year, month)
#     if ds_fc is None or ds_an is None:
#         return None

#     # Eagerly load all data before computation to avoid lazy/synchronous scheduler conflicts
#     ds_fc = ds_fc.compute()
#     ds_an = regrid(ds_an, ds_fc).compute()

#     ds_fc = lsm_filter(ds_fc, lsm)
#     ds_an = lsm_filter(ds_an, lsm)

#     times = get_common_times(ds_fc, ds_an)
#     ds_fc_t = fix_doy(ds_fc.sel(time=times))
#     ds_an_t = fix_doy(ds_an.sel(time=times))

#     bias = ds_fc_t.t2m.values - ds_an_t.t2m.values
#     ds_bias = make_ds(
#         "t2m_bias",
#         bias,
#         {
#             "time": ds_fc_t.time.values,
#             "latitude": ds_fc_t.latitude.values,
#             "longitude": ds_fc_t.longitude.values
#         },
#         {"description": "2m temperature forecast bias", "units": "degC"}
#     )

#     ds_clim = regrid(xr.open_dataset(CLIM_PATH).compute(), ds_fc_t)
#     clim = lsm_filter(xr.DataArray(
#         ds_clim.t2m.values[ds_fc_t.doy.values - 1],
#         dims=["time", "latitude", "longitude"],
#         coords={
#             "time": ds_fc_t.time.values,
#             "latitude": ds_fc_t.latitude.values,
#             "longitude": ds_fc_t.longitude.values,
#         }
#     ), lsm)
#     fc_anom = ds_fc_t.t2m - clim
#     an_anom = ds_an_t.t2m - clim

#     weights = np.cos(np.deg2rad(ds_fc_t.latitude))
#     acc = float(
#         (fc_anom * an_anom).weighted(weights).sum(dim=["time", "latitude", "longitude"]) / (
#             np.sqrt((fc_anom**2).weighted(weights).sum(dim=["time", "latitude", "longitude"])) *
#             np.sqrt((an_anom**2).weighted(weights).sum(dim=["time", "latitude", "longitude"]))
#         )
#     )

#     return {
#         "source": "gridded",
#         "model": model,
#         "init": fix_init(init),
#         "valid_hour": (int(fix_init(init)[:2]) + lead) % 24,
#         "lead": int(lead),
#         "year": year,
#         "month": month,
#         "bias": float(aw_avg(ds_bias.t2m_bias)),
#         "mae": float(aw_avg(abs(ds_bias.t2m_bias))),
#         "mse": float(aw_avg(ds_bias.t2m_bias**2)),
#         "acc": acc
#     }


# def compute_scores_safe(model, init, lead, year, month, lsm_coords, lsm_values):
#     """
#     Reconstruct the LSM dataset from raw arrays inside the worker to avoid
#     serializing xarray datasets across the Dask network for every future.
#     """
#     lsm = make_ds(
#         "lsm",
#         lsm_values,
#         {"latitude": lsm_coords[0], "longitude": lsm_coords[1]},
#         {}
#     )
#     try:
#         result = compute_headline_scores(model, init, lead, year, month, lsm)
#         return result, None
#     except Exception as e:
#         return None, f"{model} {init} lead={lead} {year}/{month}: {e}"


# def compute_county_scores():
#     df = duckdb.query(f"""
#         SELECT
#             'county' as source,
#             'ifs' as model,
#             STRFTIME(init_time, '%H%M') as init,
#             HOUR(valid_time) as valid_hour,
#             CAST(lead_time AS INT) as lead,
#             YEAR(valid_time) as year,
#             MONTH(valid_time) as month,
#             SUM(bias * aland) / SUM(aland) as bias,
#             SUM(abs_error * aland) / SUM(aland) as mae,
#             SUM(aland * bias**2) / SUM(aland) as mse,
#             SUM(aland * fc_anom * an_anom) / (
#                 SQRT(SUM(aland * fc_anom**2)) * SQRT(SUM(aland * an_anom**2))
#             ) as acc
#         FROM read_parquet('{IFS_COUNTY_PATH}')
#         GROUP BY STRFTIME(init_time, '%H%M'), lead_time, YEAR(valid_time), MONTH(valid_time), HOUR(valid_time)
#         UNION ALL
#         SELECT
#             'county' as source,
#             'aifs' as model,
#             STRFTIME(init_time, '%H%M') as init,
#             HOUR(valid_time) as valid_hour,
#             CAST(lead_time AS INT) as lead,
#             YEAR(valid_time) as year,
#             MONTH(valid_time) as month,
#             SUM(bias * aland) / SUM(aland) as bias,
#             SUM(abs_error * aland) / SUM(aland) as mae,
#             SUM(aland * bias**2) / SUM(aland) as mse,
#             SUM(aland * fc_anom * an_anom) / (
#                 SQRT(SUM(aland * fc_anom**2)) * SQRT(SUM(aland * an_anom**2))
#             ) as acc
#         FROM read_parquet('{AIFS_COUNTY_PATH}')
#         GROUP BY STRFTIME(init_time, '%H%M'), lead_time, YEAR(valid_time), MONTH(valid_time), HOUR(valid_time)
#         ORDER BY model, lead ASC
#     """).df()
#     return df


# def resume_progress(out_path):
#     if os.path.exists(out_path):
#         df = pd.read_csv(out_path, dtype={"model": str, "init": str, "lead": "Int64", "year": "Int64", "month": "Int64"})
#         df = df.dropna(subset=["model", "init", "lead", "year", "month"])
#         completed = set(
#             df[df["source"] == "gridded"][["model", "init", "lead", "year", "month"]]
#             .apply(lambda r: (r["model"], r["init"], int(r["lead"]), int(r["year"]), int(r["month"])), axis=1)
#             .tolist()
#         )
#         write_header = False
#     else:
#         completed = set()
#         write_header = True
#     return completed, write_header


# def main():
#     ds_fc_ifs = read_fc_files("ifs", 0, 0, 2021, 4)
#     ds_fc_aifs = read_fc_files("aifs", 0, 0, 2024, 3)

#     ds_lsm_ifs = get_lsm(ds_fc_ifs)
#     ds_lsm_aifs = get_lsm(ds_fc_aifs)

#     # Extract raw numpy arrays so workers reconstruct LSM locally
#     # rather than serializing full xarray datasets per future
#     lsm_coords_ifs = (ds_lsm_ifs.latitude.values, ds_lsm_ifs.longitude.values)
#     lsm_values_ifs = ds_lsm_ifs.lsm.values
#     lsm_coords_aifs = (ds_lsm_aifs.latitude.values, ds_lsm_aifs.longitude.values)
#     lsm_values_aifs = ds_lsm_aifs.lsm.values

#     completed, write_header = resume_progress(OUT_PATH)

#     combinations = [
#         (model, init, lead, year, month)
#         for model in MODELS
#         for init in INITS
#         for lead in LEADS
#         for year in YEARS
#         for month in MONTHS
#         if fc_data_exists(model, init, lead, year, month)
#         and (model, fix_init(init), int(lead), int(year), int(month)) not in completed
#     ]
#     print(f"{len(combinations)} combinations remaining.")

#     client = Client(n_workers=NUM_WORKERS, threads_per_worker=1, processes=True)
#     print(f"Dashboard: {client.dashboard_link}", flush=True)
#     try:
#         futures = []
#         for model, init, lead, year, month in combinations:
#             if model == "aifs":
#                 lsm_coords, lsm_values = lsm_coords_aifs, lsm_values_aifs
#             else:
#                 lsm_coords, lsm_values = lsm_coords_ifs, lsm_values_ifs
#             futures.append(
#                 client.submit(
#                     compute_scores_safe,
#                     model, init, lead, year, month,
#                     lsm_coords, lsm_values
#                 )
#             )

#         failures = []
#         with tqdm(as_completed(futures), total=len(futures), position=0, leave=True, dynamic_ncols=True, file=sys.stderr) as pbar:
#             for future in pbar:
#                 try:
#                     result, error = future.result()
#                     if error:
#                         failures.append(error)
#                         pbar.set_postfix_str(f"FAILED: {error[:60]}")
#                     elif result is not None:
#                         pd.DataFrame([result]).to_csv(OUT_PATH, mode="a", header=write_header, index=False)
#                         write_header = False
#                         pbar.set_postfix_str(f"last: {result['model']} {result['init']} lead={result['lead']} {result['year']}/{result['month']:02d}")
#                 except Exception as e:
#                     failures.append(str(e))
#                     pbar.set_postfix_str(f"FAILED: {str(e)[:60]}")
#         if failures:
#             print(f"\n{len(failures)} failures:", file=sys.stderr)
#             for f in failures:
#                 print(f"  {f}", file=sys.stderr)
#     finally:
#         client.shutdown()
#         client.close()

#     df_gridded = pd.read_csv(OUT_PATH, dtype={"model": str, "init": str, "lead": int, "year": int, "month": int})
#     df_county = compute_county_scores()
#     df_county["lead"] = df_county["lead"].astype("int64")

#     df_all = pd.concat([df_gridded, df_county], ignore_index=True)
#     df_all.to_csv(OUT_PATH, index=False)
#     print(f"Done. {len(df_gridded)} gridded + {len(df_county)} county records saved.")

#     validate_completeness(df_gridded)


# def validate_completeness(df_gridded):
#     expected = {
#         (model, fix_init(init), int(lead), int(year), int(month))
#         for model in MODELS
#         for init in INITS
#         for lead in LEADS
#         for year in YEARS
#         for month in MONTHS
#         if fc_data_exists(model, init, lead, year, month)
#     }

#     present = set(
#         df_gridded[df_gridded["source"] == "gridded"][["model", "init", "lead", "year", "month"]]
#         .apply(lambda r: (r["model"], r["init"], int(r["lead"]), int(r["year"]), int(r["month"])), axis=1)
#         .tolist()
#     )

#     missing = expected - present
#     unexpected = present - expected

#     if not missing and not unexpected:
#         print(f"Validation passed: all {len(expected)} expected combinations are present.")
#     else:
#         if missing:
#             print(f"Validation FAILED: {len(missing)} missing combinations:")
#             for combo in sorted(missing):
#                 print(f"  model={combo[0]} init={combo[1]} lead={combo[2]} year={combo[3]} month={combo[4]}")
#         if unexpected:
#             print(f"Validation WARNING: {len(unexpected)} unexpected combinations in output:")
#             for combo in sorted(unexpected):
#                 print(f"  model={combo[0]} init={combo[1]} lead={combo[2]} year={combo[3]} month={combo[4]}")


# if __name__ == "__main__":
#     main()
import xarray as xr
import xesmf as xe
import glob
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import numpy as np
from tqdm import tqdm
from dask.distributed import Client, as_completed
import sys
import duckdb

SCRATCH = "/glade/derecho/scratch/dcalhoun/"
IFS_FC_DIR = SCRATCH + "ecmwf/ifs/fc/0.125/2t/"
AIFS_FC_DIR = SCRATCH + "ecmwf/aifs/fc/0.25/2t/"
AN_DIR = SCRATCH + "ecmwf/ifs/an/0.125/2t/"
ERA_DIR = SCRATCH + "ecmwf/era5/era5_2t/"
LSM_PATH = SCRATCH + "ecmwf/ifs/land_sea_mask.nc"
CLIM_PATH = SCRATCH + "ecmwf/era5/era5_2t_climatology_1991_2020.nc"
IFS_COUNTY_PATH = SCRATCH + "aggregated/ifs_fc_bias_anom_2t_county.parquet"
AIFS_COUNTY_PATH = SCRATCH + "aggregated/aifs_fc_bias_anom_2t_county.parquet"
OUT_PATH = SCRATCH + "headline_scores.csv"
NUM_WORKERS = 32

MODELS = ["ifs", "aifs"]
INITS = [0, 12]
LEADS = [0, 6, 12, 18, 24, 36, 48, 60, 72, 84, 96, 108, 120, 168, 240]
YEARS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


def read_fc_files(model, init, lead, year, month):
    init = fix_init(init)
    assert model in ("ifs", "aifs"), "Must specify a model (ifs, aifs)"
    dir = {"ifs": IFS_FC_DIR, "aifs": AIFS_FC_DIR}
    files = glob.glob(dir[model] + f"{init}/{lead}/{year}/{month:02}/*.nc")
    if files:
        ds = xr.open_mfdataset(files)
        if model == "aifs":
            ds = fix_longitude(ds)
        return ds
    return None


def read_an_files(init, lead, year, month):
    init = fix_init(init)
    valid_step = relativedelta(hours=int(lead))
    start = datetime.strptime(f"{year}-{month}-01-{init}", "%Y-%m-%d-%H%M")
    end = start + relativedelta(months=1, days=-1)
    valid_dates = pd.date_range(start + valid_step, end + valid_step, freq="D")
    files = []
    for date in valid_dates:
        file = AN_DIR + f"{date.year}/{date.month:02}/ifs_an_2t_{date.year}{date.month:02}{date.day:02}.nc"
        if os.path.exists(file):
            files.append(file)
    if files:
        return xr.open_mfdataset(files)
    return None


def fc_data_exists(model, init, lead, year, month):
    init = fix_init(init)
    dir = {"ifs": IFS_FC_DIR, "aifs": AIFS_FC_DIR}
    return bool(glob.glob(dir[model] + f"{init}/{lead}/{year}/{month:02}/*.nc"))


def fix_init(init):
    # Accept integers (0, 12) or already-formatted strings ("0000", "1200")
    init_int = int(str(init).lstrip("0") or "0")
    assert init_int in (0, 12), f"Invalid init '{init}' (must be 00z or 12z)"
    return {0: "0000", 12: "1200"}[init_int]


def fix_doy(ds):
    mask = ~((ds.time.dt.month == 2) & (ds.time.dt.day == 29))
    ds = ds.sel(time=mask)
    times = pd.DatetimeIndex(ds.time.values)
    ds["doy"] = ("time", pd.to_datetime(times.strftime("2001-%m-%d")).dayofyear)
    return ds


def fix_longitude(ds):
    ds["longitude"] = (ds.longitude + 180) % 360 - 180
    return ds.sortby("longitude")


def _add_bounds(ds):
    """
    Add lat/lon bounds required by xesmf conservative regridding.
    xesmf expects lat_b/lon_b as 2D arrays of shape (n_lat+1, n_lon+1)
    on dimensions named lat_b/lon_b (not latitude_b/longitude_b).
    Bounds are inferred from midpoints assuming a regular grid.
    """
    lat = ds.latitude.values
    lon = ds.longitude.values
    dlat = np.abs(np.diff(lat).mean())
    dlon = np.abs(np.diff(lon).mean())
    lat_b_1d = np.concatenate([[lat[0] - dlat / 2], lat + dlat / 2])
    lon_b_1d = np.concatenate([[lon[0] - dlon / 2], lon + dlon / 2])
    lon_b_2d, lat_b_2d = np.meshgrid(lon_b_1d, lat_b_1d)
    return ds.assign_coords(
        lat_b=(["lat_b", "lon_b"], lat_b_2d),
        lon_b=(["lat_b", "lon_b"], lon_b_2d),
    )


def coarsen(ds, ds_target):
    """
    Coarsen ds to the grid of ds_target using conservative regridding via xesmf.
    Conservative regridding computes exact fractional area overlaps between source
    and target grid cells, making it correct for flux/field downsampling.
    The regridder is built once per call; cache externally if calling in a hot loop.
    """
    ds_src = _add_bounds(ds)
    regridder = xe.Regridder(ds_src, ds_target, method="conservative", unmapped_to_nan=True, ignore_degenerate=True)
    return regridder(ds_src, keep_attrs=True)


def regrid(ds, ds_coords):
    return ds.interp(latitude=ds_coords.latitude, longitude=ds_coords.longitude)


def get_common_times(ds1, ds2):
    return np.intersect1d(ds1.time, ds2.time)


def make_ds(name, var, coords, attrs):
    return xr.Dataset(
        {name: (list(coords.keys()), var)},
        coords=coords,
        attrs=attrs
    )


def get_lsm(ds_coords):
    lsm = fix_longitude(xr.open_dataset(LSM_PATH))
    lsm_val = lsm.lsm.interp(
        latitude=ds_coords.latitude,
        longitude=ds_coords.longitude
    ).compute().values
    if lsm_val.ndim == 3:
        lsm_val = lsm_val[0]
    return make_ds(
        "lsm",
        lsm_val,
        {"latitude": ds_coords.latitude.values, "longitude": ds_coords.longitude.values},
        {}
    )


def lsm_filter(ds, ds_lsm):
    if ds is None:
        return None
    return ds.where(ds_lsm.lsm > 0)


def aw_avg(da):
    weights = np.cos(np.deg2rad(da.latitude))
    return da.mean(dim="time").weighted(weights).mean().values


def compute_headline_scores(model, init, lead, year, month, lsm, resolution, aifs_lsm=None):
    ds_fc = read_fc_files(model, init, lead, year, month)
    ds_an = read_an_files(init, lead, year, month)
    if ds_fc is None or ds_an is None:
        return None

    # Eagerly load all data before computation to avoid lazy/synchronous scheduler conflicts
    ds_fc = ds_fc.compute()
    ds_an = regrid(ds_an, ds_fc).compute()

    if resolution == "0.25" and model == "ifs":
        aifs_grid = xr.Dataset(coords={"latitude": aifs_lsm.latitude, "longitude": aifs_lsm.longitude})
        ds_fc = coarsen(ds_fc, aifs_grid)
        ds_an = coarsen(ds_an, aifs_grid)
        lsm = aifs_lsm

    ds_fc = lsm_filter(ds_fc, lsm)
    ds_an = lsm_filter(ds_an, lsm)

    times = get_common_times(ds_fc, ds_an)
    ds_fc_t = fix_doy(ds_fc.sel(time=times))
    ds_an_t = fix_doy(ds_an.sel(time=times))

    bias = ds_fc_t.t2m.values - ds_an_t.t2m.values
    ds_bias = make_ds(
        "t2m_bias",
        bias,
        {
            "time": ds_fc_t.time.values,
            "latitude": ds_fc_t.latitude.values,
            "longitude": ds_fc_t.longitude.values
        },
        {"description": "2m temperature forecast bias", "units": "degC"}
    )

    ds_clim = regrid(xr.open_dataset(CLIM_PATH).compute(), ds_fc_t)
    clim = lsm_filter(xr.DataArray(
        ds_clim.t2m.values[ds_fc_t.doy.values - 1],
        dims=["time", "latitude", "longitude"],
        coords={
            "time": ds_fc_t.time.values,
            "latitude": ds_fc_t.latitude.values,
            "longitude": ds_fc_t.longitude.values,
        }
    ), lsm)
    fc_anom = ds_fc_t.t2m - clim
    an_anom = ds_an_t.t2m - clim

    weights = np.cos(np.deg2rad(ds_fc_t.latitude))
    acc = float(
        (fc_anom * an_anom).weighted(weights).sum(dim=["time", "latitude", "longitude"]) / (
            np.sqrt((fc_anom**2).weighted(weights).sum(dim=["time", "latitude", "longitude"])) *
            np.sqrt((an_anom**2).weighted(weights).sum(dim=["time", "latitude", "longitude"]))
        )
    )

    return {
        "source": "gridded",
        "model": model,
        "resolution": resolution,
        "init": fix_init(init),
        "valid_hour": (int(fix_init(init)[:2]) + lead) % 24,
        "lead": int(lead),
        "year": year,
        "month": month,
        "bias": float(aw_avg(ds_bias.t2m_bias)),
        "mae": float(aw_avg(abs(ds_bias.t2m_bias))),
        "mse": float(aw_avg(ds_bias.t2m_bias**2)),
        "acc": acc
    }


def compute_scores_safe(model, init, lead, year, month, lsm_coords, lsm_values, resolution, aifs_lsm_coords=None, aifs_lsm_values=None):
    """
    Reconstruct the LSM dataset from raw arrays inside the worker to avoid
    serializing xarray datasets across the Dask network for every future.
    """
    lsm = make_ds(
        "lsm",
        lsm_values,
        {"latitude": lsm_coords[0], "longitude": lsm_coords[1]},
        {}
    )
    aifs_lsm = None
    if aifs_lsm_coords is not None and aifs_lsm_values is not None:
        aifs_lsm = make_ds(
            "lsm",
            aifs_lsm_values,
            {"latitude": aifs_lsm_coords[0], "longitude": aifs_lsm_coords[1]},
            {}
        )
    try:
        result = compute_headline_scores(model, init, lead, year, month, lsm, resolution, aifs_lsm)
        return result, None
    except Exception as e:
        return None, f"{model} {init} lead={lead} {year}/{month} res={resolution}: {e}"


def compute_county_scores():
    df = duckdb.query(f"""
        SELECT
            'county' as source,
            'ifs' as model,
            '0.125' as resolution,
            STRFTIME(init_time, '%H%M') as init,
            HOUR(valid_time) as valid_hour,
            CAST(lead_time AS INT) as lead,
            YEAR(valid_time) as year,
            MONTH(valid_time) as month,
            SUM(bias * aland) / SUM(aland) as bias,
            SUM(abs_error * aland) / SUM(aland) as mae,
            SUM(aland * bias**2) / SUM(aland) as mse,
            SUM(aland * fc_anom * an_anom) / (
                SQRT(SUM(aland * fc_anom**2)) * SQRT(SUM(aland * an_anom**2))
            ) as acc
        FROM read_parquet('{IFS_COUNTY_PATH}')
        GROUP BY STRFTIME(init_time, '%H%M'), lead_time, YEAR(valid_time), MONTH(valid_time), HOUR(valid_time)
        UNION ALL
        SELECT
            'county' as source,
            'aifs' as model,
            '0.25' as resolution,
            STRFTIME(init_time, '%H%M') as init,
            HOUR(valid_time) as valid_hour,
            CAST(lead_time AS INT) as lead,
            YEAR(valid_time) as year,
            MONTH(valid_time) as month,
            SUM(bias * aland) / SUM(aland) as bias,
            SUM(abs_error * aland) / SUM(aland) as mae,
            SUM(aland * bias**2) / SUM(aland) as mse,
            SUM(aland * fc_anom * an_anom) / (
                SQRT(SUM(aland * fc_anom**2)) * SQRT(SUM(aland * an_anom**2))
            ) as acc
        FROM read_parquet('{AIFS_COUNTY_PATH}')
        GROUP BY STRFTIME(init_time, '%H%M'), lead_time, YEAR(valid_time), MONTH(valid_time), HOUR(valid_time)
        ORDER BY model, lead ASC
    """).df()
    return df


def resume_progress(out_path):
    if not os.path.exists(out_path):
        return set(), True

    df = pd.read_csv(out_path, on_bad_lines="warn")
    gridded = df[df["source"] == "gridded"]
    completed = set(
        zip(
            gridded["model"],
            gridded["resolution"].astype(str),
            gridded["init"].astype(str),
            gridded["lead"].astype(int),
            gridded["year"].astype(int),
            gridded["month"].astype(int),
        )
    )
    return completed, False


def main():
    ds_fc_ifs = read_fc_files("ifs", 0, 0, 2021, 4)
    ds_fc_aifs = read_fc_files("aifs", 0, 0, 2024, 3)

    ds_lsm_ifs = get_lsm(ds_fc_ifs)
    ds_lsm_aifs = get_lsm(ds_fc_aifs)

    # Extract raw numpy arrays so workers reconstruct LSM locally
    # rather than serializing full xarray datasets per future
    lsm_coords_ifs = (ds_lsm_ifs.latitude.values, ds_lsm_ifs.longitude.values)
    lsm_values_ifs = ds_lsm_ifs.lsm.values
    lsm_coords_aifs = (ds_lsm_aifs.latitude.values, ds_lsm_aifs.longitude.values)
    lsm_values_aifs = ds_lsm_aifs.lsm.values

    completed, write_header = resume_progress(OUT_PATH)

    # Native resolution combinations (IFS @ 0.125, AIFS @ 0.25)
    # Plus coarsened IFS @ 0.25 for direct comparison with AIFS
    RESOLUTIONS = {"ifs": ["0.125", "0.25"], "aifs": ["0.25"]}
    combinations = [
        (model, init, lead, year, month, res)
        for model in MODELS
        for res in RESOLUTIONS[model]
        for init in INITS
        for lead in LEADS
        for year in YEARS
        for month in MONTHS
        if fc_data_exists(model, init, lead, year, month)
        and (model, res, fix_init(init), int(lead), int(year), int(month)) not in completed
    ]
    print(f"{len(combinations)} combinations remaining.")

    client = Client(n_workers=NUM_WORKERS, threads_per_worker=1, processes=True)
    print(f"Dashboard: {client.dashboard_link}", flush=True)
    try:
        futures = []
        for model, init, lead, year, month, res in combinations:
            if model == "aifs":
                lsm_coords, lsm_values = lsm_coords_aifs, lsm_values_aifs
            else:
                lsm_coords, lsm_values = lsm_coords_ifs, lsm_values_ifs
            futures.append(
                client.submit(
                    compute_scores_safe,
                    model, init, lead, year, month,
                    lsm_coords, lsm_values, res,
                    lsm_coords_aifs, lsm_values_aifs
                )
            )

        failures = []
        with tqdm(as_completed(futures), total=len(futures), position=0, leave=True, dynamic_ncols=True, file=sys.stderr) as pbar:
            for future in pbar:
                try:
                    result, error = future.result()
                    if error:
                        failures.append(error)
                        pbar.set_postfix_str(f"FAILED: {error[:60]}")
                    elif result is not None:
                        pd.DataFrame([result]).to_csv(OUT_PATH, mode="a", header=write_header, index=False)
                        write_header = False
                        pbar.set_postfix_str(f"last: {result['model']} {result['init']} lead={result['lead']} {result['year']}/{result['month']:02d}")
                except Exception as e:
                    failures.append(str(e))
                    pbar.set_postfix_str(f"FAILED: {str(e)[:60]}")
        if failures:
            print(f"\n{len(failures)} failures:", file=sys.stderr)
            for f in failures:
                print(f"  {f}", file=sys.stderr)
    finally:
        try:
            client.shutdown(timeout=30)
        except Exception:
            pass
        try:
            client.close(timeout=10)
        except Exception:
            pass

    if not os.path.exists(OUT_PATH):
        print("No output file found — no results were written. Exiting.", file=sys.stderr)
        return

    df_gridded = pd.read_csv(OUT_PATH, on_bad_lines="warn")
    df_gridded = df_gridded.dropna(subset=["model", "init", "lead", "year", "month"])

    df_county = compute_county_scores()
    df_county["lead"] = df_county["lead"].astype("int64")

    df_all = pd.concat([df_gridded, df_county], ignore_index=True)
    df_all.to_csv(OUT_PATH, index=False)
    print(f"Done. {len(df_gridded)} gridded + {len(df_county)} county records saved.")

    validate_completeness(df_gridded)


def validate_completeness(df_gridded):
    RESOLUTIONS = {"ifs": ["0.125", "0.25"], "aifs": ["0.25"]}
    expected = {
        (model, res, fix_init(init), int(lead), int(year), int(month))
        for model in MODELS
        for res in RESOLUTIONS[model]
        for init in INITS
        for lead in LEADS
        for year in YEARS
        for month in MONTHS
        if fc_data_exists(model, init, lead, year, month)
    }

    present = set(
        df_gridded[df_gridded["source"] == "gridded"][["model", "resolution", "init", "lead", "year", "month"]]
        .apply(lambda r: (r["model"], r["resolution"], r["init"], int(r["lead"]), int(r["year"]), int(r["month"])), axis=1)
        .tolist()
    )

    missing = expected - present
    unexpected = present - expected

    if not missing and not unexpected:
        print(f"Validation passed: all {len(expected)} expected combinations are present.")
    else:
        if missing:
            print(f"Validation FAILED: {len(missing)} missing combinations:")
            for combo in sorted(missing):
                print(f"  model={combo[0]} res={combo[1]} init={combo[2]} lead={combo[3]} year={combo[4]} month={combo[5]}")
        if unexpected:
            print(f"Validation WARNING: {len(unexpected)} unexpected combinations in output:")
            for combo in sorted(unexpected):
                print(f"  model={combo[0]} res={combo[1]} init={combo[2]} lead={combo[3]} year={combo[4]} month={combo[5]}")


if __name__ == "__main__":
    main()