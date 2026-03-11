"""
Aggregate ERA5 to US counties (1991-2020), then compute per-county per-day-of-year climatology.

Workflow:
  1. Build list of (path, date) for ERA5 - monthly files, one entry per day
  2. Initialize GeoAggregator from one ERA5 file
  3. For each month (parallel or sequential): read daily means, aggregate to counties,
     write per-month parquet atomically to era5_monthly/
  4. Concatenate monthly parquets → full dataset + climatology
"""

import os
import sys
import glob
from itertools import groupby
import logging
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import pandas as pd
import xarray as xr
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nwp_census_eval import aggregate as wxagg
from nwp_census_eval.aggregate import _maybe_progress
import xagg
from config import (
    ERA5_DIR        as ERA_DIR,
    SHAPEFILE_PATH,
    AGGREGATED_DIR  as SAVE_DIR,
    ERA5_CLIM_START as START,
    ERA5_CLIM_END   as END,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Config ---
VERBOSE  = True
PARAM    = '2t'
VAR_NAME = 't2m'

_WEIGHTMAP = None  # set in main(); inherited by forked workers

# Per-month parquets are staged here to keep SAVE_DIR clean
MONTHLY_DIR = os.path.join(SAVE_DIR, 'era5_monthly')

# ---------------------------------------------------------------------------
# Table-level metadata (sidecar JSON files)
# ---------------------------------------------------------------------------
TABLE_METADATA = {
    "era5_2t_county": {
        "description": f"ERA5 daily-mean 2m temperature aggregated to US counties ({START} to {END}, leap days excluded).",
        "variables": {
            "geo_id": {
                "units": "1",
                "long_name": "County FIPS code (string).",
            },
            "time": {
                "units": "UTC",
                "long_name": "Date of daily mean (valid time).",
            },
            VAR_NAME: {
                "units": "K",
                "long_name": "Daily-mean 2m air temperature from ERA5.",
            },
        },
    },
    "era5_2t_county_climatology": {
        "description": f"ERA5 2m temperature daily climatology aggregated to US counties for {START} to {END}, with all leap days (February 29) excluded from both the daily means and the climatology construction.",
        "variables": {
            "geo_id": {
                "units": "1",
                "long_name": "County FIPS code (string).",
            },
            "day_of_year": {
                "units": "1",
                "long_name": "Day of year (1–365) used for climatology.",
            },
            f"{VAR_NAME}_clim": {
                "units": "K",
                "long_name": "ERA5 2m temperature climatology at county level.",
            },
        },
    },
}


def write_table_metadata(table_name: str, path: str) -> None:
    """Write sidecar JSON metadata for a given table next to its parquet file."""
    meta = TABLE_METADATA.get(table_name)
    if meta is None:
        logging.warning("No metadata definition found for table %r; skipping", table_name)
        return
    base, _ = os.path.splitext(path)
    meta_path = base + ".meta.json"
    tmp_path = meta_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp_path, meta_path)
    logging.info("Wrote metadata for %s to %s", table_name, meta_path)


# ---------------------------------------------------------------------------
# Per-month checkpoint helpers
# ---------------------------------------------------------------------------
def _month_parquet_path(yr, mo):
    return os.path.join(MONTHLY_DIR, f"era5_2t_county_{yr}_{mo:02d}.parquet")


def _is_month_done(yr, mo):
    """Return True if the month parquet exists and is readable."""
    path = _month_parquet_path(yr, mo)
    if not os.path.exists(path):
        return False
    try:
        pd.read_parquet(path, columns=['geo_id'])
        return True
    except Exception:
        logging.warning("Corrupt month checkpoint %s; will reprocess", path)
        return False


def _write_month_atomic(yr, mo, df):
    """Write a month DataFrame to its parquet atomically via a .tmp file."""
    path = _month_parquet_path(yr, mo)
    tmp = path + ".tmp"
    df.to_parquet(tmp)
    os.replace(tmp, path)
    logging.info("Checkpoint: %d-%02d saved (%d rows)", yr, mo, len(df))


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def build_era5_files(era_dir, start, end, param):
    """
    Build list of (path, date) for ERA5.
    ERA5 on GLADE: era_dir/{YYYYMM}/*{param}*.nc — monthly files with hourly data.
    Returns one (path, date) per day; leap days excluded.
    """
    dates = pd.date_range(start=start, end=end, freq='D')
    files = []
    for date in dates:
        if date.month == 2 and date.day == 29:
            continue
        path_pattern = os.path.join(era_dir, date.strftime('%Y%m'), f'*{param}*.nc')
        paths = glob.glob(path_pattern)
        if paths:
            files.append((paths[0], date))
    return files


# ---------------------------------------------------------------------------
# Per-day aggregation
# ---------------------------------------------------------------------------
def aggregate_era5_day(path, date, weightmap, var_name):
    """
    For one day: read ERA5 file, take daily mean over hours, aggregate to counties.
    Returns DataFrame with columns geo_id, time, var_name.
    """
    with xr.open_dataset(path) as ds:
        ds_day = ds.sel(time=date.strftime('%Y-%m-%d'))
        ds_daily = ds_day.mean(dim='time', skipna=True)
    ds_daily = xagg.fix_ds(ds_daily)
    var = var_name if var_name in ds_daily.data_vars else list(ds_daily.data_vars)[0]
    aggregated = xagg.aggregate(ds_daily, weightmap, silent=True)
    df = (
        aggregated.to_dataframe()
        .dropna(subset=[var])
        .reset_index()
        .drop(columns=['poly_idx'])
        .rename(columns={'GEOID': 'geo_id'})
    )
    df['time'] = pd.to_datetime(date)
    return df[['geo_id', 'time', var]].rename(columns={var: var_name})


# ---------------------------------------------------------------------------
# Month-level worker
# ---------------------------------------------------------------------------
def _process_month(args):
    """
    Aggregate one (year, month) group of ERA5 days.
    Returns ((yr, mo), df_month) or ((yr, mo), None).
    Relies on forked workers inheriting _WEIGHTMAP.
    """
    (yr, mo), month_files, position = args
    if _WEIGHTMAP is None:
        raise RuntimeError("Worker has no weightmap; expected forked workers to inherit _WEIGHTMAP.")

    results = []
    iterable = _maybe_progress(
        month_files, len(month_files), f"{yr}-{mo:02d}",
        silent=not VERBOSE, position=position,
    )
    for path, date in iterable:
        results.append(aggregate_era5_day(path, date, _WEIGHTMAP, VAR_NAME))

    if not results:
        return (yr, mo), None
    return (yr, mo), pd.concat(results, ignore_index=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(n_parallel: int = 1):
    os.makedirs(SAVE_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)

    # 1. Build file list and group by month
    era_files = build_era5_files(ERA_DIR, START, END, PARAM)
    if not era_files:
        logging.warning("No ERA5 files found; exiting")
        return
    logging.info("Found %d days", len(era_files))

    def month_key(item):
        _, date = item
        return (date.year, date.month)
    months = [(k, list(g)) for k, g in groupby(era_files, month_key)]

    # 2. Filter to months not yet checkpointed
    pending = [(key, files) for key, files in months if not _is_month_done(*key)]
    n_done = len(months) - len(pending)
    logging.info("%d/%d months already done; %d pending", n_done, len(months), len(pending))

    # 3. Aggregate pending months (single code path for sequential and parallel)
    if pending:
        global _WEIGHTMAP
        _WEIGHTMAP = wxagg.GeoAggregator(
            shapefile_path=SHAPEFILE_PATH, grid_path=era_files[0][0], silent=not VERBOSE
        ).weightmap

        n_parallel = max(1, int(n_parallel))
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=n_parallel, mp_context=ctx) as executor:
            futures = {
                executor.submit(_process_month, (key, files, pos)): key
                for pos, (key, files) in enumerate(pending)
            }
            for fut in as_completed(futures):
                (yr, mo), df_month = fut.result()
                if df_month is not None:
                    _write_month_atomic(yr, mo, df_month)

    # 4. Concatenate all monthly parquets
    monthly_dfs = []
    for (yr, mo), _ in months:
        path = _month_parquet_path(yr, mo)
        if os.path.exists(path):
            monthly_dfs.append(pd.read_parquet(path))
        else:
            logging.warning("Month %d-%02d parquet missing; excluded from outputs", yr, mo)

    if not monthly_dfs:
        logging.warning("No monthly data; cannot produce outputs")
        return

    df_era = pd.concat(monthly_dfs, ignore_index=True)

    # 5. Save full aggregated table atomically
    era_path = os.path.join(SAVE_DIR, 'era5_2t_county_1991_2020.parquet')
    tmp = era_path + ".tmp"
    df_era.to_parquet(tmp)
    os.replace(tmp, era_path)
    write_table_metadata("era5_2t_county", era_path)
    logging.info("Saved full ERA5 table (%d rows) to %s", len(df_era), era_path)

    # 6. Compute and save climatology atomically
    df_era['day_of_year'] = pd.to_datetime(df_era['time']).dt.dayofyear
    clim = df_era.groupby(['geo_id', 'day_of_year'])[VAR_NAME].mean().reset_index()
    clim = clim.rename(columns={VAR_NAME: f'{VAR_NAME}_clim'})

    clim_path = os.path.join(SAVE_DIR, 'era5_2t_county_climatology_1991_2020.parquet')
    tmp = clim_path + ".tmp"
    clim.to_parquet(tmp)
    os.replace(tmp, clim_path)
    write_table_metadata("era5_2t_county_climatology", clim_path)
    logging.info("Saved climatology (%d rows) to %s", len(clim), clim_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Aggregate ERA5 to US counties and compute climatology.')
    parser.add_argument(
        '--n-parallel',
        type=int,
        default=1,
        help='Number of months to process simultaneously (default: 1).',
    )
    args = parser.parse_args()
    main(n_parallel=args.n_parallel)
