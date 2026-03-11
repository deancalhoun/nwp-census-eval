"""
Aggregate ERA5 to US counties (1991-2020), then compute per-county per-day-of-year climatology.

Workflow:
  1. Build list of (path, date) for ERA5 - monthly files, one entry per day
  2. Initialize GeoAggregator from one ERA5 file
  3. For each day: read all hours, take daily mean, aggregate to counties
  4. Save aggregated ERA5 table
  5. Compute climatology: groupby(geo_id, day_of_year).mean()
  6. Save county climatology
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

_WEIGHTMAP = None  # set in main(); inherited by workers when using fork

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
        "description": "ERA5 2m temperature daily climatology aggregated to US counties for {START} to {END}, with all leap days (February 29) excluded from both the daily means and the climatology construction.",
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
        logging.warning(f"No metadata definition found for table {table_name!r}; skipping metadata write")
        return

    base, _ = os.path.splitext(path)
    meta_path = base + ".meta.json"
    tmp_path = meta_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp_path, meta_path)
    logging.info("Wrote metadata for %s to %s", table_name, meta_path)


def build_era5_files(era_dir, start, end, param):
    """
    Build list of (path, date) for ERA5.
    ERA5 on GLADE: era_dir/{YYYYMM}/*{param}*.nc - monthly files with hourly data.
    Returns one (path, date) per day.
    """
    dates = pd.date_range(start=start, end=end, freq='D')
    files = []
    for date in dates:
        if date.month == 2 and date.day == 29:
            continue  # skip leap days
        path_pattern = os.path.join(era_dir, date.strftime('%Y%m'), f'*{param}*.nc')
        paths = glob.glob(path_pattern)
        if paths:
            files.append((paths[0], date))
    return files


def aggregate_era5_day(path, date, weightmap, var_name):
    """
    For one day: read ERA5 file, take daily mean over hours, aggregate to counties.
    Returns DataFrame with columns geo_id, time, var_name.
    """
    with xr.open_dataset(path) as ds:
        ds_day = ds.sel(time=date.strftime('%Y-%m-%d'))
        ds_daily = ds_day.mean(dim='time', skipna=True)
    ds_daily = xagg.fix_ds(ds_daily)  # convert lon to -180:180, rename to lat/lon (required by weightmap)
    # ERA5 variable may be t2m, 2t, VAR_2T, etc.
    var = var_name if var_name in ds_daily.data_vars else list(ds_daily.data_vars)[0]
    aggregated = xagg.aggregate(ds_daily, weightmap, silent=True)  # per-day: keep quiet
    df = aggregated.to_dataframe().dropna(subset=[var]).reset_index().drop(columns=['poly_idx']).rename(columns={'GEOID': 'geo_id'})
    df['time'] = pd.to_datetime(date)
    return df[['geo_id', 'time', var]].rename(columns={var: var_name})


def _process_month(args):
    """
    Helper for parallel processing: aggregate one (year, month) group.
    Returns ((yr, mo), df_month) or ((yr, mo), None) if nothing new was done.
    """
    (yr, mo), month_files, done_dates, position = args
    if _WEIGHTMAP is None:
        raise RuntimeError("Worker has no weightmap; expected forked workers to inherit _WEIGHTMAP.")

    month_results = []
    # Per-month progress bar on its own line (position) so multiple workers are visible
    iterable = _maybe_progress(
        month_files,
        len(month_files),
        f"{yr}-{mo:02d}",
        silent=not VERBOSE,
        position=position,
    )
    for path, date in iterable:
        date_str = pd.to_datetime(date).normalize().strftime('%Y-%m-%d')
        if date_str in done_dates:
            continue
        df_day = aggregate_era5_day(path, date, _WEIGHTMAP, VAR_NAME)
        month_results.append(df_day)

    if not month_results:
        return (yr, mo), None

    df_month = pd.concat(month_results, ignore_index=True)
    return (yr, mo), df_month


def _chunked(seq, n):
    """Yield successive n-sized chunks from a sequence."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main(n_parallel: int = 1):
    os.makedirs(SAVE_DIR, exist_ok=True)
    era_path = os.path.join(SAVE_DIR, 'era5_2t_county_1991_2020.parquet')

    # 1. Build ERA5 file list and group by month
    era_files = build_era5_files(ERA_DIR, START, END, PARAM)
    logging.info(f'Found {len(era_files)} days')

    # Load checkpoint if it exists
    done_dates = set()
    df_existing = None
    if os.path.exists(era_path):
        df_existing = pd.read_parquet(era_path)
        done_dates = set(pd.to_datetime(df_existing['time']).dt.strftime('%Y-%m-%d'))
        logging.info(f'Resuming: {len(done_dates)} days already in checkpoint')

    # Group by (year, month) for checkpointing
    def month_key(item):
        path, date = item
        return (date.year, date.month)
    months = [(k, list(g)) for k, g in groupby(era_files, month_key)]

    # 2. Initialize grid path (weightmap source from first file)
    grid_path = era_files[0][0]

    # 3. Aggregate each month, optionally in parallel, checkpoint after each
    n_parallel = max(1, int(n_parallel))

    if n_parallel == 1:
        # Original sequential behavior (with progress bar), preserves existing checkpointing semantics
        geo_agg = wxagg.GeoAggregator(shapefile_path=SHAPEFILE_PATH, grid_path=grid_path, silent=not VERBOSE)
        weightmap = geo_agg.weightmap

        for (yr, mo), month_files in months:
            month_results = []
            for path, date in _maybe_progress(month_files, len(month_files), f"{yr}-{mo:02d}", silent=not VERBOSE):
                date_str = pd.to_datetime(date).normalize().strftime('%Y-%m-%d')
                if date_str in done_dates:
                    continue
                df_day = aggregate_era5_day(path, date, weightmap, VAR_NAME)
                month_results.append(df_day)

            if not month_results:
                continue
            df_month = pd.concat(month_results, ignore_index=True)
            df_existing = pd.concat([df_existing, df_month], ignore_index=True) if df_existing is not None else df_month
            # Update done_dates so resumed runs skip completed days
            done_dates.update(pd.to_datetime(df_month['time']).dt.strftime('%Y-%m-%d'))
            df_existing.to_parquet(era_path)
            write_table_metadata("era5_2t_county", era_path)
            logging.info(f'Checkpoint: saved through {yr}-{mo:02d} ({len(df_existing)} days total)')
    else:
        # Parallel over months using a process pool. Weightmap is computed once in parent and inherited by workers.
        logging.info(f'Running with up to {n_parallel} months in parallel')
        global _WEIGHTMAP
        if _WEIGHTMAP is None:
            geo_agg = wxagg.GeoAggregator(shapefile_path=SHAPEFILE_PATH, grid_path=grid_path, silent=True)
            _WEIGHTMAP = geo_agg.weightmap

        # Use fork so workers inherit the precomputed weightmap without pickling.
        with ProcessPoolExecutor(max_workers=n_parallel, mp_context=mp.get_context("fork")) as executor:
            for batch in _chunked(months, n_parallel):
                futures = []
                for position, (key, month_files) in enumerate(batch):
                    futures.append(
                        executor.submit(
                            _process_month,
                            (key, month_files, done_dates, position),
                        )
                    )

                for fut in as_completed(futures):
                    (yr, mo), df_month = fut.result()
                    if df_month is None:
                        continue
                    df_existing = pd.concat([df_existing, df_month], ignore_index=True) if df_existing is not None else df_month
                    # Update done_dates and checkpoint after each completed month
                    done_dates.update(pd.to_datetime(df_month['time']).dt.strftime('%Y-%m-%d'))
                    df_existing.to_parquet(era_path)
                    write_table_metadata("era5_2t_county", era_path)
                    logging.info(f'Checkpoint: saved through {yr}-{mo:02d} ({len(df_existing)} days total)')

    df_era = df_existing
    if df_era is None:
        logging.warning('No data aggregated')
        return

    # 4. Compute climatology: per county, per day-of-year
    df_era['day_of_year'] = pd.to_datetime(df_era['time']).dt.dayofyear
    clim = df_era.groupby(['geo_id', 'day_of_year'])[VAR_NAME].mean().reset_index()
    clim = clim.rename(columns={VAR_NAME: f'{VAR_NAME}_clim'})

    # 5. Save climatology
    clim_path = os.path.join(SAVE_DIR, 'era5_2t_county_climatology_1991_2020.parquet')
    clim.to_parquet(clim_path)
    write_table_metadata("era5_2t_county_climatology", clim_path)
    logging.info(f'Saved county climatology to {clim_path}')


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
