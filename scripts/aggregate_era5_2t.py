"""
Aggregate ERA5 to US counties (1991-2020), then compute per-county per-day-of-year climatology.

Workflow:
  1. Build list of (path, date) for ERA5 - monthly files, one entry per day
  2. Initialize GeoAggregator from one ERA5 file
  3. For each month (parallel or sequential): read daily means, aggregate to counties,
     write per-month parquet atomically to era5_monthly/
  4. Compute climatology incrementally (one month at a time) — no full concat
  5. Optionally write full consolidated table via --write-full-table
"""

import os
import sys
import glob
import time
from itertools import groupby
import logging
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import pandas as pd
import pyarrow.parquet as pq
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
# Climatology guard
# ---------------------------------------------------------------------------
def _clim_is_valid(clim_path: str) -> bool:
    """Return True if the climatology parquet exists and has the expected structure."""
    if not os.path.exists(clim_path):
        return False
    try:
        meta = pq.read_metadata(clim_path)
        schema = meta.schema.to_arrow_schema()
        names = set(schema.names)
        required = {f"{VAR_NAME}_clim", "geo_id", "day_of_year"}
        if not required.issubset(names):
            logging.warning("Climatology parquet missing columns %s; will recompute", required - names)
            return False
        df_doy = pq.read_table(clim_path, columns=["day_of_year"]).to_pandas()
        n_doys = df_doy["day_of_year"].nunique()
        if n_doys != 365:
            logging.warning("Climatology has %d unique DOYs (expected 365); will recompute", n_doys)
            return False
        return True
    except Exception as exc:
        logging.warning("Cannot read climatology parquet (%s); will recompute", exc)
        return False


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
    Workers are always silent; progress is tracked at the month level in main().
    """
    (yr, mo), month_files = args
    if _WEIGHTMAP is None:
        raise RuntimeError("Worker has no weightmap; expected forked workers to inherit _WEIGHTMAP.")

    results = []
    for path, date in month_files:
        results.append(aggregate_era5_day(path, date, _WEIGHTMAP, VAR_NAME))

    if not results:
        return (yr, mo), None
    return (yr, mo), pd.concat(results, ignore_index=True)


# ---------------------------------------------------------------------------
# Incremental climatology
# ---------------------------------------------------------------------------
def _compute_clim_incremental(months):
    """
    Compute climatology by reading one monthly parquet at a time.
    Peak memory = one month + two small Series (~18 MB for 3,100 counties × 365 DOYs).
    """
    t0 = time.time()
    acc_sum, acc_cnt = None, None
    try:
        from tqdm import tqdm as _tqdm
        bar = _tqdm(months, total=len(months), desc="Climatology", unit="mo")
    except ImportError:
        bar = months
    for (yr, mo), _ in bar:
        path = _month_parquet_path(yr, mo)
        if not os.path.exists(path):
            logging.warning("Month %d-%02d parquet missing; excluded from climatology", yr, mo)
            continue
        df = pd.read_parquet(path)
        df['day_of_year'] = pd.to_datetime(df['time']).dt.dayofyear
        grp = df.groupby(['geo_id', 'day_of_year'])[VAR_NAME]
        s = grp.sum()
        c = grp.count()
        acc_sum = s if acc_sum is None else acc_sum.add(s, fill_value=0)
        acc_cnt = c if acc_cnt is None else acc_cnt.add(c, fill_value=0)
    if acc_sum is None:
        return None
    clim = (acc_sum / acc_cnt).rename(f'{VAR_NAME}_clim').reset_index()
    logging.info("[%.0fs] Climatology computed (%d rows)", time.time() - t0, len(clim))
    return clim


# ---------------------------------------------------------------------------
# Optional full-table streaming writer
# ---------------------------------------------------------------------------
def _write_full_table_streaming(months, era_path):
    """
    Stream all monthly parquets into one consolidated parquet without loading
    more than one month at a time. Atomic rename at end. Casts each table to
    the schema of the first to avoid dtype mismatches across months.
    """
    import pyarrow as pa  # noqa: F401
    t0 = time.time()
    tmp = era_path + ".tmp"
    writer = None
    schema = None
    total_rows = 0
    for (yr, mo), _ in months:
        path = _month_parquet_path(yr, mo)
        if not os.path.exists(path):
            logging.warning("Month %d-%02d parquet missing; excluded from full table", yr, mo)
            continue
        table = pq.read_table(path)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(tmp, schema)
        else:
            table = table.cast(schema)
        writer.write_table(table)
        total_rows += len(table)
    if writer is not None:
        writer.close()
        os.replace(tmp, era_path)
        write_table_metadata("era5_2t_county", era_path)
        logging.info("[%.0fs] Saved full ERA5 table (%d rows) to %s", time.time() - t0, total_rows, era_path)
    else:
        logging.warning("No monthly data; full table not written")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(n_parallel: int = 1, write_full_table: bool = False):
    t_start = time.time()
    logging.info("=== aggregate_era5_2t.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S"))
    os.makedirs(SAVE_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)

    clim_path = os.path.join(SAVE_DIR, 'era5_2t_county_climatology_1991_2020.parquet')

    # 0. Early exit if climatology already valid and full table not requested
    if not write_full_table and _clim_is_valid(clim_path):
        logging.info("Climatology already valid at %s; nothing to do.", clim_path)
        return

    # 1. Build file list and group by month
    era_files = build_era5_files(ERA_DIR, START, END, PARAM)
    if not era_files:
        logging.warning("No ERA5 files found; exiting")
        return
    dates = [d for _, d in era_files]
    logging.info(
        "Found %d days | %s to %s",
        len(era_files), dates[0].strftime("%Y-%m-%d"), dates[-1].strftime("%Y-%m-%d"),
    )

    def month_key(item):
        _, date = item
        return (date.year, date.month)
    months = [(k, list(g)) for k, g in groupby(era_files, month_key)]

    # 2. Filter to months not yet checkpointed
    pending = [(key, files) for key, files in months if not _is_month_done(*key)]
    n_done = len(months) - len(pending)
    logging.info(
        "Months: %d total | %d done | %d pending | range %d-%02d to %d-%02d",
        len(months), n_done, len(pending),
        months[0][0][0], months[0][0][1],
        months[-1][0][0], months[-1][0][1],
    )

    # 3. Aggregate pending months (single code path for sequential and parallel)
    if pending:
        global _WEIGHTMAP
        _WEIGHTMAP = wxagg.GeoAggregator(
            shapefile_path=SHAPEFILE_PATH, grid_path=era_files[0][0], silent=not VERBOSE
        ).weightmap

        n_parallel = max(1, int(n_parallel))
        ctx = mp.get_context("fork")
        t_agg = time.time()
        try:
            from tqdm import tqdm as _tqdm
            bar = _tqdm(total=len(pending), desc="ERA5 months", unit="mo")
        except ImportError:
            bar = None
        with ProcessPoolExecutor(max_workers=n_parallel, mp_context=ctx) as executor:
            futures = {
                executor.submit(_process_month, (key, files)): key
                for key, files in pending
            }
            for fut in as_completed(futures):
                (yr, mo), df_month = fut.result()
                if df_month is not None:
                    _write_month_atomic(yr, mo, df_month)
                if bar:
                    bar.update(1)
        if bar:
            bar.close()
        logging.info("[%.0fs] Aggregation phase complete", time.time() - t_agg)

    # 4. Compute climatology incrementally
    t_clim = time.time()
    clim = _compute_clim_incremental(months)
    if clim is None:
        logging.warning("No monthly data; cannot produce climatology")
        return
    tmp = clim_path + ".tmp"
    clim.to_parquet(tmp)
    os.replace(tmp, clim_path)
    write_table_metadata("era5_2t_county_climatology", clim_path)
    logging.info("[%.0fs] Saved climatology (%d rows) to %s", time.time() - t_clim, len(clim), clim_path)

    # 5. Optionally write full consolidated table
    if write_full_table:
        era_path = os.path.join(SAVE_DIR, 'era5_2t_county_1991_2020.parquet')
        _write_full_table_streaming(months, era_path)

    logging.info("[%.0fs] Done.", time.time() - t_start)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Aggregate ERA5 to US counties and compute climatology.')
    parser.add_argument(
        '--n-parallel',
        type=int,
        default=1,
        help='Number of months to process simultaneously (default: 1).',
    )
    parser.add_argument(
        '--write-full-table',
        action='store_true',
        default=False,
        help='Also write the full concatenated ERA5 table (default: off; monthly parquets remain queryable via DuckDB).',
    )
    args = parser.parse_args()
    main(n_parallel=args.n_parallel, write_full_table=args.write_full_table)
