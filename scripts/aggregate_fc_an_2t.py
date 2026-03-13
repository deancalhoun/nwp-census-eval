"""
Aggregate IFS forecasts and analyses to US counties, then compute bias,
absolute error, and anomalies relative to ERA5 climatology.

Workflow:
  1. Build lists of forecast files (path, init_time, lead_time)
     and analysis files (path, valid_time).
  2. Align by valid_time so every forecast has a matching analysis.
  3. Initialize GeoAggregator once for the weightmap.
  4. Aggregate forecasts and analyses to counties (monthly checkpoints,
     optional parallelism).
  5. Merge on (geo_id, valid_time) -> bias, abs_error.
  6. Join ERA5 county climatology -> forecast & analysis anomalies.
  7. Save all outputs as parquet.
"""

import os
import sys
import glob
import logging
import argparse
import json
import time
import subprocess
from itertools import groupby
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nwp_census_eval import aggregate as wxagg
from nwp_census_eval.aggregate import _maybe_progress
import xagg
from config import (
    SHAPEFILE_PATH,
    IFS_FC_DIR       as FC_DIR,
    AIFS_FC_DIR,
    IFS_AN_DIR       as AN_DIR,
    ERA5_CLIM_PATH   as CLIM_PATH,
    AGGREGATED_DIR   as SAVE_DIR,
    IFS_START        as START,
    IFS_END          as END,
    LEAD_TIMES,
    WEIGHTMAP_CACHE_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
VERBOSE  = True
FC_FREQ  = "12h"
AN_FREQ  = "6h"
VAR_NAME = "t2m"

_WEIGHTMAP = None  # set in main(); inherited by workers when using fork

# ---------------------------------------------------------------------------
# Table-level metadata (for sidecar JSON files)
# ---------------------------------------------------------------------------
TABLE_METADATA = {
    "ifs_fc_2t_county": {
        "description": "IFS 2m temperature forecasts aggregated to US counties.",
        "variables": {
            "geo_id": {
                "units": "1",
                "long_name": "County FIPS code (string).",
            },
            "valid_time": {
                "units": "UTC",
                "long_name": "Forecast valid time.",
            },
            "init_time": {
                "units": "UTC",
                "long_name": "Forecast initialization time.",
            },
            "lead_time": {
                "units": "hours",
                "long_name": "Forecast lead time (valid_time - init_time).",
            },
            VAR_NAME: {
                "units": "K",
                "long_name": "2m air temperature (forecast).",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "ifs_an_2t_county": {
        "description": "IFS 2m temperature analyses aggregated to US counties.",
        "variables": {
            "geo_id": {
                "units": "1",
                "long_name": "County FIPS code (string).",
            },
            "time": {
                "units": "UTC",
                "long_name": "Analysis valid time.",
            },
            VAR_NAME: {
                "units": "K",
                "long_name": "2m air temperature (analysis).",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "ifs_fc_bias_2t_county": {
        "description": "IFS forecast vs analysis verification at county level (bias and absolute error).",
        "variables": {
            "geo_id": {"units": "1", "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC", "long_name": "Forecast valid time."},
            "init_time": {"units": "UTC", "long_name": "Forecast initialization time."},
            "lead_time": {"units": "hours", "long_name": "Forecast lead time."},
            f"{VAR_NAME}_fc": {
                "units": "K",
                "long_name": "2m air temperature (forecast).",
            },
            f"{VAR_NAME}_an": {
                "units": "K",
                "long_name": "2m air temperature (analysis).",
            },
            "bias": {
                "units": "K",
                "long_name": "Forecast bias (forecast minus analysis).",
            },
            "abs_error": {
                "units": "K",
                "long_name": "Absolute forecast error.",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "ifs_fc_bias_anom_2t_county": {
        "description": "IFS forecast and analysis anomalies relative to ERA5 climatology at county level.",
        "variables": {
            "geo_id": {"units": "1", "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC", "long_name": "Forecast valid time."},
            "init_time": {"units": "UTC", "long_name": "Forecast initialization time."},
            "lead_time": {"units": "hours", "long_name": "Forecast lead time."},
            f"{VAR_NAME}_fc": {
                "units": "K",
                "long_name": "2m air temperature (forecast).",
            },
            f"{VAR_NAME}_an": {
                "units": "K",
                "long_name": "2m air temperature (analysis).",
            },
            "bias": {
                "units": "K",
                "long_name": "Forecast bias (forecast minus analysis).",
            },
            "abs_error": {
                "units": "K",
                "long_name": "Absolute forecast error.",
            },
            "day_of_year": {
                "units": "1",
                "long_name": "Day of year (1-366) used for climatology lookup.",
            },
            f"{VAR_NAME}_clim": {
                "units": "K",
                "long_name": "ERA5 2m temperature climatology (1991–2020) at county level.",
            },
            "fc_anom": {
                "units": "K",
                "long_name": "Forecast anomaly relative to ERA5 climatology.",
            },
            "an_anom": {
                "units": "K",
                "long_name": "Analysis anomaly relative to ERA5 climatology.",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "aifs_fc_2t_county": {
        "description": "AIFS 2m temperature forecasts aggregated to US counties.",
        "variables": {
            "geo_id": {
                "units": "1",
                "long_name": "County FIPS code (string).",
            },
            "valid_time": {
                "units": "UTC",
                "long_name": "Forecast valid time.",
            },
            "init_time": {
                "units": "UTC",
                "long_name": "Forecast initialization time.",
            },
            "lead_time": {
                "units": "hours",
                "long_name": "Forecast lead time (valid_time - init_time).",
            },
            VAR_NAME: {
                "units": "K",
                "long_name": "2m air temperature (forecast).",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "aifs_fc_bias_2t_county": {
        "description": "AIFS forecast vs IFS analysis verification at county level (bias and absolute error).",
        "variables": {
            "geo_id": {"units": "1", "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC", "long_name": "Forecast valid time."},
            "init_time": {"units": "UTC", "long_name": "Forecast initialization time."},
            "lead_time": {"units": "hours", "long_name": "Forecast lead time."},
            f"{VAR_NAME}_fc": {
                "units": "K",
                "long_name": "2m air temperature (forecast).",
            },
            f"{VAR_NAME}_an": {
                "units": "K",
                "long_name": "2m air temperature (analysis from IFS).",
            },
            "bias": {
                "units": "K",
                "long_name": "Forecast bias (forecast minus analysis).",
            },
            "abs_error": {
                "units": "K",
                "long_name": "Absolute forecast error.",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "aifs_fc_bias_anom_2t_county": {
        "description": "AIFS forecast and IFS analysis anomalies relative to ERA5 climatology at county level.",
        "variables": {
            "geo_id": {"units": "1", "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC", "long_name": "Forecast valid time."},
            "init_time": {"units": "UTC", "long_name": "Forecast initialization time."},
            "lead_time": {"units": "hours", "long_name": "Forecast lead time."},
            f"{VAR_NAME}_fc": {
                "units": "K",
                "long_name": "2m air temperature (forecast).",
            },
            f"{VAR_NAME}_an": {
                "units": "K",
                "long_name": "2m air temperature (analysis from IFS).",
            },
            "bias": {
                "units": "K",
                "long_name": "Forecast bias (forecast minus analysis).",
            },
            "abs_error": {
                "units": "K",
                "long_name": "Absolute forecast error.",
            },
            "day_of_year": {
                "units": "1",
                "long_name": "Day of year (1-366) used for climatology lookup.",
            },
            f"{VAR_NAME}_clim": {
                "units": "K",
                "long_name": "ERA5 2m temperature climatology (1991–2020) at county level.",
            },
            "fc_anom": {
                "units": "K",
                "long_name": "Forecast anomaly relative to ERA5 climatology.",
            },
            "an_anom": {
                "units": "K",
                "long_name": "Analysis anomaly relative to ERA5 climatology.",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
            },
        },
    },
    "aifs_vs_ifs_fc_bias_comparison_2t_county": {
        "description": "Comparison of AIFS vs IFS forecast verification at county level on common dates.",
        "variables": {
            "geo_id": {"units": "1", "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC", "long_name": "Forecast valid time (common to both systems)."},
            "lead_time": {"units": "hours", "long_name": "Forecast lead time."},
            "bias_ifs": {
                "units": "K",
                "long_name": "IFS forecast bias (forecast minus analysis).",
            },
            "abs_error_ifs": {
                "units": "K",
                "long_name": "IFS absolute forecast error.",
            },
            "bias_aifs": {
                "units": "K",
                "long_name": "AIFS forecast bias (forecast minus analysis).",
            },
            "abs_error_aifs": {
                "units": "K",
                "long_name": "AIFS absolute forecast error.",
            },
            "bias_diff": {
                "units": "K",
                "long_name": "AIFS bias minus IFS bias.",
            },
            "abs_error_diff": {
                "units": "K",
                "long_name": "AIFS absolute error minus IFS absolute error.",
            },
            "aland": {
                "units": "km2",
                "long_name": "County land area (used as relative area weight).",
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

# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def build_fc_files(fc_dir, start, end, freq, lead_times):
    """Return [(path, init_time_str, lead_time), ...] for IFS forecasts."""
    dates = pd.date_range(start=start, end=end, freq=freq)
    fc_files = []
    for init_time in dates:
        date_str = init_time.strftime("%Y%m%d%H%M")
        year, month, day, hour = date_str[:4], date_str[4:6], date_str[6:8], date_str[8:]
        for lead_time in lead_times:
            pattern = os.path.join(fc_dir, hour, str(lead_time), year, month, f"*{day}.nc")
            paths = glob.glob(pattern)
            if paths:
                fc_files.append((paths[0], str(init_time), lead_time))
    return fc_files

def build_an_files(an_dir, start, end, freq):
    """Return [(path, time_str), ...] for IFS analyses."""
    dates = pd.date_range(start=start, end=end, freq=freq)
    an_files = []
    for date in dates:
        date_str = date.strftime("%Y%m%d")
        year, month = date_str[:4], date_str[4:6]
        pattern = os.path.join(an_dir, year, month, f"*{date_str}.nc")
        paths = glob.glob(pattern)
        if paths:
            an_files.append((paths[0], str(date)))
    return an_files

def align_fc_an(fc_files, an_files):
    """Keep only (fc, an) entries whose valid_time has a match on the other side.

    Returns (fc_aligned, an_aligned) where:
      - fc_aligned keeps *every* forecast whose valid_time appears in an_files
        (multiple lead times / init times sharing a valid_time are all retained)
      - an_aligned is deduplicated so each valid_time appears exactly once
    """
    from collections import defaultdict
    valid_from_fc = defaultdict(list)
    for path, init, lead in fc_files:
        vt = pd.to_datetime(init) + pd.Timedelta(hours=int(lead))
        valid_from_fc[vt].append((path, init, lead))
    valid_from_an = {pd.to_datetime(t): (path, t) for path, t in an_files}
    common_valid = sorted(set(valid_from_fc) & set(valid_from_an))

    fc_aligned = []
    for vt in common_valid:
        fc_aligned.extend(valid_from_fc[vt])
    an_aligned = [valid_from_an[vt] for vt in common_valid]
    return fc_aligned, an_aligned

# ---------------------------------------------------------------------------
# Per-file aggregation (standalone, usable by workers)
# ---------------------------------------------------------------------------
def _open_netcdf(path):
    """Open a file as xarray Dataset; runs grib_to_netcdf in-place if file is GRIB format."""
    try:
        return xr.open_dataset(path, engine="netcdf4")
    except OSError as exc:
        if "Unknown file format" not in str(exc):
            raise
    # File has .nc extension but is GRIB format — convert in-place
    logging.warning("Converting GRIB-format file to NetCDF: %s", path)
    tmp = path + ".converting"
    result = subprocess.run(
        ["grib_to_netcdf", "-o", tmp, path],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not os.path.exists(tmp):
        raise RuntimeError(f"grib_to_netcdf failed for {path}: {result.stderr.strip()}")
    os.replace(tmp, path)
    return xr.open_dataset(path, engine="netcdf4")


def aggregate_fc_file(path, init_time, lead_time, weightmap, var_name):
    """Aggregate one forecast file to counties using *weightmap*."""
    ds = _open_netcdf(path)
    ds = xagg.fix_ds(ds)
    aggregated = xagg.aggregate(ds, weightmap, silent=True)
    df = (
        aggregated.to_dataframe()
        .dropna(subset=[var_name])
        .reset_index()
        .drop(columns=["poly_idx"])
        .rename(columns={"time": "valid_time", "GEOID": "geo_id"})
    )
    df["lead_time"] = lead_time
    df["init_time"] = pd.to_datetime(df["valid_time"]) - pd.to_timedelta(lead_time, unit="h")
    return df[["geo_id", "valid_time", "init_time", "lead_time", var_name]]

def aggregate_an_file(path, time, weightmap, var_name):
    """Aggregate one analysis file to counties using *weightmap*."""
    with _open_netcdf(path) as ds:
        ds_slice = ds.sel(time=pd.to_datetime(time), method="nearest")
    ds_slice = xagg.fix_ds(ds_slice)
    aggregated = xagg.aggregate(ds_slice, weightmap, silent=True)
    df = (
        aggregated.to_dataframe()
        .dropna(subset=[var_name])
        .reset_index()
        .drop(columns=["poly_idx"])
        .rename(columns={"GEOID": "geo_id"})
    )
    df["time"] = pd.to_datetime(time)
    return df[["geo_id", "time", var_name]]

# ---------------------------------------------------------------------------
# Month-level workers (parallel)
# ---------------------------------------------------------------------------
def _fc_checkpoint_key(init_time, lead_time):
    vt = (pd.to_datetime(init_time) + pd.Timedelta(hours=int(lead_time))).strftime("%Y-%m-%d %H:%M:%S")
    return f"{vt}|{int(lead_time)}"


def _process_fc_chunk(args):
    """Aggregate one (lead_time, year, month) group of forecast files.
    Returns ((lead, yr, mo), df_chunk | None).
    """
    (lead, yr, mo), chunk_files, done_keys, position = args
    if _WEIGHTMAP is None:
        raise RuntimeError("Worker has no weightmap; expected forked workers to inherit _WEIGHTMAP.")

    results = []
    for path, init_time, lead_time in chunk_files:
        key = _fc_checkpoint_key(init_time, lead_time)
        if key in done_keys:
            continue
        try:
            df = aggregate_fc_file(path, init_time, lead_time, _WEIGHTMAP, VAR_NAME)
        except Exception as exc:
            logging.warning("Skipping corrupt file %s: %s", path, exc)
            continue
        results.append(df)

    if not results:
        return (lead, yr, mo), None
    return (lead, yr, mo), pd.concat(results, ignore_index=True)

def _process_an_month(args):
    """Aggregate one (year, month) group of analysis files.
    Returns ((yr, mo), df_month | None).
    """
    (yr, mo), month_files, done_times, position = args
    if _WEIGHTMAP is None:
        raise RuntimeError("Worker has no weightmap; expected forked workers to inherit _WEIGHTMAP.")

    results = []
    for path, time in month_files:
        t_str = pd.to_datetime(time).strftime("%Y-%m-%d %H:%M:%S")
        if t_str in done_times:
            continue
        try:
            df = aggregate_an_file(path, time, _WEIGHTMAP, VAR_NAME)
        except Exception as exc:
            logging.warning("Skipping corrupt file %s: %s", path, exc)
            continue
        results.append(df)

    if not results:
        return (yr, mo), None
    return (yr, mo), pd.concat(results, ignore_index=True)

def _process_chunk(args):
    """Dispatch to the correct worker based on tag ('fc' or 'an')."""
    tag, group_key, chunk_files, done_keys, position = args
    if tag == "fc":
        return ("fc",) + _process_fc_chunk((group_key, chunk_files, done_keys, position))
    return ("an",) + _process_an_month((group_key, chunk_files, done_keys, position))

def _chunked(seq, n):
    """Yield successive n-sized chunks from a sequence."""
    for i in range(0, len(seq), n):
        yield seq[i : i + n]

# ---------------------------------------------------------------------------
# Shapefile-based area weights
# ---------------------------------------------------------------------------
def load_area_weights(shapefile_path):
    """Load county area weights (scaled ALAND) from shapefile, keyed by geo_id.

    ALAND in the shapefile is in square meters; for efficiency we rescale to
    square kilometers (divide by 1e6). Only relative sizes matter for
    downstream area-weighted spatial averages, so this constant factor does
    not affect results.
    """
    gdf = gpd.read_file(shapefile_path)

    if "GEOID" in gdf.columns:
        geoid_col = "GEOID"
    else:
        raise KeyError("Expected 'GEOID' column in county shapefile for geo_id mapping")

    if "ALAND" not in gdf.columns:
        raise KeyError("Expected 'ALAND' column in county shapefile for area weights")

    area_df = gdf[[geoid_col, "ALAND"]].rename(
        columns={geoid_col: "geo_id", "ALAND": "aland"}
    )
    # Convert to float and rescale from m^2 to km^2
    area_df["aland"] = area_df["aland"].astype(float) / 1e6
    return area_df[["geo_id", "aland"]]

# ---------------------------------------------------------------------------
# Bias, absolute error, anomalies
# ---------------------------------------------------------------------------
def compute_bias(df_fc, df_an, var_name):
    """Merge forecast and analysis tables -> bias and absolute error."""
    df = pd.merge(
        df_fc, df_an,
        left_on=["geo_id", "valid_time"],
        right_on=["geo_id", "time"],
        suffixes=("_fc", "_an"),
        how="inner",
    )
    df["bias"] = df[f"{var_name}_fc"] - df[f"{var_name}_an"]
    df["abs_error"] = df["bias"].abs()
    keep = [
        "geo_id", "valid_time", "init_time", "lead_time",
        f"{var_name}_fc", f"{var_name}_an", "bias", "abs_error",
    ]
    return df[keep]

def compute_anomalies(df_error, clim_path, var_name):
    """Add forecast/analysis anomalies relative to ERA5 county climatology."""
    clim = pd.read_parquet(clim_path)
    df = df_error.copy()
    df["day_of_year"] = pd.to_datetime(df["valid_time"]).dt.dayofyear
    df = df.merge(
        clim[["geo_id", "day_of_year", f"{var_name}_clim"]],
        on=["geo_id", "day_of_year"],
        how="inner",
    )
    df["fc_anom"] = df[f"{var_name}_fc"] - df[f"{var_name}_clim"]
    df["an_anom"] = df[f"{var_name}_an"] - df[f"{var_name}_clim"]
    return df

# ---------------------------------------------------------------------------
# Aggregation driver (sequential / parallel, with monthly checkpoints)
# ---------------------------------------------------------------------------
def _group_fc_by_lead_month(fc_files):
    """Group forecast files by (lead_time, year, month) of valid_time."""
    def key(item):
        path, init, lead = item
        vt = pd.to_datetime(init) + pd.Timedelta(hours=int(lead))
        return (int(lead), vt.year, vt.month)
    sorted_files = sorted(fc_files, key=key)
    return [(k, list(g)) for k, g in groupby(sorted_files, key)]

def _group_an_by_month(an_files):
    """Group analysis files by time (year, month)."""
    def key(item):
        path, t = item
        dt = pd.to_datetime(t)
        return (dt.year, dt.month)
    sorted_files = sorted(an_files, key=key)
    return [(k, list(g)) for k, g in groupby(sorted_files, key)]

def _make_done_keys(df, key_fn):
    """Build the set of checkpoint keys from an existing DataFrame."""
    return set(key_fn(df))

def _an_key_fn(df):
    """Checkpoint key for analysis: just time."""
    return pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

def _fc_key_fn(df):
    """Checkpoint key for forecasts: valid_time + lead_time (so different leads aren't collapsed)."""
    vt = pd.to_datetime(df["valid_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return vt + "|" + df["lead_time"].astype(int).astype(str)

def _run_all(all_chunks, fc_save_path, an_save_path, n_parallel):
    """Process all tagged (fc + an) chunks in a shared pool with independent checkpoints.

    Parameters
    ----------
    all_chunks : list of (tag, group_key, file_list)
        tag is 'fc' or 'an'; group_key is e.g. (lead, yr, mo) or (yr, mo).
    fc_save_path, an_save_path : parquet output paths
    n_parallel : number of concurrent workers (1 = sequential)

    Returns
    -------
    (df_fc, df_an) : the completed DataFrames (or None if nothing aggregated)
    """
    states = {
        "fc": {"df": None, "done": set(), "path": fc_save_path,
               "key_fn": _fc_key_fn, "label": "Forecast"},
        "an": {"df": None, "done": set(), "path": an_save_path,
               "key_fn": _an_key_fn, "label": "Analysis"},
    }
    for st in states.values():
        tmp_path = st["path"] + ".tmp"
        # Prefer final path; if missing, try recovering from a complete .tmp (saved but rename not yet done)
        load_path = st["path"] if os.path.exists(st["path"]) else (tmp_path if os.path.exists(tmp_path) else None)
        if load_path is not None:
            try:
                st["df"] = pd.read_parquet(load_path)
                st["done"] = _make_done_keys(st["df"], st["key_fn"])
                if load_path == tmp_path:
                    os.rename(tmp_path, st["path"])
                    logging.info(f"{st['label']} checkpoint: recovered from .tmp, {len(st['done'])} entries already done")
                else:
                    logging.info(f"{st['label']} checkpoint: {len(st['done'])} entries already done")
            except Exception as e:
                broken_path = load_path + ".broken"
                logging.warning(
                    f"{st['label']} checkpoint unreadable ({e}); moving to {broken_path} and starting fresh"
                )
                try:
                    os.rename(load_path, broken_path)
                except OSError:
                    pass
                st["df"] = None
                st["done"] = set()

    def _handle_result(tag, group_key, df_chunk):
        if df_chunk is None:
            return
        st = states[tag]
        st["df"] = pd.concat([st["df"], df_chunk], ignore_index=True) if st["df"] is not None else df_chunk
        st["done"].update(st["key_fn"](df_chunk))
        # Atomic write: write to .tmp then rename so a killed process never leaves a broken footer
        tmp_path = st["path"] + ".tmp"
        st["df"].to_parquet(tmp_path)
        os.rename(tmp_path, st["path"])
        logging.info(f"{st['label']} checkpoint: saved {group_key} ({len(st['df'])} rows total)")

    try:
        from tqdm import tqdm as _tqdm
        bar = _tqdm(total=len(all_chunks), desc="FC+AN chunks", unit="chunk")
    except ImportError:
        bar = None

    if n_parallel == 1:
        for tag, group_key, chunk_files in all_chunks:
            _, gk, df_chunk = _process_chunk((tag, group_key, chunk_files, states[tag]["done"], None))
            _handle_result(tag, gk, df_chunk)
            if bar:
                bar.update(1)
    else:
        logging.info(f"Running with up to {n_parallel} workers (fc + an shared pool)")
        ctx = mp.get_context("fork")
        executor = ProcessPoolExecutor(max_workers=n_parallel, mp_context=ctx)
        try:
            futures = [
                executor.submit(
                    _process_chunk,
                    (tag, group_key, chunk_files, states[tag]["done"], None),
                )
                for tag, group_key, chunk_files in all_chunks
            ]
            for fut in as_completed(futures):
                tag, group_key, df_chunk = fut.result()
                _handle_result(tag, group_key, df_chunk)
                if bar:
                    bar.update(1)
        except KeyboardInterrupt:
            logging.warning("Interrupted — shutting down workers (completed chunks are checkpointed).")
            executor.shutdown(wait=False, cancel_futures=True)
            if bar:
                bar.close()
            raise
        else:
            executor.shutdown(wait=True)

    if bar:
        bar.close()
    return states["fc"]["df"], states["an"]["df"]

# ---------------------------------------------------------------------------
# AIFS forecast aggregation and system comparison
# ---------------------------------------------------------------------------
def aggregate_aifs_forecasts(fc_files, fc_save_path, n_parallel):
    """Aggregate AIFS forecast files to counties using the global weightmap.

    Parameters
    ----------
    fc_files : list of (path, init_time_str, lead_time)
    fc_save_path : str
    n_parallel : int

    Returns
    -------
    pandas.DataFrame or None
        Aggregated AIFS forecast table, or None if nothing was aggregated.
    """
    if _WEIGHTMAP is None:
        raise RuntimeError("Weightmap must be initialized before aggregating AIFS forecasts.")

    fc_chunks = _group_fc_by_lead_month(fc_files)
    if not fc_chunks:
        return None

    results = []

    try:
        from tqdm import tqdm as _tqdm
        bar = _tqdm(total=len(fc_chunks), desc="AIFS chunks", unit="chunk")
    except ImportError:
        bar = None

    if n_parallel == 1:
        for group_key, chunk_files in fc_chunks:
            _, df_chunk = _process_fc_chunk((group_key, chunk_files, set(), None))
            if df_chunk is not None:
                results.append(df_chunk)
            if bar:
                bar.update(1)
    else:
        logging.info(f"Running AIFS aggregation with up to {n_parallel} workers")
        ctx = mp.get_context("fork")
        executor = ProcessPoolExecutor(max_workers=n_parallel, mp_context=ctx)
        try:
            futures = [
                executor.submit(_process_fc_chunk, (group_key, chunk_files, set(), None))
                for group_key, chunk_files in fc_chunks
            ]
            for fut in as_completed(futures):
                _, df_chunk = fut.result()
                if df_chunk is not None:
                    results.append(df_chunk)
                if bar:
                    bar.update(1)
        except KeyboardInterrupt:
            logging.warning("Interrupted — shutting down AIFS workers.")
            executor.shutdown(wait=False, cancel_futures=True)
            if bar:
                bar.close()
            raise
        else:
            executor.shutdown(wait=True)

    if bar:
        bar.close()

    if not results:
        return None

    df_fc = pd.concat(results, ignore_index=True)
    tmp_path = fc_save_path + ".tmp"
    df_fc.to_parquet(tmp_path)
    os.rename(tmp_path, fc_save_path)
    logging.info("AIFS forecast checkpoint: saved %d rows to %s", len(df_fc), fc_save_path)
    return df_fc


def build_aifs_ifs_comparison(df_error_ifs, df_error_aifs, area_weights):
    """Build per-time AIFS vs IFS comparison on common dates.

    The comparison is restricted to the intersection of dates/times where
    both systems have bias/absolute error for a given (geo_id, lead_time).
    """
    key_cols = ["geo_id", "valid_time", "lead_time"]
    cols = key_cols + ["bias", "abs_error"]

    df_ifs = df_error_ifs[cols].rename(
        columns={"bias": "bias_ifs", "abs_error": "abs_error_ifs"}
    )
    df_aifs = df_error_aifs[cols].rename(
        columns={"bias": "bias_aifs", "abs_error": "abs_error_aifs"}
    )

    df = pd.merge(df_ifs, df_aifs, on=key_cols, how="inner")
    df["bias_diff"] = df["bias_aifs"] - df["bias_ifs"]
    df["abs_error_diff"] = df["abs_error_aifs"] - df["abs_error_ifs"]

    # Attach area weights so downstream spatial averages can be area-weighted
    df = df.merge(area_weights, on="geo_id", how="left")
    return df

# ---------------------------------------------------------------------------
# Restore forecast table from bias table
# ---------------------------------------------------------------------------
def restore_fc_from_bias(save_dir=None, error_path=None, fc_path=None, var_name=VAR_NAME):
    """Reconstruct ifs_fc_2t_county.parquet from ifs_fc_bias_2t_county.parquet.

    The bias table has (geo_id, valid_time, init_time, lead_time, t2m_fc, ...).
    The forecast table needs (geo_id, valid_time, init_time, lead_time, t2m).
    """
    save_dir = save_dir or SAVE_DIR
    error_path = error_path or os.path.join(save_dir, "ifs_fc_bias_2t_county.parquet")
    fc_path = fc_path or os.path.join(save_dir, "ifs_fc_2t_county.parquet")

    if not os.path.exists(error_path):
        logging.error(f"Bias table not found: {error_path}")
        return False

    logging.info(f"Reading bias table from {error_path} ...")
    df = pd.read_parquet(error_path)
    fc_cols = ["geo_id", "valid_time", "init_time", "lead_time", f"{var_name}_fc"]
    # Preserve area weights if available in the bias table
    if "aland" in df.columns:
        fc_cols.append("aland")
    df = df[fc_cols]
    df = df.rename(columns={f"{var_name}_fc": var_name})
    logging.info(f"Restoring forecast table: {len(df)} rows -> {fc_path}")
    tmp_path = fc_path + ".tmp"
    df.to_parquet(tmp_path)
    os.rename(tmp_path, fc_path)
    write_table_metadata("ifs_fc_2t_county", fc_path)
    logging.info("Done.")
    return True

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(n_parallel: int = 1):
    t_start = time.time()
    logging.info("=== aggregate_fc_an_2t.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S"))
    global _WEIGHTMAP
    os.makedirs(SAVE_DIR, exist_ok=True)

    # Output paths for IFS
    fc_path = os.path.join(SAVE_DIR, "ifs_fc_2t_county.parquet")
    an_path = os.path.join(SAVE_DIR, "ifs_an_2t_county.parquet")
    error_path = os.path.join(SAVE_DIR, "ifs_fc_bias_2t_county.parquet")
    anom_path = os.path.join(SAVE_DIR, "ifs_fc_bias_anom_2t_county.parquet")

    # Output paths for AIFS and comparison
    aifs_fc_path = os.path.join(SAVE_DIR, "aifs_fc_2t_county.parquet")
    aifs_error_path = os.path.join(SAVE_DIR, "aifs_fc_bias_2t_county.parquet")
    aifs_anom_path = os.path.join(SAVE_DIR, "aifs_fc_bias_anom_2t_county.parquet")
    compare_path = os.path.join(SAVE_DIR, "aifs_vs_ifs_fc_bias_comparison_2t_county.parquet")

    n_parallel = max(1, int(n_parallel))

    # 1. Discover files
    t_disc = time.time()
    logging.info("Building file lists ...")
    fc_files_ifs = build_fc_files(FC_DIR, START, END, FC_FREQ, LEAD_TIMES)
    fc_files_aifs = build_fc_files(AIFS_FC_DIR, START, END, FC_FREQ, LEAD_TIMES)
    an_files = build_an_files(AN_DIR, START, END, AN_FREQ)
    logging.info(
        "[%.0fs] Found %d IFS forecast files, %d AIFS forecast files, %d analysis files",
        time.time() - t_disc,
        len(fc_files_ifs),
        len(fc_files_aifs),
        len(an_files),
    )

    # 2. Align IFS forecasts and analyses by valid_time
    fc_files_ifs, an_files_aligned = align_fc_an(fc_files_ifs, an_files)
    logging.info(
        "After alignment: %d IFS forecasts, %d analyses",
        len(fc_files_ifs),
        len(an_files_aligned),
    )

    if not fc_files_ifs:
        logging.warning("No aligned IFS files found; exiting")
        return

    # 3. Build weightmap once
    grid_path = fc_files_ifs[0][0]
    logging.info("Building weightmap ...")
    geo_agg = wxagg.GeoAggregator(
        shapefile_path=SHAPEFILE_PATH, grid_path=grid_path,
        silent=not VERBOSE, cache_dir=WEIGHTMAP_CACHE_DIR,
    )
    _WEIGHTMAP = geo_agg.weightmap

    # Pre-compute area weights from the shapefile
    area_weights = load_area_weights(SHAPEFILE_PATH)

    # 4. Build tagged chunk list (IFS fc + an interleaved)
    fc_chunks = _group_fc_by_lead_month(fc_files_ifs)
    an_chunks = _group_an_by_month(an_files_aligned)
    all_chunks = [("fc", key, files) for key, files in fc_chunks] + \
                 [("an", key, files) for key, files in an_chunks]
    logging.info(
        "Aggregating %d IFS fc + %d an = %d total chunks ...",
        len(fc_chunks),
        len(an_chunks),
        len(all_chunks),
    )

    # 5. Process all chunks in a shared pool (IFS only here)
    t_agg = time.time()
    df_fc_ifs, df_an = _run_all(all_chunks, fc_path, an_path, n_parallel)
    logging.info("[%.0fs] IFS aggregation phase complete", time.time() - t_agg)

    if df_fc_ifs is None or df_an is None:
        logging.warning("No IFS data aggregated")
        return

    # Attach area weights to IFS forecast and analysis tables and resave
    df_fc_ifs = df_fc_ifs.merge(area_weights, on="geo_id", how="left")
    df_an = df_an.merge(area_weights, on="geo_id", how="left")

    for path, df, label, table_name in [
        (fc_path, df_fc_ifs, "IFS forecast", "ifs_fc_2t_county"),
        (an_path, df_an, "IFS analysis", "ifs_an_2t_county"),
    ]:
        tmp_path = path + ".tmp"
        df.to_parquet(tmp_path)
        os.rename(tmp_path, path)
        logging.info("Saved %s table with area weights to %s", label, path)
        write_table_metadata(table_name, path)

    # 6. Aggregate AIFS forecasts using the same weightmap
    df_fc_aifs = None
    if fc_files_aifs:
        logging.info("Aggregating AIFS forecasts ...")
        df_fc_aifs = aggregate_aifs_forecasts(fc_files_aifs, aifs_fc_path, n_parallel)
        if df_fc_aifs is not None:
            df_fc_aifs = df_fc_aifs.merge(area_weights, on="geo_id", how="left")
            tmp_path = aifs_fc_path + ".tmp"
            df_fc_aifs.to_parquet(tmp_path)
            os.rename(tmp_path, aifs_fc_path)
            logging.info(
                "Saved AIFS forecast table with area weights to %s",
                aifs_fc_path,
            )
            write_table_metadata("aifs_fc_2t_county", aifs_fc_path)
        else:
            logging.warning("No AIFS forecast data aggregated")
    else:
        logging.info("No AIFS forecast files discovered; skipping AIFS aggregation")

    # 7. Bias and absolute error for IFS
    logging.info("Computing IFS bias and absolute error ...")
    df_error_ifs = compute_bias(df_fc_ifs, df_an, VAR_NAME)
    df_error_ifs = df_error_ifs.merge(area_weights, on="geo_id", how="left")
    df_error_ifs.to_parquet(error_path)
    logging.info(
        "Saved IFS bias table (%d rows) to %s",
        len(df_error_ifs),
        error_path,
    )
    write_table_metadata("ifs_fc_bias_2t_county", error_path)

    # 8. Bias and absolute error for AIFS (if available)
    df_error_aifs = None
    if df_fc_aifs is not None and not df_fc_aifs.empty:
        logging.info("Computing AIFS bias and absolute error ...")
        df_error_aifs = compute_bias(df_fc_aifs, df_an, VAR_NAME)
        df_error_aifs = df_error_aifs.merge(area_weights, on="geo_id", how="left")
        df_error_aifs.to_parquet(aifs_error_path)
        logging.info(
            "Saved AIFS bias table (%d rows) to %s",
            len(df_error_aifs),
            aifs_error_path,
        )
        write_table_metadata("aifs_fc_bias_2t_county", aifs_error_path)
    else:
        logging.info("No AIFS forecast data available for bias computation")

    # 9. Anomalies relative to ERA5 climatology (IFS and AIFS)
    if os.path.exists(CLIM_PATH):
        logging.info("Computing IFS anomalies ...")
        df_anom_ifs = compute_anomalies(df_error_ifs, CLIM_PATH, VAR_NAME)
        df_anom_ifs = df_anom_ifs.merge(area_weights, on="geo_id", how="left")
        df_anom_ifs.to_parquet(anom_path)
        logging.info(
            "Saved IFS anomaly table (%d rows) to %s",
            len(df_anom_ifs),
            anom_path,
        )
        write_table_metadata("ifs_fc_bias_anom_2t_county", anom_path)

        if df_error_aifs is not None:
            logging.info("Computing AIFS anomalies ...")
            df_anom_aifs = compute_anomalies(df_error_aifs, CLIM_PATH, VAR_NAME)
            df_anom_aifs = df_anom_aifs.merge(area_weights, on="geo_id", how="left")
            df_anom_aifs.to_parquet(aifs_anom_path)
            logging.info(
                "Saved AIFS anomaly table (%d rows) to %s",
                len(df_anom_aifs),
                aifs_anom_path,
            )
            write_table_metadata("aifs_fc_bias_anom_2t_county", aifs_anom_path)
    else:
        logging.warning(
            f"ERA5 climatology not found at {CLIM_PATH}; skipping anomaly computation "
            "for both IFS and AIFS"
        )

    # 10. Comparison table: AIFS vs IFS accuracy on common dates
    if df_error_aifs is not None:
        logging.info("Building AIFS vs IFS comparison table ...")
        df_compare = build_aifs_ifs_comparison(df_error_ifs, df_error_aifs, area_weights)
        tmp_path = compare_path + ".tmp"
        df_compare.to_parquet(tmp_path)
        os.rename(tmp_path, compare_path)
        logging.info(
            "Saved AIFS vs IFS comparison table (%d rows) to %s",
            len(df_compare),
            compare_path,
        )
        write_table_metadata("aifs_vs_ifs_fc_bias_comparison_2t_county", compare_path)

    logging.info("[%.0fs] Done.", time.time() - t_start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate IFS forecasts & analyses to US counties; compute bias, abs error, and anomalies."
    )
    parser.add_argument("--start", default=START, help=f"Start date (default: {START})")
    parser.add_argument("--end", default=END, help=f"End date (default: {END})")
    parser.add_argument("--fc-freq", default=FC_FREQ, help=f"Init-time frequency (default: {FC_FREQ})")
    parser.add_argument("--an-freq", default=AN_FREQ, help=f"Analysis valid-time frequency (default: {AN_FREQ})")
    parser.add_argument(
        "--lead-times", nargs="+", type=int, default=LEAD_TIMES,
        help=f"Lead times in hours (default: {LEAD_TIMES})",
    )
    parser.add_argument(
        "--n-parallel", type=int, default=1,
        help="Number of months to process simultaneously (default: 1).",
    )
    parser.add_argument(
        "--restore-fc-from-bias",
        action="store_true",
        help="Reconstruct ifs_fc_2t_county.parquet from the bias table and exit.",
    )
    args = parser.parse_args()

    if args.restore_fc_from_bias:
        restore_fc_from_bias()
        sys.exit(0)

    START = args.start
    END = args.end
    FC_FREQ = args.fc_freq
    AN_FREQ = args.an_freq
    LEAD_TIMES = args.lead_times

    main(n_parallel=args.n_parallel)