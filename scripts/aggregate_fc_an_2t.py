"""
Aggregate IFS forecasts, IFS analyses, and AIFS forecasts to US counties.

Raw spatial aggregation only — bias, anomalies, and comparisons are computed
by compute_derived_2t.py once aggregation is complete.

Outputs monthly checkpoint parquets to:
  {AGGREGATED_DIR}/ifs_fc_monthly/ifs_fc_2t_county_{YYYY}_{MM}_lead{LLL}.parquet
  {AGGREGATED_DIR}/ifs_an_monthly/ifs_an_2t_county_{YYYY}_{MM}.parquet
  {AGGREGATED_DIR}/aifs_fc_monthly/aifs_fc_2t_county_{YYYY}_{MM}_lead{LLL}.parquet

All three data streams share a single worker pool. Workers write monthly
checkpoint parquets atomically; interrupted runs resume from the last
completed chunk.

Usage:
  python scripts/aggregate_fc_an_2t.py [--n-parallel N] [--write-full-tables]
  python scripts/aggregate_fc_an_2t.py --start 2024-01-01 --end 2024-12-31
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

import pandas as pd
import xarray as xr
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nwp_census_eval import aggregate as wxagg
import xagg
from config import (
    SHAPEFILE_PATH,
    IFS_FC_DIR      as FC_DIR,
    AIFS_FC_DIR,
    IFS_AN_DIR      as AN_DIR,
    AGGREGATED_DIR  as SAVE_DIR,
    IFS_START       as START,
    IFS_END         as END,
    LEAD_TIMES,
    WEIGHTMAP_CACHE_DIR,
    IFS_GRID,
    AIFS_GRID,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FC_FREQ  = "12h"
AN_FREQ  = "6h"
VAR_NAME = "t2m"

_WEIGHTMAPS = {}  # {grid_tag: weightmap}; set in main(); inherited by forked workers
# Maps stream tag → weightmap key (IFS fc and an share a grid; AIFS fc has its own)
_TAG_GRID = {
    "ifs_fc":  "ifs",
    "ifs_an":  "ifs",
    "aifs_fc": "aifs",
}

IFS_FC_MONTHLY_DIR  = os.path.join(SAVE_DIR, "ifs_fc_monthly")
IFS_AN_MONTHLY_DIR  = os.path.join(SAVE_DIR, "ifs_an_monthly")
AIFS_FC_MONTHLY_DIR = os.path.join(SAVE_DIR, "aifs_fc_monthly")

_TAG_DIRS = {
    "ifs_fc":  IFS_FC_MONTHLY_DIR,
    "ifs_an":  IFS_AN_MONTHLY_DIR,
    "aifs_fc": AIFS_FC_MONTHLY_DIR,
}
_TAG_STEMS = {
    "ifs_fc":  "ifs_fc_2t_county",
    "ifs_an":  "ifs_an_2t_county",
    "aifs_fc": "aifs_fc_2t_county",
}

# ---------------------------------------------------------------------------
# Table-level metadata (sidecar JSON, written only with --write-full-tables)
# ---------------------------------------------------------------------------
TABLE_METADATA = {
    "ifs_fc_2t_county": {
        "description": "IFS 2m temperature forecasts aggregated to US counties.",
        "variables": {
            "geo_id":     {"units": "1",     "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC",   "long_name": "Forecast valid time."},
            "init_time":  {"units": "UTC",   "long_name": "Forecast initialization time."},
            "lead_time":  {"units": "hours", "long_name": "Forecast lead time (valid_time - init_time)."},
            VAR_NAME:     {"units": "K",     "long_name": "2m air temperature (forecast)."},
        },
    },
    "ifs_an_2t_county": {
        "description": "IFS 2m temperature analyses aggregated to US counties.",
        "variables": {
            "geo_id": {"units": "1",   "long_name": "County FIPS code (string)."},
            "time":   {"units": "UTC", "long_name": "Analysis valid time."},
            VAR_NAME: {"units": "K",   "long_name": "2m air temperature (analysis)."},
        },
    },
    "aifs_fc_2t_county": {
        "description": "AIFS 2m temperature forecasts aggregated to US counties.",
        "variables": {
            "geo_id":     {"units": "1",     "long_name": "County FIPS code (string)."},
            "valid_time": {"units": "UTC",   "long_name": "Forecast valid time."},
            "init_time":  {"units": "UTC",   "long_name": "Forecast initialization time."},
            "lead_time":  {"units": "hours", "long_name": "Forecast lead time (valid_time - init_time)."},
            VAR_NAME:     {"units": "K",     "long_name": "2m air temperature (forecast)."},
        },
    },
}


def write_table_metadata(table_name, path):
    meta = TABLE_METADATA.get(table_name)
    if meta is None:
        return
    base, _ = os.path.splitext(path)
    tmp = base + ".meta.json.tmp"
    with open(tmp, "w") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp, base + ".meta.json")


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------
def build_fc_files(fc_dir, start, end, freq, lead_times):
    """Return [(path, init_time_str, lead_time), ...] for forecast files."""
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
    """Return [(path, time_str), ...] for analysis files."""
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
    """Keep only fc/an entries whose valid_time has a match on the other side."""
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
# Per-file aggregation
# ---------------------------------------------------------------------------
def _open_netcdf(path):
    """Open a NetCDF file; converts GRIB-format files in-place if needed."""
    try:
        return xr.open_dataset(path, engine="netcdf4")
    except OSError as exc:
        if "Unknown file format" not in str(exc):
            raise
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
    """Aggregate one forecast file to counties."""
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
    """Aggregate one analysis file to counties."""
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
# Grouping helpers
# ---------------------------------------------------------------------------
def _group_fc_by_lead_month(fc_files):
    """Group forecast files by (lead_time, year, month) of valid_time."""
    def key(item):
        path, init, lead = item
        vt = pd.to_datetime(init) + pd.Timedelta(hours=int(lead))
        return (int(lead), vt.year, vt.month)
    return [(k, list(g)) for k, g in groupby(sorted(fc_files, key=key), key)]


def _group_an_by_month(an_files):
    """Group analysis files by (year, month)."""
    def key(item):
        path, t = item
        dt = pd.to_datetime(t)
        return (dt.year, dt.month)
    return [(k, list(g)) for k, g in groupby(sorted(an_files, key=key), key)]


# ---------------------------------------------------------------------------
# Monthly checkpoint helpers
# ---------------------------------------------------------------------------
def _chunk_path(tag, yr, mo, lead=None):
    stem = _TAG_STEMS[tag]
    d = _TAG_DIRS[tag]
    if lead is not None:
        return os.path.join(d, f"{stem}_{yr}_{mo:02d}_lead{int(lead):03d}.parquet")
    return os.path.join(d, f"{stem}_{yr}_{mo:02d}.parquet")


def _is_chunk_done(tag, group_key):
    if tag in ("ifs_fc", "aifs_fc"):
        lead, yr, mo = group_key
        path = _chunk_path(tag, yr, mo, lead=lead)
    else:
        yr, mo = group_key
        path = _chunk_path(tag, yr, mo)
    if not os.path.exists(path):
        return False
    try:
        pq.read_metadata(path)
        return True
    except Exception:
        return False


def _write_chunk_atomic(tag, group_key, df, n_source_files):
    if tag in ("ifs_fc", "aifs_fc"):
        lead, yr, mo = group_key
        path = _chunk_path(tag, yr, mo, lead=lead)
    else:
        yr, mo = group_key
        path = _chunk_path(tag, yr, mo)
    tmp = path + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    logging.info("Checkpoint: %s (%d rows)", os.path.basename(path), len(df))



# ---------------------------------------------------------------------------
# Workers (forked; silent — progress tracked in main process)
# ---------------------------------------------------------------------------
def _process_fc_chunk(args):
    """Aggregate one (tag, group_key, files) fc chunk. Returns (tag, group_key, df|None)."""
    tag, group_key, files = args
    weightmap = _WEIGHTMAPS.get(_TAG_GRID.get(tag))
    if weightmap is None:
        raise RuntimeError(
            f"No weightmap for tag '{tag}'; expected forked workers to inherit _WEIGHTMAPS."
        )
    results = []
    for path, init_time, lead_time in files:
        try:
            df = aggregate_fc_file(path, init_time, lead_time, weightmap, VAR_NAME)
        except Exception as exc:
            logging.warning("Skipping %s [%s]: %s", path, type(exc).__name__, exc)
            continue
        results.append(df)
    if not results:
        return tag, group_key, None
    return tag, group_key, pd.concat(results, ignore_index=True)


def _process_an_chunk(args):
    """Aggregate one (tag, group_key, files) an chunk. Returns (tag, group_key, df|None)."""
    tag, group_key, files = args
    weightmap = _WEIGHTMAPS.get(_TAG_GRID.get(tag))
    if weightmap is None:
        raise RuntimeError(
            f"No weightmap for tag '{tag}'; expected forked workers to inherit _WEIGHTMAPS."
        )
    results = []
    for path, time_str in files:
        try:
            df = aggregate_an_file(path, time_str, weightmap, VAR_NAME)
        except Exception as exc:
            logging.warning("Skipping %s [%s]: %s", path, type(exc).__name__, exc)
            continue
        results.append(df)
    if not results:
        return tag, group_key, None
    return tag, group_key, pd.concat(results, ignore_index=True)


def _process_chunk(args):
    """Dispatch to fc or an worker based on tag."""
    tag = args[0]
    if tag in ("ifs_fc", "aifs_fc"):
        return _process_fc_chunk(args)
    return _process_an_chunk(args)


# ---------------------------------------------------------------------------
# Aggregation driver
# ---------------------------------------------------------------------------
def _run_all(all_chunks, n_parallel):
    """Process all (tag, group_key, files) chunks; write each to its monthly parquet."""
    pending = [c for c in all_chunks if not _is_chunk_done(c[0], c[1])]
    n_done = len(all_chunks) - len(pending)
    logging.info(
        "Chunks: %d total | %d done | %d pending",
        len(all_chunks), n_done, len(pending),
    )
    if not pending:
        logging.info("All chunks already done.")
        return

    try:
        from tqdm import tqdm as _tqdm
        from tqdm.contrib.logging import logging_redirect_tqdm
    except ImportError:
        _tqdm = None
        from contextlib import nullcontext as logging_redirect_tqdm

    bar = _tqdm(total=len(pending), desc="Aggregating", unit="chunk") if _tqdm else None

    chunk_n_files = {(tag, group_key): len(files) for tag, group_key, files in pending}

    def _handle(tag, group_key, df):
        if df is not None and not df.empty:
            _write_chunk_atomic(tag, group_key, df, chunk_n_files.get((tag, group_key), 0))
        if bar:
            bar.update(1)

    if n_parallel == 1:
        with logging_redirect_tqdm():
            for chunk in pending:
                tag, group_key, df = _process_chunk(chunk)
                _handle(tag, group_key, df)
    else:
        logging.info(
            "Running with up to %d workers (IFS fc + IFS an + AIFS fc shared pool)", n_parallel
        )
        ctx = mp.get_context("fork")
        executor = ProcessPoolExecutor(max_workers=n_parallel, mp_context=ctx)
        try:
            with logging_redirect_tqdm():
                futures = [executor.submit(_process_chunk, chunk) for chunk in pending]
                for fut in as_completed(futures):
                    tag, group_key, df = fut.result()
                    _handle(tag, group_key, df)
        except KeyboardInterrupt:
            logging.warning(
                "Interrupted — shutting down workers (completed chunks are checkpointed)."
            )
            executor.shutdown(wait=False, cancel_futures=True)
            if bar:
                bar.close()
            raise
        else:
            executor.shutdown(wait=True)

    if bar:
        bar.close()


# ---------------------------------------------------------------------------
# Optional full-table consolidation
# ---------------------------------------------------------------------------
def _consolidate(tag, output_path):
    """Stream all monthly parquets for a tag into one consolidated parquet."""
    monthly_dir = _TAG_DIRS[tag]
    monthly_files = sorted(glob.glob(os.path.join(monthly_dir, "*.parquet")))
    if not monthly_files:
        logging.warning("No monthly parquets for %s; skipping consolidation", tag)
        return
    tmp = output_path + ".tmp"
    writer = None
    schema = None
    total = 0
    for path in monthly_files:
        table = pq.read_table(path)
        if schema is None:
            schema = table.schema
            writer = pq.ParquetWriter(tmp, schema)
        else:
            table = table.cast(schema)
        writer.write_table(table)
        total += len(table)
    if writer:
        writer.close()
        os.replace(tmp, output_path)
        logging.info("Consolidated %s: %d rows → %s", tag, total, output_path)
        write_table_metadata(f"{_TAG_STEMS[tag]}", output_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(n_parallel=1, write_full_tables=False):
    t_start = time.time()
    logging.info("=== aggregate_fc_an_2t.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S"))

    os.makedirs(SAVE_DIR, exist_ok=True)
    os.makedirs(IFS_FC_MONTHLY_DIR, exist_ok=True)
    os.makedirs(IFS_AN_MONTHLY_DIR, exist_ok=True)
    os.makedirs(AIFS_FC_MONTHLY_DIR, exist_ok=True)

    n_parallel = max(1, int(n_parallel))

    # 1. Discover files
    # Extend END by one step of each frequency so pd.date_range includes the full
    # last day: without this, date_range(end='2025-12-31', freq='12h') stops at
    # T00, missing the T12 FC init; date_range at 6h stops at T00, missing
    # the T06/T12/T18 analysis times on Dec 31.
    fc_end = pd.Timestamp(END) + pd.Timedelta(hours=12)   # include T12 init on last day
    an_end = pd.Timestamp(END) + pd.Timedelta(hours=18)   # include T18 analysis on last day
    t_disc = time.time()
    logging.info("Building file lists ...")
    fc_files_ifs  = build_fc_files(FC_DIR,      START, fc_end, FC_FREQ, LEAD_TIMES)
    fc_files_aifs = build_fc_files(AIFS_FC_DIR, START, fc_end, FC_FREQ, LEAD_TIMES)
    an_files      = build_an_files(AN_DIR,      START, an_end, AN_FREQ)
    logging.info(
        "[%.0fs] Found %d IFS fc, %d AIFS fc, %d IFS an files",
        time.time() - t_disc, len(fc_files_ifs), len(fc_files_aifs), len(an_files),
    )

    # 2. Align IFS fc and an by valid_time
    fc_files_ifs, an_files = align_fc_an(fc_files_ifs, an_files)
    logging.info(
        "After alignment: %d IFS fc, %d IFS an (AIFS not aligned — uses IFS an as reference)",
        len(fc_files_ifs), len(an_files),
    )

    if not fc_files_ifs and not fc_files_aifs:
        logging.warning("No files found; exiting")
        return

    # 3. Build one weightmap per grid resolution (IFS=0.125°, AIFS=0.25°)
    global _WEIGHTMAPS
    _WEIGHTMAPS = {}
    if fc_files_ifs or an_files:
        ifs_grid_path = (fc_files_ifs or an_files)[0][0]
        logging.info("Building IFS weightmap (%s° grid) ...", IFS_GRID)
        _WEIGHTMAPS["ifs"] = wxagg.GeoAggregator(
            shapefile_path=SHAPEFILE_PATH, grid_path=ifs_grid_path,
            silent=True, cache_dir=WEIGHTMAP_CACHE_DIR,
        ).weightmap
    if fc_files_aifs:
        aifs_grid_path = fc_files_aifs[0][0]
        logging.info("Building AIFS weightmap (%s° grid) ...", AIFS_GRID)
        _WEIGHTMAPS["aifs"] = wxagg.GeoAggregator(
            shapefile_path=SHAPEFILE_PATH, grid_path=aifs_grid_path,
            silent=True, cache_dir=WEIGHTMAP_CACHE_DIR,
        ).weightmap

    # 4. Build unified chunk list (IFS fc + IFS an + AIFS fc)
    ifs_fc_chunks  = [("ifs_fc",  k, f) for k, f in _group_fc_by_lead_month(fc_files_ifs)]
    ifs_an_chunks  = [("ifs_an",  k, f) for k, f in _group_an_by_month(an_files)]
    aifs_fc_chunks = [("aifs_fc", k, f) for k, f in _group_fc_by_lead_month(fc_files_aifs)]
    all_chunks = ifs_fc_chunks + ifs_an_chunks + aifs_fc_chunks
    logging.info(
        "Chunks: %d IFS fc + %d IFS an + %d AIFS fc = %d total",
        len(ifs_fc_chunks), len(ifs_an_chunks), len(aifs_fc_chunks), len(all_chunks),
    )

    # 5. Aggregate all three streams in one shared pool
    t_agg = time.time()
    _run_all(all_chunks, n_parallel)
    logging.info("[%.0fs] Aggregation complete", time.time() - t_agg)

    # 6. Optional: consolidate monthly parquets into single tables
    if write_full_tables:
        logging.info("Consolidating monthly parquets into full tables ...")
        for tag, fname in [
            ("ifs_fc",  "ifs_fc_2t_county.parquet"),
            ("ifs_an",  "ifs_an_2t_county.parquet"),
            ("aifs_fc", "aifs_fc_2t_county.parquet"),
        ]:
            _consolidate(tag, os.path.join(SAVE_DIR, fname))

    logging.info("[%.0fs] Done.", time.time() - t_start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate IFS forecasts, IFS analyses, and AIFS forecasts to US counties."
    )
    parser.add_argument("--start",    default=START,   help=f"Start date (default: {START})")
    parser.add_argument("--end",      default=END,     help=f"End date (default: {END})")
    parser.add_argument("--fc-freq",  default=FC_FREQ, help=f"Init-time frequency (default: {FC_FREQ})")
    parser.add_argument("--an-freq",  default=AN_FREQ, help=f"Analysis frequency (default: {AN_FREQ})")
    parser.add_argument(
        "--lead-times", nargs="+", type=int, default=LEAD_TIMES,
        help=f"Lead times in hours (default: {LEAD_TIMES})",
    )
    parser.add_argument(
        "--n-parallel", type=int, default=1,
        help="Number of concurrent workers (default: 1).",
    )
    parser.add_argument(
        "--write-full-tables", action="store_true",
        help="Consolidate monthly parquets into single per-model parquets after aggregation.",
    )
    args = parser.parse_args()

    START      = args.start
    END        = args.end
    FC_FREQ    = args.fc_freq
    AN_FREQ    = args.an_freq
    LEAD_TIMES = args.lead_times

    main(
        n_parallel=args.n_parallel,
        write_full_tables=args.write_full_tables,
    )
