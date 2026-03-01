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
from itertools import groupby
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from censuswxindex import aggregate as wxagg
from censuswxindex.aggregate import _maybe_progress
import xagg

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
VERBOSE = True
SCRATCH = "/glade/derecho/scratch/dcalhoun"
SHAPEFILE_PATH = os.path.join(
    SCRATCH, "census/shapefiles/nhgis0001_shapefile_tl2023_us_county_2023/US_county_2023.shp"
)
FC_DIR = os.path.join(SCRATCH, "ecmwf/ifs/fc/0.125/2t")
AN_DIR = os.path.join(SCRATCH, "ecmwf/ifs/an/0.125/2t")
CLIM_PATH = os.path.join(SCRATCH, "aggregated/era5_2t_county_climatology_1991_2020.parquet")
SAVE_DIR = os.path.join(SCRATCH, "aggregated")

START = "2016-01-01"
END = "2025-12-31"
FC_FREQ = "12h"
AN_FREQ = "6h"
LEAD_TIMES = [0, 6, 12, 18, 24, 48, 72, 96, 120, 168, 240]
VAR_NAME = "t2m"


_WEIGHTMAP = None  # set in main(); inherited by workers when using fork


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
def aggregate_fc_file(path, init_time, lead_time, weightmap, var_name):
    """Aggregate one forecast file to counties using *weightmap*."""
    ds = xr.open_dataset(path)
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
    with xr.open_dataset(path) as ds:
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
    iterable = _maybe_progress(
        chunk_files, len(chunk_files), f"FC lead={lead}h {yr}-{mo:02d}",
        silent=not VERBOSE, position=position,
    )
    for path, init_time, lead_time in iterable:
        key = _fc_checkpoint_key(init_time, lead_time)
        if key in done_keys:
            continue
        df = aggregate_fc_file(path, init_time, lead_time, _WEIGHTMAP, VAR_NAME)
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
    iterable = _maybe_progress(
        month_files, len(month_files), f"AN {yr}-{mo:02d}",
        silent=not VERBOSE, position=position,
    )
    for path, time in iterable:
        t_str = pd.to_datetime(time).strftime("%Y-%m-%d %H:%M:%S")
        if t_str in done_times:
            continue
        df = aggregate_an_file(path, time, _WEIGHTMAP, VAR_NAME)
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

    if n_parallel == 1:
        for tag, group_key, chunk_files in all_chunks:
            _, gk, df_chunk = _process_chunk((tag, group_key, chunk_files, states[tag]["done"], None))
            _handle_result(tag, gk, df_chunk)
    else:
        logging.info(f"Running with up to {n_parallel} workers (fc + an shared pool)")
        ctx = mp.get_context("fork")
        with ProcessPoolExecutor(max_workers=n_parallel, mp_context=ctx) as executor:
            for batch in _chunked(all_chunks, n_parallel):
                futures = []
                for position, (tag, group_key, chunk_files) in enumerate(batch):
                    futures.append(
                        executor.submit(
                            _process_chunk,
                            (tag, group_key, chunk_files, states[tag]["done"], position),
                        )
                    )
                for fut in as_completed(futures):
                    tag, group_key, df_chunk = fut.result()
                    _handle_result(tag, group_key, df_chunk)

    return states["fc"]["df"], states["an"]["df"]


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

    fc_cols = ["geo_id", "valid_time", "init_time", "lead_time", f"{var_name}_fc"]
    logging.info(f"Reading bias table (columns {fc_cols}) from {error_path} ...")
    df = pd.read_parquet(error_path, columns=fc_cols)
    df = df.rename(columns={f"{var_name}_fc": var_name})
    logging.info(f"Restoring forecast table: {len(df)} rows -> {fc_path}")
    tmp_path = fc_path + ".tmp"
    df.to_parquet(tmp_path)
    os.rename(tmp_path, fc_path)
    logging.info("Done.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(n_parallel: int = 1):
    global _WEIGHTMAP
    os.makedirs(SAVE_DIR, exist_ok=True)

    fc_path = os.path.join(SAVE_DIR, "ifs_fc_2t_county.parquet")
    an_path = os.path.join(SAVE_DIR, "ifs_an_2t_county.parquet")
    error_path = os.path.join(SAVE_DIR, "ifs_fc_bias_2t_county.parquet")
    anom_path = os.path.join(SAVE_DIR, "ifs_fc_bias_anom_2t_county.parquet")

    n_parallel = max(1, int(n_parallel))

    # 1. Discover files
    logging.info("Building file lists ...")
    fc_files = build_fc_files(FC_DIR, START, END, FC_FREQ, LEAD_TIMES)
    an_files = build_an_files(AN_DIR, START, END, AN_FREQ)
    logging.info(f"Found {len(fc_files)} forecast files, {len(an_files)} analysis files")

    # 2. Align by valid_time
    fc_files, an_files = align_fc_an(fc_files, an_files)
    logging.info(f"After alignment: {len(fc_files)} forecasts, {len(an_files)} analyses")

    if not fc_files:
        logging.warning("No aligned files found; exiting")
        return

    # 3. Build weightmap once
    grid_path = fc_files[0][0]
    logging.info("Building weightmap ...")
    geo_agg = wxagg.GeoAggregator(
        shapefile_path=SHAPEFILE_PATH, grid_path=grid_path, silent=not VERBOSE
    )
    _WEIGHTMAP = geo_agg.weightmap

    # 4. Build tagged chunk list (fc + an interleaved)
    fc_chunks = _group_fc_by_lead_month(fc_files)
    an_chunks = _group_an_by_month(an_files)
    all_chunks = [("fc", key, files) for key, files in fc_chunks] + \
                 [("an", key, files) for key, files in an_chunks]
    logging.info("Aggregating %d fc + %d an = %d total chunks ...",
                 len(fc_chunks), len(an_chunks), len(all_chunks))

    # 5. Process all chunks in a shared pool
    df_fc, df_an = _run_all(all_chunks, fc_path, an_path, n_parallel)

    if df_fc is None or df_an is None:
        logging.warning("No data aggregated")
        return

    # 7. Bias and absolute error
    logging.info("Computing bias and absolute error ...")
    df_error = compute_bias(df_fc, df_an, VAR_NAME)
    df_error.to_parquet(error_path)
    logging.info(f"Saved bias table ({len(df_error)} rows) to {error_path}")

    # 8. Anomalies relative to ERA5 climatology
    if os.path.exists(CLIM_PATH):
        logging.info("Computing anomalies ...")
        df_anom = compute_anomalies(df_error, CLIM_PATH, VAR_NAME)
        df_anom.to_parquet(anom_path)
        logging.info(f"Saved anomaly table ({len(df_anom)} rows) to {anom_path}")
    else:
        logging.warning(f"ERA5 climatology not found at {CLIM_PATH}; skipping anomaly computation")


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
