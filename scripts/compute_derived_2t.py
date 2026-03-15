"""
Compute bias, anomaly, and model comparison tables from aggregated monthly parquets.

Reads the monthly checkpoint parquets written by aggregate_fc_an_2t.py one month
at a time — never loading more than one month across all three streams simultaneously.
All output parquets are written via streaming ParquetWriter (atomic rename at end).

Outputs (all in AGGREGATED_DIR):
  ifs_fc_bias_2t_county.parquet          — IFS bias and absolute error
  ifs_fc_bias_anom_2t_county.parquet     — IFS bias + anomalies vs ERA5 climatology
  aifs_fc_bias_2t_county.parquet         — AIFS bias and absolute error
  aifs_fc_bias_anom_2t_county.parquet    — AIFS bias + anomalies vs ERA5 climatology
  aifs_vs_ifs_fc_bias_comparison_2t_county.parquet — side-by-side comparison on common dates

Usage:
  python scripts/compute_derived_2t.py
  python scripts/compute_derived_2t.py --skip-aifs
"""

import os
import sys
import re
import glob
import json
import logging
import argparse
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import geopandas as gpd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    SHAPEFILE_PATH,
    AGGREGATED_DIR as SAVE_DIR,
    ERA5_CLIM_PATH as CLIM_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

VAR_NAME = "t2m"

IFS_FC_MONTHLY_DIR  = os.path.join(SAVE_DIR, "ifs_fc_monthly")
IFS_AN_MONTHLY_DIR  = os.path.join(SAVE_DIR, "ifs_an_monthly")
AIFS_FC_MONTHLY_DIR = os.path.join(SAVE_DIR, "aifs_fc_monthly")

# ---------------------------------------------------------------------------
# Table-level metadata (sidecar JSON)
# ---------------------------------------------------------------------------
_BIAS_COLS      = ["geo_id", "valid_time", "init_time", "lead_time",
                   f"{VAR_NAME}_fc", f"{VAR_NAME}_an", "bias", "abs_error", "aland"]
_ANOM_COLS      = _BIAS_COLS + ["day_of_year", f"{VAR_NAME}_clim", "fc_anom", "an_anom"]
_COMPARE_COLS   = ["geo_id", "valid_time", "lead_time",
                   "bias_ifs", "abs_error_ifs", "bias_aifs", "abs_error_aifs",
                   "bias_diff", "abs_error_diff", "aland"]

TABLE_METADATA = {
    "ifs_fc_bias_2t_county": {
        "description": "IFS forecast vs analysis verification at county level.",
        "variables": {
            "geo_id":               {"units": "1",     "long_name": "County FIPS code (string)."},
            "valid_time":           {"units": "UTC",   "long_name": "Forecast valid time."},
            "init_time":            {"units": "UTC",   "long_name": "Forecast initialization time."},
            "lead_time":            {"units": "hours", "long_name": "Forecast lead time."},
            f"{VAR_NAME}_fc":       {"units": "K",     "long_name": "2m temperature (forecast)."},
            f"{VAR_NAME}_an":       {"units": "K",     "long_name": "2m temperature (analysis)."},
            "bias":                 {"units": "K",     "long_name": "Forecast bias (fc minus an)."},
            "abs_error":            {"units": "K",     "long_name": "Absolute forecast error."},
            "aland":                {"units": "km2",   "long_name": "County land area."},
        },
    },
    "ifs_fc_bias_anom_2t_county": {
        "description": "IFS forecast and analysis anomalies relative to ERA5 climatology.",
        "variables": {
            "geo_id":               {"units": "1",     "long_name": "County FIPS code (string)."},
            "valid_time":           {"units": "UTC",   "long_name": "Forecast valid time."},
            "init_time":            {"units": "UTC",   "long_name": "Forecast initialization time."},
            "lead_time":            {"units": "hours", "long_name": "Forecast lead time."},
            f"{VAR_NAME}_fc":       {"units": "K",     "long_name": "2m temperature (forecast)."},
            f"{VAR_NAME}_an":       {"units": "K",     "long_name": "2m temperature (analysis)."},
            "bias":                 {"units": "K",     "long_name": "Forecast bias (fc minus an)."},
            "abs_error":            {"units": "K",     "long_name": "Absolute forecast error."},
            "day_of_year":          {"units": "1",     "long_name": "Day of year for climatology lookup."},
            f"{VAR_NAME}_clim":     {"units": "K",     "long_name": "ERA5 climatology (1991-2020)."},
            "fc_anom":              {"units": "K",     "long_name": "Forecast anomaly vs climatology."},
            "an_anom":              {"units": "K",     "long_name": "Analysis anomaly vs climatology."},
            "aland":                {"units": "km2",   "long_name": "County land area."},
        },
    },
    "aifs_fc_bias_2t_county":      None,   # same structure as ifs; set below
    "aifs_fc_bias_anom_2t_county": None,
    "aifs_vs_ifs_fc_bias_comparison_2t_county": {
        "description": "AIFS vs IFS forecast bias comparison on common dates.",
        "variables": {
            "geo_id":           {"units": "1",     "long_name": "County FIPS code (string)."},
            "valid_time":       {"units": "UTC",   "long_name": "Forecast valid time."},
            "lead_time":        {"units": "hours", "long_name": "Forecast lead time."},
            "bias_ifs":         {"units": "K",     "long_name": "IFS bias (fc minus an)."},
            "abs_error_ifs":    {"units": "K",     "long_name": "IFS absolute error."},
            "bias_aifs":        {"units": "K",     "long_name": "AIFS bias (fc minus an)."},
            "abs_error_aifs":   {"units": "K",     "long_name": "AIFS absolute error."},
            "bias_diff":        {"units": "K",     "long_name": "AIFS bias minus IFS bias."},
            "abs_error_diff":   {"units": "K",     "long_name": "AIFS abs error minus IFS abs error."},
            "aland":            {"units": "km2",   "long_name": "County land area."},
        },
    },
}
# AIFS uses same schema as IFS with AIFS description
TABLE_METADATA["aifs_fc_bias_2t_county"] = dict(
    TABLE_METADATA["ifs_fc_bias_2t_county"],
    description="AIFS forecast vs IFS analysis verification at county level.",
)
TABLE_METADATA["aifs_fc_bias_anom_2t_county"] = dict(
    TABLE_METADATA["ifs_fc_bias_anom_2t_county"],
    description="AIFS forecast and IFS analysis anomalies relative to ERA5 climatology.",
)


def _write_metadata(table_name, path):
    meta = TABLE_METADATA.get(table_name)
    if not meta:
        return
    base, _ = os.path.splitext(path)
    tmp = base + ".meta.json.tmp"
    with open(tmp, "w") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp, base + ".meta.json")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_area_weights(shapefile_path):
    gdf = gpd.read_file(shapefile_path)
    df = gdf[["GEOID", "ALAND"]].rename(columns={"GEOID": "geo_id", "ALAND": "aland"})
    df["aland"] = df["aland"].astype(float) / 1e6
    return df[["geo_id", "aland"]]


def _is_valid_parquet(path):
    if not os.path.exists(path):
        return False
    try:
        pq.read_metadata(path)
        return True
    except Exception:
        return False


def _an_months(an_monthly_dir):
    """Return sorted list of (yr, mo) for which IFS an monthly parquets exist."""
    months = []
    for path in glob.glob(os.path.join(an_monthly_dir, "*.parquet")):
        m = re.search(r"_(\d{4})_(\d{2})\.parquet$", os.path.basename(path))
        if m:
            months.append((int(m.group(1)), int(m.group(2))))
    return sorted(months)


def _load_fc_for_an_month(fc_monthly_dir, stem, yr, mo):
    """Load FC rows whose valid_time falls in (yr, mo).

    FC parquets are grouped by INIT-time month, so long-lead forecasts from the
    previous init month may have valid_times in the current month (spillover up
    to max_lead=240h = 10 days). We load both the current and previous init
    month's parquets and filter to valid_times in [month_start, month_end).
    """
    month_start = pd.Timestamp(yr, mo, 1)
    month_end   = pd.Timestamp(yr + 1, 1, 1) if mo == 12 else pd.Timestamp(yr, mo + 1, 1)

    prev_yr, prev_mo = (yr - 1, 12) if mo == 1 else (yr, mo - 1)

    frames = []
    for y, m in [(prev_yr, prev_mo), (yr, mo)]:
        for p in glob.glob(os.path.join(fc_monthly_dir, f"{stem}_{y}_{m:02d}_lead*.parquet")):
            frames.append(pd.read_parquet(p))

    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    vt = pd.to_datetime(df["valid_time"])
    df = df[(vt >= month_start) & (vt < month_end)]
    return df if not df.empty else None


def _load_an_month(an_monthly_dir, yr, mo):
    path = os.path.join(an_monthly_dir, f"ifs_an_2t_county_{yr}_{mo:02d}.parquet")
    return pd.read_parquet(path) if os.path.exists(path) else None


# ---------------------------------------------------------------------------
# Core derivation (one month at a time)
# ---------------------------------------------------------------------------
def _derive_month(df_fc, df_an, clim, area_weights, var_name):
    """Given fc and an DataFrames for one month, return (df_bias, df_anom).

    df_an must have column 'time'; we rename to 'valid_time' for the merge.
    Returns (None, None) if the join yields no rows.
    """
    df_an = df_an.rename(columns={"time": "valid_time"})

    # Bias — merge produces t2m_x / t2m_y when both frames share the column name
    df = pd.merge(
        df_fc, df_an,
        on=["geo_id", "valid_time"],
        how="inner",
    )
    df = df.rename(columns={
        var_name + "_x": f"{var_name}_fc",
        var_name + "_y": f"{var_name}_an",
    })
    if f"{var_name}_fc" not in df.columns:
        return None, None

    df["bias"]      = df[f"{var_name}_fc"] - df[f"{var_name}_an"]
    df["abs_error"] = df["bias"].abs()
    df = df.merge(area_weights, on="geo_id", how="left")

    keep_bias = ["geo_id", "valid_time", "init_time", "lead_time",
                 f"{var_name}_fc", f"{var_name}_an", "bias", "abs_error", "aland"]
    df_bias = df[[c for c in keep_bias if c in df.columns]]

    # Anomaly
    if clim is None:
        return df_bias, None
    df_a = df_bias.copy()
    _dt = pd.to_datetime(df_a["valid_time"])
    df_a["day_of_year"] = (_dt.dt.dayofyear
                           - (_dt.dt.is_leap_year & (_dt.dt.month > 2)).astype(int))
    df_a = df_a.merge(clim[["geo_id", "day_of_year", f"{var_name}_clim"]],
                      on=["geo_id", "day_of_year"], how="inner")
    df_a["fc_anom"] = df_a[f"{var_name}_fc"] - df_a[f"{var_name}_clim"]
    df_a["an_anom"] = df_a[f"{var_name}_an"] - df_a[f"{var_name}_clim"]

    keep_anom = keep_bias + ["day_of_year", f"{var_name}_clim", "fc_anom", "an_anom"]
    df_anom = df_a[[c for c in keep_anom if c in df_a.columns]]

    return df_bias, df_anom


# ---------------------------------------------------------------------------
# Streaming writers
# ---------------------------------------------------------------------------
class _StreamingWriter:
    """Write rows month-by-month via ParquetWriter; atomic rename at close."""
    def __init__(self, path):
        self.path = path
        self.tmp  = path + ".tmp"
        self._writer = None
        self._schema = None
        self.total   = 0

    def write(self, df):
        if df is None or df.empty:
            return
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self._writer is None:
            self._schema = table.schema
            self._writer = pq.ParquetWriter(self.tmp, self._schema)
        else:
            table = table.cast(self._schema)
        self._writer.write_table(table)
        self.total += len(df)

    def close(self, table_name=None):
        if self._writer is None:
            logging.warning("No data written for %s; output not created.", self.path)
            return
        self._writer.close()
        os.replace(self.tmp, self.path)
        logging.info("Saved %s (%d rows) → %s", table_name or "", self.total, self.path)
        if table_name:
            _write_metadata(table_name, self.path)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self._writer is not None:
            self._writer.close()
            if os.path.exists(self.tmp):
                os.remove(self.tmp)


# ---------------------------------------------------------------------------
# Main computation passes
# ---------------------------------------------------------------------------
def compute_ifs(area_weights, clim, bias_path, anom_path, force=False):
    """Compute IFS bias and anomaly, streaming month-by-month."""
    if not force and _is_valid_parquet(bias_path) and _is_valid_parquet(anom_path):
        logging.info("IFS derived outputs already exist; skipping (use --force to recompute).")
        return

    months = _an_months(IFS_AN_MONTHLY_DIR)
    if not months:
        logging.warning("No IFS an monthly parquets found; cannot compute IFS derived outputs.")
        return

    logging.info("Computing IFS bias and anomaly for %d months ...", len(months))
    t0 = time.time()

    bias_w = _StreamingWriter(bias_path)
    anom_w = _StreamingWriter(anom_path)
    try:
        for yr, mo in months:
            df_fc = _load_fc_for_an_month(IFS_FC_MONTHLY_DIR, "ifs_fc_2t_county", yr, mo)
            df_an = _load_an_month(IFS_AN_MONTHLY_DIR, yr, mo)
            if df_fc is None or df_an is None:
                logging.warning("Missing IFS fc or an for %d-%02d; skipping.", yr, mo)
                continue
            df_bias, df_anom = _derive_month(df_fc, df_an, clim, area_weights, VAR_NAME)
            bias_w.write(df_bias)
            anom_w.write(df_anom)
    except Exception:
        # Clean up incomplete tmp files
        for w in (bias_w, anom_w):
            if w._writer:
                w._writer.close()
            if os.path.exists(w.tmp):
                os.remove(w.tmp)
        raise

    bias_w.close("ifs_fc_bias_2t_county")
    anom_w.close("ifs_fc_bias_anom_2t_county")
    logging.info("[%.0fs] IFS derived complete.", time.time() - t0)


def compute_aifs(area_weights, clim, bias_path, anom_path, force=False):
    """Compute AIFS bias and anomaly (using IFS an as reference), streaming month-by-month."""
    if not force and _is_valid_parquet(bias_path) and _is_valid_parquet(anom_path):
        logging.info("AIFS derived outputs already exist; skipping (use --force to recompute).")
        return

    months = _an_months(IFS_AN_MONTHLY_DIR)  # align to IFS an coverage
    if not months:
        logging.warning("No IFS an monthly parquets found; cannot compute AIFS derived outputs.")
        return

    logging.info("Computing AIFS bias and anomaly for %d months ...", len(months))
    t0 = time.time()

    bias_w = _StreamingWriter(bias_path)
    anom_w = _StreamingWriter(anom_path)
    try:
        for yr, mo in months:
            df_fc = _load_fc_for_an_month(AIFS_FC_MONTHLY_DIR, "aifs_fc_2t_county", yr, mo)
            df_an = _load_an_month(IFS_AN_MONTHLY_DIR, yr, mo)
            if df_fc is None or df_an is None:
                continue
            df_bias, df_anom = _derive_month(df_fc, df_an, clim, area_weights, VAR_NAME)
            bias_w.write(df_bias)
            anom_w.write(df_anom)
    except Exception:
        for w in (bias_w, anom_w):
            if w._writer:
                w._writer.close()
            if os.path.exists(w.tmp):
                os.remove(w.tmp)
        raise

    bias_w.close("aifs_fc_bias_2t_county")
    anom_w.close("aifs_fc_bias_anom_2t_county")
    logging.info("[%.0fs] AIFS derived complete.", time.time() - t0)


def compute_comparison(ifs_bias_path, aifs_bias_path, area_weights, compare_path, force=False):
    """Build month-by-month AIFS vs IFS comparison on common (geo_id, valid_time, lead_time).

    Derives bias inline from monthly fc/an checkpoint parquets — peak memory is one
    month across all lead times (~tens of MB). ifs_bias_path and aifs_bias_path are
    kept for backward compatibility but are not read.
    """
    if not force and _is_valid_parquet(compare_path):
        logging.info("Comparison table already exists; skipping (use --force to recompute).")
        return

    months = _an_months(IFS_AN_MONTHLY_DIR)
    if not months:
        logging.warning("No IFS an monthly parquets found; skipping comparison.")
        return

    logging.info("Building AIFS vs IFS comparison table for %d months ...", len(months))
    t0 = time.time()

    comp_w = _StreamingWriter(compare_path)
    try:
        for yr, mo in months:
            df_an      = _load_an_month(IFS_AN_MONTHLY_DIR,  yr, mo)
            df_ifs_fc  = _load_fc_for_an_month(IFS_FC_MONTHLY_DIR,  "ifs_fc_2t_county",  yr, mo)
            df_aifs_fc = _load_fc_for_an_month(AIFS_FC_MONTHLY_DIR, "aifs_fc_2t_county", yr, mo)
            if df_an is None or df_ifs_fc is None or df_aifs_fc is None:
                continue

            df_an_vt = df_an.rename(columns={"time": "valid_time"})

            # IFS bias
            df_ifs = pd.merge(df_ifs_fc, df_an_vt, on=["geo_id", "valid_time"], how="inner")
            df_ifs = df_ifs.rename(columns={VAR_NAME + "_x": f"{VAR_NAME}_fc",
                                            VAR_NAME + "_y": f"{VAR_NAME}_an"})
            if f"{VAR_NAME}_fc" not in df_ifs.columns:
                continue
            df_ifs["bias_ifs"]      = df_ifs[f"{VAR_NAME}_fc"] - df_ifs[f"{VAR_NAME}_an"]
            df_ifs["abs_error_ifs"] = df_ifs["bias_ifs"].abs()
            df_ifs = df_ifs[["geo_id", "valid_time", "lead_time", "bias_ifs", "abs_error_ifs"]]

            # AIFS bias
            df_aifs = pd.merge(df_aifs_fc, df_an_vt, on=["geo_id", "valid_time"], how="inner")
            df_aifs = df_aifs.rename(columns={VAR_NAME + "_x": f"{VAR_NAME}_fc",
                                              VAR_NAME + "_y": f"{VAR_NAME}_an"})
            if f"{VAR_NAME}_fc" not in df_aifs.columns:
                continue
            df_aifs["bias_aifs"]      = df_aifs[f"{VAR_NAME}_fc"] - df_aifs[f"{VAR_NAME}_an"]
            df_aifs["abs_error_aifs"] = df_aifs["bias_aifs"].abs()
            df_aifs = df_aifs[["geo_id", "valid_time", "lead_time", "bias_aifs", "abs_error_aifs"]]

            # Merge on common keys and compute diff columns
            key = ["geo_id", "valid_time", "lead_time"]
            df = pd.merge(df_ifs, df_aifs, on=key, how="inner")
            df["bias_diff"]      = df["bias_aifs"]      - df["bias_ifs"]
            df["abs_error_diff"] = df["abs_error_aifs"] - df["abs_error_ifs"]
            df = df.merge(area_weights, on="geo_id", how="left")

            keep = [c for c in _COMPARE_COLS if c in df.columns]
            comp_w.write(df[keep])
    except Exception:
        if comp_w._writer:
            comp_w._writer.close()
        if os.path.exists(comp_w.tmp):
            os.remove(comp_w.tmp)
        raise

    comp_w.close("aifs_vs_ifs_fc_bias_comparison_2t_county")
    logging.info("[%.0fs] Comparison table complete.", time.time() - t0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(skip_aifs=False, force=False):
    t_start = time.time()
    logging.info("=== compute_derived_2t.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S"))

    # Output paths
    ifs_bias_path    = os.path.join(SAVE_DIR, "ifs_fc_bias_2t_county.parquet")
    ifs_anom_path    = os.path.join(SAVE_DIR, "ifs_fc_bias_anom_2t_county.parquet")
    aifs_bias_path   = os.path.join(SAVE_DIR, "aifs_fc_bias_2t_county.parquet")
    aifs_anom_path   = os.path.join(SAVE_DIR, "aifs_fc_bias_anom_2t_county.parquet")
    compare_path     = os.path.join(SAVE_DIR, "aifs_vs_ifs_fc_bias_comparison_2t_county.parquet")

    # Load shared inputs once
    logging.info("Loading area weights from shapefile ...")
    area_weights = _load_area_weights(SHAPEFILE_PATH)

    clim = None
    if os.path.exists(CLIM_PATH):
        logging.info("Loading ERA5 climatology from %s ...", CLIM_PATH)
        clim = pd.read_parquet(CLIM_PATH)
    else:
        logging.warning("ERA5 climatology not found at %s; anomalies will not be computed.", CLIM_PATH)

    # Pass 1: IFS
    compute_ifs(area_weights, clim, ifs_bias_path, ifs_anom_path, force=force)

    # Pass 2: AIFS
    if not skip_aifs:
        compute_aifs(area_weights, clim, aifs_bias_path, aifs_anom_path, force=force)

    # Pass 3: Comparison (reads the bias outputs written above)
    if not skip_aifs:
        compute_comparison(ifs_bias_path, aifs_bias_path, area_weights, compare_path, force=force)

    logging.info("[%.0fs] Done.", time.time() - t_start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute bias, anomaly, and comparison tables from aggregated monthly parquets."
    )
    parser.add_argument(
        "--skip-aifs", action="store_true",
        help="Skip AIFS bias/anomaly and comparison computation.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Recompute outputs even if they already exist.",
    )
    args = parser.parse_args()
    main(skip_aifs=args.skip_aifs, force=args.force)
