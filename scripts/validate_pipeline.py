"""
scripts/validate_pipeline.py — Pipeline status report.

Reads file paths and parquet metadata without loading full data into memory.
Prints one block per section; exits with code 0 if all OK, 1 if any MISSING.

Usage:
    python scripts/validate_pipeline.py
    python scripts/validate_pipeline.py --skip-download-check
"""

import os
import sys
import argparse

import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    AGGREGATED_DIR,
    ERA5_CLIM_PATH,
    ACS_DIR,
    ACS_YEAR,
    ACS_LEVEL,
    KOPPEN_PATH,
    ERA5_CLIM_START,
    ERA5_CLIM_END,
)

_MARKER = {
    "OK":      "[OK     ]",
    "PARTIAL": "[PARTIAL]",
    "MISSING": "[MISSING]",
    "SKIP":    "[SKIP   ]",
}


def _status(label, status, detail=""):
    marker = _MARKER.get(status, f"[{status}]")
    msg = f"  {marker}  {label}"
    if detail:
        msg += f"\n             {detail}"
    return msg, status


def _parquet_row_count(path):
    """Return total row count from parquet metadata without loading data."""
    try:
        meta = pq.read_metadata(path)
        return sum(meta.row_group(i).num_rows for i in range(meta.num_row_groups))
    except Exception:
        return None


def _parquet_schema_names(path):
    try:
        meta = pq.read_metadata(path)
        return set(meta.schema.to_arrow_schema().names)
    except Exception:
        return None


def _parquet_date_range(path, col):
    """Read a single column to get min/max without loading all columns."""
    try:
        tbl = pq.read_table(path, columns=[col])
        series = tbl.to_pandas()[col]
        return series.min(), series.max()
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def check_era5_monthly():
    start_yr = int(ERA5_CLIM_START[:4])
    end_yr = int(ERA5_CLIM_END[:4])
    monthly_dir = os.path.join(AGGREGATED_DIR, "era5_monthly")
    expected = [(yr, mo) for yr in range(start_yr, end_yr + 1) for mo in range(1, 13)]
    present, missing = [], []
    for yr, mo in expected:
        path = os.path.join(monthly_dir, f"era5_2t_county_{yr}_{mo:02d}.parquet")
        if os.path.exists(path) and _parquet_row_count(path) is not None:
            present.append((yr, mo))
        else:
            missing.append((yr, mo))

    n_exp, n_pres = len(expected), len(present)
    if n_pres == n_exp:
        return _status(f"ERA5 monthly parquets ({n_exp} months)", "OK",
                       f"all {n_exp} months present in {monthly_dir}")
    elif n_pres > 0:
        return _status(f"ERA5 monthly parquets ({n_pres}/{n_exp} months)", "PARTIAL",
                       f"missing {len(missing)} month(s); first: {missing[0][0]}-{missing[0][1]:02d}")
    else:
        return _status("ERA5 monthly parquets", "MISSING", f"none found in {monthly_dir}")


def check_era5_climatology():
    if not os.path.exists(ERA5_CLIM_PATH):
        return _status("ERA5 climatology", "MISSING", ERA5_CLIM_PATH)
    try:
        names = _parquet_schema_names(ERA5_CLIM_PATH)
        required = {"t2m_clim", "geo_id", "day_of_year"}
        if names is None or not required.issubset(names):
            missing_cols = required - (names or set())
            return _status("ERA5 climatology", "PARTIAL", f"missing columns: {missing_cols}")
        mn, mx = _parquet_date_range(ERA5_CLIM_PATH, "day_of_year")
        n_doys = (mx - mn + 1) if (mn is not None and mx is not None) else "?"
        rows = _parquet_row_count(ERA5_CLIM_PATH)
        if mn == 1 and mx == 365:
            return _status("ERA5 climatology", "OK",
                           f"{rows:,} rows | DOY 1–365 — {ERA5_CLIM_PATH}")
        else:
            return _status("ERA5 climatology", "PARTIAL",
                           f"{rows:,} rows | DOY {mn}–{mx} (expected 1–365)")
    except Exception as exc:
        return _status("ERA5 climatology", "MISSING", str(exc))


def check_aggregation_outputs():
    files = {
        "IFS forecast":         "ifs_fc_2t_county.parquet",
        "IFS analysis":         "ifs_an_2t_county.parquet",
        "IFS bias":             "ifs_fc_bias_2t_county.parquet",
        "IFS bias+anom":        "ifs_fc_bias_anom_2t_county.parquet",
        "AIFS forecast":        "aifs_fc_2t_county.parquet",
        "AIFS bias":            "aifs_fc_bias_2t_county.parquet",
        "AIFS bias+anom":       "aifs_fc_bias_anom_2t_county.parquet",
        "AIFS vs IFS":          "aifs_vs_ifs_fc_bias_comparison_2t_county.parquet",
    }
    results = []
    any_missing = False
    for label, fname in files.items():
        path = os.path.join(AGGREGATED_DIR, fname)
        if not os.path.exists(path):
            results.append(_status(label, "MISSING", path))
            any_missing = True
        else:
            rows = _parquet_row_count(path)
            if rows is None:
                results.append(_status(label, "MISSING", f"unreadable: {path}"))
                any_missing = True
            else:
                # Try to get date range from valid_time or time column
                for col in ("valid_time", "time"):
                    names = _parquet_schema_names(path) or set()
                    if col in names:
                        mn, mx = _parquet_date_range(path, col)
                        if mn is not None:
                            results.append(_status(label, "OK", f"{rows:,} rows | {mn} to {mx}"))
                            break
                else:
                    results.append(_status(label, "OK", f"{rows:,} rows"))
    return results, any_missing


def check_koppen():
    out_path = os.path.join(AGGREGATED_DIR, "koppen_geiger_county.parquet")
    tif_ok = os.path.exists(KOPPEN_PATH)
    tif_status = "present" if tif_ok else "MISSING"
    if not os.path.exists(out_path):
        return _status("Koppen-Geiger", "MISSING",
                       f"GeoTIFF {tif_status}: {KOPPEN_PATH} | output not found: {out_path}")
    rows = _parquet_row_count(out_path)
    if rows is None:
        return _status("Koppen-Geiger", "MISSING", f"output unreadable: {out_path}")
    return _status("Koppen-Geiger", "OK",
                   f"GeoTIFF {tif_status} | {rows:,} rows — {out_path}")


def check_acs():
    path = os.path.join(ACS_DIR, f"acs_5yr_{ACS_YEAR}", f"acs_5yr_{ACS_YEAR}_{ACS_LEVEL}.parquet")
    if not os.path.exists(path):
        return _status("ACS data", "MISSING", path)
    rows = _parquet_row_count(path)
    if rows is None:
        return _status("ACS data", "MISSING", f"unreadable: {path}")
    return _status("ACS data", "OK", f"{rows:,} rows — {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate nwp-census-eval pipeline outputs.")
    parser.add_argument(
        "--skip-download-check",
        action="store_true",
        help="Skip the IFS/AIFS raw download tree check (faster).",
    )
    args = parser.parse_args(argv)

    print("\n=== nwp-census-eval pipeline status ===\n")
    any_missing = False

    print("-- ERA5 monthly parquets --")
    msg, status = check_era5_monthly()
    print(msg)
    if status == "MISSING":
        any_missing = True

    print("\n-- ERA5 climatology --")
    msg, status = check_era5_climatology()
    print(msg)
    if status == "MISSING":
        any_missing = True

    print("\n-- Aggregation outputs --")
    results, _missing = check_aggregation_outputs()
    for msg, _ in results:
        print(msg)
    if _missing:
        any_missing = True

    print("\n-- Koppen-Geiger --")
    msg, status = check_koppen()
    print(msg)
    if status == "MISSING":
        any_missing = True

    print("\n-- ACS data --")
    msg, status = check_acs()
    print(msg)
    if status == "MISSING":
        any_missing = True

    print("\n-- IFS/AIFS downloads --")
    if args.skip_download_check:
        print(_status("IFS/AIFS downloads", "SKIP", "--skip-download-check specified")[0])
    else:
        try:
            from download_fc_an_2t import _validate_forecast, _validate_analysis  # noqa: F401
            from nwp_census_eval.data import ECMWFDataClient
            from config import (
                IFS_BASE_DIR, AIFS_BASE_DIR,
                IFS_START, IFS_END, AIFS_START, AIFS_END,
                PARAM, LEAD_TIMES, INIT_HOURS, CONUS_BOUNDS,
                IFS_GRID, AIFS_GRID,
            )
            lead_str = [str(lt) for lt in LEAD_TIMES]
            fc_client = ECMWFDataClient(
                base_dir=IFS_BASE_DIR, param=PARAM, start=IFS_START, end=IFS_END,
                lead_times=lead_str, init_hours=INIT_HOURS, grid=IFS_GRID,
                model="ifs", bounds=CONUS_BOUNDS, max_concurrent_requests=1,
            )
            ok_fc = _validate_forecast(fc_client, "IFS")
            aifs_client = ECMWFDataClient(
                base_dir=AIFS_BASE_DIR, param=PARAM, start=AIFS_START, end=AIFS_END,
                lead_times=lead_str, init_hours=INIT_HOURS, grid=AIFS_GRID,
                model="aifs", bounds=CONUS_BOUNDS, max_concurrent_requests=1,
            )
            ok_aifs = _validate_forecast(aifs_client, "AIFS")
            if ok_fc and ok_aifs:
                print(_status("IFS/AIFS downloads", "OK")[0])
            else:
                print(_status("IFS/AIFS downloads", "PARTIAL", "see above for details")[0])
                any_missing = True
        except Exception as exc:
            print(_status("IFS/AIFS downloads", "MISSING", str(exc))[0])
            any_missing = True

    print("\n" + "=" * 40)
    if any_missing:
        print("Status: INCOMPLETE (some outputs missing)")
        return 1
    print("Status: ALL OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
