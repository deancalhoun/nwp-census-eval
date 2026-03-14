"""
scripts/validate_pipeline.py — Pipeline status report.

Reads file paths and parquet metadata without loading full data into memory.
Prints one block per section; exits with code 0 if all OK, 1 if any MISSING.

Usage:
    python scripts/validate_pipeline.py                  # fast default check
    python scripts/validate_pipeline.py --check-downloads --nc-sweep
    python scripts/validate_pipeline.py --fix-grib --remove-corrupt
"""

import os
import sys
import argparse
import glob
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    IFS_FC_DIR,
    IFS_AN_DIR,
    AIFS_FC_DIR,
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


def _check_monthly_dir(label, monthly_dir):
    """Check a monthly-checkpoint directory; report parquet count."""
    if not os.path.isdir(monthly_dir):
        return _status(label, "MISSING", f"directory not found: {monthly_dir}")
    parquets = glob.glob(os.path.join(monthly_dir, "*.parquet"))
    n = len(parquets)
    if n == 0:
        return _status(label, "MISSING", f"no parquets in {monthly_dir}")
    readable = sum(1 for p in parquets if _parquet_row_count(p) is not None)
    if readable < n:
        return _status(label, "PARTIAL",
                       f"{readable}/{n} parquets readable in {monthly_dir}")
    return _status(label, "OK", f"{n:,} monthly parquets — {monthly_dir}")


def check_aggregation_outputs():
    results = []
    any_missing = False

    # Raw aggregation outputs (monthly checkpoint parquets)
    for label, subdir in [
        ("IFS forecast (monthly)", "ifs_fc_monthly"),
        ("IFS analysis (monthly)", "ifs_an_monthly"),
        ("AIFS forecast (monthly)", "aifs_fc_monthly"),
    ]:
        msg, status = _check_monthly_dir(label, os.path.join(AGGREGATED_DIR, subdir))
        results.append((msg, status))
        if status == "MISSING":
            any_missing = True

    # Derived outputs (single parquets written by compute_derived_2t.py)
    derived = {
        "IFS bias":     "ifs_fc_bias_2t_county.parquet",
        "IFS bias+anom": "ifs_fc_bias_anom_2t_county.parquet",
        "AIFS bias":    "aifs_fc_bias_2t_county.parquet",
        "AIFS bias+anom": "aifs_fc_bias_anom_2t_county.parquet",
        "AIFS vs IFS":  "aifs_vs_ifs_fc_bias_comparison_2t_county.parquet",
    }
    for label, fname in derived.items():
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
# GRIB-format .nc file sweep
# ---------------------------------------------------------------------------
_GRIB_MAGIC  = b"GRIB"
_NC3_MAGIC   = (b"CDF\x01", b"CDF\x02")
_HDF5_MAGIC  = b"\x89HDF"
_SWEEP_WORKERS = 32  # I/O-bound; more threads = faster on parallel filesystems


def _classify_file(path):
    """Return 'ok', 'grib', or 'corrupt' based on the file's magic bytes."""
    try:
        with open(path, "rb") as f:
            magic = f.read(4)
        if magic == _GRIB_MAGIC:
            return "grib"
        if magic[:3] == b"CDF" or magic == _HDF5_MAGIC:
            return "ok"
        return "corrupt"
    except OSError:
        return "corrupt"


def _scan_dir_parallel(directory):
    """Return (n_total, grib_files, corrupt_files) for a directory tree."""
    nc_files = glob.glob(os.path.join(directory, "**", "*.nc"), recursive=True)
    if not nc_files:
        return 0, [], []

    try:
        from tqdm import tqdm
        bar = tqdm(total=len(nc_files), desc=f"  scanning {os.path.basename(directory)}",
                   unit="file", leave=False)
    except ImportError:
        bar = None

    grib_files, corrupt_files = [], []
    with ThreadPoolExecutor(max_workers=_SWEEP_WORKERS) as pool:
        futures = {pool.submit(_classify_file, p): p for p in nc_files}
        for fut in as_completed(futures):
            kind = fut.result()
            if kind == "grib":
                grib_files.append(futures[fut])
            elif kind == "corrupt":
                corrupt_files.append(futures[fut])
            if bar:
                bar.update(1)
    if bar:
        bar.close()
    return len(nc_files), grib_files, corrupt_files


def check_grib_nc_files(fix=False, remove_corrupt=False):
    """Scan all .nc files in the IFS/AIFS directories for format problems.

    Reads only the first 4 bytes of each file (magic bytes) using 32 threads
    in parallel. Detects both GRIB-format files (unconverted) and corrupt files
    (neither NetCDF3/4 nor GRIB — e.g. truncated downloads).
    If fix=True, runs grib_to_netcdf in-place on GRIB files.
    If remove_corrupt=True, deletes corrupt files so they can be re-downloaded.
    """
    dirs = {
        "IFS fc":  IFS_FC_DIR,
        "IFS an":  IFS_AN_DIR,
        "AIFS fc": AIFS_FC_DIR,
    }
    results = []
    for label, directory in dirs.items():
        if not os.path.isdir(directory):
            results.append(_status(f"{label} file sweep", "SKIP",
                                   f"directory not found: {directory}"))
            continue
        n_nc, grib_files, corrupt_files = _scan_dir_parallel(directory)
        n_grib, n_corrupt = len(grib_files), len(corrupt_files)

        if n_grib == 0 and n_corrupt == 0:
            results.append(_status(f"{label} file sweep", "OK",
                                   f"{n_nc:,} .nc files — all valid NetCDF"))
            continue

        parts = []
        if n_grib:
            pct = 100 * n_grib / n_nc
            parts.append(f"{n_grib:,} GRIB ({pct:.1f}%)")
        if n_corrupt:
            pct = 100 * n_corrupt / n_nc
            parts.append(f"{n_corrupt:,} corrupt ({pct:.1f}%)")
        detail = f"{n_nc:,} files checked | bad: {', '.join(parts)}"

        if fix and grib_files:
            n_fixed, n_failed = 0, 0
            for path in sorted(grib_files):
                tmp = path + ".converting"
                r = subprocess.run(["grib_to_netcdf", "-o", tmp, path],
                                   capture_output=True)
                if r.returncode == 0 and os.path.exists(tmp):
                    os.replace(tmp, path)
                    n_fixed += 1
                else:
                    n_failed += 1
                    if os.path.exists(tmp):
                        os.remove(tmp)
            detail += f" | GRIB fixed {n_fixed:,}, failed {n_failed:,}"

        if corrupt_files and remove_corrupt:
            for path in corrupt_files:
                os.remove(path)
            detail += f" | corrupt removed {n_corrupt:,}"
            n_corrupt = 0  # cleared

        if n_corrupt or (n_grib and not fix):
            hints = []
            if n_grib and not fix:
                hints.append("GRIB: re-run with --fix-grib")
            if n_corrupt:
                hints.append("corrupt: re-run with --remove-corrupt")
            detail += " — " + "; ".join(hints)
        status = "PARTIAL" if (n_corrupt or (n_grib and not fix)) else "OK"
        results.append(_status(f"{label} file sweep", status, detail))
    return results


# ---------------------------------------------------------------------------
# NetCDF content check (xarray open + variable/grid sanity)
# ---------------------------------------------------------------------------
def _check_nc_content(path, expected_var):
    """
    Try to open a NetCDF with xarray and confirm expected_var is present and
    has lat/lon (or latitude/longitude) dimensions. Returns (ok, reason).
    """
    try:
        import xarray as xr
    except ImportError:
        return None, "xarray not available"
    try:
        with xr.open_dataset(path) as ds:
            vars_present = set(ds.data_vars) | set(ds.coords)
            if expected_var not in vars_present:
                return False, f"variable '{expected_var}' not found (have: {sorted(vars_present)})"
            dims = set(ds.dims)
            spatial = dims & {"lat", "lon", "latitude", "longitude", "x", "y"}
            if len(spatial) < 2:
                return False, f"fewer than 2 spatial dims (dims: {sorted(dims)})"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    return True, ""


def check_nc_content(max_failures_shown=20):
    """
    Open every .nc file in each download directory with xarray and verify the
    expected variable ('t2m') and spatial dimensions are present. Uses
    _SWEEP_WORKERS threads in parallel (same as the magic-byte sweep).
    Reports total failure count and up to max_failures_shown example paths.
    """
    dirs = {
        "IFS fc":  (IFS_FC_DIR,  "t2m"),
        "IFS an":  (IFS_AN_DIR,  "t2m"),
        "AIFS fc": (AIFS_FC_DIR, "t2m"),
    }
    results = []
    for label, (directory, expected_var) in dirs.items():
        if not os.path.isdir(directory):
            results.append(_status(f"{label} content check", "SKIP",
                                   f"directory not found: {directory}"))
            continue
        nc_files = glob.glob(os.path.join(directory, "**", "*.nc"), recursive=True)
        if not nc_files:
            results.append(_status(f"{label} content check", "SKIP", "no .nc files found"))
            continue

        try:
            from tqdm import tqdm
            bar = tqdm(total=len(nc_files),
                       desc=f"  content {os.path.basename(directory)}",
                       unit="file", leave=False)
        except ImportError:
            bar = None

        failures = []  # list of (path, reason)
        with ThreadPoolExecutor(max_workers=_SWEEP_WORKERS) as pool:
            futures = {pool.submit(_check_nc_content, p, expected_var): p for p in nc_files}
            for fut in as_completed(futures):
                ok, reason = fut.result()
                if ok is False:
                    failures.append((futures[fut], reason))
                if bar:
                    bar.update(1)
        if bar:
            bar.close()

        n_total, n_fail = len(nc_files), len(failures)
        if n_fail == 0:
            results.append(_status(
                f"{label} content check ({n_total:,} files)", "OK",
                f"variable '{expected_var}' present with spatial dims in all files",
            ))
        else:
            shown = failures[:max_failures_shown]
            detail = f"{n_fail:,}/{n_total:,} files failed"
            if n_fail > max_failures_shown:
                detail += f" (showing first {max_failures_shown})"
            detail += ": " + "; ".join(
                f"{os.path.basename(p)}: {r}" for p, r in shown
            )
            results.append(_status(f"{label} content check ({n_total:,} files)", "PARTIAL", detail))
    return results


# ---------------------------------------------------------------------------
# Download directory check
# ---------------------------------------------------------------------------
def check_downloads():
    """Check IFS/AIFS download directories by counting files at each level."""
    from config import IFS_FC_DIR, IFS_AN_DIR, AIFS_FC_DIR

    def _count_nc(directory):
        """Count .nc files one level deep (init_hour/lead or year/month subdirs)."""
        n = 0
        try:
            for root, dirs, files in os.walk(directory):
                n += sum(1 for f in files if f.endswith(".nc"))
        except OSError:
            return None
        return n

    results = []
    for label, directory in [
        ("IFS fc",  IFS_FC_DIR),
        ("IFS an",  IFS_AN_DIR),
        ("AIFS fc", AIFS_FC_DIR),
    ]:
        if not os.path.isdir(directory):
            results.append(_status(f"{label} downloads", "MISSING",
                                   f"directory not found: {directory}"))
            continue
        n = _count_nc(directory)
        if n is None:
            results.append(_status(f"{label} downloads", "MISSING",
                                   f"unreadable: {directory}"))
        elif n == 0:
            results.append(_status(f"{label} downloads", "MISSING",
                                   f"no .nc files in {directory}"))
        else:
            results.append(_status(f"{label} downloads", "OK",
                                   f"{n:,} .nc files — {directory}"))
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description="Validate nwp-census-eval pipeline outputs.")
    parser.add_argument(
        "--check-downloads",
        action="store_true",
        help="Check IFS/AIFS download directories (skipped by default; slow on large trees).",
    )
    parser.add_argument(
        "--nc-sweep",
        action="store_true",
        help="Scan .nc files for GRIB/corrupt content (skipped by default; slow on 100k+ files).",
    )
    parser.add_argument(
        "--fix-grib",
        action="store_true",
        help="Convert GRIB-format .nc files in place (implies --nc-sweep).",
    )
    parser.add_argument(
        "--remove-corrupt",
        action="store_true",
        help="Delete corrupt .nc files so they can be re-downloaded (implies --nc-sweep).",
    )
    parser.add_argument(
        "--content-check",
        action="store_true",
        help="Open every .nc file with xarray and verify variable/grid content (parallel, slow on large trees).",
    )
    args = parser.parse_args(argv)
    run_nc_sweep = args.nc_sweep or args.fix_grib or args.remove_corrupt

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

    print("\n-- .nc file format sweep (magic bytes) --")
    if run_nc_sweep:
        for msg, _ in check_grib_nc_files(fix=args.fix_grib, remove_corrupt=args.remove_corrupt):
            print(msg)
    else:
        print(_status(".nc file sweep", "SKIP", "pass --nc-sweep to enable")[0])

    print("\n-- .nc file content check (xarray variable/grid, all files) --")
    if args.content_check:
        for msg, _ in check_nc_content():
            print(msg)
    else:
        print(_status(".nc content check", "SKIP", "pass --content-check to enable")[0])

    print("\n-- IFS/AIFS downloads --")
    if args.check_downloads:
        for msg, status in check_downloads():
            print(msg)
            if status == "MISSING":
                any_missing = True
    else:
        print(_status("IFS/AIFS downloads", "SKIP", "pass --check-downloads to enable")[0])

    print("\n" + "=" * 40)
    if any_missing:
        print("Status: INCOMPLETE (some outputs missing)")
        return 1
    print("Status: ALL OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
