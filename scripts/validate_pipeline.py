"""
scripts/validate_pipeline.py — Comprehensive pipeline validation.

Sequential, always-full validation flow:
  Phase 1:  Source NC files (file counts + xarray content check, parallel)
  Phase 2a: ERA5 aggregation (monthly parquets + climatology)
  Phase 2b: IFS/AIFS aggregation (checkpoint completeness)
  Phase 3:  Derived outputs (informational only)

Usage:
    python scripts/validate_pipeline.py            # report only
    python scripts/validate_pipeline.py --cleanup  # report + remove flagged artifacts
"""

import calendar
import os
import sys
import argparse
import glob
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

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
    ERA5_DIR,
    IFS_FC_DIR,
    IFS_AN_DIR,
    AIFS_FC_DIR,
    IFS_START,
    IFS_END,
    AIFS_START,
    AIFS_END,
    LEAD_TIMES,
    INIT_HOURS,
)

_SWEEP_WORKERS = 32
_MAX_DETAIL_LINES = 20


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _parquet_row_count(path):
    """Metadata-only row count; returns None on error."""
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


def _parquet_unique_count(path, col):
    """Read one column and return nunique; 0 on error."""
    try:
        tbl = pq.read_table(path, columns=[col])
        return tbl[col].to_pandas().nunique()
    except Exception:
        return 0



def _iter_year_months(start_str, end_str):
    """Yield (year, month, first_day, last_day) for every month in [start, end]."""
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end   = datetime.strptime(end_str,   "%Y-%m-%d").date()
    yr, mo = start.year, start.month
    while (yr, mo) <= (end.year, end.month):
        days = calendar.monthrange(yr, mo)[1]
        first = max(start, date(yr, mo, 1))
        last  = min(end,   date(yr, mo, days))
        yield yr, mo, first, last
        mo += 1
        if mo > 12:
            mo = 1
            yr += 1


def _count_nc_in_dir(directory):
    """Count .nc files directly in a leaf directory (no recursion)."""
    try:
        return sum(1 for f in os.listdir(directory) if f.endswith(".nc"))
    except OSError:
        return 0


_GRIB_MAGIC = b"GRIB"
_HDF5_MAGIC = b"\x89HDF"
_MIN_NC_BYTES = 10_000  # real CONUS NetCDF files are much larger; flag anything smaller


def _magic_ok(path):
    """Return True if the file's magic bytes indicate NetCDF (NC3 or HDF5/NC4)."""
    try:
        with open(path, "rb") as f:
            magic = f.read(4)
        return magic[:3] == b"CDF" or magic == _HDF5_MAGIC
    except OSError:
        return False


def _size_and_magic_ok(path):
    """
    Thread-safe pre-filter: check file size and magic bytes only.
    Returns False if the file is too small or not NetCDF format.
    """
    try:
        if os.path.getsize(path) < _MIN_NC_BYTES:
            return False
    except OSError:
        return False
    return _magic_ok(path)


def _content_ok(path, expected_var):
    """
    Lazy xarray open to check that expected_var is present and has ≥2 spatial dims.
    Catches GRIB, corrupt, wrong variable, and truncated files.
    Returns (ok: bool, reason: str).
    """
    try:
        import xarray as xr
    except ImportError:
        return True, ""  # can't check; skip
    try:
        with xr.open_dataset(path, engine="netcdf4") as ds:
            vars_present = set(ds.data_vars) | set(ds.coords)
            if expected_var not in vars_present:
                shown = sorted(vars_present)[:5]
                return False, f"var '{expected_var}' missing (have: {shown})"
            dims = set(ds.dims)
            spatial = dims & {"lat", "lon", "latitude", "longitude", "x", "y"}
            if len(spatial) < 2:
                return False, f"<2 spatial dims ({sorted(dims)})"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    return True, ""


# ---------------------------------------------------------------------------
# Phase 1: Source NC files
# ---------------------------------------------------------------------------
# SourceStatus shape:
#   dict[stream_tag, dict[dir_key, {dir_exists, actual, expected, bad: [paths]}]]
#
# stream tags:  'ifs_fc', 'ifs_an', 'aifs_fc', 'era5'
# dir keys:
#   ifs_fc / aifs_fc : (init_hour, lead, yr, mo)
#   ifs_an / era5    : (yr, mo)


def _scan_leaf_dir(directory, expected):
    """
    Thread-safe pass: enumerate .nc files, check size and magic bytes.
    Returns {dir_exists, actual, expected, nc_files, suspect} where
    suspect = files that failed the size or magic-bytes check.
    No HDF5/xarray calls — safe for concurrent threads.
    """
    if not os.path.isdir(directory):
        return {"dir_exists": False, "actual": 0, "expected": expected,
                "nc_files": [], "suspect": []}
    nc_files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".nc")
    ]
    suspect = [p for p in nc_files if not _size_and_magic_ok(p)]
    return {"dir_exists": True, "actual": len(nc_files), "expected": expected,
            "nc_files": nc_files, "suspect": suspect}


def check_source_files():
    """
    Phase 1: Two-pass source-file check.

    Pass 1 (parallel, thread-safe): directory listing + file-count + size/magic-bytes.
    Pass 2 (sequential): xarray variable+coord check on files that actually need it:
        - complete dirs  → only files that failed size/magic (usually zero)
        - incomplete dirs → all files present (could be wrong-variable partial downloads)

    Keeps HDF5/xarray calls out of the thread pool, which avoids segfaults from
    HDF5's non-thread-safe global state on some builds.
    """
    source_status = {"ifs_fc": {}, "ifs_an": {}, "aifs_fc": {}, "era5": {}}
    tasks = []  # (stream, key, directory, expected)

    # IFS FC: {IFS_FC_DIR}/{init_hour}/{lead}/{year}/{month:02d}/
    for init_hour in INIT_HOURS:
        for lead in LEAD_TIMES:
            for yr, mo, first, last in _iter_year_months(IFS_START, IFS_END):
                d = os.path.join(IFS_FC_DIR, init_hour, str(lead), str(yr), f"{mo:02d}")
                tasks.append(("ifs_fc", (init_hour, lead, yr, mo), d, (last - first).days + 1))

    # AIFS FC: {AIFS_FC_DIR}/{init_hour}/{lead}/{year}/{month:02d}/
    for init_hour in INIT_HOURS:
        for lead in LEAD_TIMES:
            for yr, mo, first, last in _iter_year_months(AIFS_START, AIFS_END):
                d = os.path.join(AIFS_FC_DIR, init_hour, str(lead), str(yr), f"{mo:02d}")
                tasks.append(("aifs_fc", (init_hour, lead, yr, mo), d, (last - first).days + 1))

    # IFS AN: {IFS_AN_DIR}/{year}/{month:02d}/
    # Scan through fc_end + max(LEAD_TIMES) so every FC valid_time has a
    # matching analysis file on disk (including spillover into the next year).
    _an_end_dt = datetime.strptime(IFS_END, "%Y-%m-%d") + timedelta(hours=12 + max(LEAD_TIMES))
    _an_end_str = f"{_an_end_dt.year}-{_an_end_dt.month:02d}-{_an_end_dt.day:02d}"
    for yr, mo, first, last in _iter_year_months(IFS_START, _an_end_str):
        d = os.path.join(IFS_AN_DIR, str(yr), f"{mo:02d}")
        tasks.append(("ifs_an", (yr, mo), d, (last - first).days + 1))

    # ERA5: {ERA5_DIR}/{YYYYMM}/
    # ERA5 is one monthly file per directory (e.g. e5.oper.an.sfc.*.1991010100_1991013123.nc),
    # not one file per day like IFS/AIFS.
    start_yr = int(ERA5_CLIM_START[:4])
    end_yr   = int(ERA5_CLIM_END[:4])
    for yr in range(start_yr, end_yr + 1):
        for mo in range(1, 13):
            d = os.path.join(ERA5_DIR, f"{yr}{mo:02d}")
            tasks.append(("era5", (yr, mo), d, 1))

    # --- Pass 1: parallel directory scan (thread-safe I/O only) ---
    try:
        from tqdm import tqdm
        bar1 = tqdm(total=len(tasks), desc="Phase 1: scanning dirs", unit="dir", leave=False)
    except ImportError:
        bar1 = None

    raw = {}  # (stream, key) -> scan result
    def _scan(task):
        stream, key, directory, expected = task
        return stream, key, _scan_leaf_dir(directory, expected)

    with ThreadPoolExecutor(max_workers=_SWEEP_WORKERS) as pool:
        futures = {pool.submit(_scan, t): t for t in tasks}
        for fut in as_completed(futures):
            stream, key, result = fut.result()
            raw[(stream, key)] = result
            if bar1:
                bar1.update(1)
    if bar1:
        bar1.close()

    # --- Pass 2: sequential content check (xarray/HDF5, not thread-safe) ---
    # Complete dirs: only the suspect files (failed size/magic) — usually zero.
    # Incomplete dirs: all present files (might be partial/wrong-variable downloads).
    needs_check = []  # (stream, key, path)
    for (stream, key), info in raw.items():
        if not info["dir_exists"]:
            continue
        if info["actual"] == info["expected"]:
            for path in info["suspect"]:
                needs_check.append((stream, key, path))
        else:
            for path in info["nc_files"]:
                needs_check.append((stream, key, path))

    bad_by_key = {}  # (stream, key) -> [bad paths]
    if needs_check:
        try:
            from tqdm import tqdm
            bar2 = tqdm(total=len(needs_check), desc="Phase 1: content check",
                        unit="file", leave=False)
        except ImportError:
            bar2 = None
        for stream, key, path in needs_check:
            ok, _ = _content_ok(path, "t2m")
            if not ok:
                bad_by_key.setdefault((stream, key), []).append(path)
            if bar2:
                bar2.update(1)
        if bar2:
            bar2.close()

    # Assemble final source_status
    for (stream, key), info in raw.items():
        source_status[stream][key] = {
            "dir_exists": info["dir_exists"],
            "actual":     info["actual"],
            "expected":   info["expected"],
            "bad":        bad_by_key.get((stream, key), []),
        }

    return source_status


def _fmt_key(key):
    """Format a dir-key tuple as a human-readable string."""
    return "/".join(str(k) for k in key)


def _print_source_phase(source_status):
    """
    Print Phase 1 report.
    Returns list of all corrupt NC file paths (for cleanup).
    """
    print("=== Phase 1: Source NC Files ===")
    all_corrupt = []

    streams = [
        ("ifs_fc",  "IFS FC "),
        ("ifs_an",  "IFS AN "),
        ("aifs_fc", "AIFS FC"),
        ("era5",    "ERA5   "),
    ]
    for tag, label in streams:
        ss = source_status.get(tag, {})
        n_ok = n_incomplete = n_missing = 0
        corrupt_files = []
        detail_lines = []

        for key, info in sorted(ss.items()):
            actual   = info["actual"]
            expected = info["expected"]
            bad      = info["bad"]

            if not info["dir_exists"]:
                n_missing += 1
                if len(detail_lines) < _MAX_DETAIL_LINES:
                    detail_lines.append(f"    MISSING     {_fmt_key(key)}")
            elif bad:
                corrupt_files.extend(bad)
                all_corrupt.extend(bad)
                if len(detail_lines) < _MAX_DETAIL_LINES:
                    for p in bad:
                        detail_lines.append(f"    CORRUPT     {_fmt_key(key)}/{os.path.basename(p)}")
                if actual < expected:
                    n_incomplete += 1
                else:
                    n_ok += 1
            elif actual < expected:
                n_incomplete += 1
                if len(detail_lines) < _MAX_DETAIL_LINES:
                    detail_lines.append(
                        f"    INCOMPLETE  {_fmt_key(key)}: {actual}/{expected} files"
                    )
            else:
                n_ok += 1

        print(
            f"  {label} — {n_ok:,} dirs OK, {n_incomplete} incomplete, "
            f"{n_missing} missing, {len(corrupt_files)} corrupt files"
        )
        for line in detail_lines:
            print(line)
        extra = len(detail_lines) - _MAX_DETAIL_LINES
        if extra > 0:
            print(f"    … and {extra} more")

    # --- Alignment check ---
    # AN gaps are the dangerous case: one missing AN day silently drops every FC
    # entry whose valid_time (= init + lead) falls on that date, across all leads.
    # FC gaps just mean fewer rows in the parquet — caught by Phase 2b.
    an_gaps = {
        (yr, mo): info["expected"] - info["actual"]
        for (yr, mo), info in source_status.get("ifs_an", {}).items()
        if info["actual"] < info["expected"]
    }
    # AIFS FC also aligns against IFS AN, so check its date range too.
    aifs_an_gaps = {
        (yr, mo): gap
        for (yr, mo), gap in an_gaps.items()
        if (yr, mo) >= (int(AIFS_START[:4]), int(AIFS_START[5:7]))
    }
    fc_gaps = {
        (tag, yr, mo)
        for tag in ("ifs_fc", "aifs_fc")
        for (init_hour, lead, yr, mo), info in source_status.get(tag, {}).items()
        if info["actual"] < info["expected"]
    }

    print("  --- Alignment ---")
    if not an_gaps and not fc_gaps:
        print("  IFS AN complete — no alignment drops expected")
    else:
        if an_gaps:
            print(f"  IFS AN gaps in {len(an_gaps)} month(s) — FC valid_times on missing days will be dropped:")
            for (yr, mo), missing in sorted(an_gaps.items())[:_MAX_DETAIL_LINES]:
                aifs_note = " (affects AIFS FC too)" if (yr, mo) in aifs_an_gaps else ""
                print(f"    {yr}/{mo:02d}: {missing} AN day(s) missing{aifs_note}")
            extra = len(an_gaps) - _MAX_DETAIL_LINES
            if extra > 0:
                print(f"    … and {extra} more")
        if fc_gaps:
            by_month = {}
            for tag, yr, mo in fc_gaps:
                by_month.setdefault((yr, mo), []).append(tag)
            print(f"  FC source gaps in {len(by_month)} month(s) — missing FC inits absent from parquets (not AN-side drops):")
            for (yr, mo), tags in sorted(by_month.items())[:_MAX_DETAIL_LINES]:
                print(f"    {yr}/{mo:02d}: {', '.join(sorted(set(tags)))}")
            extra = len(by_month) - _MAX_DETAIL_LINES
            if extra > 0:
                print(f"    … and {extra} more")

    return all_corrupt, len(an_gaps)


# ---------------------------------------------------------------------------
# Phase 2a: ERA5 aggregation
# ---------------------------------------------------------------------------

def check_era5_aggregation(source_status):
    """
    Phase 2a: Validate ERA5 monthly parquets and climatology.
    Returns (flagged_parquets, remove_clim) where flagged_parquets is a list of
    (path, reason) and remove_clim is bool.
    """
    monthly_dir = os.path.join(AGGREGATED_DIR, "era5_monthly")
    start_yr = int(ERA5_CLIM_START[:4])
    end_yr   = int(ERA5_CLIM_END[:4])

    flagged = []  # (path, reason)
    n_ok = n_missing = n_stale = n_source_incomplete = 0
    detail_lines = []

    for yr in range(start_yr, end_yr + 1):
        for mo in range(1, 13):
            path = os.path.join(monthly_dir, f"era5_2t_county_{yr}_{mo:02d}.parquet")
            src_info = source_status.get("era5", {}).get((yr, mo), {})
            source_count = src_info.get("actual", None)

            if not os.path.exists(path) or _parquet_row_count(path) is None:
                n_missing += 1
                flagged.append((path, "MISSING"))
                if len(detail_lines) < _MAX_DETAIL_LINES:
                    detail_lines.append(f"    MISSING   {yr}/{mo:02d}")
                continue

            if source_count is not None:
                # Each ERA5 source file covers a full month; the parquet should have
                # one unique time per day. Feb 29 is excluded by aggregate_era5_2t.py,
                # so leap Februaries legitimately have 28 days, not 29.
                unique_times = _parquet_unique_count(path, "time")
                expected_days = calendar.monthrange(yr, mo)[1]
                if mo == 2 and calendar.isleap(yr):
                    expected_days -= 1
                if unique_times != expected_days:
                    n_stale += 1
                    flagged.append((path, f"STALE ({unique_times} times != {expected_days} expected days)"))
                    if len(detail_lines) < _MAX_DETAIL_LINES:
                        detail_lines.append(
                            f"    STALE     {yr}/{mo:02d}: {unique_times} times != {expected_days} expected days"
                        )
                    continue

            n_ok += 1

    print("=== Phase 2a: ERA5 Aggregation ===")
    total = (end_yr - start_yr + 1) * 12
    print(
        f"  Monthly parquets: {n_ok}/{total} OK"
        + (f", {n_missing} missing" if n_missing else "")
        + (f", {n_stale} stale" if n_stale else "")
        + (f", {n_source_incomplete} source-incomplete" if n_source_incomplete else "")
    )
    for line in detail_lines:
        print(line)
    extra = len(detail_lines) - _MAX_DETAIL_LINES
    if extra > 0:
        print(f"    … and {extra} more")

    # Climatology
    remove_clim = False
    if not os.path.exists(ERA5_CLIM_PATH):
        print("  Climatology: MISSING")
    else:
        names = _parquet_schema_names(ERA5_CLIM_PATH)
        required = {"t2m_clim", "geo_id", "day_of_year"}
        if names is None or not required.issubset(names):
            missing_cols = required - (names or set())
            print(f"  Climatology: PARTIAL — missing columns: {missing_cols}")
            remove_clim = bool(flagged)  # stale monthly → stale clim
        else:
            rows = _parquet_row_count(ERA5_CLIM_PATH)
            try:
                tbl = pq.read_table(ERA5_CLIM_PATH, columns=["day_of_year"])
                doys = tbl["day_of_year"].to_pandas()
                mn, mx, n_doys = int(doys.min()), int(doys.max()), doys.nunique()
            except Exception:
                mn, mx, n_doys = None, None, None
            if mn == 1 and mx == 365 and n_doys == 365:
                print(f"  Climatology: OK ({rows:,} rows, DOY 1–365)")
            else:
                print(f"  Climatology: PARTIAL ({rows:,} rows, DOY {mn}–{mx}, {n_doys} unique DOYs)")
                remove_clim = bool(flagged)

    return flagged, remove_clim


# ---------------------------------------------------------------------------
# Phase 2b: IFS/AIFS aggregation
# ---------------------------------------------------------------------------

_FC_CHUNK_PAT = re.compile(
    r"(?P<tag>ifs_fc|aifs_fc)_2t_county_(?P<yr>\d{4})_(?P<mo>\d{2})_lead(?P<lead>\d+)\.parquet$"
)
_AN_CHUNK_PAT = re.compile(
    r"ifs_an_2t_county_(?P<yr>\d{4})_(?P<mo>\d{2})\.parquet$"
)


def _expected_fc_an_parquets():
    """
    Return (expected_by_subdir, expected_times) where:
      expected_by_subdir[subdir_key] = set of expected basename strings
      expected_times[basename]       = expected unique init_time count (FC)
                                       or unique analysis time count (AN)

    FC chunks are grouped by INIT-TIME month (not valid-time month), so the
    expected set is simply init_months × leads with no spillover or boundary
    complexity. Expected inits per month = 2 × days_in_month (fc_end extends
    the last day to T12 so both T00 and T12 are always included).

    IFS AN chunks are grouped by analysis-time month. AN discovery extends to
    fc_end + max(LEAD_TIMES) so it covers all valid_times reachable from the
    FC init range (including into the next year when IFS_END is updated).
    """
    expected  = {"ifs_fc_monthly": set(), "aifs_fc_monthly": set(), "ifs_an_monthly": set()}
    exp_times = {}  # basename -> expected unique time count

    # --- FC streams: one chunk per (init_month, lead) ---
    for tag, stem, start_str, end_str in [
        ("ifs_fc_monthly",  "ifs_fc_2t_county",  IFS_START,  IFS_END),
        ("aifs_fc_monthly", "aifs_fc_2t_county", AIFS_START, AIFS_END),
    ]:
        for yr, mo, first, last in _iter_year_months(start_str, end_str):
            days = calendar.monthrange(yr, mo)[1]
            # fc_end = END + 12h ensures both T00 and T12 are included on the
            # last day, so every month always has 2 × days inits.
            n_inits = 2 * days
            for lead in LEAD_TIMES:
                name = f"{stem}_{yr}_{mo:02d}_lead{lead:03d}.parquet"
                expected[tag].add(name)
                exp_times[name] = n_inits

    # --- IFS AN: one chunk per analysis month ---
    # AN discovery extends to fc_end + max(LEAD_TIMES) to cover all FC valid_times.
    an_end_dt = (datetime.strptime(IFS_END, "%Y-%m-%d")
                 + timedelta(hours=12 + max(LEAD_TIMES)))
    an_end_str = f"{an_end_dt.year}-{an_end_dt.month:02d}-{an_end_dt.day:02d}"
    for yr, mo, first, last in _iter_year_months(IFS_START, an_end_str):
        days_in_range = (last - first).days + 1
        name = f"ifs_an_2t_county_{yr}_{mo:02d}.parquet"
        expected["ifs_an_monthly"].add(name)
        exp_times[name] = 4 * days_in_range  # 4 analysis times per day

    return expected, exp_times


def check_fc_an_aggregation():
    """
    Phase 2b: Check expected vs. present parquets and completeness of present ones.
    Returns list of (path, reason) tuples flagged for cleanup.
    """
    chunks = [
        ("ifs_fc_monthly",  _FC_CHUNK_PAT, "valid_time"),
        ("aifs_fc_monthly", _FC_CHUNK_PAT, "valid_time"),
        ("ifs_an_monthly",  _AN_CHUNK_PAT, "time"),
    ]
    expected_by_subdir, exp_times = _expected_fc_an_parquets()

    flagged = []

    print("=== Phase 2b: IFS/AIFS Aggregation ===")
    for subdir, pat, time_col in chunks:
        monthly_dir = os.path.join(AGGREGATED_DIR, subdir)
        expected_names = expected_by_subdir[subdir]
        n_expected = len(expected_names)

        if not os.path.isdir(monthly_dir):
            print(f"  {subdir}: MISSING directory ({n_expected} parquets expected)")
            continue

        found_paths = {
            os.path.basename(p): p
            for p in glob.glob(os.path.join(monthly_dir, "*.parquet"))
        }
        missing_names = sorted(expected_names - found_paths.keys())
        n_ok = n_incomplete = n_missing = len(missing_names)
        n_ok = 0
        detail_lines = []

        # Report missing first
        for name in missing_names:
            if len(detail_lines) < _MAX_DETAIL_LINES:
                detail_lines.append(f"    MISSING     {name}")

        # Check completeness of present parquets
        for name in sorted(expected_names - set(missing_names)):
            path = found_paths[name]
            # Use the exact expected time count derived from the same init enumeration
            # as the aggregation script — this correctly handles boundary months,
            # long-lead month-end spillover, and alignment clipping at IFS_END.
            expected_times = exp_times.get(name, 0)

            unique_times = _parquet_unique_count(path, time_col)
            if unique_times < expected_times:
                n_incomplete += 1
                flagged.append((path, f"INCOMPLETE ({unique_times}/{expected_times})"))
                if len(detail_lines) < _MAX_DETAIL_LINES:
                    detail_lines.append(
                        f"    INCOMPLETE  {name} ({unique_times}/{expected_times})"
                    )
            else:
                n_ok += 1

        n_present = n_expected - n_missing
        print(
            f"  {subdir}: {n_present}/{n_expected} present — {n_ok} OK"
            + (f", {n_incomplete - n_missing} incomplete" if n_incomplete > n_missing else "")
            + (f", {n_missing} missing" if n_missing else "")
        )
        for line in detail_lines:
            print(line)
        extra = len(detail_lines) - _MAX_DETAIL_LINES
        if extra > 0:
            print(f"    … and {extra} more")

    return flagged


# ---------------------------------------------------------------------------
# Phase 3: Derived outputs (informational only)
# ---------------------------------------------------------------------------

_DERIVED_PARQUETS = [
    ("IFS bias",            "ifs_fc_bias_2t_county.parquet"),
    ("IFS bias+anom",       "ifs_fc_bias_anom_2t_county.parquet"),
    ("AIFS bias",           "aifs_fc_bias_2t_county.parquet"),
    ("AIFS bias+anom",      "aifs_fc_bias_anom_2t_county.parquet"),
    ("AIFS vs IFS",         "aifs_vs_ifs_fc_bias_comparison_2t_county.parquet"),
]


def check_derived_outputs():
    """
    Phase 3: Check presence of derived output parquets (informational only).
    Returns list of missing filenames.
    """
    print("=== Phase 3: Derived Outputs ===")
    missing_names = []
    for label, fname in _DERIVED_PARQUETS:
        path = os.path.join(AGGREGATED_DIR, fname)
        if not os.path.exists(path):
            missing_names.append(fname)
            print(f"  MISSING  {label} ({fname})")
        else:
            rows = _parquet_row_count(path)
            if rows is None:
                missing_names.append(fname)
                print(f"  CORRUPT  {label} ({fname})")
            else:
                print(f"  OK       {label} ({rows:,} rows)")
    if len(missing_names) == len(_DERIVED_PARQUETS):
        print("  (none computed yet — run compute_derived_2t.py to generate)")
    return missing_names


# ---------------------------------------------------------------------------
# Additional standalone checks (always run, lightweight)
# ---------------------------------------------------------------------------

def check_koppen():
    out_path = os.path.join(AGGREGATED_DIR, "koppen_geiger_county.parquet")
    tif_ok = os.path.exists(KOPPEN_PATH)
    tif_status = "present" if tif_ok else "MISSING"
    if not os.path.exists(out_path):
        print(f"  Koppen-Geiger: MISSING output | GeoTIFF {tif_status}: {KOPPEN_PATH}")
        return False
    rows = _parquet_row_count(out_path)
    if rows is None:
        print(f"  Koppen-Geiger: CORRUPT output — {out_path}")
        return False
    print(f"  Koppen-Geiger: OK ({rows:,} rows) | GeoTIFF {tif_status}")
    return True


def check_acs():
    path = os.path.join(
        ACS_DIR, f"acs_5yr_{ACS_YEAR}", f"acs_5yr_{ACS_YEAR}_{ACS_LEVEL}.parquet"
    )
    if not os.path.exists(path):
        print(f"  ACS data: MISSING — {path}")
        return False
    rows = _parquet_row_count(path)
    if rows is None:
        print(f"  ACS data: CORRUPT — {path}")
        return False
    print(f"  ACS data: OK ({rows:,} rows)")
    return True


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup(corrupt_nc, flagged_era5, flagged_fc_an, remove_clim):
    """
    Remove exactly the flagged artifacts:
    - corrupt_nc:     list of .nc paths (fail content check)
    - flagged_era5:   list of (parquet_path, reason) ERA5 monthly parquets
    - flagged_fc_an:  list of (parquet_path, reason) FC/AN checkpoint parquets
    - remove_clim:    bool — delete ERA5 climatology

    Does NOT delete missing source files (download handles that) or derived outputs.
    """
    n_nc = n_parquet = 0

    for path in corrupt_nc:
        try:
            os.remove(path)
            print(f"  REMOVED  {path}")
            n_nc += 1
        except OSError as exc:
            print(f"  WARNING: could not delete {path}: {exc}")

    for path, reason in flagged_era5:
        if not os.path.exists(path):
            continue
        try:
            os.remove(path)
            print(f"  REMOVED  {path}  [{reason}]")
            n_parquet += 1
        except OSError as exc:
            print(f"  WARNING: could not delete {path}: {exc}")

    for path, reason in flagged_fc_an:
        if not os.path.exists(path):
            continue
        try:
            os.remove(path)
            n_parquet += 1
        except OSError as exc:
            print(f"  WARNING: could not delete {path}: {exc}")
            continue
        print(f"  REMOVED  {path}  [{reason}]")

    if remove_clim and os.path.exists(ERA5_CLIM_PATH):
        try:
            os.remove(ERA5_CLIM_PATH)
            print(f"  REMOVED  {ERA5_CLIM_PATH}  [stale ERA5 monthly parquets removed]")
        except OSError as exc:
            print(f"  WARNING: could not delete climatology: {exc}")

    print(f"\nCleanup complete: {n_nc} corrupt NC files, {n_parquet} parquets removed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Validate nwp-census-eval pipeline outputs (always runs all phases)."
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help=(
            "Remove flagged artifacts: corrupt NC files, incomplete checkpoint parquets, "
            "and ERA5 climatology if stale monthly parquets were removed. "
            "Does NOT delete missing source files or derived outputs."
        ),
    )
    args = parser.parse_args(argv)

    print("\n=== nwp-census-eval pipeline validation ===\n")

    # Phase 1
    source_status = check_source_files()
    print()
    corrupt_nc, n_an_gap_months = _print_source_phase(source_status)
    print()

    # Phase 2a
    flagged_era5, remove_clim = check_era5_aggregation(source_status)
    print()

    # Phase 2b
    flagged_fc_an = check_fc_an_aggregation()
    print()

    # Phase 3
    missing_derived = check_derived_outputs()
    print()

    # Ancillary checks
    print("=== Ancillary Outputs ===")
    check_koppen()
    check_acs()
    print()

    # Summary
    print("=== Summary ===")
    n_corrupt_nc  = len(corrupt_nc)
    n_era5_parq   = len(flagged_era5)
    n_fc_an_parq  = len(flagged_fc_an)
    n_total_parq  = n_era5_parq + n_fc_an_parq
    n_missing_der = len(missing_derived)

    if n_corrupt_nc == 0 and n_total_parq == 0 and n_an_gap_months == 0:
        print("  No issues found — pipeline artifacts look complete.")
    else:
        if n_an_gap_months:
            print(f"  {n_an_gap_months} month(s) with IFS AN gaps — FC alignment drops expected")
        if n_corrupt_nc:
            print(f"  {n_corrupt_nc} corrupt NC file(s) flagged")
        if n_era5_parq:
            print(f"  {n_era5_parq} ERA5 monthly parquet(s) flagged (missing/stale)")
        if n_fc_an_parq:
            print(f"  {n_fc_an_parq} FC/AN checkpoint parquet(s) flagged (incomplete)")
        if remove_clim:
            print("  ERA5 climatology flagged (stale)")

    if n_missing_der:
        print(f"  {n_missing_der} derived output(s) not yet computed (run compute_derived_2t.py)")

    if n_corrupt_nc or n_total_parq:
        if args.cleanup:
            print()
            print("=== Cleanup ===")
            cleanup(corrupt_nc, flagged_era5, flagged_fc_an, remove_clim)
        else:
            print()
            print("  Run with --cleanup to remove flagged artifacts.")

    any_issue = bool(n_corrupt_nc or n_total_parq or n_missing_der)
    return 1 if any_issue else 0


if __name__ == "__main__":
    raise SystemExit(main())
