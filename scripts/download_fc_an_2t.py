import sys
import os
import argparse
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nwp_census_eval.data import ECMWFDataClient


def _extend_end_date_for_analysis(end_str, lead_times):
    """
    Extend the end date so that analysis covers the maximum forecast lead time.
    """
    max_lead_hours = max(int(lead) for lead in lead_times)
    extra_days = max_lead_hours // 24
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    extended = end_date + timedelta(days=extra_days)
    return extended.strftime("%Y-%m-%d")


def _validate_forecast(client, label):
    """
    Validate that all forecast data expected by *client* are present on disk.
    """
    client._cleanup_forecast_dirs()  # noqa: SLF001
    client._refresh_existing_fc_dates()  # noqa: SLF001
    missing_dates = [d for d in client.dates if not client._does_fc_exist(d)]  # noqa: SLF001
    if not missing_dates:
        print(f"{label} forecast: OK - all {len(client.dates)} days present.")
        return True

    print(f"{label} forecast: MISSING {len(missing_dates)} day(s).")
    print("  First 20 missing dates:")
    for d in missing_dates[:20]:
        print(f"   - {d.strftime('%Y-%m-%d')}")
    return False


def _validate_analysis(client, label):
    """
    Validate that all analysis data expected by *client* are present on disk.
    """
    client._cleanup_analysis_dir()  # noqa: SLF001
    client._refresh_existing_an_dates()  # noqa: SLF001
    missing_dates = [d for d in client.dates if not client._does_an_exist(d)]  # noqa: SLF001
    if not missing_dates:
        print(f"{label} analysis: OK - all {len(client.dates)} days present.")
        return True

    print(f"{label} analysis: MISSING {len(missing_dates)} day(s).")
    print("  First 20 missing dates:")
    for d in missing_dates[:20]:
        print(f"   - {d.strftime('%Y-%m-%d')}")
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Download or validate ECMWF IFS/AIFS forecast and analysis data."
    )
    parser.add_argument(
        "--max-concurrent-requests",
        type=int,
        default=1,
        help="Maximum number of concurrent ECMWF API requests (default: 20, capped at 20).",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Only validate that expected data are present on disk; do not download.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable INFO logging (otherwise keep output minimal when validating).",
    )
    args = parser.parse_args(argv)

    ## PARAMETERS
    param = ("2t", "167.128")  # (shortName, param code)
    lead_times = ["0", "6", "12", "18", "24", "36", "48", "60", "72", "84", "96", "108", "120", "168", "240"]
    init_hours = ["0000", "1200"]
    bounds = ["49.5", "-125", "24.5", "-66.5"]  # CONUS

    ### IFS FORECAST
    base_dir = "/glade/derecho/scratch/dcalhoun/ecmwf/ifs"
    grid = "0.125"
    model = "ifs"
    start = "2016-01-01"
    end = "2025-12-31"

    # Forecast client uses the original end date
    fc_client = ECMWFDataClient(
        base_dir=base_dir,
        param=param,
        start=start,
        end=end,
        lead_times=lead_times,
        init_hours=init_hours,
        grid=grid,
        model=model,
        bounds=bounds,
        max_concurrent_requests=args.max_concurrent_requests,
    )

    # Analysis client extends the end date so that valid times beyond
    # the forecast cutoff (e.g., 10‑day leads from 2025‑12‑31) are covered.
    an_end = _extend_end_date_for_analysis(end, lead_times)
    an_client = ECMWFDataClient(
        base_dir=base_dir,
        param=param,
        start=start,
        end=an_end,
        lead_times=lead_times,
        init_hours=init_hours,
        grid=grid,
        model=model,
        bounds=bounds,
        max_concurrent_requests=args.max_concurrent_requests,
    )

    ### AIFS FORECAST
    base_dir = "/glade/derecho/scratch/dcalhoun/ecmwf/aifs"
    grid = "0.25"
    model = "aifs"
    start = "2024-03-01"
    end = "2025-12-31"

    client = ECMWFDataClient(
        base_dir=base_dir,
        param=param,
        start=start,
        end=end,
        lead_times=lead_times,
        init_hours=init_hours,
        grid=grid,
        model=model,
        bounds=bounds,
        max_concurrent_requests=args.max_concurrent_requests,
    )
    root_logger = logging.getLogger()
    previous_level = root_logger.level

    try:
        if args.validate:
            if not args.verbose:
                root_logger.setLevel(logging.WARNING)

            ok = True
            if not _validate_forecast(fc_client, "IFS"):
                ok = False
            if not _validate_analysis(an_client, "IFS"):
                ok = False
            if not _validate_forecast(client, "AIFS"):
                ok = False

            if ok:
                print("All ECMWF datasets are present.")
                return 0

            print("Some ECMWF datasets are missing.")
            return 1

        # Default behavior: download data
        fc_client.get_forecast()
        an_client.get_analysis()
        client.get_forecast()
        return 0
    finally:
        root_logger.setLevel(previous_level)


if __name__ == "__main__":
    raise SystemExit(main())