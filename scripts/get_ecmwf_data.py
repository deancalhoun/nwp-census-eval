import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from censuswxindex.data import ECMWFDataClient

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Download ECMWF IFS forecast and analysis data."
    )
    parser.add_argument(
        "--max-concurrent-requests",
        type=int,
        default=20,
        help="Maximum number of concurrent ECMWF API requests (default: 20, capped at 20).",
    )
    args = parser.parse_args(argv)

    ## PARAMETERS
    param = ("2t", "167.128")  # (shortName, param code)
    lead_times = ["0", "6", "12", "18", "24", "36", "48", "60", "72", "84", "96", "108", "120", "168", "240"]
    init_hours = ["0000", "1200"]
    bounds = ["49.5", "-125", "24.5", "-66.5"]  # CONUS

    ### IFS FORECAST AND ANALYSIS
    base_dir = "/glade/derecho/scratch/dcalhoun/ecmwf/ifs"
    grid = "0.125"
    model = "ifs"
    start = "2016-01-01"
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

    # Fetch forecast and analysis data
    client.get_forecast()
    client.get_analysis()

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

    # Fetch forecast
    client.get_forecast()

if __name__ == "__main__":
    main()