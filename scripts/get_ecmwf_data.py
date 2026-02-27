import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from censuswxindex.data import ECMWFDataClient

def main():
    base_dir = "/glade/derecho/scratch/dcalhoun/ecmwf/ifs"
    param = ('2t', '167.128')      # (shortName, param code)
    start = "2016-01-01"
    end = "2025-12-31"
    lead_times = ["0", "6", "12", "18", "24", "48", "72", "96", "120", "168", "240"]
    init_hours = ["0000", "1200"]
    grid = "0.125"
    model = "ifs"
    bounds = ['49.5','-125','24.5','-66.5'] # CONUS

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
    )

    # Fetch forecast and/or analysis data
    client.get_forecast()
    client.get_analysis()

if __name__ == "__main__":
    main()