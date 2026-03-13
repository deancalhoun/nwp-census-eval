import os
import sys
import argparse
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nwp_census_eval.data import download_acs
from config import ACS_DIR, ACS_YEAR, ACS_LEVEL, ACS_TABLES


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Download ACS 5-year estimates from the Census Bureau API."
    )
    parser.add_argument("--year",    type=int, default=ACS_YEAR,
                        help=f"ACS vintage year (default: {ACS_YEAR}).")
    parser.add_argument("--level",   default=ACS_LEVEL, choices=["county", "tract"],
                        help=f"Geographic level (default: {ACS_LEVEL}).")
    parser.add_argument("--out-dir", default=None,
                        help="Output directory (default: {ACS_DIR}/acs_5yr_{year}).")
    args = parser.parse_args(argv)
    _t_start = time.time()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("=== download_acs.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S"))

    out_dir  = args.out_dir or os.path.join(ACS_DIR, f"acs_5yr_{args.year}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"acs_5yr_{args.year}_{args.level}.parquet")

    df = download_acs(year=args.year, groups=ACS_TABLES, level=args.level, estimate_only=True, with_geometry=True)
    df.to_parquet(out_path, index=False)
    logging.info("[%.0fs] Saved %d rows → %s", time.time() - _t_start, len(df), out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
