"""
scripts/aggregate_koppen.py — Aggregate Koppen-Geiger GeoTIFF to US counties.

Uses CategoricalAggregator to produce, for each county, the dominant and
secondary Koppen-Geiger climate classification and their area percentages.

GeoTIFF preprocessing: rioxarray opens GeoTIFFs with (band, y, x) dims and
x/y coordinates. xagg.fix_ds() won't rename these to lat/lon, so the script
manually preprocesses the array and writes to a temporary NetCDF before
passing it to CategoricalAggregator.

Output: {AGGREGATED_DIR}/koppen_geiger_county.parquet
Columns: geo_id, category_1, category_1_pct, category_2, category_2_pct
"""

import os
import sys
import logging
import time

import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nwp_census_eval.aggregate import CategoricalAggregator
from config import SHAPEFILE_PATH, AGGREGATED_DIR, KOPPEN_PATH, WEIGHTMAP_CACHE_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OUT_PATH = os.path.join(AGGREGATED_DIR, "koppen_geiger_county.parquet")
VAR_NAME = "koppen"


def preprocess_koppen(tif_path: str, tmp_nc_path: str) -> None:
    """
    Convert Koppen-Geiger GeoTIFF to a NetCDF with lat/lon coordinate names.

    rioxarray opens GeoTIFFs with (band, y, x) dims and x/y coordinate names.
    xagg.fix_ds() won't rename x/y → lat/lon, so we do it manually and write
    to a temp NetCDF that CategoricalAggregator can ingest directly.
    """
    try:
        import rioxarray  # noqa: F401  registers rasterio engine
    except ImportError:
        pass
    da = xr.open_dataarray(tif_path, engine="rasterio")
    da = da.squeeze("band", drop=True)
    da = da.rename({"x": "lon", "y": "lat"})
    da = da.rename(VAR_NAME)
    ds = da.to_dataset()
    ds.to_netcdf(tmp_nc_path)


def main():
    t_start = time.time()
    logging.info("=== aggregate_koppen.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S"))
    os.makedirs(AGGREGATED_DIR, exist_ok=True)

    if os.path.exists(OUT_PATH):
        try:
            pd.read_parquet(OUT_PATH, columns=["geo_id"])
            logging.info("Output already exists and is readable: %s; skipping.", OUT_PATH)
            return
        except Exception:
            logging.warning("Existing output unreadable; will recompute.")

    if not os.path.exists(KOPPEN_PATH):
        logging.error("Koppen-Geiger GeoTIFF not found: %s", KOPPEN_PATH)
        return

    logging.info("Preprocessing Koppen-Geiger GeoTIFF: %s", KOPPEN_PATH)
    tmp_nc = KOPPEN_PATH + ".tmp.nc"
    try:
        preprocess_koppen(KOPPEN_PATH, tmp_nc)
        logging.info("[%.0fs] Preprocessing done; building weightmap and aggregating ...", time.time() - t_start)
        agg = CategoricalAggregator(
            shapefile_path=SHAPEFILE_PATH,
            categorical_file=tmp_nc,
            var_name=VAR_NAME,
            silent=False,
            cache_dir=WEIGHTMAP_CACHE_DIR,
        )
        df = agg.aggregate(tmp_nc)
    finally:
        if os.path.exists(tmp_nc):
            os.remove(tmp_nc)

    tmp_out = OUT_PATH + ".tmp"
    df.to_parquet(tmp_out, index=False)
    os.replace(tmp_out, OUT_PATH)
    logging.info("[%.0fs] Saved %d rows to %s", time.time() - t_start, len(df), OUT_PATH)


if __name__ == "__main__":
    main()
