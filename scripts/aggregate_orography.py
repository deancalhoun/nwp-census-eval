"""
scripts/aggregate_orography.py — Neighborhood terrain complexity from raw GTOPO30.

For each CONUS county centroid, buffers by multiple radii and computes:

  orography_{R}km     — std dev of raw GTOPO30 pixels within circular buffer
                        of radius R. Captures total terrain complexity.

  elev_gradient_{R}km — mean elevation of eastern semicircle minus mean elevation
                        of western semicircle within the buffer. Signed quantity:
                          negative → west higher than east (Great Plains / Rockies lee)
                          positive → east higher than west (Appalachian windward)
                          near zero → symmetric terrain (CA Central Valley)
                        Distinguishes downstream plains counties from valley counties
                        surrounded by terrain on both sides.

Inputs:
  - ELEV_GTOPO30_DIR: raw .DEM.gz tiles (W140N90, W100N90, W060N90,
                                          W140N40, W100N40, W060N40)
  - SHAPEFILE_PATH: county polygons

Output:
  - {AGGREGATED_DIR}/orography_county.parquet
  Columns: geo_id, elev_mean,
           orography_{R}km, elev_gradient_{R}km  (for each R in RADII_KM)

Usage:
    python scripts/aggregate_orography.py
"""

import gzip
import logging
import os
import sys
import tempfile
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_bounds
from rasterstats import zonal_stats
from shapely.geometry import box

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import AGGREGATED_DIR, ELEV_GTOPO30_DIR, SHAPEFILE_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

OUT_PATH   = os.path.join(AGGREGATED_DIR, "orography_county.parquet")
RADII_KM   = [100, 200, 300, 400, 500, 600, 700]
CRS_DIST   = "EPSG:5070"
CRS_GEO    = "EPSG:4326"
CONUS_FIPS = {f"{i:02d}" for i in range(1, 57) if i not in (2, 15, 60, 64, 66, 69, 72, 74, 78)}

# GTOPO30 tile constants
TILE_ROWS   = 6000
TILE_COLS   = 4800
TILE_RES    = 30 / 3600
TILE_NODATA = -9999

TILE_GRID = [
    [
        ("W140N90", 90.0, -140.0),
        ("W100N90", 90.0, -100.0),
        ("W060N90", 90.0,  -60.0),
    ],
    [
        ("W140N40", 40.0, -140.0),
        ("W100N40", 40.0, -100.0),
        ("W060N40", 40.0,  -60.0),
    ],
]

# CONUS clip bounds (with buffer)
CLIP_BUFFER   = 0.5
CONUS_LAT_MIN = 24.5 - CLIP_BUFFER
CONUS_LAT_MAX = 49.5 + CLIP_BUFFER
CONUS_LON_MIN = -125.0 - CLIP_BUFFER
CONUS_LON_MAX = -66.5 + CLIP_BUFFER


# ---------------------------------------------------------------------------
# Build clipped CONUS DEM as an in-memory GeoTIFF for rasterstats
# ---------------------------------------------------------------------------

def _read_tile(tile_name, lat_nw, lon_nw):
    path = os.path.join(ELEV_GTOPO30_DIR, f"{tile_name}.DEM.gz")
    with gzip.open(path, "rb") as f:
        raw = np.frombuffer(f.read(), dtype=">i2")
    data = raw.reshape(TILE_ROWS, TILE_COLS).astype(np.float32)
    data[data == TILE_NODATA] = np.nan
    lat_ul = lat_nw - TILE_RES / 2.0
    lon_ul = lon_nw + TILE_RES / 2.0
    lats = lat_ul - np.arange(TILE_ROWS) * TILE_RES
    lons = lon_ul + np.arange(TILE_COLS) * TILE_RES
    return data, lats, lons


def build_clipped_dem_tif(tmp_path: str) -> None:
    row_arrays, row_lats = [], []
    ref_lons = None

    for row in TILE_GRID:
        band_arrays, band_lons, band_lats = [], [], None
        for tile_name, lat_nw, lon_nw in row:
            logging.info("Reading tile %s", tile_name)
            data, lats, lons = _read_tile(tile_name, lat_nw, lon_nw)
            if band_lats is None:
                band_lats = lats
            band_arrays.append(data)
            band_lons.append(lons)
        row_arrays.append(np.concatenate(band_arrays, axis=1))
        row_lats.append(band_lats)
        if ref_lons is None:
            ref_lons = np.concatenate(band_lons)

    mosaic_data = np.concatenate(row_arrays, axis=0)
    mosaic_lats = np.concatenate(row_lats)

    lat_mask = (mosaic_lats >= CONUS_LAT_MIN) & (mosaic_lats <= CONUS_LAT_MAX)
    lon_mask = (ref_lons   >= CONUS_LON_MIN) & (ref_lons   <= CONUS_LON_MAX)

    clipped = mosaic_data[np.ix_(lat_mask, lon_mask)]
    clipped_lats = mosaic_lats[lat_mask]
    clipped_lons = ref_lons[lon_mask]

    asc = np.argsort(clipped_lats)
    clipped = clipped[asc, :]
    clipped_lats = clipped_lats[asc]

    logging.info(
        "Clipped DEM: %d x %d | lat %.3f–%.3f | lon %.3f–%.3f",
        clipped.shape[0], clipped.shape[1],
        float(clipped_lats[0]), float(clipped_lats[-1]),
        float(clipped_lons[0]), float(clipped_lons[-1]),
    )

    transform = from_bounds(
        west=float(clipped_lons[0])  - TILE_RES / 2.0,
        south=float(clipped_lats[0]) - TILE_RES / 2.0,
        east=float(clipped_lons[-1]) + TILE_RES / 2.0,
        north=float(clipped_lats[-1])+ TILE_RES / 2.0,
        width=clipped.shape[1],
        height=clipped.shape[0],
    )

    data_northup = clipped[::-1, :]
    with rasterio.open(
        tmp_path, "w",
        driver="GTiff",
        height=data_northup.shape[0],
        width=data_northup.shape[1],
        count=1,
        dtype="float32",
        crs=CRS_GEO,
        transform=transform,
        nodata=np.nan,
    ) as dst:
        dst.write(data_northup, 1)

    logging.info("Written temp GeoTIFF: %s", tmp_path)


# ---------------------------------------------------------------------------
# County centroids
# ---------------------------------------------------------------------------

def load_county_centroids() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(SHAPEFILE_PATH)
    gdf = gdf[gdf["GEOID"].str[:2].isin(CONUS_FIPS)][["GEOID", "geometry"]].copy()
    gdf = gdf.rename(columns={"GEOID": "geo_id"})
    gdf_ea = gdf.to_crs(CRS_DIST)
    gdf["centroid_geo"] = gdf_ea.geometry.centroid.to_crs(CRS_GEO)
    return gdf


# ---------------------------------------------------------------------------
# Orography: std dev within full circular buffer
# ---------------------------------------------------------------------------

def compute_orography(gdf: gpd.GeoDataFrame, tif_path: str, radius_km: int) -> pd.Series:
    """Std dev of GTOPO30 pixels within circular buffer of radius_km."""
    radius_m = radius_km * 1000

    centroids_ea = gpd.GeoDataFrame(
        gdf[["geo_id"]].copy(),
        geometry=gdf.to_crs(CRS_DIST).geometry.centroid,
        crs=CRS_DIST,
    )
    buffers_ea = centroids_ea.copy()
    buffers_ea["geometry"] = centroids_ea.geometry.buffer(radius_m)
    buffers_geo = buffers_ea.to_crs(CRS_GEO)

    logging.info("orography_%dkm: zonal_stats std (%d polygons) ...", radius_km, len(buffers_geo))

    def _std(x):
        x = x[~np.isnan(x)]
        return float(np.std(x, ddof=1)) if len(x) > 1 else 0.0

    stats = zonal_stats(
        buffers_geo,
        tif_path,
        add_stats={"orography": _std},
        nodata=np.nan,
        all_touched=False,
    )

    values = [s.get("orography", 0.0) or 0.0 for s in stats]
    return pd.Series(values, index=gdf["geo_id"].values, name=f"orography_{radius_km}km")


# ---------------------------------------------------------------------------
# Elevation gradient: mean(east semicircle) - mean(west semicircle)
# ---------------------------------------------------------------------------

def compute_elev_gradient(gdf: gpd.GeoDataFrame, tif_path: str, radius_km: int) -> pd.Series:
    """
    Signed east-west elevation gradient within circular buffer.
    Computed in EPSG:5070; semicircles split at centroid x coordinate.
    negative → west higher (Great Plains/Rockies lee)
    positive → east higher (Appalachian windward)
    near zero → symmetric (CA Central Valley, flat plains)
    """
    radius_m = radius_km * 1000

    gdf_ea = gdf.to_crs(CRS_DIST)
    centroids_ea = gdf_ea.geometry.centroid

    east_geoms, west_geoms = [], []
    for centroid in centroids_ea:
        circle = centroid.buffer(radius_m)
        east_half = box(centroid.x, centroid.y - radius_m - 1,
                        centroid.x + radius_m + 1, centroid.y + radius_m + 1)
        west_half = box(centroid.x - radius_m - 1, centroid.y - radius_m - 1,
                        centroid.x, centroid.y + radius_m + 1)
        east_geoms.append(circle.intersection(east_half))
        west_geoms.append(circle.intersection(west_half))

    east_gdf = gpd.GeoDataFrame(gdf[["geo_id"]].copy(), geometry=east_geoms, crs=CRS_DIST).to_crs(CRS_GEO)
    west_gdf = gpd.GeoDataFrame(gdf[["geo_id"]].copy(), geometry=west_geoms, crs=CRS_DIST).to_crs(CRS_GEO)

    logging.info("elev_gradient_%dkm: zonal_stats east/west means (%d polygons each) ...", radius_km, len(gdf))

    east_stats = zonal_stats(east_gdf, tif_path, stats=["mean"], nodata=np.nan)
    west_stats = zonal_stats(west_gdf, tif_path, stats=["mean"], nodata=np.nan)

    east_means = np.array([s.get("mean") or np.nan for s in east_stats])
    west_means = np.array([s.get("mean") or np.nan for s in west_stats])
    gradient   = east_means - west_means

    return pd.Series(gradient, index=gdf["geo_id"].values, name=f"elev_gradient_{radius_km}km")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    t_start = time.time()
    logging.info(
        "=== aggregate_orography.py | start %s ===", time.strftime("%Y-%m-%d %H:%M:%S")
    )
    os.makedirs(AGGREGATED_DIR, exist_ok=True)

    if os.path.exists(OUT_PATH):
        try:
            pd.read_parquet(OUT_PATH, columns=["geo_id"])
            logging.info("Output already exists: %s; skipping.", OUT_PATH)
            return
        except Exception:
            logging.warning("Existing output unreadable; will recompute.")

    tmp_tif = tempfile.mktemp(suffix=".tif", dir=AGGREGATED_DIR)
    try:
        build_clipped_dem_tif(tmp_tif)

        logging.info("Computing county elev_mean via zonal_stats ...")
        gdf = load_county_centroids()
        gdf_geo = gdf[["geo_id", "geometry"]].to_crs(CRS_GEO)
        mean_stats = zonal_stats(gdf_geo, tmp_tif, stats=["mean"], nodata=np.nan)
        elev_mean = pd.Series(
            [s.get("mean") for s in mean_stats],
            index=gdf["geo_id"].values,
            name="elev_mean",
        )

        result = elev_mean.reset_index().rename(columns={"index": "geo_id"})
        result.columns = ["geo_id", "elev_mean"]

        for radius_km in RADII_KM:
            t0 = time.time()

            s_oro = compute_orography(gdf, tmp_tif, radius_km)
            s_grad = compute_elev_gradient(gdf, tmp_tif, radius_km)

            result = (
                result
                .merge(s_oro.reset_index().rename(columns={"index": "geo_id"}),  on="geo_id", how="left")
                .merge(s_grad.reset_index().rename(columns={"index": "geo_id"}), on="geo_id", how="left")
            )

            logging.info(
                "[%.0fs] %dkm | orography: mean=%.1f std=%.1f | gradient: mean=%.1f std=%.1f",
                time.time() - t0, radius_km,
                result[f"orography_{radius_km}km"].mean(),
                result[f"orography_{radius_km}km"].std(),
                result[f"elev_gradient_{radius_km}km"].mean(),
                result[f"elev_gradient_{radius_km}km"].std(),
            )

    finally:
        if os.path.exists(tmp_tif):
            os.remove(tmp_tif)

    tmp_out = OUT_PATH + ".tmp"
    result.to_parquet(tmp_out, index=False)
    os.replace(tmp_out, OUT_PATH)
    logging.info("[%.0fs] Saved %d rows to %s", time.time() - t_start, len(result), OUT_PATH)


if __name__ == "__main__":
    main()