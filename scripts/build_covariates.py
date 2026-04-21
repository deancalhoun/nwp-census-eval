"""
scripts/build_covariates.py — Assemble county covariate table for regression.

Transformations applied and stored:
  stations_norm    = log1p(stations_100km) normalized 0–1
  pop_density_norm = log1p(pop_density) normalized 0–1
  gradient_norm    = elev_gradient_300km / max(|min|,|max|)  [zero-preserving]
  stations_resid   = residual of stations_norm ~ pop_density_norm
  elderly_resid    = residual of pct_elderly_prop ~ pop_density_norm
  disabled_resid   = residual of pct_disabled_prop ~ pop_density_norm
  pct_*_prop       = pct_* / 100
  RPL_THEMES       = CDC/ATSDR SVI overall percentile rank, 0–1 (no transformation needed)

Regression domain notes:
  d0:  stations_norm + gradient_norm
  d00: stations_resid + gradient_norm + pop_density_norm
  d1:  d00 + pct_poverty_prop
  d2:  d00 + pct_black/hispanic/asian_prop (pct_white dropped — compositional VIF)
  d3:  d00 + pct_no_internet_prop + pct_non_english_prop
  d4:  d00 + elderly_resid + disabled_resid
  d5:  d00 + RPL_THEMES (CDC SVI overall composite)

Output:
    {AGGREGATED_DIR}/county_covariates.parquet

Usage:
    python scripts/build_covariates.py
"""

import logging
import os
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.linear_model import LinearRegression

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import AGGREGATED_DIR, ACS_DIR, ACS_YEAR, ACS_LEVEL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

OUT_PATH = os.path.join(AGGREGATED_DIR, "county_covariates.parquet")

CONUS_FIPS = {f"{i:02d}" for i in range(1, 57) if i not in (2, 15, 60, 64, 66, 69, 72, 74, 78)}

GPKG_PATH    = "/glade/derecho/scratch/dcalhoun/station_locations/conus_county_station_density.gpkg"
DENSITY_PATH = os.path.join(AGGREGATED_DIR, "county_station_density.parquet")
SVI_PATH     = "/glade/derecho/scratch/dcalhoun/SVI_2022_US_county.csv"

KOPPEN_CLASS_MAP = {
    **{k: "A" for k in [1, 2, 3]},
    **{k: "B" for k in [4, 5, 6, 7, 8, 9]},
    **{k: "C" for k in range(10, 20)},
    **{k: "D" for k in range(20, 32)},
    **{k: "E" for k in [32, 33]},
}

DIVISION_MAP = {
    "09": 1, "23": 1, "25": 1, "33": 1, "44": 1, "50": 1,
    "34": 2, "36": 2, "42": 2,
    "17": 3, "18": 3, "26": 3, "39": 3, "55": 3,
    "19": 4, "20": 4, "27": 4, "29": 4, "31": 4, "38": 4, "46": 4,
    "10": 5, "11": 5, "12": 5, "13": 5, "24": 5, "37": 5, "45": 5, "51": 5, "54": 5,
    "01": 6, "21": 6, "28": 6, "47": 6,
    "05": 7, "22": 7, "40": 7, "48": 7,
    "04": 8, "08": 8, "16": 8, "30": 8, "32": 8, "35": 8, "49": 8, "56": 8,
    "06": 9, "41": 9, "53": 9,
}

PCT_VARS = [
    "pct_poverty", "pct_white", "pct_black", "pct_hispanic", "pct_asian",
    "pct_no_internet", "pct_disabled", "pct_non_english", "pct_elderly",
]

ACS_PATH = os.path.join(ACS_DIR, f"acs_5yr_{ACS_YEAR}", f"acs_5yr_{ACS_YEAR}_{ACS_LEVEL}.parquet")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_station_density() -> pd.DataFrame:
    logging.info("Loading station density ...")
    df = pd.read_parquet(DENSITY_PATH, columns=["geo_id", "stations_100km"])
    df["log_stations"] = np.log1p(df["stations_100km"])
    return df[["geo_id", "stations_100km", "log_stations"]]


def load_area() -> pd.DataFrame:
    logging.info("Loading area ...")
    gdf = gpd.read_file(GPKG_PATH, columns=["GEOID", "ALAND"])
    df = gdf[["GEOID", "ALAND"]].rename(columns={"GEOID": "geo_id"}).copy()
    df["area_km2"] = df["ALAND"].astype(float) / 1e6
    return df[["geo_id", "area_km2"]]


def load_orography() -> pd.DataFrame:
    path = os.path.join(AGGREGATED_DIR, "orography_county.parquet")
    logging.info("Loading orography ...")
    return pd.read_parquet(path, columns=["geo_id", "elev_mean", "elev_gradient_300km"])


def load_koppen() -> pd.DataFrame:
    path = os.path.join(AGGREGATED_DIR, "koppen_geiger_county.parquet")
    logging.info("Loading Koppen ...")
    df = pd.read_parquet(path, columns=["geo_id", "category_1"])
    df["koppen_class"] = df["category_1"].map(KOPPEN_CLASS_MAP).fillna("D")
    return df[["geo_id", "koppen_class"]]


def load_svi() -> pd.DataFrame:
    logging.info("Loading SVI from %s ...", SVI_PATH)
    svi = pd.read_csv(SVI_PATH, usecols=["FIPS", "RPL_THEMES"])
    svi["geo_id"] = svi["FIPS"].astype(str).str.zfill(5)
    svi = svi[svi["RPL_THEMES"] != -999]  # drop missing flag just in case
    svi = svi.rename(columns={"RPL_THEMES": "SVI"})
    logging.info("SVI: %d counties, range [%.3f, %.3f]",
                 len(svi), svi["SVI"].min(), svi["SVI"].max())
    return svi[["geo_id", "SVI"]].reset_index(drop=True)


def load_acs() -> pd.DataFrame:
    logging.info("Loading ACS ...")
    acs = pd.read_parquet(ACS_PATH)
    if hasattr(acs, "geometry"):
        acs = pd.DataFrame(acs.drop(columns=["geometry"], errors="ignore"))

    acs = acs[acs["STATE"].isin(CONUS_FIPS)].copy()
    acs["geo_id"] = acs["STATE"].str.zfill(2) + acs["COUNTY"].str.zfill(3)

    def safe_pct(num, denom):
        n = pd.to_numeric(num, errors="coerce")
        d = pd.to_numeric(denom, errors="coerce")
        return np.where(d > 0, 100.0 * n / d, np.nan)

    pop = pd.to_numeric(acs["B01001_001E"], errors="coerce")

    acs["pct_poverty"]    = safe_pct(acs["B17001_002E"], acs["B17001_001E"])
    acs["median_income"]  = pd.to_numeric(acs["B19013_001E"], errors="coerce")
    acs["log_population"] = np.log1p(pop)

    total_race = pd.to_numeric(acs["B03002_001E"], errors="coerce")
    acs["pct_white"]    = safe_pct(acs["B03002_003E"], total_race)
    acs["pct_black"]    = safe_pct(acs["B03002_004E"], total_race)
    acs["pct_hispanic"] = safe_pct(acs["B03002_012E"], total_race)
    acs["pct_asian"]    = safe_pct(acs["B03002_006E"], total_race)

    acs["pct_no_internet"] = safe_pct(acs["B28002_013E"], acs["B28002_001E"])

    male_dis_cols   = ["B18101_004E","B18101_007E","B18101_010E",
                       "B18101_013E","B18101_016E","B18101_019E"]
    female_dis_cols = ["B18101_023E","B18101_026E","B18101_029E",
                       "B18101_032E","B18101_035E","B18101_038E"]
    disabled = sum(pd.to_numeric(acs[c], errors="coerce").fillna(0)
                   for c in male_dis_cols + female_dis_cols)
    acs["pct_disabled"] = safe_pct(disabled, acs["B18101_001E"])

    lep_cols = ["C16002_004E", "C16002_007E", "C16002_010E", "C16002_013E"]
    lep = sum(pd.to_numeric(acs[c], errors="coerce").fillna(0) for c in lep_cols)
    acs["pct_non_english"] = safe_pct(lep, acs["C16002_001E"])

    male_elderly_cols   = ["B01001_020E","B01001_021E","B01001_022E",
                           "B01001_023E","B01001_024E","B01001_025E"]
    female_elderly_cols = ["B01001_044E","B01001_045E","B01001_046E",
                           "B01001_047E","B01001_048E","B01001_049E"]
    elderly = sum(pd.to_numeric(acs[c], errors="coerce").fillna(0)
                  for c in male_elderly_cols + female_elderly_cols)
    acs["pct_elderly"] = safe_pct(elderly, pop)

    keep = [
        "geo_id",
        "pct_poverty", "median_income", "log_population",
        "pct_white", "pct_black", "pct_hispanic", "pct_asian",
        "pct_no_internet", "pct_disabled", "pct_non_english", "pct_elderly",
    ]
    return acs[keep].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Residualization helper
# ---------------------------------------------------------------------------

def residualize(df: pd.DataFrame, y_col: str, x_col: str, out_col: str) -> pd.DataFrame:
    mask  = df[y_col].notna() & df[x_col].notna()
    X     = df.loc[mask, x_col].values.reshape(-1, 1)
    y     = df.loc[mask, y_col].values
    lr    = LinearRegression().fit(X, y)
    resid = np.full(len(df), np.nan)
    resid[mask] = y - lr.predict(X)
    df[out_col] = resid
    logging.info(
        "%s = residual(%s ~ %s): coef=%.4f intercept=%.4f range [%.4f, %.4f]",
        out_col, y_col, x_col,
        lr.coef_[0], lr.intercept_,
        np.nanmin(resid), np.nanmax(resid),
    )
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    df_stations  = load_station_density()
    df_area      = load_area()
    df_orography = load_orography()
    df_koppen    = load_koppen()
    df_acs       = load_acs()
    df_svi       = load_svi()

    df = df_stations.copy()
    df = df.merge(df_area,      on="geo_id", how="left")
    df = df.merge(df_orography, on="geo_id", how="left")
    df = df.merge(df_koppen,    on="geo_id", how="left")
    df = df.merge(df_acs,       on="geo_id", how="left")
    df = df.merge(df_svi,       on="geo_id", how="left")

    df["pop_density"] = df["log_population"] / np.log(df["area_km2"].clip(lower=1))
    df["division"]    = df["geo_id"].str[:2].map(DIVISION_MAP)

    df = df[df["geo_id"].str[:2].isin(CONUS_FIPS)].reset_index(drop=True)

    # ---------------------------------------------------------------------------
    # Transformations
    # ---------------------------------------------------------------------------

    for raw, norm in [
        ("stations_100km", "stations_norm"),
        ("pop_density",    "pop_density_norm"),
    ]:
        log_vals = np.log1p(df[raw])
        df[norm] = (log_vals - log_vals.min()) / (log_vals.max() - log_vals.min())
        logging.info("%s: log1p range [%.4f, %.4f]", norm, log_vals.min(), log_vals.max())

    g     = df["elev_gradient_300km"]
    scale = max(abs(g.min()), abs(g.max()))
    df["gradient_norm"] = g / scale
    logging.info("gradient_norm: scale=%.2f", scale)

    for col in PCT_VARS:
        df[f"{col}_prop"] = df[col] / 100.0

    df = residualize(df, "stations_norm",     "pop_density_norm", "stations_resid")
    df = residualize(df, "pct_elderly_prop",  "pop_density_norm", "elderly_resid")
    df = residualize(df, "pct_disabled_prop", "pop_density_norm", "disabled_resid")

    # ---------------------------------------------------------------------------
    # Final column order
    # ---------------------------------------------------------------------------
    df = df[[
        "geo_id",
        # Raw
        "stations_100km", "log_stations",
        "elev_mean", "elev_gradient_300km",
        "koppen_class", "division",
        "pct_poverty", "median_income", "log_population", "pop_density",
        "pct_white", "pct_black", "pct_hispanic", "pct_asian",
        "pct_no_internet", "pct_disabled", "pct_non_english", "pct_elderly",
        "SVI",
        # Transformed
        "stations_norm", "pop_density_norm", "gradient_norm",
        "stations_resid", "elderly_resid", "disabled_resid",
        "pct_poverty_prop", "pct_white_prop", "pct_black_prop", "pct_hispanic_prop",
        "pct_asian_prop", "pct_no_internet_prop", "pct_disabled_prop",
        "pct_non_english_prop", "pct_elderly_prop",
    ]]

    logging.info("County covariates: %d rows x %d columns", *df.shape)
    logging.info("Koppen distribution:\n%s", df["koppen_class"].value_counts().to_string())
    logging.info("Division distribution:\n%s",
                 df["division"].value_counts().sort_index().to_string())
    logging.info("SVI: %d non-null, range [%.3f, %.3f]",
                 df["SVI"].notna().sum(),
                 df["SVI"].min(), df["SVI"].max())

    null_counts = df.isnull().sum()
    if null_counts.any():
        logging.warning("Null counts:\n%s", null_counts[null_counts > 0].to_string())
    else:
        logging.info("No nulls found.")

    tmp = OUT_PATH + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, OUT_PATH)
    logging.info("Saved %d rows → %s", len(df), OUT_PATH)


if __name__ == "__main__":
    main()