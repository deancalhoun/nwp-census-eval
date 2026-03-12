import pandas as pd
import geopandas as gpd
import censusdis.data as ced

def _build_geo_id(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Build a standardized geo_id FIPS column from Census geography components."""
    if level == "county":
        if "STATE" in df.columns and "COUNTY" in df.columns:
            df = df.copy()
            df["geo_id"] = df["STATE"].str.zfill(2) + df["COUNTY"].str.zfill(3)
    elif level == "tract":
        if "STATE" in df.columns and "COUNTY" in df.columns and "TRACT" in df.columns:
            df = df.copy()
            df["geo_id"] = (
                df["STATE"].str.zfill(2)
                + df["COUNTY"].str.zfill(3)
                + df["TRACT"].str.zfill(6)
            )
    return df

def download_acs(
    year: int,
    groups: list[str],
    level: str = "county",
    state: str = "*",
    estimate_only: bool = True,
    with_geometry: bool = False,
) -> pd.DataFrame | gpd.GeoDataFrame:
    """Download ACS 5-year data via censusdis.

    Args:
        year: ACS vintage year (e.g. 2023).
        groups: List of ACS table codes (e.g. ["B01001", "B19013"]).
        level: Geographic level — "county" or "tract".
        state: State FIPS code string, or "*" for all states.
        estimate_only: If True, keep only estimate columns (suffix "E").
        with_geometry: If True, return a GeoDataFrame with polygon boundaries.

    Returns:
        DataFrame (or GeoDataFrame) with a standardized geo_id FIPS column.
    """
    geo_kwargs: dict = {"state": state}
    if level in ("county", "tract"):
        geo_kwargs["county"] = "*"
    if level == "tract":
        geo_kwargs["tract"] = "*"

    df = ced.download(
        dataset="acs/acs5",
        vintage=year,
        group=groups,
        with_geometry=with_geometry,
        **geo_kwargs,
    )

    if estimate_only:
        geo_col_names = {"NAME", "STATE", "COUNTY", "TRACT", "geometry"}
        geo_cols = [c for c in df.columns if c.upper() in geo_col_names or c in geo_col_names]
        est_cols = [c for c in df.columns if c.upper().endswith("E") and c not in geo_cols]
        df = df[geo_cols + est_cols]

    df = _build_geo_id(df, level)
    return df