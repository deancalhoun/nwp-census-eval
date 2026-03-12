"""
scripts/config.py — Centralised configuration for the nwp-census-eval pipeline.

Override the root path without editing source:
    export NWP_SCRATCH=/my/scratch && python scripts/run_pipeline.py
"""
import os

SCRATCH = os.environ.get("NWP_SCRATCH", "/glade/derecho/scratch/dcalhoun")

# ECMWF model directories
IFS_BASE_DIR  = os.path.join(SCRATCH, "ecmwf/ifs")
AIFS_BASE_DIR = os.path.join(SCRATCH, "ecmwf/aifs")

IFS_FC_DIR   = os.path.join(IFS_BASE_DIR,  "fc/0.125/2t")
IFS_AN_DIR   = os.path.join(IFS_BASE_DIR,  "an/0.125/2t")
AIFS_FC_DIR  = os.path.join(AIFS_BASE_DIR, "fc/0.25/2t")  # 0.25° grid

# ERA5 raw input directory
ERA5_DIR = os.path.join(SCRATCH, "ecmwf/era5/era5_2t")

# Census / shapefile paths
SHAPEFILE_PATH = os.path.join(
    SCRATCH,
    "census/shapefiles/nhgis0003_shapefile_tl2024_us_county_2024/US_county_2024.shp",
)
ACS_DIR = os.path.join(SCRATCH, "census/data/acs5")

# Aggregated output directory — ALL processed parquets go here, including ERA5 climatology
AGGREGATED_DIR = os.path.join(SCRATCH, "aggregated")
ERA5_CLIM_PATH = os.path.join(
    AGGREGATED_DIR, "era5_2t_county_climatology_1991_2020.parquet"
)

# Date ranges
IFS_START  = "2016-01-01"
IFS_END    = "2025-12-31"
AIFS_START = "2024-03-01"
AIFS_END   = "2025-12-31"

ERA5_CLIM_START = "1991-01-01"
ERA5_CLIM_END   = "2020-12-31"

# Forecast parameters
PARAM      = ("2t", "167.128")   # (shortName, MARS code)
LEAD_TIMES = [0, 6, 12, 18, 24, 36, 48, 60, 72, 84, 96, 108, 120, 168, 240]
INIT_HOURS = ["0000", "1200"]
CONUS_BOUNDS = ["49.5", "-125", "24.5", "-66.5"]   # [N, W, S, E]
IFS_GRID   = "0.125"
AIFS_GRID  = "0.25"

# ACS census defaults
ACS_YEAR   = 2024
ACS_LEVEL  = "county"
ACS_TABLES = [
    # Age structure — thermal vulnerability (% elderly 65+, % children <5)
    "B01001",
    # Race/ethnicity — environmental justice; mutually exclusive NH-White/Black/Hispanic/Asian/Other
    "B03002",
    # Education — % without HS diploma; social vulnerability / adaptive capacity
    "B15003",
    # Poverty — % below poverty line; socioeconomic vulnerability
    "B17001",
    # Median household income — economic resources for heating/cooling
    "B19013",
    # Gini index of income inequality — distribution, not just median
    "B19083",
    # Employment status — unemployment rate; outdoor worker exposure
    "B23025",
    # Housing unit count — urbanization proxy (combined with shapefile area for density)
    "B25001",
    # Tenure — % renters; reduced control over building thermal quality
    "B25003",
    # Median year structure built — housing age → insulation quality / AC absence
    "B25035",
    # House heating fuel — % using wood/kerosene/none; inadequate heating exposure
    "B25040",
    # Disability status — physical vulnerability to temperature extremes
    "B18101",
    # Health insurance coverage — % uninsured; healthcare access post heat/cold event
    "B27001",
    # Household language / English ability — % linguistically isolated from warnings
    "C16002",
    # Internet subscriptions — % without broadband; cannot access digital forecasts
    "B28002",
]
