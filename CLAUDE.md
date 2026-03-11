# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Human-centered NWP (Numerical Weather Prediction) model validation and benchmarking. Compares ECMWF IFS and AIFS 2m temperature forecasts against analyses, aggregated to U.S. census geographies (counties, tracts) and evaluated relative to ERA5 climatology. Designed to run on NCAR GLADE (Derecho HPC); data paths are hardcoded to `/glade/derecho/scratch/dcalhoun/`.

## Environment Setup

```bash
conda env create -f environment.yml
conda activate nwp-census-eval
```

External system dependency: `ecCodes` (`grib_filter`, `grib_to_netcdf`) must be available on PATH for GRIB post-processing in `ECMWFDataClient`.

Required environment variables:
- `CENSUS_API_KEY` — U.S. Census Bureau API key; used by `censusdis` (reads from env var or `~/.censusdis/api_key.txt`)
- ECMWF MARS credentials — standard `~/.ecmwfapirc` file (used by `ECMWFDataClient`)

## Running Scripts

```bash
# Download ECMWF IFS/AIFS forecast and analysis data
python scripts/download_fc_an_2t.py [--max-concurrent-requests N] [--validate] [--verbose]

# Download ACS census data
python scripts/download_acs.py

# Aggregate ERA5 to US counties and compute 1991-2020 climatology
python scripts/aggregate_era5_2t.py [--n-parallel N]

# Aggregate IFS/AIFS forecasts and analyses to counties; compute bias, errors, anomalies
python scripts/aggregate_fc_an_2t.py [--start YYYY-MM-DD] [--end YYYY-MM-DD] \
    [--n-parallel N] [--restore-fc-from-bias]
```

## Architecture

### Package: `nwp_census_eval/`

**`aggregate.py`** — Core spatial aggregation classes using `xagg` for area-weighted pixel-to-polygon aggregation:
- `GeoAggregator` — base class; builds a `weightmap` from a shapefile and sample grid once, then reuses it across files
- `ForecastAggregator(GeoAggregator)` — aggregates `(path, init_time, lead_time)` tuples; produces `geo_id / valid_time / init_time / lead_time / t2m` tables
- `AnalysisAggregator(GeoAggregator)` — aggregates `(path, time)` tuples
- `CategoricalAggregator(GeoAggregator)` — aggregates categorical gridded fields to top-2 category + percentage per polygon
- All subclasses have a `from_GeoAggregator()` classmethod to reuse an already-computed weightmap without redoing the expensive pixel overlap calculation

**`data.py`** — Data retrieval clients:
- `ECMWFDataClient` — downloads IFS/AIFS forecasts and analyses from ECMWF MARS API; handles concurrent requests, GRIB→NetCDF conversion, file sorting into `{year}/{month}/` subdirectories, and idempotent skip-if-exists logic
- `download_acs()` — thin wrapper around `censusdis.data.download()` for ACS 5-year estimates; supports `county` and `tract` levels, estimate-only column filtering, and optional `with_geometry=True` for GeoDataFrame output
- Standalone functions `retrieve_forecast_data`, `retrieve_analysis_data` wrap `ECMWFDataClient` for simple one-off use

### Scripts

Scripts in `scripts/` use `sys.path.insert` to import from the repo root.

`scripts/aggregate_fc_an_2t.py` is self-contained with its own file discovery (`build_fc_files`, `build_an_files`), alignment (`align_fc_an`), parallel processing (`ProcessPoolExecutor` with `fork` context to inherit the weightmap), and monthly checkpointing to parquet. Outputs include sidecar `.meta.json` files alongside each parquet.

### Data Pipeline

1. **Download** — ECMWF MARS → GRIB files split by `grib_filter` into `fc/{grid}/{param}/{init_hour}/{lead_time}/{year}/{month}/` and `an/{grid}/{param}/{year}/{month}/`, then converted to NetCDF
2. **ERA5 climatology** — aggregate ERA5 daily means to counties, groupby `(geo_id, day_of_year)` → `era5_2t_county_climatology_1991_2020.parquet`
3. **IFS/AIFS aggregation** — area-weighted aggregation to counties → parquet tables for forecasts, analyses, bias (`fc - an`), and anomalies (`fc/an - ERA5 climatology`)
4. **Census** — ACS 5-year estimates downloaded per state, joined into a single table

### Key Design Patterns

- Weightmap is computed once per grid+shapefile pair and reused — the expensive step is `xagg.pixel_overlaps()`; passing `from_GeoAggregator()` avoids recomputing it
- `xagg.fix_ds()` is called on every dataset before aggregation to normalize coordinate names to `lat`/`lon` and convert 0–360 longitudes to −180–180
- Aggregation scripts use `mp.get_context("fork")` so worker processes inherit the in-memory weightmap without pickling
- Parquet writes use atomic rename (`write to .tmp`, then `os.rename`) so interrupted runs leave a recoverable checkpoint
