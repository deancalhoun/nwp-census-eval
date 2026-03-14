# nwp-census-eval

Human-centered NWP model validation and benchmarking. Compares ECMWF IFS and AIFS 2m
temperature forecasts against analyses, aggregated to U.S. Census counties and evaluated
relative to ERA5 climatology. Forecast errors are linked to census sociodemographics via
a Bayesian BYM2 spatial model to assess whether forecast quality is equitably distributed
across population groups.

Designed to run on NCAR GLADE (Derecho HPC). Default data paths point to
`/glade/derecho/scratch/dcalhoun/`; override with the `NWP_SCRATCH` environment variable.

---

## Environment setup

```bash
conda env create -f environment.yml
conda activate nwp-census-eval
```

External dependency: `ecCodes` (`grib_filter`, `grib_to_netcdf`) must be on `PATH` for
GRIB post-processing in `ECMWFDataClient`.

---

## Configuration

| Variable | Purpose |
|---|---|
| `NWP_SCRATCH` | Root scratch directory (default: `/glade/derecho/scratch/dcalhoun`) |
| `CENSUS_API_KEY` | U.S. Census Bureau API key (or `~/.censusdis/api_key.txt`) |
| `~/.ecmwfapirc` | ECMWF MARS credentials (standard file, required for downloads) |

All derived paths are defined in `scripts/config.py`.

---

## Running the pipeline

```bash
# Full pipeline (all steps in order)
python scripts/run_pipeline.py

# Subset of steps
python scripts/run_pipeline.py --steps aggregate-era5 aggregate-fc

# Pass extra flags to the last step (after --)
python scripts/run_pipeline.py --steps aggregate-fc -- --n-parallel 8 --start 2024-01-01

# Single step
python scripts/run_pipeline.py --steps figures
```

Pipeline steps (run in dependency order):

| Step | Script | Description |
|---|---|---|
| `download` | `download_fc_an_2t.py` | ECMWF IFS/AIFS forecasts and analyses via MARS |
| `aggregate-era5` | `aggregate_era5_2t.py` | ERA5 county aggregation + climatology |
| `aggregate-fc` | `aggregate_fc_an_2t.py` | IFS/AIFS spatial aggregation to county monthly parquets |
| `compute-derived` | `compute_derived_2t.py` | Bias, anomaly, and model comparison tables |
| `koppen` | `aggregate_koppen.py` | Koppen-Geiger climate classification by county |
| `acs` | `download_acs.py` | Census ACS 5-year demographic estimates |
| `validate` | `validate_pipeline.py` | Pipeline status report (OK/PARTIAL/MISSING) |
| `figures` | `figures/run_figures.py` | Publication figure scripts |

---

## Analysis workflow

The analysis stage runs after the pipeline is complete. Steps must be run in order:

1. **Explore bias** — run `notebooks/01_explore_bias.ipynb`
2. **Build adjacency matrix** — run `notebooks/02_build_adjacency.ipynb`
   (writes `notebooks/data/adjacency_W.npz`, `notebooks/data/node_order.csv`)
3. **Prepare model data** — run `notebooks/03_prep_model_data.ipynb`
   (writes `notebooks/data/model_input.parquet`)
4. **Fit BYM2 model** — run manually (not a pipeline step):
   ```bash
   # Default: lead time = 24h. Override with BYM2_LEAD_TIME env var.
   BYM2_LEAD_TIME=48 python scripts/run_bym2.py
   ```
   Writes `notebooks/data/bym2_trace_lead{N}h.nc`.
5. **Diagnostics** — run `notebooks/05_diagnostics.ipynb`
6. **Visualize results** — run `notebooks/06_visualize_results.ipynb`

---

## Project structure

```
nwp-census-eval/
├── nwp_census_eval/          # Python package
│   ├── aggregate.py          # GeoAggregator, ForecastAggregator, AnalysisAggregator
│   ├── db.py                 # PipelineDB — DuckDB query layer over aggregated parquets
│   └── data/                 # ECMWFDataClient, download_acs()
├── scripts/
│   ├── config.py             # All paths and parameters (single source of truth)
│   ├── run_pipeline.py       # Unified pipeline entry point
│   ├── download_fc_an_2t.py  # ECMWF MARS download
│   ├── aggregate_era5_2t.py  # ERA5 aggregation + climatology
│   ├── aggregate_fc_an_2t.py # IFS/AIFS spatial aggregation
│   ├── compute_derived_2t.py # Bias, anomaly, comparison tables
│   ├── aggregate_koppen.py   # Koppen-Geiger classification
│   ├── download_acs.py       # Census ACS download
│   ├── validate_pipeline.py  # Pipeline status report
│   ├── run_bym2.py           # BYM2 spatial model (manual step; requires notebooks 01–03)
│   └── figures/
│       ├── run_figures.py    # Wrapper: runs all figure scripts
│       ├── bias_map.py       # County choropleth of mean bias
│       ├── anomaly_seasonal.py   # Seasonal anomaly heatmaps
│       ├── demographic_scatter.py # Bias vs climate zone scatter
│       └── ifs_vs_aifs.py    # IFS vs AIFS skill comparison panels
├── notebooks/
│   ├── 01_explore_bias.ipynb
│   ├── 02_build_adjacency.ipynb
│   ├── 03_prep_model_data.ipynb
│   ├── 05_diagnostics.ipynb
│   └── 06_visualize_results.ipynb
├── environment.yml
└── README.md
```

`notebooks/data/` and `notebooks/figures/` are derived outputs and are git-ignored.

---

## Data requirements

All raw data lives under `$NWP_SCRATCH` on GLADE:

```
$NWP_SCRATCH/
├── ecmwf/
│   ├── fc/{grid}/{param}/{init_hour}/{lead_time}/{year}/{month}/  ← IFS/AIFS fc NetCDF
│   └── an/{grid}/{param}/{year}/{month}/                          ← IFS analysis NetCDF
└── era5/
    └── 2t/{year}/                                                 ← ERA5 daily NetCDF
```

Aggregated outputs are written to `$NWP_SCRATCH/aggregated/`.
