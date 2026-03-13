"""
nwp_census_eval/db.py — DuckDB query layer for pipeline outputs.

Registers all pipeline parquets as lazy views; never loads full tables into
Python memory. Provides convenience methods for common queries.

Usage:
    from nwp_census_eval.db import PipelineDB

    db = PipelineDB()
    print(db.registered_views())
    df = db.query("SELECT geo_id, AVG(bias) FROM ifs_bias GROUP BY geo_id")
    df = db.query_bias(model="ifs", lead_times=[24, 48], start="2024-01-01")
    df = db.summary_stats(model="ifs")

    # Override path for local use (data synced from GLADE):
    db = PipelineDB(aggregated_dir="/local/path/to/aggregated")
"""

import glob as _glob
import os
import sys
from typing import List, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Default aggregated_dir from config (optional; graceful fallback)
# ---------------------------------------------------------------------------
try:
    _scripts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
    sys.path.insert(0, _scripts_dir)
    from config import AGGREGATED_DIR as _DEFAULT_AGGREGATED_DIR
except ImportError:
    _DEFAULT_AGGREGATED_DIR = os.path.join(
        os.environ.get("NWP_SCRATCH", "/glade/derecho/scratch/dcalhoun"), "aggregated"
    )

# ---------------------------------------------------------------------------
# View registry: view_name → parquet filename relative to aggregated_dir
# ---------------------------------------------------------------------------
_VIEWS = {
    "ifs_fc":      "ifs_fc_2t_county.parquet",
    "ifs_an":      "ifs_an_2t_county.parquet",
    "ifs_bias":    "ifs_fc_bias_2t_county.parquet",
    "ifs_anom":    "ifs_fc_bias_anom_2t_county.parquet",
    "aifs_fc":     "aifs_fc_2t_county.parquet",
    "aifs_bias":   "aifs_fc_bias_2t_county.parquet",
    "aifs_anom":   "aifs_fc_bias_anom_2t_county.parquet",
    "ifs_vs_aifs": "aifs_vs_ifs_fc_bias_comparison_2t_county.parquet",
    "era5_clim":   "era5_2t_county_climatology_1991_2020.parquet",
    "koppen":      "koppen_geiger_county.parquet",
}

_ERA5_MONTHLY_GLOB = "era5_monthly/era5_2t_county_*.parquet"


class PipelineDB:
    """
    Thin DuckDB wrapper that registers pipeline parquets as lazy views.

    Parameters
    ----------
    aggregated_dir : str, optional
        Path to the aggregated output directory. Defaults to the path from
        scripts/config.py (or NWP_SCRATCH env var + '/aggregated').
    """

    def __init__(self, aggregated_dir: Optional[str] = None):
        try:
            import duckdb
        except ImportError as exc:
            raise ImportError(
                "duckdb is required for PipelineDB. Install with: conda install duckdb"
            ) from exc
        self._duckdb = duckdb
        self._dir = aggregated_dir or _DEFAULT_AGGREGATED_DIR
        self._conn = duckdb.connect()
        self._registered: List[str] = []
        self._register_views()

    def _register_views(self) -> None:
        """Register all present parquets as DuckDB views."""
        for view_name, fname in _VIEWS.items():
            path = os.path.join(self._dir, fname)
            if os.path.exists(path):
                self._conn.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f"SELECT * FROM read_parquet('{path}')"
                )
                self._registered.append(view_name)

        # ERA5 monthly glob (enables querying raw monthly data)
        monthly_files = sorted(_glob.glob(os.path.join(self._dir, _ERA5_MONTHLY_GLOB)))
        if monthly_files:
            paths_sql = ", ".join(f"'{p}'" for p in monthly_files)
            self._conn.execute(
                f"CREATE OR REPLACE VIEW era5_monthly AS "
                f"SELECT * FROM read_parquet([{paths_sql}])"
            )
            self._registered.append("era5_monthly")

    def registered_views(self) -> List[str]:
        """Return the list of successfully registered view names."""
        return list(self._registered)

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a raw SQL query and return a pandas DataFrame."""
        return self._conn.execute(sql).df()

    def query_bias(
        self,
        model: str = "ifs",
        lead_times: Optional[List[int]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        geo_ids: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Query the bias table for a given model with optional filters.

        Parameters
        ----------
        model : "ifs" or "aifs"
        lead_times : list of int hours, or None for all
        start, end : ISO date strings for valid_time filter
        geo_ids : list of county FIPS codes, or None for all
        """
        view = f"{model}_bias"
        if view not in self._registered:
            raise ValueError(
                f"View '{view}' not registered. Run the aggregation pipeline first."
            )
        conditions = []
        if lead_times:
            lt_list = ", ".join(str(x) for x in lead_times)
            conditions.append(f"lead_time IN ({lt_list})")
        if start:
            conditions.append(f"valid_time >= '{start}'")
        if end:
            conditions.append(f"valid_time <= '{end}'")
        if geo_ids:
            ids_list = ", ".join(f"'{g}'" for g in geo_ids)
            conditions.append(f"geo_id IN ({ids_list})")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return self._conn.execute(f"SELECT * FROM {view} {where}").df()

    def summary_stats(self, model: str = "ifs") -> pd.DataFrame:
        """
        Compute mean bias, RMSE, MAE, and observation count per (geo_id, lead_time).

        Parameters
        ----------
        model : "ifs" or "aifs"
        """
        view = f"{model}_bias"
        if view not in self._registered:
            raise ValueError(f"View '{view}' not registered.")
        sql = f"""
            SELECT
                geo_id,
                lead_time,
                AVG(bias)              AS mean_bias,
                SQRT(AVG(bias * bias)) AS rmse,
                AVG(abs_error)         AS mae,
                COUNT(*)               AS n_obs
            FROM {view}
            GROUP BY geo_id, lead_time
            ORDER BY geo_id, lead_time
        """
        return self._conn.execute(sql).df()

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __repr__(self) -> str:
        return (
            f"PipelineDB(aggregated_dir={self._dir!r})\n"
            f"Registered views: {self._registered}"
        )
