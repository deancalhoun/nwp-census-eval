"""
scripts/run_regression.py — Fit bias and MAE regressions with bootstrap inference.

Specification
-------------
Outcomes:  bias, mae
Periods:   ifs_full, ifs_common, aifs_common
Domains:   d0, d00, d1, d2, d3, d4, d5

Aggregation: county × month × valid_hour × lead_time means (~895K rows)

Formula (d0):
    outcome ~ stations_norm + gradient_norm
            + cos_month + sin_month + lead_norm + valid_hour_12
            + C(koppen_class, Treatment('C'))
            + C(division, Treatment(9))

Formula (d00):
    outcome ~ stations_resid + gradient_norm + pop_density_norm
            + cos_month + sin_month + lead_norm + valid_hour_12
            + C(koppen_class, Treatment('C'))
            + C(division, Treatment(9))

d1  adds: pct_poverty_prop
d2  adds: pct_black_prop, pct_hispanic_prop, pct_asian_prop
d3  adds: pct_no_internet_prop, pct_non_english_prop
d4  adds: elderly_resid, disabled_resid
d5  adds: RPL_THEMES  (CDC/ATSDR SVI overall composite, 0–1 percentile rank)
(d1-d5 all use d00 base)

Variable notes:
  stations_resid  = residual of stations_norm ~ pop_density_norm
  elderly_resid   = residual of pct_elderly_prop ~ pop_density_norm
  disabled_resid  = residual of pct_disabled_prop ~ pop_density_norm
  pct_white_prop  dropped from d2 (compositional VIF ~17)
  median_income   dropped — pct_poverty_prop used instead
  RPL_THEMES      already 0–1, no transformation needed

Categorical FE:
  koppen_class: reference = C (temperate)
  division:     reference = 9 (Pacific)

Total: 2 outcomes × 3 periods × 7 domains = 42 regressions

Outputs in AGGREGATED_DIR/regression_results_v2/:
    {outcome}_{period}_{domain}.parquet
    Columns: term, coef, se_boot, ci_lower, ci_upper, pval_boot, r2, r2_adj, n_obs

Usage:
    python scripts/run_regression.py [--n-boot 500] [--n-jobs 32]
    python scripts/run_regression.py --outcome bias --period ifs_full --domain d0 --n-boot 10 --n-jobs 4
"""

import argparse
import logging
import multiprocessing as mp
import os
import sys
import time
import warnings

import duckdb
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

warnings.filterwarnings("ignore")

os.environ["OMP_NUM_THREADS"]      = "1"
os.environ["MKL_NUM_THREADS"]      = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"]  = "1"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import AGGREGATED_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RESULTS_DIR = os.path.join(AGGREGATED_DIR, "regression_results_v2")
COV_PATH    = os.path.join(AGGREGATED_DIR, "county_covariates.parquet")

N_BOOT = 500
N_JOBS = 32
RNG    = np.random.default_rng(42)

BIAS_PERIODS = {
    "ifs_full":    os.path.join(AGGREGATED_DIR, "bias_ifs_full.parquet"),
    "ifs_common":  os.path.join(AGGREGATED_DIR, "bias_ifs_common.parquet"),
    "aifs_common": os.path.join(AGGREGATED_DIR, "bias_aifs_common.parquet"),
}

FE       = "C(koppen_class, Treatment('C')) + C(division, Treatment(9))"
TEMPORAL = "cos_month + sin_month + lead_norm + valid_hour_12"

BASE_D0   = f"stations_norm + gradient_norm + {TEMPORAL}"
BASE_D1D5 = f"stations_resid + gradient_norm + pop_density_norm + {TEMPORAL}"

DOMAINS = {
    "d0":  {"base": BASE_D0,   "demo": ""},
    "d00": {"base": BASE_D1D5, "demo": ""},
    "d1":  {"base": BASE_D1D5, "demo": "SVI"},
    "d2":  {"base": BASE_D1D5, "demo": "pct_poverty_prop"},
    "d3":  {"base": BASE_D1D5, "demo": "pct_black_prop + pct_hispanic_prop + pct_asian_prop"},
    "d4":  {"base": BASE_D1D5, "demo": "pct_no_internet_prop + pct_non_english_prop"},
    "d5":  {"base": BASE_D1D5, "demo": "elderly_resid + disabled_resid"},
}

# ---------------------------------------------------------------------------
# Global state for fork-based parallelism
# ---------------------------------------------------------------------------
_GLOBAL_DF       = None
_GLOBAL_FORMULA  = None
_GLOBAL_PARAMS   = None
_GLOBAL_COUNTIES = None


def _init_globals(df, formula, param_names):
    global _GLOBAL_DF, _GLOBAL_FORMULA, _GLOBAL_PARAMS, _GLOBAL_COUNTIES
    _GLOBAL_DF       = df
    _GLOBAL_FORMULA  = formula
    _GLOBAL_PARAMS   = param_names
    _GLOBAL_COUNTIES = df["geo_id"].unique()


def _one_boot(seed: int) -> list:
    rng        = np.random.default_rng(seed)
    counties   = _GLOBAL_COUNTIES
    n_counties = len(counties)
    sampled    = rng.choice(counties, size=n_counties, replace=True)
    frames     = []
    for i, county in enumerate(sampled):
        sub = _GLOBAL_DF[_GLOBAL_DF["geo_id"] == county].copy()
        sub["geo_id"] = f"boot_{i}"
        frames.append(sub)
    df_boot = pd.concat(frames, ignore_index=True)
    try:
        result = smf.ols(_GLOBAL_FORMULA, data=df_boot).fit()
        return [result.params.get(p, np.nan) for p in _GLOBAL_PARAMS]
    except Exception:
        return [np.nan] * len(_GLOBAL_PARAMS)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_covariates() -> pd.DataFrame:
    cov = pd.read_parquet(COV_PATH)
    cov = cov.dropna(subset=["median_income"]).reset_index(drop=True)
    logging.info(
        "Covariates: %d counties | stations_norm [%.3f, %.3f] | "
        "stations_resid [%.3f, %.3f] | SVI [%.3f, %.3f]",
        len(cov),
        cov["stations_norm"].min(),  cov["stations_norm"].max(),
        cov["stations_resid"].min(), cov["stations_resid"].max(),
        cov["SVI"].min(),            cov["SVI"].max(),
    )
    return cov


def aggregate_bias_mae(path: str, cov: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='32GB'; SET threads=8;")
    df = con.execute(f"""
        SELECT
            geo_id,
            month,
            valid_hour,
            lead_time,
            AVG(bias)      AS bias,
            AVG(abs_error) AS mae
        FROM read_parquet('{path}')
        GROUP BY geo_id, month, valid_hour, lead_time
    """).df()
    con.close()

    df = df.merge(cov, on="geo_id", how="inner")

    df["cos_month"]     = np.cos(2 * np.pi * df["month"] / 12)
    df["sin_month"]     = np.sin(2 * np.pi * df["month"] / 12)
    df["lead_norm"]     = (df["lead_time"] - 12) / (240 - 12)
    df["valid_hour_12"] = (df["valid_hour"] == 12).astype(int)

    return df


# ---------------------------------------------------------------------------
# Formula construction
# ---------------------------------------------------------------------------

def build_formula(outcome: str, domain: str) -> str:
    spec = DOMAINS[domain]
    if spec["demo"]:
        return f"{outcome} ~ {spec['base']} + {spec['demo']} + {FE}"
    else:
        return f"{outcome} ~ {spec['base']} + {FE}"


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def bootstrap_coefs(df, formula, param_names, n_boot, rng, n_jobs):
    _init_globals(df, formula, param_names)
    seeds = rng.integers(0, 2**31, size=n_boot).tolist()
    ctx   = mp.get_context("fork")
    with ctx.Pool(processes=n_jobs) as pool:
        results = pool.map(_one_boot, seeds)
    return np.array(results)


# ---------------------------------------------------------------------------
# Fitting and results
# ---------------------------------------------------------------------------

def fit_ols(df, formula):
    return smf.ols(formula, data=df).fit()


def assemble_results(result, boot_coefs, param_names, n_obs):
    coefs    = result.params
    se_boot  = np.nanstd(boot_coefs, axis=0, ddof=1)
    ci_lower = np.nanpercentile(boot_coefs, 2.5,  axis=0)
    ci_upper = np.nanpercentile(boot_coefs, 97.5, axis=0)
    pval     = np.array([
        2 * min(
            np.nanmean(boot_coefs[:, i] <= 0) if coefs.get(p, 0) > 0
            else np.nanmean(boot_coefs[:, i] >= 0),
            1.0,
        )
        for i, p in enumerate(param_names)
    ])
    return pd.DataFrame({
        "term":      param_names,
        "coef":      [coefs.get(p, np.nan) for p in param_names],
        "se_boot":   se_boot,
        "ci_lower":  ci_lower,
        "ci_upper":  ci_upper,
        "pval_boot": pval,
        "r2":        result.rsquared,
        "r2_adj":    result.rsquared_adj,
        "n_obs":     n_obs,
    })


# ---------------------------------------------------------------------------
# Run one regression
# ---------------------------------------------------------------------------

def run_one(outcome, period, domain, df, n_boot, n_jobs, out_dir):
    out_path = os.path.join(out_dir, f"{outcome}_{period}_{domain}.parquet")
    if os.path.exists(out_path):
        logging.info("Already exists: %s; skipping.", out_path)
        return

    formula = build_formula(outcome, domain)
    logging.info("=== %s | %s | %s ===", outcome.upper(), period, domain)
    logging.info("Formula: %s", formula)
    logging.info("N obs: %d | n_boot: %d | n_jobs: %d", len(df), n_boot, n_jobs)

    t0     = time.time()
    result = fit_ols(df, formula)
    params = result.params.index.tolist()
    logging.info(
        "[%.0fs] Point estimate: R²=%.4f R²_adj=%.4f n_params=%d",
        time.time() - t0, result.rsquared, result.rsquared_adj, len(params),
    )

    t0       = time.time()
    boot_arr = bootstrap_coefs(df, formula, params, n_boot, RNG, n_jobs)
    logging.info("[%.0fs] Bootstrap complete.", time.time() - t0)

    df_out = assemble_results(result, boot_arr, params, len(df))
    tmp    = out_path + ".tmp"
    df_out.to_parquet(tmp, index=False)
    os.replace(tmp, out_path)
    logging.info("Saved → %s", out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(outcome_filter, period_filter, domain_filter, n_boot, n_jobs):
    t_start = time.time()
    logging.info(
        "=== run_regression.py v2 | start %s | n_boot=%d | n_jobs=%d ===",
        time.strftime("%Y-%m-%d %H:%M:%S"), n_boot, n_jobs,
    )
    os.makedirs(RESULTS_DIR, exist_ok=True)

    cov = load_covariates()

    logging.info("Aggregating bias and MAE ...")
    datasets = {}
    for period, path in BIAS_PERIODS.items():
        datasets[period] = aggregate_bias_mae(path, cov)
        logging.info("  %s: %d rows", period, len(datasets[period]))

    outcomes = ["bias", "mae"]
    periods  = ["ifs_full", "ifs_common", "aifs_common"]
    domains  = ["d0", "d00", "d1", "d2", "d3", "d4", "d5"]

    for domain in domains:
        if domain_filter and domain != domain_filter:
            continue
        for outcome in outcomes:
            if outcome_filter and outcome != outcome_filter:
                continue
            for period in periods:
                if period_filter and period != period_filter:
                    continue
                df = datasets[period]
                run_one(outcome, period, domain, df, n_boot, n_jobs, RESULTS_DIR)

    logging.info("[%.0fs] All regressions complete.", time.time() - t_start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outcome", default=None, choices=["bias", "mae"])
    parser.add_argument("--period",  default=None,
                        choices=["ifs_full", "ifs_common", "aifs_common"])
    parser.add_argument("--domain",  default=None,
                        choices=["d0", "d00", "d1", "d2", "d3", "d4", "d5"])
    parser.add_argument("--n-boot",  type=int, default=N_BOOT)
    parser.add_argument("--n-jobs",  type=int, default=N_JOBS)
    args = parser.parse_args()
    main(args.outcome, args.period, args.domain, args.n_boot, args.n_jobs)