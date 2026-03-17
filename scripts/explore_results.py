#!/usr/bin/env python3
"""
explore_results.py — Comprehensive exploratory analysis of NWP 2m temperature bias.

Sections
--------
  §1  Skill by lead time (IFS)
  §2  IFS vs AIFS comparison (common period only)
  §3  Monthly timeseries — bias, MAE, ACC
  §4  Seasonal cycle by lead time (one line per lead)
  §5  Seasonal maps — bias, MAE, forecast/analysis anomalies
  §6  Bias and error histograms
  §7  Koppen-Geiger climate region interactions
  §8  Demographic interactions (census)
  §9  Summary table (CSV)
  §10 MSE decomposition maps (3×3 panel, bias sign × anomaly sign)
  §11 Joint PDF — observed anomaly vs forecast bias

All spatial means are area-weighted using the `aland` column embedded in
every bias/anom parquet.  ACC is pooled across counties and dates (fast
first-order estimate); per-county ACC maps are in the seasonal anomaly section.

Usage
-----
    python scripts/explore_results.py [--out-dir PATH]

Outputs
-------
    {OUT_DIR}/
        01_skill_lead/
        02_model_comparison/
        03_timeseries/
        04_seasonal_cycle/
        05_seasonal_maps/
        06_histograms/
        07_climate_regions/
        08_demographics/
        10_mse_decomp/
        11_joint_pdf/
        summary.csv
"""

import argparse
import os
import sys
import traceback
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.cm as mcm
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import geopandas as gpd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from nwp_census_eval.db import PipelineDB
from config import SHAPEFILE_PATH

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONUS_XLIM = (-125, -66.5)
CONUS_YLIM = (24.5, 49.5)
MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
RDBU      = "RdBu_r"
REDS      = "YlOrRd"
PDF_BIN_K = 0.5   # bin width (K) for §11 joint PDF

# Beck et al. (2018) 30-class Koppen-Geiger integer codes
KOPPEN_NAMES = {
    1:  "Af — Tropical rainforest",
    2:  "Am — Tropical monsoon",
    3:  "Aw — Tropical savanna",
    4:  "BWh — Hot desert",
    5:  "BWk — Cold desert",
    6:  "BSh — Hot steppe",
    7:  "BSk — Cold steppe",
    8:  "Csa — Mediterranean hot-summer",
    9:  "Csb — Mediterranean warm-summer",
    14: "Cfa — Humid subtropical",
    15: "Cfb — Oceanic",
    17: "Dsa — Cont. dry hot-summer",
    18: "Dsb — Cont. dry warm-summer",
    19: "Dsc — Cont. dry cold-summer",
    21: "Dwa — Cont. dry-winter hot-summer",
    22: "Dwb — Cont. dry-winter warm-summer",
    23: "Dwc — Cont. dry-winter cold-summer",
    25: "Dfa — Humid continental hot-summer",
    26: "Dfb — Humid continental warm-summer",
    27: "Dfc — Subarctic",
    29: "ET — Tundra",
}

def koppen_label(code):
    """Return a human-readable label for a Koppen integer code."""
    try:
        return KOPPEN_NAMES.get(int(code), str(code))
    except (TypeError, ValueError):
        return str(code)

def _season_sql(time_col="valid_time"):
    return f"""CASE
        WHEN MONTH({time_col}) IN (12, 1, 2) THEN 'DJF'
        WHEN MONTH({time_col}) IN (3,  4, 5) THEN 'MAM'
        WHEN MONTH({time_col}) IN (6,  7, 8) THEN 'JJA'
        ELSE                                      'SON'
    END"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def savefig(fig, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    → {path}")


def county_map(ax, gdf, col, cmap, vmin, vmax, title, cbar_label):
    """Plot a county choropleth on *ax* clipped to CONUS."""
    gdf.plot(
        column=col, ax=ax, cmap=cmap, vmin=vmin, vmax=vmax,
        linewidth=0.05, edgecolor="black",
        missing_kwds={"color": "#cccccc"},
    )
    ax.set_xlim(*CONUS_XLIM)
    ax.set_ylim(*CONUS_YLIM)
    ax.axis("off")
    ax.set_title(title, fontsize=9)
    sm = plt.cm.ScalarMappable(
        cmap=cmap, norm=mcolors.Normalize(vmin=vmin, vmax=vmax))
    sm.set_array([])
    plt.colorbar(sm, ax=ax, orientation="horizontal",
                 pad=0.02, fraction=0.046, label=cbar_label)


def prep_geo(df):
    df = df.copy()
    df["geo_id"] = df["geo_id"].astype(str).str.zfill(5)
    return df


def aw_mean(df, col):
    """Area-weighted mean using aland column."""
    return (df[col] * df["aland"]).sum() / df["aland"].sum()


def section(title):
    bar = "─" * 60
    print(f"\n{bar}\n  {title}\n{bar}")


def try_section(name, fn):
    try:
        fn()
    except Exception:
        print(f"  WARNING: section failed — {name}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out-dir", default="figures/explore",
                   help="Output root (default: figures/explore)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    OUT  = args.out_dir
    os.makedirs(OUT, exist_ok=True)

    # ── Connect ──────────────────────────────────────────────────────────────
    print("Connecting to pipeline DB …")
    db = PipelineDB()
    db._conn.execute("SET memory_limit='24GB'; SET threads=8;")
    views = set(db.registered_views())
    print(f"  views: {sorted(views)}")

    HAS_ANOM      = "ifs_anom"    in views
    HAS_AIFS      = "aifs_bias"   in views
    HAS_AIFS_ANOM = "aifs_anom"   in views
    HAS_VS        = "ifs_vs_aifs" in views
    HAS_KOPPEN    = "koppen"      in views

    def q(sql):
        return db.query(sql)

    # All available lead times — computed once, reused by §5–§8, §10, §11
    all_leads_bias = sorted(
        q("SELECT DISTINCT lead_time FROM ifs_bias ORDER BY lead_time")["lead_time"].tolist()
    )
    lead_default = 24 if 24 in all_leads_bias else (all_leads_bias[0] if all_leads_bias else 24)

    # ── Load shapefile once ───────────────────────────────────────────────────
    print("Loading county shapefile …")
    gdf_base = (
        gpd.read_file(SHAPEFILE_PATH)[["GEOID", "NAME", "geometry"]]
        .rename(columns={"GEOID": "geo_id"})
        .to_crs("EPSG:4326")
    )

    def merge_map(df, col):
        return gdf_base.merge(prep_geo(df)[["geo_id", col]], on="geo_id", how="left")

    summary_rows = []

    # ══════════════════════════════════════════════════════════════════════════
    # §1  Skill by lead time — IFS
    # ══════════════════════════════════════════════════════════════════════════
    def sec1():
        section("§1  Skill by lead time — IFS")
        df = q("""
            SELECT lead_time,
                SUM(bias       * aland) / SUM(aland)              AS aw_bias,
                SQRT(SUM(bias*bias * aland) / SUM(aland))         AS aw_rmse,
                SUM(abs_error  * aland) / SUM(aland)              AS aw_mae
            FROM ifs_bias
            GROUP BY lead_time ORDER BY lead_time
        """)

        fig, axes = plt.subplots(1, 3, figsize=(15, 4))
        for ax, col, ylabel, title in zip(
            axes,
            ["aw_bias", "aw_rmse", "aw_mae"],
            ["Bias (K)", "RMSE (K)", "MAE (K)"],
            ["Mean bias", "RMSE", "MAE"],
        ):
            ax.plot(df["lead_time"], df[col], "o-", ms=4, color="#1f77b4")
            if col == "aw_bias":
                ax.axhline(0, color="k", lw=0.8, ls="--")
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel(ylabel)
            ax.set_title(f"IFS — {title}")
            ax.grid(True, alpha=0.3)
        fig.suptitle("IFS 2m Temperature Forecast Skill (area-weighted)", fontsize=11, y=1.01)
        plt.tight_layout()
        savefig(fig, f"{OUT}/01_skill_lead/ifs_skill_by_lead.png")

        if HAS_ANOM:
            df_acc = q("""
                SELECT lead_time, CORR(fc_anom, an_anom) AS acc
                FROM ifs_anom GROUP BY lead_time ORDER BY lead_time
            """)
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.plot(df_acc["lead_time"], df_acc["acc"], "o-", ms=4, color="#1f77b4")
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel("ACC (pooled)")
            ax.set_ylim(df_acc["acc"].min() - 0.05, 1)
            ax.set_title("IFS — Anomaly Correlation Coefficient by lead time")
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            savefig(fig, f"{OUT}/01_skill_lead/ifs_acc_by_lead.png")

        for _, row in df.iterrows():
            summary_rows.append({
                "model": "IFS", "group": "all",
                "lead_time": int(row["lead_time"]),
                "aw_mean_bias": row["aw_bias"],
                "aw_rmse": row["aw_rmse"],
                "aw_mae": row["aw_mae"],
            })

        # Print key lead times
        key = df[df["lead_time"].isin([24, 72, 120, 240])].set_index("lead_time")
        print(f"\n  {'Lead':>6}  {'Bias':>8}  {'RMSE':>8}  {'MAE':>8}")
        for lt, row in key.iterrows():
            print(f"  {lt:>5}h  {row['aw_bias']:>+8.3f}  {row['aw_rmse']:>8.3f}  {row['aw_mae']:>8.3f}")

    try_section("§1", sec1)

    # ══════════════════════════════════════════════════════════════════════════
    # §2  IFS vs AIFS comparison — common period
    # ══════════════════════════════════════════════════════════════════════════
    def sec2():
        if not HAS_VS:
            print("  Skipped — ifs_vs_aifs view not registered")
            return
        section("§2  IFS vs AIFS comparison (common period)")

        df = q("""
            SELECT lead_time,
                SUM(bias_ifs         * aland) / SUM(aland)              AS aw_bias_ifs,
                SUM(bias_aifs        * aland) / SUM(aland)              AS aw_bias_aifs,
                SQRT(SUM(bias_ifs*bias_ifs   * aland) / SUM(aland))    AS aw_rmse_ifs,
                SQRT(SUM(bias_aifs*bias_aifs * aland) / SUM(aland))    AS aw_rmse_aifs,
                SUM(abs_error_ifs    * aland) / SUM(aland)              AS aw_mae_ifs,
                SUM(abs_error_aifs   * aland) / SUM(aland)              AS aw_mae_aifs
            FROM ifs_vs_aifs
            GROUP BY lead_time ORDER BY lead_time
        """)

        fig, axes = plt.subplots(1, 3, figsize=(15, 4))
        for ax, (c_ifs, c_aifs), ylabel, title in zip(
            axes,
            [("aw_bias_ifs", "aw_bias_aifs"),
             ("aw_rmse_ifs", "aw_rmse_aifs"),
             ("aw_mae_ifs",  "aw_mae_aifs")],
            ["Bias (K)", "RMSE (K)", "MAE (K)"],
            ["Mean bias", "RMSE", "MAE"],
        ):
            ax.plot(df["lead_time"], df[c_ifs],  "o-", ms=4, label="IFS",  color="#1f77b4")
            ax.plot(df["lead_time"], df[c_aifs], "s-", ms=4, label="AIFS", color="#ff7f0e")
            if "bias" in c_ifs:
                ax.axhline(0, color="k", lw=0.8, ls="--")
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel(ylabel)
            ax.set_title(title)
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
        fig.suptitle("IFS vs AIFS — common forecast period (area-weighted)", fontsize=11, y=1.01)
        plt.tight_layout()
        savefig(fig, f"{OUT}/02_model_comparison/ifs_vs_aifs_skill.png")

        if HAS_ANOM and HAS_AIFS_ANOM:
            aifs_start = q("SELECT MIN(valid_time) FROM aifs_bias").iloc[0, 0]
            df_acc_ifs  = q(f"""
                SELECT lead_time, CORR(fc_anom, an_anom) AS acc
                FROM ifs_anom WHERE valid_time >= '{aifs_start}'
                GROUP BY lead_time ORDER BY lead_time
            """)
            df_acc_aifs = q("""
                SELECT lead_time, CORR(fc_anom, an_anom) AS acc
                FROM aifs_anom GROUP BY lead_time ORDER BY lead_time
            """)
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.plot(df_acc_ifs["lead_time"],  df_acc_ifs["acc"],  "o-", ms=4,
                    label="IFS",  color="#1f77b4")
            ax.plot(df_acc_aifs["lead_time"], df_acc_aifs["acc"], "s-", ms=4,
                    label="AIFS", color="#ff7f0e")
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel("ACC (pooled)")
            acc_min = min(df_acc_ifs["acc"].min(), df_acc_aifs["acc"].min())
            ax.set_ylim(acc_min - 0.05, 1)
            ax.set_title("ACC — IFS vs AIFS, common period")
            ax.legend()
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            savefig(fig, f"{OUT}/02_model_comparison/ifs_vs_aifs_acc.png")

    try_section("§2", sec2)

    # ══════════════════════════════════════════════════════════════════════════
    # §3  Monthly timeseries — bias, MAE, ACC
    # ══════════════════════════════════════════════════════════════════════════
    def sec3():
        section("§3  Timeseries (daily + 30-day moving average)")

        MA = 30  # rolling window in days

        def add_ma(df, col):
            return df[col].rolling(MA, center=True, min_periods=MA // 2).mean()

        # Daily bias and MAE
        df_ts = q("""
            SELECT CAST(valid_time AS DATE) AS day,
                SUM(bias      * aland) / SUM(aland) AS aw_bias,
                SUM(abs_error * aland) / SUM(aland) AS aw_mae
            FROM ifs_bias
            GROUP BY CAST(valid_time AS DATE)
            ORDER BY day
        """)
        df_ts["day"] = pd.to_datetime(df_ts["day"])

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6), sharex=True)
        ax1.plot(df_ts["day"], df_ts["aw_bias"], lw=0.5, color="#aaaaaa", alpha=0.6)
        ax1.plot(df_ts["day"], add_ma(df_ts, "aw_bias"), lw=1.8, color="#d6604d",
                 label=f"{MA}-day MA")
        ax1.axhline(0, color="k", lw=0.8, ls="--")
        ax1.set_ylabel("Bias (K)")
        ax1.set_title("IFS — area-weighted mean bias (all leads, all counties)")
        ax1.legend(fontsize=8, loc="upper right")
        ax1.grid(True, alpha=0.3, axis="y")

        ax2.plot(df_ts["day"], df_ts["aw_mae"], lw=0.5, color="#aaaaaa", alpha=0.6)
        ax2.plot(df_ts["day"], add_ma(df_ts, "aw_mae"), lw=1.8, color="#444444",
                 label=f"{MA}-day MA")
        ax2.set_ylabel("MAE (K)")
        ax2.set_xlabel("Date")
        ax2.set_title("IFS — area-weighted MAE")
        ax2.legend(fontsize=8, loc="upper right")
        ax2.grid(True, alpha=0.3)
        plt.tight_layout()
        savefig(fig, f"{OUT}/03_timeseries/ifs_daily_bias_mae.png")

        if HAS_ANOM:
            acc_leads = sorted(q("SELECT DISTINCT lead_time FROM ifs_anom ORDER BY lead_time")["lead_time"])
            for lt in acc_leads:
                df_acc = q(f"""
                    SELECT CAST(valid_time AS DATE) AS day,
                        CORR(fc_anom, an_anom) AS acc
                    FROM ifs_anom
                    WHERE lead_time = {lt}
                    GROUP BY CAST(valid_time AS DATE)
                    ORDER BY day
                """)
                df_acc["day"] = pd.to_datetime(df_acc["day"])
                fig, ax = plt.subplots(figsize=(14, 4))
                ax.plot(df_acc["day"], df_acc["acc"], lw=0.5, color="#aaaaaa", alpha=0.6)
                ax.plot(df_acc["day"], add_ma(df_acc, "acc"), lw=1.8, color="#2166ac",
                        label=f"{MA}-day MA")
                ax.set_ylim(df_acc["acc"].min() - 0.05, 1)
                ax.set_ylabel("ACC (pooled across counties)")
                ax.set_xlabel("Date")
                ax.set_title(f"IFS — daily ACC, lead {lt}h")
                ax.legend(fontsize=8, loc="upper right")
                ax.grid(True, alpha=0.3)
                plt.tight_layout()
                savefig(fig, f"{OUT}/03_timeseries/ifs_daily_acc_lead{lt}h.png")

        # Per-lead timeseries overlay for key leads
        LEAD_LINES = [lt for lt in [24, 72, 120, 240]
                      if lt in set(q("SELECT DISTINCT lead_time FROM ifs_bias")["lead_time"])]
        df_lead_ts = q(f"""
            SELECT CAST(valid_time AS DATE) AS day,
                lead_time,
                SUM(bias * aland) / SUM(aland) AS aw_bias
            FROM ifs_bias
            WHERE lead_time IN ({",".join(str(l) for l in LEAD_LINES)})
            GROUP BY CAST(valid_time AS DATE), lead_time
            ORDER BY day, lead_time
        """)
        df_lead_ts["day"] = pd.to_datetime(df_lead_ts["day"])
        cmap_lt = mcm.get_cmap("plasma", len(LEAD_LINES))
        fig, ax = plt.subplots(figsize=(14, 4))
        for i, lt in enumerate(LEAD_LINES):
            d = df_lead_ts[df_lead_ts["lead_time"] == lt].sort_values("day")
            ma = d["aw_bias"].rolling(MA, center=True, min_periods=MA // 2).mean()
            ax.plot(d["day"], d["aw_bias"], lw=0.4, color=cmap_lt(i), alpha=0.3)
            ax.plot(d["day"], ma, lw=1.8, color=cmap_lt(i), label=f"{lt}h")
        ax.axhline(0, color="k", lw=0.8, ls="--")
        ax.set_ylabel("Area-weighted mean bias (K)")
        ax.set_xlabel("Date")
        ax.set_title(f"IFS — daily bias by lead time ({MA}-day MA, bold)")
        ax.legend(title="Lead", fontsize=8, ncol=len(LEAD_LINES))
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        savefig(fig, f"{OUT}/03_timeseries/ifs_daily_bias_by_lead.png")

    try_section("§3", sec3)

    # ══════════════════════════════════════════════════════════════════════════
    # §4  Month × lead time heatmaps
    # ══════════════════════════════════════════════════════════════════════════
    def sec4():
        section("§4  Seasonal cycle — month × lead heatmaps")
        df = q("""
            SELECT MONTH(valid_time) AS month,
                lead_time,
                SUM(bias      * aland) / SUM(aland) AS aw_bias,
                SUM(abs_error * aland) / SUM(aland) AS aw_mae
            FROM ifs_bias
            GROUP BY MONTH(valid_time), lead_time
            ORDER BY month, lead_time
        """)
        leads  = sorted(df["lead_time"].unique())
        months = list(range(1, 13))

        for col, cmap, label, fname, diverging in [
            ("aw_bias", RDBU, "Bias (K)", "ifs_bias_month_x_lead.png",  True),
            ("aw_mae",  REDS, "MAE (K)",  "ifs_mae_month_x_lead.png",   False),
        ]:
            pivot = (df.pivot(index="lead_time", columns="month", values=col)
                     .reindex(index=leads, columns=months))
            absmax = pivot.abs().max().max()
            vmin = -absmax if diverging else pivot.min().min()
            vmax =  absmax if diverging else pivot.max().max()

            fig, ax = plt.subplots(figsize=(13, 5))
            im = ax.imshow(pivot.values, aspect="auto", cmap=cmap,
                           vmin=vmin, vmax=vmax, origin="lower")
            ax.set_xticks(range(12))
            ax.set_xticklabels(MONTH_LABELS)
            ax.set_yticks(range(len(leads)))
            ax.set_yticklabels([f"{lt}h" for lt in leads])
            ax.set_xlabel("Month")
            ax.set_ylabel("Lead time")
            ax.set_title(f"IFS — area-weighted {label} by month × lead time")
            plt.colorbar(im, ax=ax, label=label)
            plt.tight_layout()
            savefig(fig, f"{OUT}/04_seasonal_cycle/{fname}")

        if HAS_ANOM:
            df_acc_ml = q("""
                SELECT MONTH(valid_time) AS month,
                    lead_time,
                    CORR(fc_anom, an_anom) AS acc
                FROM ifs_anom
                GROUP BY MONTH(valid_time), lead_time
                ORDER BY month, lead_time
            """)
            pivot_acc = (df_acc_ml.pivot(index="lead_time", columns="month", values="acc")
                         .reindex(index=leads, columns=months))
            fig, ax = plt.subplots(figsize=(13, 5))
            im = ax.imshow(pivot_acc.values, aspect="auto", cmap="viridis",
                           vmin=0, vmax=1, origin="lower")
            ax.set_xticks(range(12))
            ax.set_xticklabels(MONTH_LABELS)
            ax.set_yticks(range(len(leads)))
            ax.set_yticklabels([f"{lt}h" for lt in leads])
            ax.set_xlabel("Month")
            ax.set_ylabel("Lead time")
            ax.set_title("IFS — ACC (pooled) by month × lead time")
            plt.colorbar(im, ax=ax, label="ACC")
            plt.tight_layout()
            savefig(fig, f"{OUT}/04_seasonal_cycle/ifs_acc_month_x_lead.png")

    try_section("§4", sec4)

    # ══════════════════════════════════════════════════════════════════════════
    # §5  Seasonal maps
    # ══════════════════════════════════════════════════════════════════════════
    def sec5():
        section("§5  Maps (all leads)")

        for lead in all_leads_bias:
            # — All-time average maps ─────────────────────────────────────────────
            df_all = q(f"""
                SELECT geo_id,
                    AVG(bias)      AS mean_bias,
                    AVG(abs_error) AS mae,
                    AVG(aland)     AS aland
                FROM ifs_bias WHERE lead_time = {lead}
                GROUP BY geo_id
            """)
            df_all = prep_geo(df_all)

            for metric, cmap, label, diverging, fname in [
                ("mean_bias", RDBU, "Bias (K)", True,  f"ifs_bias_alltime_lead{lead}h.png"),
                ("mae",       REDS, "MAE (K)",  False, f"ifs_mae_alltime_lead{lead}h.png"),
            ]:
                absmax = df_all[metric].abs().quantile(0.98)
                vmin = -absmax if diverging else df_all[metric].quantile(0.02)
                vmax =  absmax if diverging else df_all[metric].quantile(0.98)
                gdf = merge_map(df_all, metric)
                aw = aw_mean(df_all, metric)
                fig, ax = plt.subplots(figsize=(12, 6))
                county_map(ax, gdf, metric, cmap, vmin, vmax,
                           f"IFS {label} — all time, lead {lead}h  (aw={aw:+.3f} K)"
                           if diverging else
                           f"IFS {label} — all time, lead {lead}h  (aw={aw:.3f} K)",
                           label)
                plt.tight_layout()
                savefig(fig, f"{OUT}/05_seasonal_maps/{fname}")

            if HAS_ANOM:
                df_all_anom = q(f"""
                    SELECT geo_id,
                        AVG(fc_anom)           AS fc_anom,
                        AVG(an_anom)           AS an_anom,
                        CORR(fc_anom, an_anom) AS acc
                    FROM ifs_anom WHERE lead_time = {lead}
                    GROUP BY geo_id
                """)
                df_all_anom = prep_geo(df_all_anom)
                absmax_anom = max(df_all_anom["fc_anom"].abs().quantile(0.98),
                                  df_all_anom["an_anom"].abs().quantile(0.98))

                for metric, label, fname in [
                    ("fc_anom", "FC anomaly (K)", f"ifs_fc_anom_alltime_lead{lead}h.png"),
                    ("an_anom", "AN anomaly (K)", f"ifs_an_anom_alltime_lead{lead}h.png"),
                ]:
                    gdf = merge_map(df_all_anom, metric)
                    fig, ax = plt.subplots(figsize=(12, 6))
                    county_map(ax, gdf, metric, RDBU, -absmax_anom, absmax_anom,
                               f"IFS {label} — all time, lead {lead}h", label)
                    plt.tight_layout()
                    savefig(fig, f"{OUT}/05_seasonal_maps/{fname}")

                gdf_acc = merge_map(df_all_anom, "acc")
                fig, ax = plt.subplots(figsize=(12, 6))
                county_map(ax, gdf_acc, "acc", "viridis", 0, 1,
                           f"IFS per-county ACC — all time, lead {lead}h", "ACC")
                plt.tight_layout()
                savefig(fig, f"{OUT}/05_seasonal_maps/ifs_acc_alltime_lead{lead}h.png")

            # — Seasonal bias and MAE maps ─────────────────────────────────────────
            df_seas = q(f"""
                SELECT geo_id,
                    {_season_sql()} AS season,
                    AVG(bias)      AS mean_bias,
                    AVG(abs_error) AS mae,
                    AVG(aland)     AS aland
                FROM ifs_bias WHERE lead_time = {lead}
                GROUP BY geo_id, season
            """)
            df_seas = prep_geo(df_seas)

            for metric, cmap, label, diverging, fname in [
                ("mean_bias", RDBU, "Bias (K)", True,  f"ifs_bias_seasonal_lead{lead}h.png"),
                ("mae",       REDS, "MAE (K)",  False, f"ifs_mae_seasonal_lead{lead}h.png"),
            ]:
                absmax = df_seas[metric].abs().quantile(0.98)
                vmin = -absmax if diverging else df_seas[metric].quantile(0.02)
                vmax =  absmax if diverging else df_seas[metric].quantile(0.98)

                fig, axes = plt.subplots(2, 2, figsize=(16, 9))
                for ax, season in zip(axes.flatten(), ["DJF", "MAM", "JJA", "SON"]):
                    d = df_seas[df_seas["season"] == season]
                    gdf = merge_map(d, metric)
                    county_map(ax, gdf, metric, cmap, vmin, vmax, season, label)
                    aw = aw_mean(d, metric)
                    ax.set_title(f"{season}  (aw={aw:+.3f} K)" if diverging
                                 else f"{season}  (aw={aw:.3f} K)", fontsize=9)
                fig.suptitle(f"IFS {label} — lead {lead}h", fontsize=11)
                plt.tight_layout()
                savefig(fig, f"{OUT}/05_seasonal_maps/{fname}")

            # — Forecast and analysis anomaly maps ────────────────────────────────
            if HAS_ANOM:
                df_anom = q(f"""
                    SELECT geo_id,
                        {_season_sql()} AS season,
                        AVG(fc_anom) AS fc_anom,
                        AVG(an_anom) AS an_anom,
                        AVG(aland)   AS aland
                    FROM ifs_anom WHERE lead_time = {lead}
                    GROUP BY geo_id, season
                """)
                df_anom = prep_geo(df_anom)
                absmax = max(df_anom["fc_anom"].abs().quantile(0.98),
                            df_anom["an_anom"].abs().quantile(0.98))

                for metric, label, fname in [
                    ("fc_anom", "Forecast anomaly (K)",  f"ifs_fc_anom_seasonal_lead{lead}h.png"),
                    ("an_anom", "Analysis anomaly (K)",  f"ifs_an_anom_seasonal_lead{lead}h.png"),
                ]:
                    fig, axes = plt.subplots(2, 2, figsize=(16, 9))
                    for ax, season in zip(axes.flatten(), ["DJF", "MAM", "JJA", "SON"]):
                        d = df_anom[df_anom["season"] == season]
                        gdf = merge_map(d, metric)
                        county_map(ax, gdf, metric, RDBU, -absmax, absmax, season, label)
                        aw = aw_mean(d, metric)
                        ax.set_title(f"{season}  (aw={aw:+.3f} K)", fontsize=9)
                    fig.suptitle(f"IFS {label} — lead {lead}h", fontsize=11)
                    plt.tight_layout()
                    savefig(fig, f"{OUT}/05_seasonal_maps/{fname}")

                # Per-county ACC map (seasonal)
                df_acc_county = q(f"""
                    SELECT geo_id,
                        {_season_sql()} AS season,
                        CORR(fc_anom, an_anom) AS acc
                    FROM ifs_anom WHERE lead_time = {lead}
                    GROUP BY geo_id, season
                """)
                df_acc_county = prep_geo(df_acc_county)
                fig, axes = plt.subplots(2, 2, figsize=(16, 9))
                for ax, season in zip(axes.flatten(), ["DJF", "MAM", "JJA", "SON"]):
                    d = df_acc_county[df_acc_county["season"] == season]
                    gdf = merge_map(d, "acc")
                    county_map(ax, gdf, "acc", "viridis", 0, 1, season, "ACC")
                    mean_acc = d["acc"].mean()
                    ax.set_title(f"{season}  (mean={mean_acc:.3f})", fontsize=9)
                fig.suptitle(f"IFS per-county ACC — lead {lead}h", fontsize=11)
                plt.tight_layout()
                savefig(fig, f"{OUT}/05_seasonal_maps/ifs_acc_county_seasonal_lead{lead}h.png")

    try_section("§5", sec5)

    # ══════════════════════════════════════════════════════════════════════════
    # §6  Histograms
    # ══════════════════════════════════════════════════════════════════════════
    def sec6():
        section("§6  Histograms")
        HIST_LEADS = [lt for lt in [24, 72, 120, 240] if lt in all_leads_bias]

        df_hist = q(f"""
            SELECT lead_time, geo_id,
                AVG(bias)      AS mean_bias,
                AVG(abs_error) AS mae
            FROM ifs_bias
            WHERE lead_time IN ({",".join(str(l) for l in HIST_LEADS)})
            GROUP BY lead_time, geo_id
        """)

        # Multi-lead bias/MAE histogram overlay
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        for ax, col, xlabel in [
            (axes[0], "mean_bias", "County mean bias (K)"),
            (axes[1], "mae",       "County mean MAE (K)"),
        ]:
            for lt in HIST_LEADS:
                d = df_hist[df_hist["lead_time"] == lt][col].dropna()
                ax.hist(d, bins=60, alpha=0.5, density=True, label=f"{lt}h")
            if col == "mean_bias":
                ax.axvline(0, color="k", lw=0.8, ls="--")
            ax.set_xlabel(xlabel)
            ax.set_ylabel("Density")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)
        fig.suptitle("IFS — county mean bias and MAE distributions by lead time", fontsize=11)
        plt.tight_layout()
        savefig(fig, f"{OUT}/06_histograms/ifs_bias_mae_hist.png")

        # Seasonal histograms — loop over all leads
        for lead in all_leads_bias:
            df_seas_hist = q(f"""
                SELECT geo_id,
                    {_season_sql()} AS season,
                    AVG(bias)      AS mean_bias,
                    AVG(abs_error) AS mae
                FROM ifs_bias WHERE lead_time = {lead}
                GROUP BY geo_id, season
            """)
            fig, axes = plt.subplots(2, 4, figsize=(16, 7), sharey="row")
            for row, (col, xlabel) in enumerate([("mean_bias", "County mean bias (K)"),
                                                  ("mae",       "County mean MAE (K)")]):
                for ax, season in zip(axes[row], ["DJF", "MAM", "JJA", "SON"]):
                    d = df_seas_hist[df_seas_hist["season"] == season][col].dropna()
                    ax.hist(d, bins=50, density=True, color="#1f77b4", alpha=0.75)
                    if col == "mean_bias":
                        ax.axvline(0, color="k", lw=0.8, ls="--")
                    ax.set_title(season, fontsize=10)
                    ax.set_xlabel(xlabel, fontsize=8)
                    if ax is axes[row][0]:
                        ax.set_ylabel("Density", fontsize=8)
                    ax.grid(True, alpha=0.3)
            fig.suptitle(f"IFS — county mean bias and MAE distributions by season (lead {lead}h)",
                         fontsize=11)
            plt.tight_layout()
            savefig(fig, f"{OUT}/06_histograms/ifs_seasonal_hist_lead{lead}h.png")

        # Bias distribution map — loop over all leads
        for lead in all_leads_bias:
            if lead in HIST_LEADS:
                d_lead = df_hist[df_hist["lead_time"] == lead]
            else:
                d_lead = q(f"""
                    SELECT lead_time, geo_id,
                        AVG(bias)      AS mean_bias,
                        AVG(abs_error) AS mae
                    FROM ifs_bias WHERE lead_time = {lead}
                    GROUP BY lead_time, geo_id
                """)
            if d_lead.empty:
                continue
            low, high = d_lead["mean_bias"].quantile([0.05, 0.95])
            gdf_bias = merge_map(d_lead, "mean_bias")
            absmax = d_lead["mean_bias"].abs().quantile(0.98)
            fig, ax = plt.subplots(figsize=(12, 6))
            county_map(ax, gdf_bias, "mean_bias", RDBU, -absmax, absmax,
                       f"IFS lead {lead}h — county mean bias (full record)", "Bias (K)")
            fig.suptitle(f"5th/95th pctile counties: [{low:+.3f}, {high:+.3f}] K", fontsize=9)
            plt.tight_layout()
            savefig(fig, f"{OUT}/06_histograms/ifs_lead{lead}h_mean_bias_map.png")

    try_section("§6", sec6)

    # ══════════════════════════════════════════════════════════════════════════
    # §7  Koppen-Geiger climate region interactions
    # ══════════════════════════════════════════════════════════════════════════
    def sec7():
        if not HAS_KOPPEN:
            print("  Skipped — koppen view not registered")
            return
        section("§7  Koppen-Geiger climate region interactions")

        df_kop = q("""
            SELECT k.category_1 AS koppen, b.lead_time,
                SUM(b.bias       * b.aland) / SUM(b.aland)             AS aw_bias,
                SQRT(SUM(b.bias*b.bias * b.aland) / SUM(b.aland))     AS aw_rmse,
                SUM(b.abs_error  * b.aland) / SUM(b.aland)             AS aw_mae
            FROM ifs_bias b
            JOIN koppen k ON b.geo_id = k.geo_id
            GROUP BY k.category_1, b.lead_time
            ORDER BY k.category_1, b.lead_time
        """)
        classes  = sorted(df_kop["koppen"].dropna().unique())
        labels   = [koppen_label(c) for c in classes]
        cmap_kop = mcm.get_cmap("tab10", len(classes))

        fig, axes = plt.subplots(1, 3, figsize=(17, 5))
        for ax, col, ylabel, title in zip(
            axes,
            ["aw_bias", "aw_rmse", "aw_mae"],
            ["Bias (K)", "RMSE (K)", "MAE (K)"],
            ["Mean bias", "RMSE", "MAE"],
        ):
            for i, (klass, lbl) in enumerate(zip(classes, labels)):
                d = df_kop[df_kop["koppen"] == klass]
                ax.plot(d["lead_time"], d[col], "o-", ms=3, lw=1.2,
                        color=cmap_kop(i), label=lbl)
            if col == "aw_bias":
                ax.axhline(0, color="k", lw=0.8, ls="--")
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel(ylabel)
            ax.set_title(f"IFS {title} by Koppen class")
            ax.grid(True, alpha=0.3)
        handles, leg_labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, leg_labels, loc="lower center", ncol=min(len(classes), 4),
                   fontsize=7, bbox_to_anchor=(0.5, -0.12))
        fig.suptitle("IFS skill by Koppen-Geiger climate classification", fontsize=11)
        plt.tight_layout()
        savefig(fig, f"{OUT}/07_climate_regions/ifs_skill_by_koppen.png")

        # Koppen × season heatmap — loop over all leads
        for lead in all_leads_bias:
            df_ks = q(f"""
                SELECT k.category_1 AS koppen,
                    {_season_sql('b.valid_time')} AS season,
                    SUM(b.bias * b.aland) / SUM(b.aland) AS aw_bias
                FROM ifs_bias b
                JOIN koppen k ON b.geo_id = k.geo_id
                WHERE b.lead_time = {lead}
                GROUP BY k.category_1, season
            """)
            pivot_ks = (df_ks.pivot(index="koppen", columns="season", values="aw_bias")
                        .reindex(columns=["DJF", "MAM", "JJA", "SON"]))
            pivot_ks_labels = [koppen_label(c) for c in pivot_ks.index]
            absmax = pivot_ks.abs().max().max()
            fig, ax = plt.subplots(figsize=(10, max(4, len(classes) * 0.55 + 1)))
            im = ax.imshow(pivot_ks.values, cmap=RDBU, vmin=-absmax, vmax=absmax, aspect="auto")
            ax.set_xticks(range(4))
            ax.set_xticklabels(["DJF", "MAM", "JJA", "SON"])
            ax.set_yticks(range(len(pivot_ks)))
            ax.set_yticklabels(pivot_ks_labels, fontsize=8)
            for (r, c), val in np.ndenumerate(pivot_ks.values):
                if not np.isnan(val):
                    ax.text(c, r, f"{val:+.2f}", ha="center", va="center", fontsize=8)
            plt.colorbar(im, ax=ax, label="Area-weighted bias (K)")
            ax.set_title(f"IFS mean bias — Koppen × season (lead {lead}h)")
            plt.tight_layout()
            savefig(fig, f"{OUT}/07_climate_regions/ifs_bias_koppen_seasonal_lead{lead}h.png")

        # Add to summary
        for _, row in df_kop.iterrows():
            summary_rows.append({
                "model": "IFS", "group": f"koppen_{row['koppen']}",
                "lead_time": int(row["lead_time"]),
                "aw_mean_bias": row["aw_bias"],
                "aw_rmse": row["aw_rmse"],
                "aw_mae": row["aw_mae"],
            })

    try_section("§7", sec7)

    # ══════════════════════════════════════════════════════════════════════════
    # §8  Demographic interactions
    # ══════════════════════════════════════════════════════════════════════════
    def sec8():
        model_input = os.path.join(
            os.path.dirname(__file__), "..", "notebooks", "data", "model_input.parquet"
        )
        if not os.path.exists(model_input):
            print(f"  Skipped — {model_input} not found (run notebook 03 first)")
            return
        section("§8  Demographic interactions")

        df_mi  = pd.read_parquet(model_input)
        demo_cols = [c for c in df_mi.columns if c.startswith("demo_")]
        if not demo_cols:
            print("  Skipped — no demo_ columns found in model_input.parquet")
            return

        # County mean bias averaged across all day-of-year at lead_default
        avail_mi_leads = sorted(df_mi["lead_time"].unique()) if "lead_time" in df_mi.columns else []
        demo_lead = lead_default if lead_default in avail_mi_leads else (avail_mi_leads[0] if avail_mi_leads else lead_default)
        df_county = (
            df_mi[df_mi["lead_time"] == demo_lead]
            .groupby("geo_id")[["mean_bias"] + demo_cols]
            .mean()
            .reset_index()
            .dropna(subset=["mean_bias"])
        )
        print(f"  {len(df_county):,} counties at lead {demo_lead}h")

        # Scatter grid
        n_demo = len(demo_cols)
        ncols  = 4
        nrows  = int(np.ceil(n_demo / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 3.8, nrows * 3.2))
        axes = axes.flatten()
        corrs = {}

        for i, dcol in enumerate(demo_cols):
            ax = axes[i]
            d  = df_county[["mean_bias", dcol]].dropna()
            ax.scatter(d[dcol], d["mean_bias"], s=4, alpha=0.35, color="#1f77b4", rasterized=True)
            if len(d) > 10:
                m, b = np.polyfit(d[dcol], d["mean_bias"], 1)
                xs = np.linspace(d[dcol].min(), d[dcol].max(), 100)
                ax.plot(xs, m * xs + b, "r-", lw=1.2)
                corrs[dcol] = d["mean_bias"].corr(d[dcol])
            ax.axhline(0, color="k", lw=0.7, ls="--")
            label = dcol.replace("demo_", "").replace("_", " ")
            ax.set_xlabel(label, fontsize=8)
            ax.set_ylabel("Mean bias (K)", fontsize=8)
            r = corrs.get(dcol, float("nan"))
            ax.set_title(f"r = {r:.3f}", fontsize=8)
            ax.tick_params(labelsize=7)
            ax.grid(True, alpha=0.2)

        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        fig.suptitle(f"IFS {demo_lead}h county mean bias vs demographic indices", fontsize=11)
        plt.tight_layout()
        savefig(fig, f"{OUT}/08_demographics/ifs_bias_vs_demo_scatter.png")

        # Correlation bar chart
        if corrs:
            corr_s = pd.Series(corrs).sort_values()
            bar_colors = ["#d6604d" if v > 0 else "#2166ac" for v in corr_s.values]
            fig, ax = plt.subplots(figsize=(8, max(4, len(corr_s) * 0.4 + 1)))
            ax.barh(corr_s.index.str.replace("demo_", "").str.replace("_", " "),
                    corr_s.values, color=bar_colors)
            ax.axvline(0, color="k", lw=0.8)
            ax.set_xlabel("Pearson r with county mean bias")
            ax.set_title(f"IFS {demo_lead}h — bias–demographic correlations")
            ax.grid(True, alpha=0.3, axis="x")
            plt.tight_layout()
            savefig(fig, f"{OUT}/08_demographics/ifs_bias_demo_correlations.png")

            print("\n  Top correlations (|r| > 0.1):")
            for name, r in corr_s.items():
                if abs(r) > 0.1:
                    print(f"    {name.replace('demo_',''):30s}  r = {r:+.3f}")

    try_section("§8", sec8)

    # ══════════════════════════════════════════════════════════════════════════
    # §10  MSE decomposition maps (3×3 panel — bias sign × anomaly sign)
    # ══════════════════════════════════════════════════════════════════════════
    def sec10():
        if not HAS_ANOM:
            print("  Skipped — ifs_anom view not registered")
            return
        section("§10  MSE decomposition maps")

        all_leads_anom = sorted(
            q("SELECT DISTINCT lead_time FROM ifs_anom ORDER BY lead_time")["lead_time"].tolist()
        )

        # (row, col, column_name, panel_title)
        panel_specs = [
            (0, 0, "mse_total",     "Total"),
            (0, 1, "mse_cold_bias", "Cold bias"),
            (0, 2, "mse_hot_bias",  "Hot bias"),
            (1, 0, "mse_cold_anom", "Cold anom"),
            (1, 1, "mse_ca_cb",     "Cold anom + Cold bias"),
            (1, 2, "mse_ca_hb",     "Cold anom + Hot bias"),
            (2, 0, "mse_hot_anom",  "Hot anom"),
            (2, 1, "mse_ha_cb",     "Hot anom + Cold bias"),
            (2, 2, "mse_ha_hb",     "Hot anom + Hot bias"),
        ]

        def _query_mse(lead, where_extra=""):
            return prep_geo(q(f"""
                SELECT geo_id,
                    AVG(bias * bias)                                              AS mse_total,
                    AVG(CASE WHEN bias    < 0 THEN bias*bias END)                 AS mse_cold_bias,
                    AVG(CASE WHEN bias    > 0 THEN bias*bias END)                 AS mse_hot_bias,
                    AVG(CASE WHEN an_anom < 0 THEN bias*bias END)                 AS mse_cold_anom,
                    AVG(CASE WHEN an_anom > 0 THEN bias*bias END)                 AS mse_hot_anom,
                    AVG(CASE WHEN an_anom < 0 AND bias < 0 THEN bias*bias END)    AS mse_ca_cb,
                    AVG(CASE WHEN an_anom < 0 AND bias > 0 THEN bias*bias END)    AS mse_ca_hb,
                    AVG(CASE WHEN an_anom > 0 AND bias < 0 THEN bias*bias END)    AS mse_ha_cb,
                    AVG(CASE WHEN an_anom > 0 AND bias > 0 THEN bias*bias END)    AS mse_ha_hb
                FROM ifs_anom
                WHERE lead_time = {lead}{where_extra}
                GROUP BY geo_id
            """))

        def _plot_mse9(df, label, lead, fname, vmax):
            fig, axes = plt.subplots(3, 3, figsize=(18, 14), constrained_layout=True)
            for ri, ci, col, title in panel_specs:
                ax = axes[ri, ci]
                gdf = merge_map(df, col)
                county_map(ax, gdf, col, REDS, 0, vmax, title, "MSE (K²)")
            fig.suptitle(f"IFS MSE decomposition — {label}, lead {lead}h", fontsize=13)
            savefig(fig, fname)

        for lead in all_leads_anom:
            df_all = _query_mse(lead)
            vmax = float(df_all["mse_total"].quantile(0.98))

            _plot_mse9(df_all, "all time", lead,
                       f"{OUT}/10_mse_decomp/ifs_mse9_alltime_lead{lead}h.png", vmax)

            for season in ["DJF", "MAM", "JJA", "SON"]:
                df_seas = _query_mse(lead, f" AND {_season_sql()} = '{season}'")
                _plot_mse9(df_seas, season, lead,
                           f"{OUT}/10_mse_decomp/ifs_mse9_{season}_lead{lead}h.png", vmax)

    try_section("§10", sec10)

    # ══════════════════════════════════════════════════════════════════════════
    # §11  Joint PDF — observed anomaly vs forecast bias
    # ══════════════════════════════════════════════════════════════════════════
    def sec11():
        if not HAS_ANOM:
            print("  Skipped — ifs_anom view not registered")
            return
        section("§11  Joint PDF — anomaly vs bias")

        all_leads_anom = sorted(
            q("SELECT DISTINCT lead_time FROM ifs_anom ORDER BY lead_time")["lead_time"].tolist()
        )
        PDF_XLIM = (-10, 10)  # observed anomaly axis
        PDF_YLIM = (-10, 10)  # forecast bias axis

        def _query_pdf(lead, where_extra=""):
            return q(f"""
                SELECT
                    ROUND(an_anom / {PDF_BIN_K}) * {PDF_BIN_K}  AS anom_bin,
                    ROUND(bias    / {PDF_BIN_K}) * {PDF_BIN_K}  AS bias_bin,
                    SUM(aland)                                   AS total_area
                FROM ifs_anom
                WHERE lead_time = {lead}{where_extra}
                GROUP BY anom_bin, bias_bin
                ORDER BY anom_bin, bias_bin
            """)

        def _pivot_pdf(df):
            """Clamp to axis limits, pivot to 2D grid, and normalise by total area."""
            df = df[
                (df["anom_bin"] >= PDF_XLIM[0]) & (df["anom_bin"] <= PDF_XLIM[1]) &
                (df["bias_bin"] >= PDF_YLIM[0]) & (df["bias_bin"] <= PDF_YLIM[1])
            ].copy()
            if df.empty:
                return pd.DataFrame()
            pivot = df.pivot_table(index="bias_bin", columns="anom_bin",
                                   values="total_area", aggfunc="sum", fill_value=0)
            total = pivot.values.sum()
            if total > 0:
                pivot = pivot / total
            return pivot

        def _plot_pdf_ax(ax, pivot, title, norm=None):
            if pivot.empty or pivot.values.sum() == 0:
                ax.text(0.5, 0.5, "No data", ha="center", va="center",
                        transform=ax.transAxes)
                ax.set_title(title, fontsize=9)
                return None
            if norm is None:
                vmax = float(pivot.values.max())
                norm = mcolors.LogNorm(vmin=1e-4, vmax=max(vmax, 1e-3))
            extent = [
                float(pivot.columns.min()) - PDF_BIN_K / 2,
                float(pivot.columns.max()) + PDF_BIN_K / 2,
                float(pivot.index.min())   - PDF_BIN_K / 2,
                float(pivot.index.max())   + PDF_BIN_K / 2,
            ]
            im = ax.imshow(pivot.values, cmap="plasma", norm=norm,
                           origin="lower", extent=extent, aspect="auto")
            ax.axhline(0, color="white", lw=0.8, alpha=0.6)
            ax.axvline(0, color="white", lw=0.8, alpha=0.6)
            ax.set_xlim(*PDF_XLIM)
            ax.set_ylim(*PDF_YLIM)
            ax.set_xlabel("Observed anomaly (K)", fontsize=8)
            ax.set_ylabel("Forecast bias (K)", fontsize=8)
            ax.set_title(title, fontsize=9)
            # Quadrant corner labels
            for tx, ty, lbl in [
                (-8,  8, "Cold anom\nHot bias"),
                ( 8,  8, "Hot anom\nHot bias"),
                (-8, -8, "Cold anom\nCold bias"),
                ( 8, -8, "Hot anom\nCold bias"),
            ]:
                ax.text(tx, ty, lbl, ha="center", va="center",
                        fontsize=6, color="white", alpha=0.7)
            return im

        for lead in all_leads_anom:
            # — All-time single panel ─────────────────────────────────────────
            pivot_all = _pivot_pdf(_query_pdf(lead))
            fig, ax = plt.subplots(figsize=(7, 6))
            im = _plot_pdf_ax(ax, pivot_all, f"IFS — all time, lead {lead}h")
            if im is not None:
                plt.colorbar(im, ax=ax, label="Area-weighted density")
            plt.tight_layout()
            savefig(fig, f"{OUT}/11_joint_pdf/ifs_joint_pdf_alltime_lead{lead}h.png")

            # — Seasonal 2×2 ──────────────────────────────────────────────────
            pivots = {
                s: _pivot_pdf(_query_pdf(lead, f" AND {_season_sql()} = '{s}'"))
                for s in ["DJF", "MAM", "JJA", "SON"]
            }
            nonempty = [p for p in pivots.values() if not p.empty and p.values.sum() > 0]
            seas_vmax = max((float(p.values.max()) for p in nonempty), default=1e-3)
            seas_norm = mcolors.LogNorm(vmin=1e-4, vmax=max(seas_vmax, 1e-3))

            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            last_im = None
            for ax, season in zip(axes.flatten(), ["DJF", "MAM", "JJA", "SON"]):
                im = _plot_pdf_ax(ax, pivots[season], season, norm=seas_norm)
                if im is not None:
                    last_im = im
            fig.suptitle(
                f"IFS — joint PDF (observed anomaly vs forecast bias) by season, lead {lead}h",
                fontsize=11)
            if last_im is not None:
                fig.colorbar(last_im, ax=axes.ravel().tolist(),
                             label="Area-weighted density", shrink=0.6)
            plt.tight_layout()
            savefig(fig, f"{OUT}/11_joint_pdf/ifs_joint_pdf_seasonal_lead{lead}h.png")

    try_section("§11", sec11)

    # ══════════════════════════════════════════════════════════════════════════
    # §9  Summary table
    # ══════════════════════════════════════════════════════════════════════════
    section("§9  Summary")
    if summary_rows:
        df_sum = pd.DataFrame(summary_rows)
        out_csv = f"{OUT}/summary.csv"
        df_sum.to_csv(out_csv, index=False, float_format="%.4f")
        print(f"  Saved {len(df_sum):,} rows → {out_csv}")

    db.close()
    print(f"\nDone.  All figures in: {OUT}/")


if __name__ == "__main__":
    main()
