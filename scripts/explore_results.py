#!/usr/bin/env python3
"""
explore_results.py — Comprehensive exploratory analysis of NWP 2m temperature bias.

Sections
--------
  1  Aggregate skill curves — bias, RMSE, MAE, skill score, ACC vs lead time
     (IFS full record, AIFS, IFS restricted to AIFS period for fair comparison)
  2  Monthly timeseries — area-weighted bias, MAE by lead
  3  Seasonal cycle — lead × month and lead × DOY heatmaps / Hovmöller
  4  Choropleth maps — bias, RMSE, skill score, ACC per lead × season (per model)
  5  MSE variance decomposition maps — MSE = fc_var + an_var − 2·cov per lead × season
  6  9-panel MSE decomposition — bias sign × anomaly sign (IFS)
  7  Joint PDF — observed anomaly vs forecast bias (IFS)
  8  Histograms — county bias/MAE distributions by lead and season (IFS + AIFS)
  9  Koppen-Geiger climate region interactions (IFS + AIFS)
  10 Demographic interactions — correlation vs lead, scatter at default lead
  11 Summary CSV

Baseline / skill score
----------------------
  When f = c (forecast = climatology), (f−a)² = (a−c)² = an_anom².
  Skill score = 1 − MSE / an_var  (0 = climatology skill, 1 = perfect, <0 = worse).
  All spatial means are area-weighted via the `aland` column.
  ACC uses the area-weighted correlation formula (not CORR(), which is unweighted).

Models
------
  IFS     — full IFS record
  AIFS    — where parquet is registered
  IFS_sub — IFS restricted to the AIFS valid-time window (apples-to-apples)

Usage
-----
    python scripts/explore_results.py [--out-dir PATH] [--leads L1,L2,...]
                                      [--skip-maps] [--include-off-cycle]
                                      [--models ifs,aifs,ifs_sub]

Outputs
-------
    {OUT_DIR}/
        01_skill/
        02_timeseries/
        03_seasonal_cycle/
        04_maps/
        05_mse_decomp/
        06_mse9/
        07_joint_pdf/
        08_histograms/
        09_koppen/
        10_demographics/
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
from nwp_census_eval.db import PipelineDB
try:
    from scripts.config import SHAPEFILE_PATH
except ImportError:
    import config as _cfg
    SHAPEFILE_PATH = _cfg.SHAPEFILE_PATH

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CONUS_XLIM   = (-125, -66.5)
CONUS_YLIM   = (24.5, 49.5)
MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
RDBU     = "RdBu_r"
REDS     = "YlOrRd"
SKILL_CM = "RdYlGn"      # red = poor skill, green = good
PDF_BIN_K = 0.5

MODEL_COLORS   = {"IFS": "#1f77b4", "AIFS": "#ff7f0e", "IFS_sub": "#aec7e8"}
MODEL_LS       = {"IFS": "-",       "AIFS": "--",       "IFS_sub": ":"}
MODEL_MARKERS  = {"IFS": "o",       "AIFS": "s",        "IFS_sub": "^"}

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
    try:
        return KOPPEN_NAMES.get(int(code), str(code))
    except (TypeError, ValueError):
        return str(code)


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _season_sql(col="valid_time"):
    return f"""CASE
        WHEN MONTH({col}) IN (12, 1, 2) THEN 'DJF'
        WHEN MONTH({col}) IN (3,  4, 5) THEN 'MAM'
        WHEN MONTH({col}) IN (6,  7, 8) THEN 'JJA'
        ELSE                                  'SON'
    END"""


def _wa_acc(fc="fc_anom", an="an_anom", w="aland"):
    """Area-weighted correlation (ACC) as a SQL expression.

    Implements:  Σw(xy)/Σw − (Σwx/Σw)(Σwy/Σw)
                 ─────────────────────────────────────────
                 √(Var_w(x)) · √(Var_w(y))
    """
    return f"""(
        SUM({w}*{fc}*{an})/SUM({w})
        - (SUM({w}*{fc})/SUM({w})) * (SUM({w}*{an})/SUM({w}))
    ) / NULLIF(
        SQRT(ABS(SUM({w}*{fc}*{fc})/SUM({w}) - POW(SUM({w}*{fc})/SUM({w}), 2))) *
        SQRT(ABS(SUM({w}*{an}*{an})/SUM({w}) - POW(SUM({w}*{an})/SUM({w}), 2))),
        0
    )"""


def _wa_skill(bias="bias", an="an_anom", w="aland"):
    """MSE skill score = 1 − MSE/an_var, area-weighted.

    Baseline: climatology (f=c → MSE = an_var → skill = 0).
    """
    return (
        f"1.0 - (SUM({w}*{bias}*{bias})/SUM({w}))"
        f" / NULLIF(SUM({w}*{an}*{an})/SUM({w}), 0)"
    )


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def savefig(fig, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    {path}")


def _plot_county(ax, gdf, col, cmap, vmin, vmax, title):
    """County choropleth on *ax* — no colorbar (caller manages it)."""
    gdf.plot(column=col, ax=ax, cmap=cmap, vmin=vmin, vmax=vmax,
             linewidth=0.05, edgecolor="black",
             missing_kwds={"color": "#cccccc"})
    ax.set_xlim(*CONUS_XLIM)
    ax.set_ylim(*CONUS_YLIM)
    ax.axis("off")
    ax.set_title(title, fontsize=8)


def _sm(cmap, vmin, vmax):
    """Return a ScalarMappable for use with fig.colorbar."""
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=mcolors.Normalize(vmin=vmin, vmax=vmax))
    sm.set_array([])
    return sm


def prep_geo(df):
    df = df.copy()
    df["geo_id"] = df["geo_id"].astype(str).str.zfill(5)
    return df


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
    p.add_argument("--leads", default=None,
                   help="Comma-separated lead times to process, e.g. 24,48,72 (default: all)")
    p.add_argument("--models", default="ifs,aifs,ifs_sub",
                   help="Comma-separated models: ifs,aifs,ifs_sub (default: all)")
    p.add_argument("--skip-maps", action="store_true",
                   help="Skip choropleth sections (4, 5, 6) — much faster")
    p.add_argument("--include-off-cycle", action="store_true",
                   help="Include 6z/18z init times (excluded by default)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    OUT  = args.out_dir
    os.makedirs(OUT, exist_ok=True)
    requested_models = {m.strip().lower() for m in args.models.split(",")}

    # ── Connect ──────────────────────────────────────────────────────────────
    print("Connecting to pipeline DB …")
    db = PipelineDB()
    db._conn.execute("SET memory_limit='24GB'; SET threads=8;")
    views = set(db.registered_views())
    print(f"  views: {sorted(views)}")

    HAS_ANOM      = "ifs_anom"    in views
    HAS_AIFS      = "aifs_bias"   in views
    HAS_AIFS_ANOM = "aifs_anom"   in views
    HAS_KOPPEN    = "koppen"      in views

    def q(sql):
        return db.query(sql)

    # ── Off-cycle filter (shadows views in-place) ─────────────────────────
    # duckdb_views().sql returns the full DDL ("CREATE VIEW name AS <query>").
    # We need only the inner <query> part to use it as a subquery.
    import re as _re
    if not args.include_off_cycle:
        for v in ["ifs_bias", "ifs_anom", "aifs_bias", "aifs_anom"]:
            if v not in views:
                continue
            orig = db._conn.execute(
                f"SELECT sql FROM duckdb_views() WHERE view_name = '{v}'"
            ).fetchone()
            if orig:
                ddl = orig[0]
                # Strip "CREATE [OR REPLACE] VIEW name AS " to get just the query
                m = _re.search(r"\bVIEW\s+\S+\s+AS\b", ddl, _re.IGNORECASE)
                inner = ddl[m.end():].strip() if m else ddl
                db._conn.execute(
                    f"CREATE OR REPLACE VIEW {v} AS "
                    f"SELECT * FROM ({inner}) _t "
                    f"WHERE HOUR(init_time) NOT IN (6, 18)"
                )
        print("  Off-cycle 6z/18z excluded (pass --include-off-cycle to override)")

    # ── Build MODELS list ─────────────────────────────────────────────────
    # Each entry: (display_name, bias_view, anom_view_or_None)
    MODELS = []
    if "ifs" in requested_models:
        MODELS.append(("IFS", "ifs_bias", "ifs_anom" if HAS_ANOM else None))

    if HAS_AIFS and "aifs" in requested_models:
        MODELS.append(("AIFS", "aifs_bias", "aifs_anom" if HAS_AIFS_ANOM else None))

    if HAS_AIFS and "ifs_sub" in requested_models:
        # Restrict IFS to the AIFS valid_time window for direct comparison
        t0, t1 = q("SELECT MIN(valid_time), MAX(valid_time) FROM aifs_bias").iloc[0]
        db._conn.execute(
            f"CREATE OR REPLACE VIEW ifs_bias_sub AS "
            f"SELECT * FROM ifs_bias WHERE valid_time BETWEEN '{t0}' AND '{t1}'"
        )
        sub_anom = None
        if HAS_ANOM and HAS_AIFS_ANOM:
            db._conn.execute(
                f"CREATE OR REPLACE VIEW ifs_anom_sub AS "
                f"SELECT * FROM ifs_anom WHERE valid_time BETWEEN '{t0}' AND '{t1}'"
            )
            sub_anom = "ifs_anom_sub"
        MODELS.append(("IFS_sub", "ifs_bias_sub", sub_anom))
        print(f"  IFS_sub restricted to AIFS period: {t0} – {t1}")

    print(f"  Models: {[m[0] for m in MODELS]}")

    # ── Available lead times ──────────────────────────────────────────────
    all_leads = sorted(
        q("SELECT DISTINCT lead_time FROM ifs_bias ORDER BY lead_time")["lead_time"].tolist()
    )
    if args.leads is not None:
        requested = [int(x.strip()) for x in args.leads.split(",")]
        unknown = sorted(set(requested) - set(all_leads))
        if unknown:
            print(f"  WARNING: requested leads not in data: {unknown}")
        all_leads = sorted(set(requested) & set(all_leads))
        print(f"  Lead filter applied: {all_leads}")
    lead_default = 24 if 24 in all_leads else (all_leads[0] if all_leads else 24)

    # For sections requiring ifs_anom, get anom leads (may differ)
    all_leads_anom = []
    if HAS_ANOM:
        all_leads_anom = sorted(
            q("SELECT DISTINCT lead_time FROM ifs_anom ORDER BY lead_time")["lead_time"].tolist()
        )
        if args.leads is not None:
            all_leads_anom = sorted(set(all_leads_anom) & set(
                int(x.strip()) for x in args.leads.split(",")
            ))

    # ── Load shapefile once ───────────────────────────────────────────────
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
    # 1  Aggregate skill curves — all models, all leads
    # ══════════════════════════════════════════════════════════════════════════
    def sec1():
        section("1  Aggregate skill curves")

        fig, axes = plt.subplots(2, 3, figsize=(17, 9), constrained_layout=True)
        ax_bias, ax_rmse, ax_mae = axes[0]
        ax_skill, ax_acc, ax_blank = axes[1]
        ax_blank.set_visible(False)

        for mname, bv, av in MODELS:
            col = MODEL_COLORS[mname]
            ls  = MODEL_LS[mname]
            kw  = dict(color=col, ls=ls, marker="o", ms=3, lw=1.3, label=mname)

            df_b = q(f"""
                SELECT lead_time,
                    SUM(bias*aland)/SUM(aland)              AS aw_bias,
                    SQRT(SUM(bias*bias*aland)/SUM(aland))   AS aw_rmse,
                    SUM(abs_error*aland)/SUM(aland)         AS aw_mae
                FROM {bv}
                GROUP BY lead_time ORDER BY lead_time
            """)
            ax_bias.plot(df_b["lead_time"], df_b["aw_bias"], **kw)
            ax_rmse.plot(df_b["lead_time"], df_b["aw_rmse"], **kw)
            ax_mae.plot( df_b["lead_time"], df_b["aw_mae"],  **kw)

            for row in df_b.itertuples():
                summary_rows.append({
                    "model": mname, "group": "all", "lead_time": int(row.lead_time),
                    "metric": "bias",  "value": row.aw_bias,
                })
                summary_rows.append({
                    "model": mname, "group": "all", "lead_time": int(row.lead_time),
                    "metric": "rmse",  "value": row.aw_rmse,
                })
                summary_rows.append({
                    "model": mname, "group": "all", "lead_time": int(row.lead_time),
                    "metric": "mae",   "value": row.aw_mae,
                })

            if av:
                df_a = q(f"""
                    SELECT lead_time,
                        {_wa_skill()} AS aw_skill,
                        {_wa_acc()}   AS aw_acc
                    FROM {av}
                    GROUP BY lead_time ORDER BY lead_time
                """)
                ax_skill.plot(df_a["lead_time"], df_a["aw_skill"], **kw)
                ax_acc.plot(  df_a["lead_time"], df_a["aw_acc"],   **kw)

                for row in df_a.itertuples():
                    summary_rows.append({
                        "model": mname, "group": "all", "lead_time": int(row.lead_time),
                        "metric": "skill", "value": row.aw_skill,
                    })
                    summary_rows.append({
                        "model": mname, "group": "all", "lead_time": int(row.lead_time),
                        "metric": "acc",   "value": row.aw_acc,
                    })

        ax_bias.axhline(0, color="k", lw=0.8, ls="--")
        ax_skill.axhline(0, color="k", lw=0.8, ls="--")

        for ax, ylabel, title in [
            (ax_bias,  "Bias (K)",    "Mean bias"),
            (ax_rmse,  "RMSE (K)",    "RMSE"),
            (ax_mae,   "MAE (K)",     "MAE"),
            (ax_skill, "Skill score", "MSE skill score  (0 = climatology)"),
            (ax_acc,   "ACC",         "Anomaly correlation (area-weighted)"),
        ]:
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel(ylabel)
            ax.set_title(title)
            ax.grid(True, alpha=0.3)
            if ax is ax_acc:
                vals = np.concatenate([l.get_ydata() for l in ax.get_lines()
                                       if len(l.get_ydata()) > 0])
                lo = float(np.nanmin(vals)) if len(vals) > 0 else 0.0
                ax.set_ylim(lo - 0.03, 1.0)

        axes[0, 0].legend(fontsize=9)
        fig.suptitle("NWP 2m Temperature Forecast Skill (area-weighted)", fontsize=12)
        savefig(fig, f"{OUT}/01_skill/skill_curves.png")

        # Seasonal breakdown of skill by model
        for mname, bv, av in MODELS:
            df_seas = q(f"""
                SELECT {_season_sql()} AS season, lead_time,
                    SUM(bias*aland)/SUM(aland)              AS aw_bias,
                    SQRT(SUM(bias*bias*aland)/SUM(aland))   AS aw_rmse,
                    SUM(abs_error*aland)/SUM(aland)         AS aw_mae
                FROM {bv}
                GROUP BY season, lead_time ORDER BY season, lead_time
            """)
            seasons = ["DJF", "MAM", "JJA", "SON"]
            fig, axes2 = plt.subplots(1, 3, figsize=(15, 4), constrained_layout=True)
            cmap_s = mcm.get_cmap("tab10", 4)
            for ax, col, ylabel in zip(axes2,
                                       ["aw_bias", "aw_rmse", "aw_mae"],
                                       ["Bias (K)", "RMSE (K)", "MAE (K)"]):
                for i, s in enumerate(seasons):
                    d = df_seas[df_seas["season"] == s]
                    ax.plot(d["lead_time"], d[col], "o-", ms=3, lw=1.2,
                            color=cmap_s(i), label=s)
                if col == "aw_bias":
                    ax.axhline(0, color="k", lw=0.8, ls="--")
                ax.set_xlabel("Lead time (h)")
                ax.set_ylabel(ylabel)
                ax.grid(True, alpha=0.3)
                ax.legend(fontsize=8)
            fig.suptitle(f"{mname} — skill by season", fontsize=11)
            savefig(fig, f"{OUT}/01_skill/{mname.lower()}_skill_by_season.png")

            for row in df_seas.itertuples():
                for metric, val in [("bias", row.aw_bias),
                                     ("rmse", row.aw_rmse),
                                     ("mae",  row.aw_mae)]:
                    summary_rows.append({
                        "model": mname, "group": f"season_{row.season}",
                        "lead_time": int(row.lead_time), "metric": metric, "value": val,
                    })

    try_section("1", sec1)

    # ══════════════════════════════════════════════════════════════════════════
    # 2  Timeseries — area-weighted daily bias / MAE (30-day MA)
    # ══════════════════════════════════════════════════════════════════════════
    def sec2():
        section("2  Timeseries")
        MA = 30

        def _ts(bias_view):
            df = q(f"""
                SELECT CAST(valid_time AS DATE) AS day,
                    SUM(bias*aland)/SUM(aland)      AS aw_bias,
                    SUM(abs_error*aland)/SUM(aland) AS aw_mae
                FROM {bias_view}
                GROUP BY day ORDER BY day
            """)
            df["day"] = pd.to_datetime(df["day"])
            return df

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6), sharex=True)
        for mname, bv, _ in MODELS:
            df = _ts(bv)
            kw = dict(color=MODEL_COLORS[mname], lw=0.4, alpha=0.3)
            kw_ma = dict(color=MODEL_COLORS[mname], ls=MODEL_LS[mname], lw=1.8,
                         label=mname)
            ax1.plot(df["day"], df["aw_bias"], **kw)
            ax1.plot(df["day"],
                     df["aw_bias"].rolling(MA, center=True, min_periods=MA//2).mean(),
                     **kw_ma)
            ax2.plot(df["day"], df["aw_mae"], **kw)
            ax2.plot(df["day"],
                     df["aw_mae"].rolling(MA, center=True, min_periods=MA//2).mean(),
                     **kw_ma)

        ax1.axhline(0, color="k", lw=0.8, ls="--")
        ax1.set_ylabel("Bias (K)")
        ax1.set_title(f"Area-weighted mean bias — all leads, all counties ({MA}-day MA bold)")
        ax1.legend(fontsize=8, loc="upper right")
        ax1.grid(True, alpha=0.3, axis="y")
        ax2.set_ylabel("MAE (K)")
        ax2.set_xlabel("Date")
        ax2.set_title("Area-weighted MAE")
        ax2.legend(fontsize=8, loc="upper right")
        ax2.grid(True, alpha=0.3)
        plt.tight_layout()
        savefig(fig, f"{OUT}/02_timeseries/daily_bias_mae.png")

        # Per-lead timeseries overlay — IFS only
        KEY_LEADS = [lt for lt in [24, 72, 120, 240] if lt in set(all_leads)]
        if KEY_LEADS:
            df_lt = q(f"""
                SELECT CAST(valid_time AS DATE) AS day, lead_time,
                    SUM(bias*aland)/SUM(aland) AS aw_bias
                FROM ifs_bias
                WHERE lead_time IN ({",".join(str(l) for l in KEY_LEADS)})
                GROUP BY day, lead_time ORDER BY day, lead_time
            """)
            df_lt["day"] = pd.to_datetime(df_lt["day"])
            cmap_lt = mcm.get_cmap("plasma", len(KEY_LEADS))
            fig, ax = plt.subplots(figsize=(14, 4))
            for i, lt in enumerate(KEY_LEADS):
                d = df_lt[df_lt["lead_time"] == lt].sort_values("day")
                ma = d["aw_bias"].rolling(MA, center=True, min_periods=MA//2).mean()
                ax.plot(d["day"], d["aw_bias"], lw=0.4, color=cmap_lt(i), alpha=0.3)
                ax.plot(d["day"], ma, lw=1.8, color=cmap_lt(i), label=f"{lt}h")
            ax.axhline(0, color="k", lw=0.8, ls="--")
            ax.set_ylabel("Area-weighted bias (K)")
            ax.set_xlabel("Date")
            ax.set_title(f"IFS — daily bias by lead ({MA}-day MA)")
            ax.legend(title="Lead", fontsize=8, ncol=len(KEY_LEADS))
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            savefig(fig, f"{OUT}/02_timeseries/ifs_daily_bias_by_lead.png")

    try_section("2", sec2)

    # ══════════════════════════════════════════════════════════════════════════
    # 3  Seasonal cycle — lead × month / DOY heatmaps, Hovmöller
    # ══════════════════════════════════════════════════════════════════════════
    def sec3():
        section("3  Seasonal cycle")

        # Use IFS as the reference; add AIFS overlays where leads match
        def _monthly(bias_view, anom_view, label):
            df = q(f"""
                SELECT MONTH(valid_time) AS month, lead_time,
                    SUM(bias*aland)/SUM(aland)      AS aw_bias,
                    SUM(abs_error*aland)/SUM(aland) AS aw_mae
                FROM {bias_view}
                GROUP BY month, lead_time ORDER BY month, lead_time
            """)
            if df.empty:
                print(f"    {label}: no data — skipping monthly plots")
                return
            leads  = sorted(df["lead_time"].unique())
            months = list(range(1, 13))
            X, Y = np.meshgrid(range(len(leads)), range(12))

            for col, cmap, lbl, diverging, fname_suffix in [
                ("aw_bias", RDBU, "Bias (K)", True,  "bias"),
                ("aw_mae",  REDS, "MAE (K)",  False, "mae"),
            ]:
                pivot = (df.pivot(index="month", columns="lead_time", values=col)
                         .reindex(index=months, columns=leads))
                absmax = pivot.abs().max().max()
                vmin = -absmax if diverging else pivot.min().min()
                vmax =  absmax

                # Heatmap
                fig, ax = plt.subplots(figsize=(10, 5))
                im = ax.imshow(pivot.values, aspect="auto", cmap=cmap,
                               vmin=vmin, vmax=vmax, origin="lower")
                ax.set_xticks(range(len(leads)))
                ax.set_xticklabels([f"{lt}h" for lt in leads], fontsize=7)
                ax.set_yticks(range(12))
                ax.set_yticklabels(MONTH_LABELS)
                ax.set_xlabel("Lead time (h)")
                ax.set_ylabel("Month")
                ax.set_title(f"{label} — {lbl} by lead × month")
                plt.colorbar(im, ax=ax, label=lbl)
                plt.tight_layout()
                savefig(fig, f"{OUT}/03_seasonal_cycle/{label.lower()}_{fname_suffix}_lead_x_month.png")

                # Contour
                levels = np.linspace(vmin, vmax, 15)
                fig, ax = plt.subplots(figsize=(10, 5))
                cf = ax.contourf(X, Y, pivot.values, levels=levels, cmap=cmap, extend="both")
                ax.contour(X, Y, pivot.values, levels=levels, colors="k",
                           linewidths=0.3, alpha=0.4)
                ax.set_xticks(range(len(leads)))
                ax.set_xticklabels([f"{lt}h" for lt in leads], fontsize=7)
                ax.set_yticks(range(12))
                ax.set_yticklabels(MONTH_LABELS)
                ax.set_xlabel("Lead time (h)")
                ax.set_ylabel("Month")
                ax.set_title(f"{label} — {lbl} by lead × month (contour)")
                plt.colorbar(cf, ax=ax, label=lbl)
                plt.tight_layout()
                savefig(fig, f"{OUT}/03_seasonal_cycle/{label.lower()}_{fname_suffix}_lead_x_month_contour.png")

            if anom_view:
                df_a = q(f"""
                    SELECT MONTH(valid_time) AS month, lead_time,
                        {_wa_acc()} AS aw_acc,
                        {_wa_skill()} AS aw_skill
                    FROM {anom_view}
                    GROUP BY month, lead_time ORDER BY month, lead_time
                """)
                if df_a.empty:
                    print(f"    {label}: no anom data — skipping ACC/skill monthly plots")
                    return
                for col, lbl, fname_suffix in [
                    ("aw_acc",   "ACC",         "acc"),
                    ("aw_skill", "Skill score", "skill"),
                ]:
                    pivot = (df_a.pivot(index="month", columns="lead_time", values=col)
                             .reindex(index=months, columns=leads))
                    raw_min = float(np.nanmin(pivot.values)) if not np.all(np.isnan(pivot.values)) else 0.0
                    vmin = raw_min - 0.02
                    vmax = 1.0
                    levels = np.linspace(vmin, vmax, 15)
                    fig, ax = plt.subplots(figsize=(10, 5))
                    cf = ax.contourf(X, Y, pivot.values, levels=levels,
                                    cmap="viridis", extend="both")
                    ax.contour(X, Y, pivot.values, levels=levels,
                               colors="k", linewidths=0.3, alpha=0.4)
                    ax.set_xticks(range(len(leads)))
                    ax.set_xticklabels([f"{lt}h" for lt in leads], fontsize=7)
                    ax.set_yticks(range(12))
                    ax.set_yticklabels(MONTH_LABELS)
                    ax.set_xlabel("Lead time (h)")
                    ax.set_ylabel("Month")
                    ax.set_title(f"{label} — {lbl} by lead × month")
                    plt.colorbar(cf, ax=ax, label=lbl)
                    plt.tight_layout()
                    savefig(fig, f"{OUT}/03_seasonal_cycle/{label.lower()}_{fname_suffix}_lead_x_month.png")

        # Run for each model
        for mname, bv, av in MODELS:
            _monthly(bv, av, mname)

        # DOY × lead heatmap — IFS only
        df_doy = q("""
            SELECT dayofyear(MAKE_DATE(2001, MONTH(valid_time), DAY(valid_time))) AS doy,
                lead_time,
                SUM(bias*aland)/SUM(aland)      AS aw_bias,
                SUM(abs_error*aland)/SUM(aland) AS aw_mae
            FROM ifs_bias
            WHERE NOT (MONTH(valid_time) = 2 AND DAY(valid_time) = 29)
            GROUP BY doy, lead_time ORDER BY doy, lead_time
        """)
        leads_doy = sorted(df_doy["lead_time"].unique())
        doys = list(range(1, 366))
        mstarts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]

        for col, cmap, lbl, diverging, fname_suffix in [
            ("aw_bias", RDBU, "Bias (K)", True,  "bias"),
            ("aw_mae",  REDS, "MAE (K)",  False, "mae"),
        ]:
            pivot = (df_doy.pivot(index="doy", columns="lead_time", values=col)
                     .reindex(index=doys, columns=leads_doy))
            absmax = pivot.abs().max().max()
            vmin = -absmax if diverging else pivot.min().min()
            vmax =  absmax
            fig, ax = plt.subplots(figsize=(10, 12))
            im = ax.imshow(pivot.values, aspect="auto", cmap=cmap,
                           vmin=vmin, vmax=vmax, origin="lower")
            ax.set_xticks(range(len(leads_doy)))
            ax.set_xticklabels([f"{lt}h" for lt in leads_doy], fontsize=7)
            ax.set_yticks([d - 1 for d in mstarts])
            ax.set_yticklabels(MONTH_LABELS)
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel("Day of year")
            ax.set_title(f"IFS — {lbl} by lead × day-of-year")
            plt.colorbar(im, ax=ax, label=lbl)
            plt.tight_layout()
            savefig(fig, f"{OUT}/03_seasonal_cycle/ifs_{fname_suffix}_lead_x_doy.png")

        if HAS_ANOM:
            _acc_sql   = _wa_acc()
            _skill_sql = _wa_skill()
            df_acc_doy = q(f"""
                SELECT dayofyear(MAKE_DATE(2001, MONTH(valid_time), DAY(valid_time))) AS doy,
                    lead_time,
                    {_acc_sql}   AS aw_acc,
                    {_skill_sql} AS aw_skill
                FROM ifs_anom
                WHERE NOT (MONTH(valid_time) = 2 AND DAY(valid_time) = 29)
                GROUP BY doy, lead_time ORDER BY doy, lead_time
            """)
            for col, lbl, fname_suffix in [
                ("aw_acc",   "ACC",         "acc"),
                ("aw_skill", "Skill score", "skill"),
            ]:
                pivot = (df_acc_doy.pivot(index="doy", columns="lead_time", values=col)
                         .reindex(index=doys, columns=leads_doy))
                vmin = pivot.min().min() - 0.02
                vmax = 1.0
                fig, ax = plt.subplots(figsize=(10, 12))
                im = ax.imshow(pivot.values, aspect="auto", cmap="viridis",
                               vmin=vmin, vmax=vmax, origin="lower")
                ax.set_xticks(range(len(leads_doy)))
                ax.set_xticklabels([f"{lt}h" for lt in leads_doy], fontsize=7)
                ax.set_yticks([d - 1 for d in mstarts])
                ax.set_yticklabels(MONTH_LABELS)
                ax.set_xlabel("Lead time (h)")
                ax.set_ylabel("Day of year")
                ax.set_title(f"IFS — {lbl} by lead × day-of-year")
                plt.colorbar(im, ax=ax, label=lbl)
                plt.tight_layout()
                savefig(fig, f"{OUT}/03_seasonal_cycle/ifs_{fname_suffix}_lead_x_doy.png")

        # Hovmöller: lead (x) × monthly valid time (y) — IFS
        df_hov = q("""
            SELECT DATE_TRUNC('month', valid_time) AS mdate, lead_time,
                SUM(bias*aland)/SUM(aland)      AS aw_bias,
                SUM(abs_error*aland)/SUM(aland) AS aw_mae
            FROM ifs_bias
            GROUP BY mdate, lead_time ORDER BY mdate, lead_time
        """)
        df_hov["mdate"] = pd.to_datetime(df_hov["mdate"])
        months_ts = sorted(df_hov["mdate"].unique())
        n_m = len(months_ts)
        tick_idx = list(range(0, n_m, 3))
        # Hovmöller uses actual lead values on x-axis (contourf, not imshow)
        X_h, Y_h = np.meshgrid(leads_doy, range(n_m))

        for col, cmap, lbl, diverging, fname_suffix in [
            ("aw_bias", RDBU, "Bias (K)", True,  "bias"),
            ("aw_mae",  REDS, "MAE (K)",  False, "mae"),
        ]:
            pivot = (df_hov.pivot(index="mdate", columns="lead_time", values=col)
                     .reindex(index=months_ts, columns=leads_doy))
            absmax = pivot.abs().max().max()
            vmin = -absmax if diverging else pivot.min().min()
            vmax =  absmax
            levels = np.linspace(vmin, vmax, 15)
            fig, ax = plt.subplots(figsize=(10, max(6, n_m * 0.3)))
            cf = ax.contourf(X_h, Y_h, pivot.values, levels=levels, cmap=cmap, extend="both")
            ax.contour(X_h, Y_h, pivot.values, levels=levels,
                       colors="k", linewidths=0.3, alpha=0.4)
            ax.set_xticks(leads_doy)
            ax.set_xticklabels([f"{lt}h" for lt in leads_doy], fontsize=7, rotation=45, ha="right")
            ax.set_yticks(tick_idx)
            ax.set_yticklabels([months_ts[i].strftime("%b %Y") for i in tick_idx], fontsize=7)
            ax.set_xlabel("Lead time (h)")
            ax.set_ylabel("Valid month")
            ax.set_title(f"IFS — Hovmöller {lbl} (lead × valid month)")
            plt.colorbar(cf, ax=ax, label=lbl)
            plt.tight_layout()
            savefig(fig, f"{OUT}/03_seasonal_cycle/ifs_hovmoller_{fname_suffix}.png")

    try_section("3", sec3)

    # ══════════════════════════════════════════════════════════════════════════
    # 4  Choropleth maps — bias, RMSE, skill score, ACC
    #    5 rows (Annual + seasons) × 4 cols per model per lead
    # ══════════════════════════════════════════════════════════════════════════
    def sec4():
        section("4  Maps (per model, per lead)")
        SEASONS = ["DJF", "MAM", "JJA", "SON"]

        col_specs = [
            # (col, cmap, diverging, label)
            ("mean_bias",  RDBU,     True,  "Bias (K)"),
            ("rmse",       REDS,     False, "RMSE (K)"),
            ("skill",      SKILL_CM, True,  "Skill score"),
            ("acc",        "viridis",False, "ACC"),
        ]

        def _query_maps(bv, av, lead, season_filter=""):
            df_b = prep_geo(q(f"""
                SELECT geo_id,
                    SUM(bias*aland)/SUM(aland)              AS mean_bias,
                    SQRT(SUM(bias*bias*aland)/SUM(aland))   AS rmse
                FROM {bv}
                WHERE lead_time = {lead}{season_filter}
                GROUP BY geo_id
            """))
            if av:
                df_a = prep_geo(q(f"""
                    SELECT geo_id,
                        {_wa_skill()} AS skill,
                        {_wa_acc()}   AS acc
                    FROM {av}
                    WHERE lead_time = {lead}{season_filter}
                    GROUP BY geo_id
                """))
                df_b = df_b.merge(df_a, on="geo_id", how="left")
            else:
                df_b["skill"] = float("nan")
                df_b["acc"]   = float("nan")
            return df_b

        for mname, bv, av in MODELS:
            for lead in all_leads:
                # Build scale limits from all-time data (so seasonal panels are comparable)
                df_ref = _query_maps(bv, av, lead)
                scales = {}
                for col, cmap, diverging, lbl in col_specs:
                    if col not in df_ref or df_ref[col].isna().all():
                        scales[col] = (0, 1, cmap, lbl)
                        continue
                    absmax = float(df_ref[col].abs().quantile(0.98))
                    if diverging:
                        scales[col] = (-absmax, absmax, cmap, lbl)
                    else:
                        vmin = float(df_ref[col].quantile(0.02))
                        scales[col] = (vmin, absmax, cmap, lbl)

                row_labels = ["Annual"] + SEASONS
                row_queries = [df_ref] + [
                    _query_maps(bv, av, lead, f" AND {_season_sql()} = '{s}'")
                    for s in SEASONS
                ]

                fig, axes = plt.subplots(5, 4, figsize=(20, 14), constrained_layout=True)
                for ri, (row_lbl, df_row) in enumerate(zip(row_labels, row_queries)):
                    for ci, (col, cmap, diverging, lbl) in enumerate(col_specs):
                        ax = axes[ri, ci]
                        if col not in df_row or df_row[col].isna().all():
                            ax.set_visible(False)
                            continue
                        vmin, vmax, cmap_used, _ = scales[col]
                        gdf = merge_map(df_row, col)
                        _plot_county(ax, gdf, col, cmap_used, vmin, vmax, "")
                        if ri == 0:
                            ax.set_title(lbl, fontsize=9, fontweight="bold")
                        if ci == 0:
                            ax.annotate(row_lbl, xy=(-0.02, 0.5),
                                        xycoords="axes fraction",
                                        fontsize=9, ha="right", va="center",
                                        fontweight="bold")

                # One colorbar per column at the bottom
                for ci, (col, cmap, diverging, lbl) in enumerate(col_specs):
                    vmin, vmax, cmap_used, _ = scales[col]
                    fig.colorbar(_sm(cmap_used, vmin, vmax),
                                 ax=axes[:, ci].tolist(),
                                 location="bottom", pad=0.01, fraction=0.03, label=lbl)

                fig.suptitle(f"{mname} — bias / RMSE / skill / ACC  |  lead {lead}h",
                             fontsize=12)
                savefig(fig, f"{OUT}/04_maps/{mname.lower()}_maps_lead{lead}h.png")

        # IFS_sub − AIFS difference maps (bias and skill)
        ifs_sub = next((m for m in MODELS if m[0] == "IFS_sub"), None)
        aifs    = next((m for m in MODELS if m[0] == "AIFS"), None)
        if ifs_sub and aifs:
            for lead in all_leads:
                df_i = _query_maps(ifs_sub[1], ifs_sub[2], lead)
                df_a = _query_maps(aifs[1],    aifs[2],    lead)
                df_diff = df_i.set_index("geo_id")[["mean_bias", "skill", "acc"]].subtract(
                    df_a.set_index("geo_id")[["mean_bias", "skill", "acc"]]
                ).reset_index()
                fig, axes = plt.subplots(1, 3, figsize=(18, 5), constrained_layout=True)
                for ax, col, lbl in zip(axes,
                                        ["mean_bias", "skill", "acc"],
                                        ["Δ Bias (K)", "Δ Skill", "Δ ACC"]):
                    if df_diff[col].isna().all():
                        ax.set_visible(False)
                        continue
                    absmax = float(df_diff[col].abs().quantile(0.98))
                    gdf = merge_map(df_diff, col)
                    _plot_county(ax, gdf, col, RDBU, -absmax, absmax, lbl)
                    fig.colorbar(_sm(RDBU, -absmax, absmax), ax=ax,
                                 location="bottom", pad=0.02, fraction=0.05, label=lbl)
                fig.suptitle(f"IFS_sub − AIFS  |  lead {lead}h  (blue = IFS better)",
                             fontsize=11)
                savefig(fig, f"{OUT}/04_maps/ifs_sub_minus_aifs_lead{lead}h.png")

    if not args.skip_maps:
        try_section("4", sec4)
    else:
        print("  4 skipped (--skip-maps)")

    # ══════════════════════════════════════════════════════════════════════════
    # 5  MSE variance decomposition maps
    #    MSE = fc_var + an_var − 2·cov(fc_anom, an_anom)
    #    5 rows (Annual + seasons) × 4 cols — per model per lead
    # ══════════════════════════════════════════════════════════════════════════
    def sec5():
        section("5  MSE variance decomposition maps")

        # Only models that have an anom view
        anom_models = [(m, bv, av) for m, bv, av in MODELS if av]
        if not anom_models:
            print("  Skipped — no anom views available")
            return

        def _query_decomp(av, lead, season_filter=""):
            return prep_geo(q(f"""
                SELECT geo_id,
                    AVG(bias*bias)           AS mse,
                    AVG(fc_anom*fc_anom)     AS fc_var,
                    AVG(an_anom*an_anom)     AS an_var,
                    2*AVG(fc_anom*an_anom)   AS two_cov
                FROM {av}
                WHERE lead_time = {lead}{season_filter}
                GROUP BY geo_id
            """))

        SEASONS = ["DJF", "MAM", "JJA", "SON"]

        for mname, bv, av in anom_models:
            for lead in all_leads_anom:
                df_ann = _query_decomp(av, lead)
                vmax_var = float(df_ann[["mse", "fc_var", "an_var"]].quantile(0.98).max())
                cov_abs  = float(df_ann["two_cov"].abs().quantile(0.98))

                col_specs = [
                    ("mse",     REDS, 0,        vmax_var, "MSE (K²)"),
                    ("fc_var",  REDS, 0,        vmax_var, "FC variance (K²)"),
                    ("an_var",  REDS, 0,        vmax_var, "AN variance (K²)"),
                    ("two_cov", RDBU, -cov_abs, cov_abs,  "2·Cov(fc,an) (K²)"),
                ]

                row_labels = ["Annual"] + SEASONS
                row_data   = [df_ann] + [
                    _query_decomp(av, lead, f" AND {_season_sql()} = '{s}'")
                    for s in SEASONS
                ]

                fig, axes = plt.subplots(5, 4, figsize=(20, 14), constrained_layout=True)
                for ri, (row_lbl, df_row) in enumerate(zip(row_labels, row_data)):
                    for ci, (col, cmap, vmin, vmax, _) in enumerate(col_specs):
                        ax = axes[ri, ci]
                        gdf = merge_map(df_row, col)
                        _plot_county(ax, gdf, col, cmap, vmin, vmax, "")
                        if ri == 0:
                            ax.set_title(col_specs[ci][4], fontsize=9, fontweight="bold")
                        if ci == 0:
                            ax.annotate(row_lbl, xy=(-0.02, 0.5),
                                        xycoords="axes fraction",
                                        fontsize=9, ha="right", va="center",
                                        fontweight="bold")

                for ci, (col, cmap, vmin, vmax, lbl) in enumerate(col_specs):
                    fig.colorbar(_sm(cmap, vmin, vmax),
                                 ax=axes[:, ci].tolist(),
                                 location="bottom", pad=0.01, fraction=0.03, label=lbl)

                fig.suptitle(
                    f"{mname} — MSE = FC var + AN var − 2·Cov  |  lead {lead}h",
                    fontsize=12)
                savefig(fig, f"{OUT}/05_mse_decomp/{mname.lower()}_mse_decomp_lead{lead}h.png")

    if not args.skip_maps:
        try_section("5", sec5)
    else:
        print("  5 skipped (--skip-maps)")

    # ══════════════════════════════════════════════════════════════════════════
    # 6  9-panel MSE decomposition — bias sign × anomaly sign (IFS)
    # ══════════════════════════════════════════════════════════════════════════
    def sec6():
        if not HAS_ANOM:
            print("  Skipped — ifs_anom not registered")
            return
        section("6  9-panel MSE decomposition (IFS)")

        PANEL_SPECS = [
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

        def _qmse(lead, season_filter=""):
            return prep_geo(q(f"""
                SELECT geo_id,
                    AVG(bias*bias)                                               AS mse_total,
                    AVG(CASE WHEN bias    < 0 THEN bias*bias END)                AS mse_cold_bias,
                    AVG(CASE WHEN bias    > 0 THEN bias*bias END)                AS mse_hot_bias,
                    AVG(CASE WHEN an_anom < 0 THEN bias*bias END)                AS mse_cold_anom,
                    AVG(CASE WHEN an_anom > 0 THEN bias*bias END)                AS mse_hot_anom,
                    AVG(CASE WHEN an_anom < 0 AND bias < 0 THEN bias*bias END)   AS mse_ca_cb,
                    AVG(CASE WHEN an_anom < 0 AND bias > 0 THEN bias*bias END)   AS mse_ca_hb,
                    AVG(CASE WHEN an_anom > 0 AND bias < 0 THEN bias*bias END)   AS mse_ha_cb,
                    AVG(CASE WHEN an_anom > 0 AND bias > 0 THEN bias*bias END)   AS mse_ha_hb
                FROM ifs_anom
                WHERE lead_time = {lead}{season_filter}
                GROUP BY geo_id
            """))

        def _plot9(df, label, lead, fname, vmax):
            fig, axes = plt.subplots(3, 3, figsize=(18, 14), constrained_layout=True)
            for ri, ci, col, title in PANEL_SPECS:
                ax = axes[ri, ci]
                gdf = merge_map(df, col)
                _plot_county(ax, gdf, col, REDS, 0, vmax, title)
            # One shared colorbar
            fig.colorbar(_sm(REDS, 0, vmax),
                         ax=axes.ravel().tolist(),
                         location="bottom", pad=0.01, fraction=0.02, label="MSE (K²)")
            fig.suptitle(f"IFS MSE decomposition — {label}, lead {lead}h", fontsize=13)
            savefig(fig, fname)

        for lead in all_leads_anom:
            df_all = _qmse(lead)
            vmax = float(df_all["mse_total"].quantile(0.98))
            _plot9(df_all, "all time", lead,
                   f"{OUT}/06_mse9/ifs_mse9_alltime_lead{lead}h.png", vmax)
            for s in ["DJF", "MAM", "JJA", "SON"]:
                df_s = _qmse(lead, f" AND {_season_sql()} = '{s}'")
                _plot9(df_s, s, lead,
                       f"{OUT}/06_mse9/ifs_mse9_{s}_lead{lead}h.png", vmax)

    if not args.skip_maps:
        try_section("6", sec6)
    else:
        print("  6 skipped (--skip-maps)")

    # ══════════════════════════════════════════════════════════════════════════
    # 7  Joint PDF — observed anomaly vs forecast bias (IFS + AIFS)
    # ══════════════════════════════════════════════════════════════════════════
    def sec7():
        if not HAS_ANOM:
            print("  Skipped — ifs_anom not registered")
            return
        section("7  Joint PDF — anomaly vs bias")

        PDF_XLIM = (-10, 10)
        PDF_YLIM = (-10, 10)

        def _qpdf(av, lead, season_filter=""):
            return q(f"""
                SELECT
                    ROUND(an_anom / {PDF_BIN_K}) * {PDF_BIN_K} AS anom_bin,
                    ROUND(bias    / {PDF_BIN_K}) * {PDF_BIN_K} AS bias_bin,
                    SUM(aland)                                  AS total_area
                FROM {av}
                WHERE lead_time = {lead}{season_filter}
                GROUP BY anom_bin, bias_bin
                ORDER BY anom_bin, bias_bin
            """)

        def _pivot(df):
            df = df[
                (df["anom_bin"] >= PDF_XLIM[0]) & (df["anom_bin"] <= PDF_XLIM[1]) &
                (df["bias_bin"] >= PDF_YLIM[0]) & (df["bias_bin"] <= PDF_YLIM[1])
            ].copy()
            if df.empty:
                return pd.DataFrame()
            pivot = df.pivot_table(index="bias_bin", columns="anom_bin",
                                   values="total_area", aggfunc="sum", fill_value=0)
            total = pivot.values.sum()
            return pivot / total if total > 0 else pivot

        def _draw(ax, pivot, title, norm=None):
            if pivot.empty or pivot.values.sum() == 0:
                ax.text(0.5, 0.5, "No data", ha="center", va="center",
                        transform=ax.transAxes)
                ax.set_title(title, fontsize=9)
                return None
            if norm is None:
                vmax = float(pivot.values.max())
                norm = mcolors.LogNorm(vmin=1e-4, vmax=max(vmax, 1e-3))
            extent = [float(pivot.columns.min()) - PDF_BIN_K / 2,
                      float(pivot.columns.max()) + PDF_BIN_K / 2,
                      float(pivot.index.min())   - PDF_BIN_K / 2,
                      float(pivot.index.max())   + PDF_BIN_K / 2]
            im = ax.imshow(pivot.values, cmap="plasma", norm=norm,
                           origin="lower", extent=extent, aspect="auto")
            ax.axhline(0, color="white", lw=0.8, alpha=0.6)
            ax.axvline(0, color="white", lw=0.8, alpha=0.6)
            ax.set_xlim(*PDF_XLIM); ax.set_ylim(*PDF_YLIM)
            ax.set_xlabel("Observed anomaly (K)", fontsize=8)
            ax.set_ylabel("Forecast bias (K)", fontsize=8)
            ax.set_title(title, fontsize=9)
            for tx, ty, lbl in [
                (-7,  7, "Cold/Hot"), (7,  7, "Hot/Hot"),
                (-7, -7, "Cold/Cold"),(7, -7, "Hot/Cold"),
            ]:
                ax.text(tx, ty, lbl, ha="center", va="center",
                        fontsize=6, color="white", alpha=0.7)
            return im

        anom_models = [(m, av) for m, _, av in MODELS if av]

        for lead in all_leads_anom:
            # All-time: one panel per model side by side
            n_mod = len(anom_models)
            fig, axes = plt.subplots(1, n_mod, figsize=(7 * n_mod, 6),
                                     constrained_layout=True)
            if n_mod == 1:
                axes = [axes]
            all_pivots = [_pivot(_qpdf(av, lead)) for _, av in anom_models]
            vmaxes = [float(p.values.max()) for p in all_pivots if not p.empty]
            shared_norm = mcolors.LogNorm(vmin=1e-4, vmax=max(vmaxes, default=1e-3))
            last_im = None
            for ax, (mname, _), pivot in zip(axes, anom_models, all_pivots):
                im = _draw(ax, pivot, f"{mname} — all time, lead {lead}h", shared_norm)
                if im is not None:
                    last_im = im
            if last_im:
                fig.colorbar(last_im, ax=axes, label="Area-weighted density")
            savefig(fig, f"{OUT}/07_joint_pdf/joint_pdf_alltime_lead{lead}h.png")

            # Seasonal 2×2 per model
            for mname, av in anom_models:
                pivots_s = {s: _pivot(_qpdf(av, lead, f" AND {_season_sql()} = '{s}'"))
                            for s in ["DJF", "MAM", "JJA", "SON"]}
                nonempty = [p for p in pivots_s.values() if not p.empty]
                seas_vmax = max((float(p.values.max()) for p in nonempty), default=1e-3)
                seas_norm = mcolors.LogNorm(vmin=1e-4, vmax=max(seas_vmax, 1e-3))
                fig, axes = plt.subplots(2, 2, figsize=(12, 10))
                last_im = None
                for ax, s in zip(axes.flatten(), ["DJF", "MAM", "JJA", "SON"]):
                    im = _draw(ax, pivots_s[s], s, seas_norm)
                    if im:
                        last_im = im
                fig.suptitle(
                    f"{mname} — joint PDF by season, lead {lead}h", fontsize=11)
                if last_im:
                    fig.colorbar(last_im, ax=axes.ravel().tolist(),
                                 label="Area-weighted density", shrink=0.6)
                plt.tight_layout()
                savefig(fig, f"{OUT}/07_joint_pdf/{mname.lower()}_joint_pdf_seasonal_lead{lead}h.png")

    try_section("7", sec7)

    # ══════════════════════════════════════════════════════════════════════════
    # 8  Histograms — county-level bias/MAE distributions
    # ══════════════════════════════════════════════════════════════════════════
    def sec8():
        section("8  Histograms")
        # Use 4 representative leads; fall back to all if fewer than 4 available
        KEY_LEADS = sorted(lt for lt in [24, 72, 120, 240] if lt in set(all_leads))
        if not KEY_LEADS:
            KEY_LEADS = all_leads[:4]

        # Multi-lead bias/MAE overlay — all models
        fig, axes = plt.subplots(1, 2, figsize=(12, 4), constrained_layout=True)
        for mname, bv, _ in MODELS:
            df = q(f"""
                SELECT lead_time, geo_id, AVG(bias) AS mean_bias, AVG(abs_error) AS mae
                FROM {bv}
                WHERE lead_time IN ({",".join(str(l) for l in KEY_LEADS)})
                GROUP BY lead_time, geo_id
            """)
            cmap_lt = mcm.get_cmap("plasma", len(KEY_LEADS))
            ls = MODEL_LS[mname]
            for i, lt in enumerate(KEY_LEADS):
                d = df[df["lead_time"] == lt]
                axes[0].hist(d["mean_bias"].dropna(), bins=60, alpha=0.4, density=True,
                             color=cmap_lt(i), histtype="step", lw=1.5, ls=ls,
                             label=f"{mname} {lt}h")
                axes[1].hist(d["mae"].dropna(), bins=60, alpha=0.4, density=True,
                             color=cmap_lt(i), histtype="step", lw=1.5, ls=ls)

        axes[0].axvline(0, color="k", lw=0.8, ls="--")
        axes[0].set_xlabel("County mean bias (K)"); axes[0].set_ylabel("Density")
        axes[0].set_title("County mean bias — all models, key leads")
        axes[0].legend(fontsize=7, ncol=2)
        axes[1].set_xlabel("County mean MAE (K)"); axes[1].set_ylabel("Density")
        axes[1].set_title("County mean MAE — all models, key leads")
        savefig(fig, f"{OUT}/08_histograms/bias_mae_hist.png")

        # Seasonal histograms — IFS, all leads
        for lead in all_leads:
            df_s = q(f"""
                SELECT geo_id, {_season_sql()} AS season,
                    AVG(bias) AS mean_bias, AVG(abs_error) AS mae
                FROM ifs_bias WHERE lead_time = {lead}
                GROUP BY geo_id, season
            """)
            fig, axes = plt.subplots(2, 4, figsize=(16, 7), sharey="row")
            for row, (col, xlabel) in enumerate([
                ("mean_bias", "County mean bias (K)"),
                ("mae",       "County mean MAE (K)"),
            ]):
                for ax, season in zip(axes[row], ["DJF", "MAM", "JJA", "SON"]):
                    d = df_s[df_s["season"] == season][col].dropna()
                    ax.hist(d, bins=50, density=True, color="#1f77b4", alpha=0.75)
                    if col == "mean_bias":
                        ax.axvline(0, color="k", lw=0.8, ls="--")
                    ax.set_title(season, fontsize=10)
                    ax.set_xlabel(xlabel, fontsize=8)
                    if ax is axes[row][0]:
                        ax.set_ylabel("Density", fontsize=8)
                    ax.grid(True, alpha=0.3)
            fig.suptitle(f"IFS — county bias/MAE by season (lead {lead}h)", fontsize=11)
            plt.tight_layout()
            savefig(fig, f"{OUT}/08_histograms/ifs_seasonal_hist_lead{lead}h.png")

    try_section("8", sec8)

    # ══════════════════════════════════════════════════════════════════════════
    # 9  Koppen-Geiger climate region interactions
    # ══════════════════════════════════════════════════════════════════════════
    def sec9():
        if not HAS_KOPPEN:
            print("  Skipped — koppen view not registered")
            return
        section("9  Koppen-Geiger interactions")

        # Skill by Koppen class vs lead — all models
        fig, axes = plt.subplots(1, 3, figsize=(17, 5), constrained_layout=True)
        classes = None

        for mname, bv, av in MODELS:
            ls     = MODEL_LS[mname]
            marker = MODEL_MARKERS[mname]
            df_k = q(f"""
                SELECT k.category_1 AS koppen, b.lead_time,
                    SUM(b.bias*b.aland)/SUM(b.aland)              AS aw_bias,
                    SQRT(SUM(b.bias*b.bias*b.aland)/SUM(b.aland)) AS aw_rmse,
                    SUM(b.abs_error*b.aland)/SUM(b.aland)         AS aw_mae
                FROM {bv} b JOIN koppen k ON b.geo_id = k.geo_id
                GROUP BY k.category_1, b.lead_time
                ORDER BY k.category_1, b.lead_time
            """)
            if df_k.empty:
                continue
            if classes is None:
                classes = sorted(df_k["koppen"].dropna().unique())
            cmap_k = mcm.get_cmap("tab10", len(classes))

            for ax, col_name, ylabel in zip(axes,
                                            ["aw_bias", "aw_rmse", "aw_mae"],
                                            ["Bias (K)", "RMSE (K)", "MAE (K)"]):
                for i, klass in enumerate(classes):
                    d = df_k[df_k["koppen"] == klass]
                    # Label every class for IFS (color key); other models
                    # are distinguished by linestyle + marker only
                    ax.plot(d["lead_time"], d[col_name], ls=ls, lw=1.1,
                            color=cmap_k(i), ms=3, marker=marker,
                            label=koppen_label(klass) if mname == "IFS" else "")

            for row in df_k.itertuples():
                for metric, val in [("bias", row.aw_bias),
                                     ("rmse", row.aw_rmse), ("mae", row.aw_mae)]:
                    summary_rows.append({
                        "model": mname, "group": f"koppen_{row.koppen}",
                        "lead_time": int(row.lead_time), "metric": metric, "value": val,
                    })

        axes[0].axhline(0, color="k", lw=0.8, ls="--")
        for ax, ylabel, title in zip(axes,
                                     ["Bias (K)", "RMSE (K)", "MAE (K)"],
                                     ["Mean bias", "RMSE", "MAE"]):
            ax.set_xlabel("Lead time (h)"); ax.set_ylabel(ylabel)
            ax.set_title(f"Skill by Koppen class — {title}")
            ax.grid(True, alpha=0.3)

        # Two-part legend: Koppen class colors (from IFS lines) + model style guide
        koppen_handles, koppen_lbls = axes[0].get_legend_handles_labels()
        import matplotlib.lines as mlines
        style_handles = [
            mlines.Line2D([], [], color="k", ls=MODEL_LS[m], marker=MODEL_MARKERS[m],
                          ms=4, lw=1.1, label=m)
            for m, _, _ in MODELS
        ]
        fig.legend(koppen_handles + style_handles,
                   koppen_lbls + [m for m, _, _ in MODELS],
                   loc="lower center",
                   ncol=min((len(classes or []) + len(MODELS)), 5),
                   fontsize=7, bbox_to_anchor=(0.5, -0.18))
        fig.suptitle("Skill by Koppen-Geiger class — color = class, style = model",
                     fontsize=11)
        plt.tight_layout()
        savefig(fig, f"{OUT}/09_koppen/skill_by_koppen.png")

        # Koppen × season heatmap — IFS, per lead
        for lead in all_leads:
            df_ks = q(f"""
                SELECT k.category_1 AS koppen,
                    {_season_sql('b.valid_time')} AS season,
                    SUM(b.bias*b.aland)/SUM(b.aland) AS aw_bias
                FROM ifs_bias b JOIN koppen k ON b.geo_id = k.geo_id
                WHERE b.lead_time = {lead}
                GROUP BY k.category_1, season
            """)
            pivot = (df_ks.pivot(index="koppen", columns="season", values="aw_bias")
                     .reindex(columns=["DJF", "MAM", "JJA", "SON"]))
            labels = [koppen_label(c) for c in pivot.index]
            absmax = pivot.abs().max().max()
            fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.55 + 1)))
            im = ax.imshow(pivot.values, cmap=RDBU, vmin=-absmax, vmax=absmax, aspect="auto")
            ax.set_xticks(range(4))
            ax.set_xticklabels(["DJF", "MAM", "JJA", "SON"])
            ax.set_yticks(range(len(pivot)))
            ax.set_yticklabels(labels, fontsize=8)
            for (r, c), val in np.ndenumerate(pivot.values):
                if not np.isnan(val):
                    ax.text(c, r, f"{val:+.2f}", ha="center", va="center", fontsize=8)
            plt.colorbar(im, ax=ax, label="Area-weighted bias (K)")
            ax.set_title(f"IFS mean bias — Koppen × season (lead {lead}h)")
            plt.tight_layout()
            savefig(fig, f"{OUT}/09_koppen/ifs_koppen_season_lead{lead}h.png")

    try_section("9", sec9)

    # ══════════════════════════════════════════════════════════════════════════
    # 10  Demographic interactions
    #     - Correlation vs lead (all leads) as line plots
    #     - Scatter grid at lead_default for top variables
    # ══════════════════════════════════════════════════════════════════════════
    def sec10():
        model_input = os.path.join(
            os.path.dirname(__file__), "..", "notebooks", "data", "model_input.parquet"
        )
        if not os.path.exists(model_input):
            print(f"  Skipped — {model_input} not found (run notebook 03 first)")
            return
        section("10  Demographic interactions")

        df_mi    = pd.read_parquet(model_input)
        demo_cols = [c for c in df_mi.columns if c.startswith("demo_")]
        if not demo_cols:
            print("  No demo_ columns found")
            return

        avail_leads = sorted(df_mi["lead_time"].unique()) if "lead_time" in df_mi.columns else [lead_default]

        # — Correlation vs lead (all leads) ───────────────────────────────────
        corr_by_lead = {}
        for lt in avail_leads:
            d = df_mi[df_mi["lead_time"] == lt].groupby("geo_id")[["mean_bias"] + demo_cols].mean()
            corr_by_lead[lt] = {dc: d["mean_bias"].corr(d[dc]) for dc in demo_cols}

        df_corr = pd.DataFrame(corr_by_lead).T  # index=lead, cols=demo_cols

        # Sort demo variables by |max correlation| across leads
        order = df_corr.abs().max().sort_values(ascending=False).index.tolist()
        top8  = order[:8]

        cmap_d = mcm.get_cmap("tab10", len(top8))
        fig, ax = plt.subplots(figsize=(12, 5))
        for i, dc in enumerate(top8):
            ax.plot(df_corr.index, df_corr[dc], "o-", ms=3, lw=1.3,
                    color=cmap_d(i), label=dc.replace("demo_", "").replace("_", " "))
        ax.axhline(0, color="k", lw=0.8, ls="--")
        ax.set_xlabel("Lead time (h)")
        ax.set_ylabel("Pearson r with county mean bias")
        ax.set_title("IFS bias–demographic correlation vs lead time (top 8 variables)")
        ax.legend(fontsize=8, ncol=2, bbox_to_anchor=(1.01, 1), loc="upper left")
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        savefig(fig, f"{OUT}/10_demographics/bias_demo_corr_vs_lead.png")

        # Bar chart at lead_default
        demo_lead = lead_default if lead_default in avail_leads else avail_leads[0]
        corr_at_default = df_corr.loc[demo_lead].sort_values()
        bar_colors = ["#d6604d" if v > 0 else "#2166ac" for v in corr_at_default.values]
        fig, ax = plt.subplots(figsize=(8, max(4, len(corr_at_default) * 0.4 + 1)))
        ax.barh(corr_at_default.index.str.replace("demo_", "").str.replace("_", " "),
                corr_at_default.values, color=bar_colors)
        ax.axvline(0, color="k", lw=0.8)
        ax.set_xlabel("Pearson r with county mean bias")
        ax.set_title(f"IFS {demo_lead}h — bias–demographic correlations")
        ax.grid(True, alpha=0.3, axis="x")
        plt.tight_layout()
        savefig(fig, f"{OUT}/10_demographics/bias_demo_corr_bar_lead{demo_lead}h.png")

        # Scatter grid for top variables at lead_default
        df_county = (df_mi[df_mi["lead_time"] == demo_lead]
                     .groupby("geo_id")[["mean_bias"] + top8]
                     .mean().reset_index().dropna(subset=["mean_bias"]))
        ncols = 4
        nrows = int(np.ceil(len(top8) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 3.8, nrows * 3.2))
        axes = axes.flatten()
        for i, dc in enumerate(top8):
            ax = axes[i]
            d  = df_county[["mean_bias", dc]].dropna()
            ax.scatter(d[dc], d["mean_bias"], s=4, alpha=0.35,
                       color="#1f77b4", rasterized=True)
            if len(d) > 10:
                m, b = np.polyfit(d[dc], d["mean_bias"], 1)
                xs = np.linspace(d[dc].min(), d[dc].max(), 100)
                ax.plot(xs, m * xs + b, "r-", lw=1.2)
            ax.axhline(0, color="k", lw=0.7, ls="--")
            r = corr_by_lead[demo_lead].get(dc, float("nan"))
            ax.set_xlabel(dc.replace("demo_", "").replace("_", " "), fontsize=8)
            ax.set_ylabel("Mean bias (K)", fontsize=8)
            ax.set_title(f"r = {r:.3f}", fontsize=8)
            ax.tick_params(labelsize=7)
            ax.grid(True, alpha=0.2)
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)
        fig.suptitle(f"IFS {demo_lead}h — county bias vs demographic indices (top 8)", fontsize=11)
        plt.tight_layout()
        savefig(fig, f"{OUT}/10_demographics/bias_demo_scatter_lead{demo_lead}h.png")

        print(f"\n  Top correlations at lead {demo_lead}h (|r| > 0.1):")
        for dc, r in corr_at_default.items():
            if abs(r) > 0.1:
                print(f"    {dc.replace('demo_',''):30s}  r = {r:+.3f}")

    try_section("10", sec10)

    # ══════════════════════════════════════════════════════════════════════════
    # 11  Summary CSV
    # ══════════════════════════════════════════════════════════════════════════
    section("11  Summary")
    if summary_rows:
        df_sum = pd.DataFrame(summary_rows)
        out_csv = f"{OUT}/summary.csv"
        df_sum.to_csv(out_csv, index=False, float_format="%.4f")
        print(f"  Saved {len(df_sum):,} rows to {out_csv}")
        # Quick pivot for display
        pivot_sum = (df_sum[df_sum["group"] == "all"]
                     .pivot_table(index=["model", "lead_time"],
                                  columns="metric", values="value"))
        print(pivot_sum.to_string())

    db.close()
    print(f"\nDone.  All figures in: {OUT}/")


if __name__ == "__main__":
    main()
