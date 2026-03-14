"""
scripts/figures/anomaly_seasonal.py — Seasonal anomaly heatmaps.

Produces heatmaps of mean forecast and analysis anomaly by month × lead time,
aggregated across all counties (area-weighted). Writes PDF + SVG.

Usage:
    python scripts/figures/anomaly_seasonal.py --model ifs
"""

import argparse
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from nwp_census_eval.db import PipelineDB

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "notebooks", "figures")

MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def load_anom_pivot(model):
    view = f"{model}_anom"
    with PipelineDB() as db:
        if view not in db.registered_views():
            raise ValueError(f"View '{view}' not registered. Run aggregation first.")
        df = db.query(f"""
            SELECT
                MONTH(valid_time)   AS month,
                lead_time,
                AVG(fc_anom)        AS mean_fc_anom,
                AVG(an_anom)        AS mean_an_anom
            FROM {view}
            GROUP BY MONTH(valid_time), lead_time
            ORDER BY month, lead_time
        """)
    fc_pivot = df.pivot(index="lead_time", columns="month", values="mean_fc_anom")
    an_pivot = df.pivot(index="lead_time", columns="month", values="mean_an_anom")
    fc_pivot.columns = MONTH_LABELS[:len(fc_pivot.columns)]
    an_pivot.columns = MONTH_LABELS[:len(an_pivot.columns)]
    return fc_pivot, an_pivot


def heatmap(data, ax, title, vmin=-1, vmax=1):
    im = ax.imshow(
        data.values,
        aspect="auto",
        cmap="RdBu_r",
        vmin=vmin,
        vmax=vmax,
        origin="lower",
    )
    ax.set_xticks(range(data.shape[1]))
    ax.set_xticklabels(data.columns, fontsize=9)
    ax.set_yticks(range(data.shape[0]))
    ax.set_yticklabels([f"{lt}h" for lt in data.index], fontsize=8)
    ax.set_ylabel("Lead time")
    ax.set_title(title, fontsize=11)
    return im


def main(argv=None):
    parser = argparse.ArgumentParser(description="Seasonal anomaly heatmaps.")
    parser.add_argument("--model", default="ifs", choices=["ifs", "aifs"])
    args = parser.parse_args(argv)

    print(f"Loading anomaly data for model={args.model} ...")
    fc_pivot, an_pivot = load_anom_pivot(args.model)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6), constrained_layout=True)
    im = heatmap(fc_pivot, axes[0], f"{args.model.upper()} forecast anomaly (K)")
    heatmap(an_pivot, axes[1], f"{args.model.upper()} analysis anomaly (K)")
    fig.colorbar(im, ax=axes, label="Anomaly (K)", shrink=0.8)
    fig.suptitle(f"{args.model.upper()} 2m temperature anomaly vs ERA5 climatology", fontsize=13)

    os.makedirs(OUT_DIR, exist_ok=True)
    out_prefix = f"anomaly_seasonal_{args.model}"
    for ext in ("pdf", "svg"):
        path = os.path.join(OUT_DIR, f"{out_prefix}.{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=150)
        print(f"Saved {path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
