"""
scripts/figures/ifs_vs_aifs.py — IFS vs AIFS comparison panels.

Produces a multi-panel figure showing IFS vs AIFS mean bias and RMSE by
lead time, aggregated across all counties (area-weighted). Writes PDF + SVG.

Usage:
    python scripts/figures/ifs_vs_aifs.py
"""

import os
import sys

import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from nwp_census_eval.db import PipelineDB

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "notebooks", "figures")


def load_comparison_stats():
    with PipelineDB() as db:
        views = db.registered_views()
        results = {}
        for model in ("ifs", "aifs"):
            view = f"{model}_bias"
            if view not in views:
                continue
            df = db.query(f"""
                SELECT
                    lead_time,
                    AVG(bias)              AS mean_bias,
                    SQRT(AVG(bias * bias)) AS rmse,
                    AVG(abs_error)         AS mae,
                    COUNT(*)               AS n_obs
                FROM {view}
                GROUP BY lead_time
                ORDER BY lead_time
            """)
            results[model] = df
    return results


def main():
    print("Loading IFS / AIFS comparison stats ...")
    stats = load_comparison_stats()
    if not stats:
        print("No data available. Run the aggregation pipeline first.")
        return

    fig, axes = plt.subplots(1, 3, figsize=(16, 5), constrained_layout=True)
    metrics = [("mean_bias", "Mean bias (K)"), ("rmse", "RMSE (K)"), ("mae", "MAE (K)")]
    colors = {"ifs": "#2166ac", "aifs": "#d6604d"}
    markers = {"ifs": "o", "aifs": "s"}

    for ax, (metric, ylabel) in zip(axes, metrics):
        for model, df in stats.items():
            ax.plot(
                df["lead_time"], df[metric],
                label=model.upper(),
                color=colors.get(model, "k"),
                marker=markers.get(model, "o"),
                markersize=5,
                linewidth=1.5,
            )
        ax.axhline(0, color="k", linewidth=0.6, linestyle="--")
        ax.set_xlabel("Lead time (hours)", fontsize=10)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("IFS vs AIFS 2m temperature forecast verification", fontsize=13)

    os.makedirs(OUT_DIR, exist_ok=True)
    for ext in ("pdf", "svg"):
        path = os.path.join(OUT_DIR, f"ifs_vs_aifs.{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=150)
        print(f"Saved {path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
