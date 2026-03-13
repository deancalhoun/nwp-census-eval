"""
analysis/figures/demographic_scatter.py — Bias vs demographic vulnerability scatter.

Plots county-level mean forecast bias against a composite or individual
demographic vulnerability index. Each point is a county, coloured by
Koppen-Geiger climate zone. Writes PDF + SVG.

Usage:
    python analysis/figures/demographic_scatter.py --model ifs --lead 24
"""

import argparse
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from nwp_census_eval.db import PipelineDB

OUT_DIR = os.path.join(os.path.dirname(__file__))


def load_data(model, lead_time):
    with PipelineDB() as db:
        views = db.registered_views()

        # Mean bias per county
        df_bias = db.query(f"""
            SELECT geo_id, AVG(bias) AS mean_bias
            FROM {model}_bias
            WHERE lead_time = {lead_time}
            GROUP BY geo_id
        """)

        # Koppen-Geiger category
        if "koppen" in views:
            df_koppen = db.query("SELECT geo_id, category_1 AS koppen FROM koppen")
            df_bias = df_bias.merge(df_koppen, on="geo_id", how="left")

    return df_bias


def main(argv=None):
    parser = argparse.ArgumentParser(description="Bias vs vulnerability scatter.")
    parser.add_argument("--model", default="ifs", choices=["ifs", "aifs"])
    parser.add_argument("--lead", type=int, default=24)
    args = parser.parse_args(argv)

    print(f"Loading data: model={args.model} lead={args.lead}h ...")
    df = load_data(args.model, args.lead)

    fig, ax = plt.subplots(figsize=(10, 7))

    if "koppen" in df.columns:
        for zone, group in df.groupby("koppen"):
            ax.scatter(
                group.index, group["mean_bias"],
                label=str(zone), alpha=0.5, s=15,
            )
        ax.legend(title="Koppen zone", bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)
    else:
        ax.scatter(range(len(df)), df["mean_bias"], alpha=0.5, s=15)

    ax.axhline(0, color="k", linewidth=0.8, linestyle="--")
    ax.set_xlabel("County (ranked by mean bias)", fontsize=11)
    ax.set_ylabel("Mean bias (K)", fontsize=11)
    ax.set_title(
        f"{args.model.upper()} 2m temperature bias by county | lead {args.lead}h",
        fontsize=12,
    )

    out_prefix = f"demographic_scatter_{args.model}_lead{args.lead}h"
    for ext in ("pdf", "svg"):
        path = os.path.join(OUT_DIR, f"{out_prefix}.{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=150)
        print(f"Saved {path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
