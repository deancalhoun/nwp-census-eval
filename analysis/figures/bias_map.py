"""
analysis/figures/bias_map.py — County-level mean bias choropleth.

Produces a publication-quality choropleth map of IFS or AIFS mean 2m
temperature forecast bias at the US county level. Writes PDF + SVG.

Usage:
    python analysis/figures/bias_map.py --model ifs --lead 24
    python analysis/figures/bias_map.py --model aifs --lead 48 --start 2024-01-01
"""

import argparse
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from nwp_census_eval.db import PipelineDB

OUT_DIR = os.path.join(os.path.dirname(__file__))


def load_mean_bias(model, lead_time, start=None, end=None):
    with PipelineDB() as db:
        df = db.query_bias(model=model, lead_times=[lead_time], start=start, end=end)
    return df.groupby("geo_id")["bias"].mean().reset_index().rename(columns={"bias": "mean_bias"})


def make_choropleth(gdf, vmin=-2, vmax=2, title="", out_prefix="bias_map"):
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    cmap = plt.cm.RdBu_r
    norm = mcolors.TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)

    fig, ax = plt.subplots(
        1, 1,
        figsize=(14, 8),
        subplot_kw={"projection": ccrs.AlbersEqualArea(central_longitude=-96, standard_parallels=(29.5, 45.5))},
    )
    ax.set_extent([-130, -65, 23, 50], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.STATES, linewidth=0.3, edgecolor="gray")
    ax.add_feature(cfeature.BORDERS, linewidth=0.5)

    gdf_proj = gdf.to_crs(ax.projection.proj4_init)
    gdf_proj.plot(
        column="mean_bias",
        ax=ax,
        cmap=cmap,
        norm=norm,
        linewidth=0,
        missing_kwds={"color": "lightgray"},
        legend=False,
    )

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Mean bias (K)", fontsize=11)
    ax.set_title(title, fontsize=13, pad=10)

    for ext in ("pdf", "svg"):
        path = os.path.join(OUT_DIR, f"{out_prefix}.{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=150)
        print(f"Saved {path}")
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser(description="County-level mean bias choropleth.")
    parser.add_argument("--model", default="ifs", choices=["ifs", "aifs"])
    parser.add_argument("--lead", type=int, default=24, help="Lead time in hours.")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    args = parser.parse_args(argv)

    import geopandas as gpd

    print(f"Loading mean bias: model={args.model} lead={args.lead}h ...")
    df_bias = load_mean_bias(args.model, args.lead, args.start, args.end)

    # Attach to shapefile geometry for plotting
    from config import SHAPEFILE_PATH  # noqa: E402
    gdf = gpd.read_file(SHAPEFILE_PATH)[["GEOID", "geometry"]].rename(columns={"GEOID": "geo_id"})
    gdf = gdf.merge(df_bias, on="geo_id", how="left")

    title = f"{args.model.upper()} 2m temperature mean bias | lead {args.lead}h"
    if args.start or args.end:
        title += f"\n{args.start or ''} to {args.end or ''}"

    out_prefix = f"bias_map_{args.model}_lead{args.lead}h"
    make_choropleth(gdf, title=title, out_prefix=out_prefix)


if __name__ == "__main__":
    main()
