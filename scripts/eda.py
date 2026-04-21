import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import duckdb
import os
import io
from scipy import stats
from statsmodels.stats.outliers_influence import variance_inflation_factor

AGGREGATED_DIR = "/glade/derecho/scratch/dcalhoun/aggregated"
NOTEBOOKS_DIR  = "/glade/u/home/dcalhoun/nwp-census-eval/notebooks"
COV_PATH       = os.path.join(AGGREGATED_DIR, "county_covariates.parquet")
BIAS_PATH      = os.path.join(AGGREGATED_DIR, "bias_ifs_full.parquet")

os.makedirs(NOTEBOOKS_DIR, exist_ok=True)

def savefig(name):
    plt.savefig(os.path.join(NOTEBOOKS_DIR, f"{name}.pdf"), dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved {name}.pdf")

# ---------------------------------------------------------------------------
# Load covariates
# ---------------------------------------------------------------------------
cov = pd.read_parquet(COV_PATH).dropna(subset=["median_income"]).reset_index(drop=True)
pop_total = np.expm1(cov["log_population"])
cov["pop_weight"] = pop_total / pop_total.sum()

# ---------------------------------------------------------------------------
# Load outcome means per county
# ---------------------------------------------------------------------------
con = duckdb.connect()
outcomes_df = con.execute(f"""
    SELECT geo_id,
           AVG(bias)      AS mean_bias,
           AVG(abs_error) AS mean_mae
    FROM read_parquet('{BIAS_PATH}')
    GROUP BY geo_id
""").df()
con.close()

df = cov.merge(outcomes_df, on="geo_id", how="inner")

# ---------------------------------------------------------------------------
# Variable groups
# ---------------------------------------------------------------------------
GEO_D0   = ["stations_norm", "gradient_norm"]
GEO_D1D4 = ["stations_resid", "gradient_norm", "pop_density_norm"]
DEMO_D3  = ["SVI"]
DEMO_D1  = ["pct_poverty_prop"]
DEMO_D2  = ["pct_black_prop", "pct_hispanic_prop", "pct_asian_prop"]
DEMO_D4  = ["pct_no_internet_prop", "pct_non_english_prop"]
DEMO_D5  = ["elderly_resid", "disabled_resid"]
ALL_CONT = (["stations_norm", "gradient_norm", "pop_density_norm", "stations_resid",
             "elderly_resid", "disabled_resid"] +
            DEMO_D3 + DEMO_D1 + DEMO_D2 + DEMO_D4 +
            ["pct_elderly_prop", "pct_disabled_prop"])
OUTCOMES = ["mean_bias", "mean_mae"]

KOPPEN_NAMES = {
    "A": "A (Tropical)", "B": "B (Arid)",
    "C": "C (Temperate)", "D": "D (Continental)",
}
DIVISION_NAMES = {
    1: "New England", 2: "Mid Atlantic", 3: "E N Central",
    4: "W N Central", 5: "S Atlantic",   6: "E S Central",
    7: "W S Central", 8: "Mountain",     9: "Pacific",
}
df["division_name"] = df["division"].map(DIVISION_NAMES)
df["koppen_label"]  = df["koppen_class"].map(KOPPEN_NAMES)
kop_order  = sorted(df["koppen_class"].unique())
kop_labels = [KOPPEN_NAMES[k] for k in kop_order]

# ---------------------------------------------------------------------------
# 1. County-count histograms
# ---------------------------------------------------------------------------
plot_vars = (GEO_D0 + ["pop_density_norm", "stations_resid", "elderly_resid",
                        "disabled_resid"] + DEMO_D3 + DEMO_D1 + DEMO_D2 + DEMO_D4 +
             ["pct_elderly_prop", "pct_disabled_prop"])
ncols = 5
nrows = int(np.ceil(len(plot_vars) / ncols))
fig, axes = plt.subplots(nrows, ncols, figsize=(20, 4 * nrows))
fig.suptitle("County-count distributions — continuous predictors", fontsize=13)
axes = axes.flatten()
for i, var in enumerate(plot_vars):
    ax = axes[i]
    ax.hist(df[var].dropna(), bins=40, color="#378ADD", alpha=0.85,
            edgecolor="white", linewidth=0.3)
    ax.set_title(var, fontsize=8)
    ax.set_ylabel("Counties", fontsize=7)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.3)
for j in range(i + 1, len(axes)):
    axes[j].set_visible(False)
fig.tight_layout()
savefig("01_county_count_histograms")

# ---------------------------------------------------------------------------
# 2. Categorical bar charts
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("County-count distributions — categorical predictors", fontsize=13)

vc = df["koppen_class"].value_counts()[kop_order]
axes[0].bar(range(len(kop_order)), vc.values, color="#378ADD", alpha=0.85)
axes[0].set_xticks(range(len(kop_order)))
axes[0].set_xticklabels(kop_labels, fontsize=9)
axes[0].set_title("Koppen class", fontsize=11)
axes[0].set_ylabel("Counties", fontsize=9)
axes[0].spines[["top", "right"]].set_visible(False)
axes[0].grid(axis="y", alpha=0.3)

div_counts = df.groupby("division")["geo_id"].count()
axes[1].bar(range(len(div_counts)), div_counts.values, color="#378ADD", alpha=0.85)
axes[1].set_xticks(range(len(div_counts)))
axes[1].set_xticklabels([DIVISION_NAMES[d] for d in div_counts.index],
                         rotation=30, ha="right", fontsize=8)
axes[1].set_title("Census division", fontsize=11)
axes[1].set_ylabel("Counties", fontsize=9)
axes[1].spines[["top", "right"]].set_visible(False)
axes[1].grid(axis="y", alpha=0.3)
fig.tight_layout()
savefig("02_categorical_counts")

# ---------------------------------------------------------------------------
# 3. Population-weighted histograms
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(1, 4, figsize=(18, 5))
fig.suptitle("Population-weighted distributions (% CONUS population)", fontsize=13)

for ax, var in zip(axes[:2], GEO_D0):
    bins    = np.linspace(df[var].min(), df[var].max(), 31)
    bin_idx = np.digitize(df[var], bins) - 1
    bin_idx = bin_idx.clip(0, len(bins) - 2)
    pop_pct = np.zeros(len(bins) - 1)
    for b in range(len(bins) - 1):
        pop_pct[b] = df.loc[bin_idx == b, "pop_weight"].sum() * 100
    ax.bar(bins[:-1], pop_pct, width=np.diff(bins),
           color="#D85A30", alpha=0.85, align="edge", edgecolor="white", linewidth=0.3)
    ax.set_title(var, fontsize=10)
    ax.set_ylabel("% CONUS population", fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

kop_pop = df.groupby("koppen_class")["pop_weight"].sum() * 100
axes[2].bar(range(len(kop_order)), kop_pop[kop_order].values, color="#D85A30", alpha=0.85)
axes[2].set_xticks(range(len(kop_order)))
axes[2].set_xticklabels(kop_labels, fontsize=9)
axes[2].set_title("Koppen class", fontsize=10)
axes[2].set_ylabel("% CONUS population", fontsize=8)
axes[2].spines[["top", "right"]].set_visible(False)
axes[2].grid(axis="y", alpha=0.3)

div_pop = df.groupby("division")["pop_weight"].sum() * 100
axes[3].bar(range(len(div_pop)), div_pop.values, color="#D85A30", alpha=0.85)
axes[3].set_xticks(range(len(div_pop)))
axes[3].set_xticklabels([DIVISION_NAMES[d] for d in div_pop.index],
                         rotation=30, ha="right", fontsize=8)
axes[3].set_title("Census division", fontsize=10)
axes[3].set_ylabel("% CONUS population", fontsize=8)
axes[3].spines[["top", "right"]].set_visible(False)
axes[3].grid(axis="y", alpha=0.3)
fig.tight_layout()
savefig("03_population_weighted")

# ---------------------------------------------------------------------------
# 4. Scatter matrices per domain
# ---------------------------------------------------------------------------
def scatter_matrix_panel(df, vars, title, fname):
    n = len(vars)
    fig, axes = plt.subplots(n, n, figsize=(4 * n, 4 * n))
    if n == 1:
        axes = np.array([[axes]])
    fig.suptitle(title, fontsize=13, y=1.01)
    for i, v1 in enumerate(vars):
        for j, v2 in enumerate(vars):
            ax = axes[i, j]
            if i == j:
                ax.hist(df[v1].dropna(), bins=30, color="#378ADD", alpha=0.8)
                ax.set_ylabel(v1, fontsize=7)
            else:
                mask = df[v2].notna() & df[v1].notna()
                x, y = df.loc[mask, v2], df.loc[mask, v1]
                r, _ = stats.pearsonr(x, y)
                color = "#D85A30" if abs(r) > 0.5 else "#378ADD"
                ax.scatter(x, y, s=3, alpha=0.3, color=color)
                ax.text(0.05, 0.93, f"r={r:.2f}", transform=ax.transAxes,
                        fontsize=9, color=color,
                        fontweight="bold" if abs(r) > 0.5 else "normal")
            ax.spines[["top", "right"]].set_visible(False)
            if i == n - 1:
                ax.set_xlabel(v2, fontsize=7)
    fig.tight_layout()
    savefig(fname)

scatter_matrix_panel(df, GEO_D0,
                     "Scatter matrix — d0 (stations_norm, gradient_norm)",
                     "04a_scatter_d0")
scatter_matrix_panel(df, GEO_D1D4,
                     "Scatter matrix — d00 base (stations_resid, gradient_norm, pop_density_norm)",
                     "04b_scatter_d00")
scatter_matrix_panel(df, GEO_D1D4 + DEMO_D3,
                     "Scatter matrix — d3 predictors (SVI)",
                     "04e_scatter_d3")
scatter_matrix_panel(df, GEO_D1D4 + DEMO_D1,
                     "Scatter matrix — d1 predictors", "04c_scatter_d1")
scatter_matrix_panel(df, GEO_D1D4 + DEMO_D2,
                     "Scatter matrix — d2 predictors", "04d_scatter_d2")
scatter_matrix_panel(df, GEO_D1D4 + DEMO_D4,
                     "Scatter matrix — d4 predictors", "04f_scatter_d4")
scatter_matrix_panel(df, GEO_D1D4 + DEMO_D5,
                     "Scatter matrix — d5 predictors", "04g_scatter_d5")

# ---------------------------------------------------------------------------
# 5. Cross-correlations: geographic vs demographic
# ---------------------------------------------------------------------------
row_vars = GEO_D0 + ["pop_density_norm"]
col_vars = DEMO_D3 + DEMO_D1 + DEMO_D2 + DEMO_D4 + ["pct_elderly_prop", "pct_disabled_prop"]
cross_r  = np.zeros((len(row_vars), len(col_vars)))
for i, gv in enumerate(row_vars):
    for j, dv in enumerate(col_vars):
        mask = df[gv].notna() & df[dv].notna()
        if mask.sum() > 10:
            cross_r[i, j], _ = stats.pearsonr(df.loc[mask, gv], df.loc[mask, dv])

fig, ax = plt.subplots(figsize=(16, 4))
im = ax.imshow(cross_r, vmin=-1, vmax=1, cmap="RdBu_r", aspect="auto")
ax.set_xticks(range(len(col_vars)))
ax.set_xticklabels(col_vars, rotation=45, ha="right", fontsize=9)
ax.set_yticks(range(len(row_vars)))
ax.set_yticklabels(row_vars, fontsize=9)
for i in range(len(row_vars)):
    for j in range(len(col_vars)):
        r      = cross_r[i, j]
        color  = "white" if abs(r) > 0.5 else "black"
        weight = "bold"  if abs(r) > 0.5 else "normal"
        ax.text(j, i, f"{r:.2f}", ha="center", va="center",
                fontsize=8, color=color, fontweight=weight)
plt.colorbar(im, ax=ax, shrink=0.8)
ax.set_title("Cross-correlations: geographic vs demographic (|r|>0.5 flagged bold)", fontsize=11)
fig.tight_layout()
savefig("05_cross_correlations")

# ---------------------------------------------------------------------------
# 6. Full correlation heatmap
# ---------------------------------------------------------------------------
corr_vars = (GEO_D0 + ["pop_density_norm", "stations_resid"] +
             DEMO_D3 + DEMO_D1 + DEMO_D2 + DEMO_D4 +
             ["pct_elderly_prop", "pct_disabled_prop", "elderly_resid", "disabled_resid"])
corr = df[corr_vars].corr()
fig, ax = plt.subplots(figsize=(16, 14))
im = ax.imshow(corr.values, vmin=-1, vmax=1, cmap="RdBu_r", aspect="auto")
ax.set_xticks(range(len(corr_vars)))
ax.set_xticklabels(corr_vars, rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(len(corr_vars)))
ax.set_yticklabels(corr_vars, fontsize=8)
for i in range(len(corr_vars)):
    for j in range(len(corr_vars)):
        r = corr.values[i, j]
        if i != j:
            color  = "white" if abs(r) > 0.5 else "black"
            weight = "bold"  if abs(r) > 0.5 else "normal"
            ax.text(j, i, f"{r:.2f}", ha="center", va="center",
                    fontsize=7, color=color, fontweight=weight)
plt.colorbar(im, ax=ax, shrink=0.8)
ax.set_title("Full correlation heatmap (|r|>0.5 flagged bold)", fontsize=11)
fig.tight_layout()
savefig("06_full_correlation_heatmap")

# ---------------------------------------------------------------------------
# 7. Koppen x Division heatmap
# ---------------------------------------------------------------------------
ct = pd.crosstab(df["koppen_label"], df["division_name"])
fig, ax = plt.subplots(figsize=(12, 4))
im = ax.imshow(ct.values, cmap="Blues", aspect="auto")
ax.set_xticks(range(len(ct.columns)))
ax.set_xticklabels(ct.columns, rotation=30, ha="right", fontsize=9)
ax.set_yticks(range(len(ct.index)))
ax.set_yticklabels(ct.index, fontsize=9)
for i in range(len(ct.index)):
    for j in range(len(ct.columns)):
        n     = ct.values[i, j]
        color = "white" if n > ct.values.max() * 0.6 else "black"
        ax.text(j, i, str(n), ha="center", va="center", fontsize=9, color=color)
plt.colorbar(im, ax=ax, shrink=0.8, label="Counties")
ax.set_title("County counts: Koppen class × Census division", fontsize=11)
fig.tight_layout()
savefig("07_koppen_division_heatmap")

# ---------------------------------------------------------------------------
# 8. VIF tables
# ---------------------------------------------------------------------------
def compute_vif(vars, label, df):
    vif_df = df[vars].dropna()
    X = np.column_stack([vif_df[v] for v in vif_df.columns])
    result = pd.DataFrame({
        "variable": vif_df.columns.tolist(),
        "VIF": [variance_inflation_factor(X, i) for i in range(X.shape[1])],
    })
    print(f"\n=== VIF — {label} ===")
    print(result.to_string(index=False))
    return result

vif_d0  = compute_vif(GEO_D0,                  "d0",  df)
vif_d00 = compute_vif(GEO_D1D4,                "d00", df)
vif_d3  = compute_vif(GEO_D1D4 + DEMO_D3,      "d3 (SVI)",  df)
vif_d1  = compute_vif(GEO_D1D4 + DEMO_D1,      "d1",  df)
vif_d2  = compute_vif(GEO_D1D4 + DEMO_D2,      "d2",  df)
vif_d4  = compute_vif(GEO_D1D4 + DEMO_D4,      "d4",  df)
vif_d5  = compute_vif(GEO_D1D4 + DEMO_D5,      "d5",  df)

# ---------------------------------------------------------------------------
# 9. Outcome boxplots by categorical FE
# ---------------------------------------------------------------------------
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Outcome distributions by categorical FE (IFS full, county means)", fontsize=13)

for col, (outcome, ylabel) in enumerate(zip(OUTCOMES, ["Mean bias (K)", "Mean MAE (K)"])):
    ax = axes[0, col]
    groups = [df.loc[df["koppen_class"] == k, outcome].dropna().values for k in kop_order]
    bp = ax.boxplot(groups, patch_artist=True,
                    medianprops=dict(color="black", linewidth=1.5),
                    flierprops=dict(marker="o", markersize=2, alpha=0.3),
                    whiskerprops=dict(linewidth=0.8),
                    capprops=dict(linewidth=0.8))
    for patch in bp["boxes"]:
        patch.set_facecolor("#B5D4F4"); patch.set_alpha(0.85)
    ax.set_xticklabels(kop_labels, fontsize=9)
    ax.set_title(f"{ylabel} by Koppen class", fontsize=11)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.4)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

    ax = axes[1, col]
    div_order = sorted(df["division"].unique())
    groups = [df.loc[df["division"] == d, outcome].dropna().values for d in div_order]
    bp = ax.boxplot(groups, patch_artist=True,
                    medianprops=dict(color="black", linewidth=1.5),
                    flierprops=dict(marker="o", markersize=2, alpha=0.3),
                    whiskerprops=dict(linewidth=0.8),
                    capprops=dict(linewidth=0.8))
    for patch in bp["boxes"]:
        patch.set_facecolor("#B5D4F4"); patch.set_alpha(0.85)
    ax.set_xticklabels([DIVISION_NAMES[d] for d in div_order],
                        rotation=30, ha="right", fontsize=8)
    ax.set_title(f"{ylabel} by Census division", fontsize=11)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.4)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.3)

fig.tight_layout()
savefig("08_outcome_boxplots_categorical")

# ---------------------------------------------------------------------------
# 10. Outcome scatter vs continuous predictors
# ---------------------------------------------------------------------------
scatter_vars = (GEO_D0 + ["pop_density_norm", "stations_resid"] +
                DEMO_D3 + DEMO_D1 + DEMO_D2 + DEMO_D4 +
                ["pct_elderly_prop", "pct_disabled_prop"])
n_vars = len(scatter_vars)
fig, axes = plt.subplots(2, n_vars, figsize=(4 * n_vars, 8))
fig.suptitle("Outcome vs continuous predictors (county means, IFS full)", fontsize=13, y=1.01)

for col, var in enumerate(scatter_vars):
    for row, (outcome, ylabel) in enumerate(zip(OUTCOMES, ["Mean bias (K)", "Mean MAE (K)"])):
        ax = axes[row, col]
        mask = df[var].notna() & df[outcome].notna()
        x, y = df.loc[mask, var], df.loc[mask, outcome]
        r, _ = stats.pearsonr(x, y)
        r2    = r ** 2
        color = "#D85A30" if abs(r) > 0.5 else "#378ADD"
        ax.scatter(x, y, s=4, alpha=0.3, color=color)
        xr = np.linspace(x.min(), x.max(), 100)
        ax.plot(xr, np.polyval(np.polyfit(x, y, 1), xr), color=color, linewidth=1.5)
        ax.text(0.05, 0.93, f"R²={r2:.3f}", transform=ax.transAxes, fontsize=8,
                color=color, fontweight="bold" if abs(r) > 0.5 else "normal")
        ax.axhline(0, color="black", linewidth=0.6, linestyle="--", alpha=0.3)
        ax.set_xlabel(var, fontsize=7)
        if col == 0:
            ax.set_ylabel(ylabel, fontsize=8)
        if row == 0:
            ax.set_title(var, fontsize=8)
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(alpha=0.2)

fig.tight_layout()
savefig("09_outcome_scatter")

# ---------------------------------------------------------------------------
# 11. R² heatmap
# ---------------------------------------------------------------------------
r2_matrix = np.zeros((len(OUTCOMES), len(scatter_vars)))
for i, outcome in enumerate(OUTCOMES):
    for j, var in enumerate(scatter_vars):
        mask = df[var].notna() & df[outcome].notna()
        if mask.sum() > 10:
            r, _ = stats.pearsonr(df.loc[mask, var], df.loc[mask, outcome])
            r2_matrix[i, j] = r ** 2

fig, ax = plt.subplots(figsize=(16, 3))
im = ax.imshow(r2_matrix, vmin=0, vmax=1, cmap="Blues", aspect="auto")
ax.set_xticks(range(len(scatter_vars)))
ax.set_xticklabels(scatter_vars, rotation=45, ha="right", fontsize=9)
ax.set_yticks(range(len(OUTCOMES)))
ax.set_yticklabels(["Mean bias", "Mean MAE"], fontsize=9)
for i in range(len(OUTCOMES)):
    for j in range(len(scatter_vars)):
        r2     = r2_matrix[i, j]
        color  = "white" if r2 > 0.5 else "black"
        weight = "bold"  if r2 > 0.25 else "normal"
        ax.text(j, i, f"{r2:.3f}", ha="center", va="center",
                fontsize=9, color=color, fontweight=weight)
plt.colorbar(im, ax=ax, shrink=0.8)
ax.set_title("R² heatmap: outcome vs each predictor (R²>0.25 flagged bold)", fontsize=11)
fig.tight_layout()
savefig("10_r2_heatmap")

# ---------------------------------------------------------------------------
# Save summary text
# ---------------------------------------------------------------------------
output = io.StringIO()

def section(title):
    output.write(f"\n{'='*60}\n{title}\n{'='*60}\n")

section("DATASET OVERVIEW")
output.write(f"Counties: {len(df)}\n")
output.write(f"Total CONUS population: {pop_total.sum():,.0f}\n")

section("REGRESSION SPECIFICATION")
output.write("d0:  stations_norm + gradient_norm + FE\n")
output.write("d00: stations_resid + gradient_norm + pop_density_norm + FE\n")
output.write("d3:  d00 + SVI (CDC/ATSDR overall composite, 0-1)\n")
output.write("d1:  d00 + pct_poverty_prop\n")
output.write("d2:  d00 + pct_black/hispanic/asian_prop\n")
output.write("d4:  d00 + pct_no_internet_prop + pct_non_english_prop\n")
output.write("d5:  d00 + elderly_resid + disabled_resid\n")
output.write("\nResidualizations (all against pop_density_norm):\n")
output.write("  stations_resid  = stations_norm  ~ pop_density_norm\n")
output.write("  elderly_resid   = pct_elderly_prop  ~ pop_density_norm\n")
output.write("  disabled_resid  = pct_disabled_prop ~ pop_density_norm\n")
output.write("pct_white_prop dropped from d2 (compositional VIF ~17)\n")
output.write("median_income dropped — pct_poverty_prop used instead\n")

section("KOPPEN CLASS DISTRIBUTION")
kop_counts = df["koppen_class"].value_counts()[kop_order]
kop_pops   = df.groupby("koppen_class")["pop_weight"].sum() * 100
for k in kop_order:
    output.write(f"  {KOPPEN_NAMES[k]}: {kop_counts[k]} counties ({kop_pops[k]:.1f}% pop)\n")

section("DIVISION DISTRIBUTION")
for d in sorted(df["division"].unique()):
    n   = (df["division"] == d).sum()
    pct = df.loc[df["division"] == d, "pop_weight"].sum() * 100
    output.write(f"  {DIVISION_NAMES[d]}: {n} counties ({pct:.1f}% pop)\n")

section("KOPPEN x DIVISION CROSSTAB")
output.write(ct.to_string()); output.write("\n")

section("VIF — D0")
output.write(vif_d0.to_string(index=False)); output.write("\n")

section("VIF — D00")
output.write(vif_d00.to_string(index=False)); output.write("\n")

section("VIF — D3 (SVI)")
output.write(vif_d3.to_string(index=False)); output.write("\n")

section("VIF — D1")
output.write(vif_d1.to_string(index=False)); output.write("\n")

section("VIF — D2")
output.write(vif_d2.to_string(index=False)); output.write("\n")

section("VIF — D4")
output.write(vif_d4.to_string(index=False)); output.write("\n")

section("VIF — D5")
output.write(vif_d5.to_string(index=False)); output.write("\n")

section("R² HEATMAP")
r2_df = pd.DataFrame(r2_matrix, index=["mean_bias", "mean_mae"],
                     columns=scatter_vars).round(3)
output.write(r2_df.to_string()); output.write("\n")

section("OUTCOME SUMMARY BY KOPPEN CLASS")
for outcome in OUTCOMES:
    output.write(f"\n{outcome}:\n")
    output.write(df.groupby("koppen_label")[outcome].describe().round(3).to_string())
    output.write("\n")

section("OUTCOME SUMMARY BY DIVISION")
for outcome in OUTCOMES:
    output.write(f"\n{outcome}:\n")
    output.write(df.groupby("division_name")[outcome].describe().round(3).to_string())
    output.write("\n")

txt_path = os.path.join(NOTEBOOKS_DIR, "eda_summary.txt")
with open(txt_path, "w") as f:
    f.write(output.getvalue())
print(f"Saved eda_summary.txt")