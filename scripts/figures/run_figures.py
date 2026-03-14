"""
scripts/figures/run_figures.py — Run all publication figure scripts sequentially.

Thin wrapper that mirrors the run_pipeline.py pattern. Each figure script reads
from AGGREGATED_DIR via PipelineDB and writes PDF + SVG to notebooks/figures/.

Usage:
    python scripts/figures/run_figures.py
    python scripts/figures/run_figures.py --output-dir /custom/output
"""

import argparse
import os
import subprocess
import sys

FIGURES_DIR = os.path.dirname(os.path.abspath(__file__))

FIGURE_SCRIPTS = [
    "ifs_vs_aifs.py",
    "bias_map.py",
    "anomaly_seasonal.py",
    "demographic_scatter.py",
]


def run_script(script, extra_args):
    cmd = [sys.executable, os.path.join(FIGURES_DIR, script)] + extra_args
    print(f"\n{'='*60}\n  {' '.join(cmd)}\n{'='*60}\n")
    return subprocess.run(cmd).returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run all figure scripts.")
    parser.add_argument(
        "--output-dir", default=None,
        help="Override output directory for figures (default: notebooks/figures/).",
    )
    args = parser.parse_args(argv)

    extra = []
    if args.output_dir:
        # Scripts read OUT_DIR from module-level; override via env var instead
        os.environ["NWP_FIGURES_DIR"] = args.output_dir

    failed = []
    for script in FIGURE_SCRIPTS:
        rc = run_script(script, extra)
        if rc != 0:
            print(f"\nWARNING: {script} exited {rc}.")
            failed.append(script)

    if failed:
        print(f"\nDone with errors: {failed}")
        return 1
    print(f"\nDone ({len(FIGURE_SCRIPTS)} figure scripts).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
