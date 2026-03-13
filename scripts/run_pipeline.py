"""
scripts/run_pipeline.py — Unified pipeline entry point.

Steps (in dependency order):
  download       download_fc_an_2t.py  — ECMWF IFS/AIFS forecast + analysis data
  aggregate-era5 aggregate_era5_2t.py  — ERA5 county aggregation + climatology
  aggregate-fc   aggregate_fc_an_2t.py — IFS/AIFS aggregation + bias/anomalies
  acs            download_acs.py       — Census ACS data

Usage:
  python scripts/run_pipeline.py                         # all steps
  python scripts/run_pipeline.py --steps aggregate-era5 aggregate-fc
  python scripts/run_pipeline.py --steps aggregate-fc -- --n-parallel 8
"""
import argparse
import os
import subprocess
import sys

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
STEPS = [
    ("download",       "download_fc_an_2t.py"),
    ("aggregate-era5", "aggregate_era5_2t.py"),
    ("aggregate-fc",   "aggregate_fc_an_2t.py"),
    ("koppen",         "aggregate_koppen.py"),
    ("acs",            "download_acs.py"),
    ("validate",       "validate_pipeline.py"),
]
STEP_NAMES = [s[0] for s in STEPS]
STEP_MAP   = dict(STEPS)


def run_step(script, extra):
    cmd = [sys.executable, os.path.join(SCRIPTS_DIR, script)] + extra
    print(f"\n{'='*60}\n  {' '.join(cmd)}\n{'='*60}\n")
    return subprocess.run(cmd).returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the nwp-census-eval pipeline.")
    parser.add_argument("--steps", nargs="+", default=STEP_NAMES,
                        choices=STEP_NAMES, metavar="STEP",
                        help=f"Steps to run (default: all). Choices: {', '.join(STEP_NAMES)}.")
    args, extra = parser.parse_known_args(argv)

    requested = set(args.steps)
    to_run = [(name, script) for name, script in STEPS if name in requested]

    for i, (name, script) in enumerate(to_run):
        step_extra = extra if i == len(to_run) - 1 else []
        rc = run_step(script, step_extra)
        if rc != 0:
            print(f"\nERROR: step '{name}' exited {rc}.")
            try:
                ans = input("Continue to next step? [y/N]: ").strip().lower()
            except EOFError:
                ans = "n"
            if ans != "y":
                return rc

    print(f"\nDone ({len(to_run)} step(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
