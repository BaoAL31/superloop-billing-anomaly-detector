"""Regenerate-and-diff reproducibility gate (spec §8, §11).

Regenerates the Bronze fixtures + ground-truth manifest from the fixed SEED and
byte-diffs each committed file. Any drift in the generator's output fails the check,
so the committed ground truth can never silently diverge from what the code produces.

Usage (from repo root):  python scripts/check_fixtures.py
"""
from __future__ import annotations

import filecmp
import os
import sys
import tempfile

# Ensure the repo root is importable regardless of how the script is invoked.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import config
from src.generator import write_fixtures

FILES = [
    "customers.csv",
    "ledger_entries.csv",
    "invoices.csv",
    "invoice_allocations.csv",
    config.MANIFEST_PATH,
]


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="fixture_check_")
    write_fixtures(out_dir=tmp)

    failed = []
    for name in FILES:
        committed = os.path.join(config.FIXTURE_DIR, name)
        regenerated = os.path.join(tmp, name)
        if not filecmp.cmp(committed, regenerated, shallow=False):
            failed.append(name)
            print(f"MISMATCH: {name}")

    if failed:
        print("Reproducibility check FAILED — fixtures drift from generator output: "
              f"{', '.join(failed)}. Commit the regenerated fixtures or fix the generator.")
        return 1

    print("Reproducibility check PASSED: fixtures byte-match generator output.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
