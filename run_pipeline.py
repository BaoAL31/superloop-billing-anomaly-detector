#!/usr/bin/env python
"""Local pipeline orchestrator for the Superloop billing-anomaly detector.

Medallion stages, each with idempotent writes (``mode="overwrite"``) and a pre-flight
upstream-table check that fails with a one-line human-readable error:

    python run_pipeline.py                 # full end-to-end
    python run_pipeline.py --stage generate
    python run_pipeline.py --stage bronze   # fixtures/CSV -> data/bronze (Delta)
    python run_pipeline.py --stage silver   # Bronze -> Silver
    python run_pipeline.py --stage gold     # Silver -> Gold flagged_accounts
    python run_pipeline.py --stage dashboard # Gold -> dashboard.html

Stages are decoupled from ``generate``: silver/gold/dashboard read the committed
fixtures, so a reviewer can run the detector on the committed data without ever
re-running generation. Run ``--stage generate`` only to refresh fixtures.
"""
from __future__ import annotations

import argparse
import os
import sys

from src import config

BRONZE = os.path.join(config.DATA_DIR, "bronze")
SILVER = os.path.join(config.DATA_DIR, "silver")
GOLD = os.path.join(config.DATA_DIR, "gold")


def _require(*paths: str) -> None:
    missing = [p for p in paths if not os.path.isdir(p)]
    if missing:
        sys.exit(f"Pre-flight failed: upstream stage not built — missing: {', '.join(missing)}. "
                 f"Run the prior stage first.")


def stage_generate() -> None:
    from src.generator import write_fixtures

    write_fixtures()
    print("fixtures regenerated ->", config.FIXTURE_DIR)


def _read_bronze_tables(spark):
    tables = {}
    for name in ["customers", "ledger_entries", "invoices", "invoice_allocations"]:
        tables[name] = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(os.path.join(config.FIXTURE_DIR, f"{name}.csv"))
        )
    return tables


def stage_bronze() -> None:
    _require(config.FIXTURE_DIR)
    from src.spark import build_session

    spark = build_session("bronze")
    try:
        tables = _read_bronze_tables(spark)
        for name, df in tables.items():
            df.write.format("delta").mode("overwrite").save(os.path.join(BRONZE, name))
        print("Bronze written ->", BRONZE)
    finally:
        spark.stop()


def stage_silver() -> None:
    _require(BRONZE)
    from src.spark import build_session
    from src.silver import build_silver

    spark = build_session("silver")
    try:
        tables = {n: spark.read.format("delta").load(os.path.join(BRONZE, n))
                  for n in ["customers", "ledger_entries", "invoices", "invoice_allocations"]}
        silver = build_silver(spark, tables)
        for name, df in silver.items():
            df.write.format("delta").mode("overwrite").save(os.path.join(SILVER, name))
        print("Silver written ->", SILVER)
    finally:
        spark.stop()


def stage_gold() -> None:
    _require(BRONZE, SILVER)
    from src.spark import build_session
    from src.gold import run_gold

    spark = build_session("gold")
    try:
        bronze = {n: spark.read.format("delta").load(os.path.join(BRONZE, n))
                  for n in ["customers", "ledger_entries", "invoices", "invoice_allocations"]}
        silver = {n: spark.read.format("delta").load(os.path.join(SILVER, n))
                  for n in ["silver_running_balance", "silver_invoices", "silver_notices"]}
        flagged = run_gold(silver, bronze)
        flagged.write.format("delta").mode("overwrite").save(os.path.join(GOLD, "flagged_accounts"))
        print(f"Gold written -> {os.path.join(GOLD, 'flagged_accounts')} "
              f"({flagged.count()} flagged rows)")
    finally:
        spark.stop()


def stage_dashboard() -> None:
    _require(GOLD)
    from src.spark import build_session
    from src.dashboard import render_from_spark

    spark = build_session("dashboard")
    try:
        flagged = spark.read.format("delta").load(os.path.join(GOLD, "flagged_accounts"))
        out = render_from_spark(flagged, config.DASHBOARD_PATH)
        print("dashboard written ->", out)
    finally:
        spark.stop()


STAGES = {
    "generate": stage_generate,
    "bronze": stage_bronze,
    "silver": stage_silver,
    "gold": stage_gold,
    "dashboard": stage_dashboard,
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=list(STAGES), default=None,
                        help="run a single stage (default: full end-to-end)")
    args = parser.parse_args(argv)

    if args.stage:
        STAGES[args.stage]()
    else:
        for name in ["generate", "bronze", "silver", "gold", "dashboard"]:
            print(f"== stage: {name} ==")
            STAGES[name]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
