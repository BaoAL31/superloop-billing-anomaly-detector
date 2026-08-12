"""Shared pytest fixtures. A Spark session is expensive to start, so it is created
once per session and reused by all Silver/Gold/integration tests.
"""
import os
import sys

import pytest

from src import config


@pytest.fixture(scope="session")
def spark():
    # Ensure the worker interpreter + loopback env are set before the JVM starts.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from src.spark import build_plain_session

    session = build_plain_session("superloop-tests")
    yield session
    session.stop()


def make_df(spark, rows, columns):
    """Build a Spark DataFrame from a list of dicts (date columns kept as ISO strings)."""
    data = [tuple(r.get(c) for c in columns) for r in rows]
    return spark.createDataFrame(data, schema=columns)


def load_fixtures(spark, fixture_dir=config.FIXTURE_DIR):
    """Read the Bronze CSV fixtures into Spark DataFrames."""
    import pandas as pd

    tables = {}
    for name in ["customers", "ledger_entries", "invoices", "invoice_allocations"]:
        df = pd.read_csv(os.path.join(fixture_dir, f"{name}.csv"))
        tables[name] = spark.createDataFrame(df)
    return tables
