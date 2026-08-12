"""Spark session factory with the local Windows/standalone recipe baked in.

This encodes every quirk that blocks a naive `pip install` → `getOrCreate()` on
Windows, so a reviewer's clone-and-run just works:

- ``HADOOP_HOME`` must be set (Windows needs ``winutils.exe``); if it is set we
  also expose its ``bin`` as ``java.library.path`` so Hadoop's native ``NativeIO``
  loads (otherwise Delta reads fail with ``UnsatisfiedLinkError``).
- The driver is pinned to loopback and ``PYSPARK_PYTHON`` to the interpreter that
  launched us, otherwise the Python worker can't connect back on Windows.
- Delta JARs come from Maven via ``configure_spark_with_delta_pip``.

On macOS/Linux ``HADOOP_HOME`` is unneeded and all of this is harmless no-ops.
"""
from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _configure_environment() -> None:
    """Ensure the interpreter/loopback env vars are set before the JVM spawns workers."""
    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable
    if "PYSPARK_DRIVER_PYTHON" not in os.environ:
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _java_library_path() -> str | None:
    """Return the hadoop ``bin`` dir to put on java.library.path, if HADOOP_HOME set."""
    home = os.environ.get("HADOOP_HOME")
    if not home:
        return None
    bin_dir = os.path.join(home, "bin")
    if not os.path.isdir(bin_dir):
        return None
    # Forward slashes only: a Windows backslash inside -Djava.library.path=... is
    # mangled when passed through SparkSubmit and hadoop.dll silently fails to load.
    return bin_dir.replace(os.sep, "/")


def build_session(app_name: str = "superloop-billing-anomaly-detector") -> SparkSession:
    """Create (or reuse) a local Delta-enabled SparkSession."""
    _configure_environment()

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        # Keep the driver on loopback: on Windows the default hostname can resolve
        # to an address the Python worker cannot reach (-> "worker failed to connect").
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Delta Lake support.
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    lib_path = _java_library_path()
    if lib_path:
        builder = builder.config(
            "spark.driver.extraJavaOptions", f"-Djava.library.path={lib_path}"
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()
