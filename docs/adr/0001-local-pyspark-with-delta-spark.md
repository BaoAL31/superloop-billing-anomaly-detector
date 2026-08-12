# ADR-0001 — Local PySpark + delta-spark as the primary, clone-and-run path

**Status:** Accepted
**Date:** (in-progress grilling session)

## Context

The spec's tech-stack section (§7) hedged between mirroring Superloop's hosted
Databricks/Spark stack and running locally. For a portfolio project, the deciding
constraint is: *can a reviewer actually run it?* A hosted Databricks Community
Workspace forces signup, quotas, and infra wait on every reviewer. But a purely local
pandas pipeline would not demonstrate the PySpark/Delta skills the target role lists.

## Decision

Build the pipeline in **local PySpark + `delta-spark`**, installed via pip, run as a
standalone Spark session (Java + Python under the hood — no cluster, no account).
The deliverable is `git clone` → `pip install -r requirements.txt` → `python
run_pipeline.py`. Delta tables are written and read with the same
`spark.read/write.format("delta")` calls used on a real Databricks cluster.

The medallion structure is preserved as staged directories (Bronze → Silver → Gold).

## Consequences

- **Pro:** 100% of the code is what Superloop actually writes — same PySpark DataFrame
  API, same Delta table format, same SQL. Moving it to Databricks later requires no code
  change, only an execution target.
- **Pro:** Reviewer gets zero signup, zero quota risk, zero Databricks infra wait.
- **Pro:** `pip install delta-spark pyspark` is one line and runs on a laptop.
- **Con:** Not literally running inside a Databricks workspace; job-orchestration is a
  local script (`run_pipeline.py` with `argparse` stages) rather than a Databricks Job.
  Noted in the README as the 1:1 mapping.
- **Con:** Spark launch overhead on small synthetic data; negligible at this scale.

## Alternatives considered

- **Hosted Databricks Community Edition** — faithful execution, but reviewer friction and
  account/quota risk outweigh the benefit for a portfolio artifact.
- **Pure pandas + SQL (DuckDB/DataFusion)** — easiest to run, but drops PySpark/Delta
  fidelity that the JD explicitly lists.
