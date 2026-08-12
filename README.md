# Superloop billing-anomaly detector

A reproducible local detector for a Superloop-style ISP billing pipeline. It finds
billing anomalies — wrongful arrears notices, orphaned credits, missing statements,
dishonoured debits, and duplicate invoices — using a faithful, local
**PySpark + Delta Lake** medallion pipeline with **deterministic synthetic data** as
ground truth.

Everything is clone-and-run, no hosted Databricks, no signup, no quota risk (see
`docs/adr/0001-local-pyspark-with-delta-spark.md`). The same PySpark DataFrame API,
Delta table format and Bronze→Silver→Gold medallion structure a real production
pipeline would use — only the execution location is local.

## Quickstart

```bash
git clone <repo-url>
cd superloop-anomaly-billing-detector
pip install -r requirements.txt
python run_pipeline.py          # full end-to-end: generate -> Bronze -> Silver -> Gold -> dashboard
```

This produces:

- `fixtures/` — committed deterministic Bronze CSVs + `ground_truth_manifest.csv`
- `data/bronze`, `data/silver`, `data/gold` — Delta tables (git-ignored)
- `dashboard.html` — a single self-contained, deterministic Plotly dashboard

### Java & Windows requirements

PySpark needs **Java 17+** on `PATH` (tested on Java 19). On **Windows** only, Spark
also needs `winutils.exe` and `hadoop.dll`:

```bash
set HADOOP_HOME=C:\path\to\winutils    # a folder containing bin/winutils.exe and bin/hadoop.dll
set PATH=%HADOOP_HOME%\bin;%PATH%
```

`src/spark.py` bakes in the remaining Windows quirks (loopback driver, pinned worker
interpreter, forward-slash `java.library.path`) so the pipeline runs unchanged.
On macOS/Linux none of this is needed.

### Stages

```bash
python run_pipeline.py --stage generate     # (re)generate fixtures + manifest
python run_pipeline.py --stage bronze       # fixtures/CSV -> data/bronze (Delta)
python run_pipeline.py --stage silver       # running balance, allocated totals, notices
python run_pipeline.py --stage gold         # 5 rules -> flagged_accounts
python run_pipeline.py --stage dashboard    # gold -> dashboard.html
```

Each stage is idempotent (`mode="overwrite"`) and pre-flighted: it fails with a
one-line error if its upstream tables are missing. `silver`/`gold`/`dashboard` read
the **committed fixtures**, so a reviewer can run the detector on the ground-truth
data without ever re-running generation.

## The five rules

| Rule | Anomaly | Severity |
|------|---------|----------|
| #1 | **Wrongful arrears notice** — a notice fired while the account was in credit | high → **critical** past threshold |
| #2 | **Orphaned credit** — a credit/payment unapplied for > 30 days | warning |
| #3 | **Statement continuity break** — gap between consecutive statements > 3 days | medium |
| #4 | **Dishonoured debit with no cause** — failed debit while account in credit | critical |
| #5 | **Duplicate invoice** — overlapping statement periods, near-identical amount | medium → **high** past threshold |

Escalation follows the *escalation principle* (spec §6): magnitude-escalation only
where the dollar-at-risk is well-defined (#1, #5); the rest are static. **Honest
caveat:** the `notice_escalation_threshold`/`duplicate_escalation_threshold` defaults
($100) and `grace_days` are configurable in `src/config.py` — `grace_days` is anchored
to Superloop's published terms, the $100 thresholds are **not** and are defaults only.

## Design decisions

These are the outcomes of the grilling / domain-modeling session (`CONTEXT.md` for the
full glossary, `billing_anomaly_detector_spec.md` for the consolidated spec, and
`docs/adr/0001-*` for the stack). Each maps to a piece of the implementation:

1. **Ledger model** — the running account balance, keyed `(customer_id, effective_date)`,
   is the source of truth. `payments` + *independent credits* are credit sources; an
   **overpayment is emergent state**, never a stored additive row.
2. **Allocation layer** — a separate `invoice_allocations` table is *deliberately allowed
   to be wrong/stale*; the documented bug is a **reconciliation failure** between the
   balance view and the allocation view.
3. **Notice generation** — notices are **computed in Silver** by a naive dunning rule firing
   at `due_date + grace_days` when `allocated_total < amount_due`. Defects are injected at
   the allocation layer. A credit arriving after the window closes is a legitimate unpaid
   notice, **not** a bug; a credit present before evaluation but never swept is the injected
   bug.
4. **Stack** — local PySpark + `delta-spark`, medallion as staged dirs, clone-and-run
   (ADR-0001); no hosted Databricks.
5. **Rules #3/#5 disjoint** — #3 is gaps-only, #5 is overlaps-only, with configurable
   thresholds; orthogonal injection plus a small tracked-overlap subset exercises rule
   independence.
6. **Severity (escalation principle)** — magnitude-escalation only where the dollar-at-risk
   is well-defined (#1, #5); the rest static. Default thresholds are honestly **not**
   Superloop-anchored.
7. **Explanation** — a deterministic per-rule template (always on) + an **optional**, gated
   NIM paraphrase layer that never invents facts and is excluded from CI.
8. **Reproducibility** — plain-Python/pandas generator, CSV fixtures, explicit
   `random.Random(SEED)`, counted injection, a ground-truth manifest, and a
   regenerate-and-diff CI gate.
9. **Dashboard** — a single deterministic `dashboard.html` with fixed `div_id`s and inline
   Plotly.js; text baked at generation time; byte-diff covered.
10. **Testing** — three layers (Silver plumbing, per-rule, Gold-vs-manifest); staged
    `run_pipeline.py` with `mode="overwrite"` and pre-flight checks.

## Reproducibility

The generator (`src/generator.py`) runs in **plain single-threaded Python/pandas**
(never PySpark — avoids Spark's partitioning determinism trap), uses an explicitly
instantiated `random.Random(SEED)`, and never iterates sets (per-process hash seed
would break byte-reproducibility). It writes committed CSV fixtures and a
`ground_truth_manifest.csv` with exact counted injection (37 allocation, 12 orphaned
credits, 9 gaps, 6 duplicates, 4 dishonours; a 3-customer tracked-overlap set exercises
rule independence). CI regenerates from the fixed seed and **byte-diffs** the output
against the committed fixtures.

## Dashboard

`dashboard.html` is a single file with **inline Plotly.js** (no CDN), fixed `div_id`s
(`notices-by-rule`, `flags-over-time`, `drilldown-table`) in fixed order, and all text
baked in at generation time (view-time never calls anything). Output is byte-stable;
the deterministic-render is covered by a unit test. It never depends on the optional
LLM layer.

## Optional LLM notice-paraphrase (NVIDIA NIM)

Explanations are deterministic per-rule templates (always on). An optional NIM
paraphrase layer can rephrase them; it is **key-gated, never in the core path, capped
at ~40 req/min, and excluded from CI**. Set `NIM_API_KEY` to enable. NIM's free tier
is limited to dev/testing/research (see `src/config.py`). **The key is never
committed.**

## Testing

```bash
python -m pytest tests/ -q
```

Three layers (spec §9):

1. **Silver transforms** — balance accumulation, `allocated_total_at_sent` timing,
   notice boundary.
2. **Per-rule unit tests** — each rule on crafted mini-fixtures (detection + severity).
3. **Gold-vs-manifest integration** — 100% recall, zero false positives, severity
   matches the manifest, simulation sanity, and rule independence.

All Spark tests share one session fixture and write only to `tmp_path`. CI runs the
fixture reproducibility gate plus the full suite on GitHub Actions (Ubuntu, Java 17).

## Repository layout

```
run_pipeline.py         # staged orchestration
src/generator.py        # deterministic synthetic data + ground-truth manifest
src/silver.py           # running balance, allocated totals, computed notices
src/gold.py             # 5 rules + severity + explanations
src/dashboard.py        # deterministic Plotly dashboard
src/spark.py            # local Delta-spark session factory
tests/                  # Silver / rules / integration / dashboard / generator
scripts/check_fixtures.py  # regenerate-and-diff reproducibility gate
fixtures/               # committed Bronze CSVs + manifest
docs/adr/0001-*         # stack decision
CONTEXT.md              # domain glossary
billing_anomaly_detector_spec.md  # the grilling/domain-modeling output spec
```

## Configuration reference

All tunables live in `src/config.py` as named constants:

| Constant | Default | Meaning |
|---|---|---|
| `SEED` | `20250311` | Determinism seed for the generator |
| `POPULATION` | `1000` | Number of synthetic customers |
| `INJECTION_COUNTS` | `{allocation:37, stale_unswept_credit:12, statement_gap:9, duplicate_invoice:6, dishonour:4}` | Exact per-family defect counts |
| `TRACKED_OVERLAP` | `3` | Customers hit by both rule #1 and #3 (independence test) |
| `GRACE_DAYS` | `7` | Naive notice fires at `due_date + grace_days` (Superloop-anchored) |
| `ORPHAN_DAYS` | `30` | Unapplied credit becomes orphaned past this window |
| `GAP_THRESHOLD_DAYS` | `3` | Rule #3 minimum gap |
| `AMOUNT_TOLERANCE_RATIO` / `_MIN_DOLLARS` | `0.01` / `$1` | Rule #5 near-identical amount tolerance |
| `NOTICE_ESCALATION_THRESHOLD` | `$100` | Rule #1 high → critical (default, **not** provider-anchored) |
| `DUPLICATE_ESCALATION_THRESHOLD` | `$100` | Rule #5 medium → high (default, **not** provider-anchored) |
| `SEVERITY_BASE` | `{1:high, 2:warning, 3:medium, 4:critical, 5:medium}` | Static severity base per rule |
| `NIM_*` | — | Optional LLM paraphrase: endpoint, pinned model id, 40 req/min cap, `NIM_API_KEY` env var |
| `FIXTURE_DIR` / `DATA_DIR` / `DASHBOARD_PATH` | `fixtures` / `data` / `dashboard.html` | Stage I/O locations |

## Honest scope notes

- Synthetic data models a simplified monthly subscription; real Superloop billing
  has more payment methods, proration and taxes. The **plumbing and rule logic** is
  the deliverable; the data is illustrative.
- The $100 escalation thresholds are explicit, configurable defaults, **not**
  provider-anchored evidence.
- Local `local[*]` execution is for analysis; a production deployment would run the
  identical code on a cluster or hosted Spark/Databricks.
