# Billing Anomaly Detector — Project Spec
*Portfolio project targeting Superloop's Graduate Data Analyst role*

> Consolidated from the design session. Domain vocabulary lives in `CONTEXT.md`;
> the stack decision is recorded in `docs/adr/0001-local-pyspark-with-delta-spark.md`.

## 1. Motivation

A verified customer complaint (Whirlpool Forums, Mar 2025) describes receiving repeated
arrears notices while their Superloop account was actually in credit. The customer's own
statement showed a broken date sequence — one statement period jumping straight from 2020
to 2024. The root cause is a **data integrity problem in how statement periods are
generated or joined**, and more fundamentally a **reconciliation failure** between the
account's true balance and the per-invoice allocation state that drives arrears notices.
This project reproduces that failure mode on synthetic data and builds a pipeline to
detect it (and related billing anomalies) automatically, mirroring the "ensure strong
data quality, governance, and adherence to best practices" work called out in Superloop's
Graduate Data Analyst listing.

## 2. Objective

Build a Databricks-style pipeline that ingests raw billing events, flags accounts with
anomalous arrears/credit states, and outputs a prioritized list a billing/support team
could act on — with enough documentation that a reviewer can see you understood *why* each
rule exists, not just that you wrote a filter. The pipeline is **local-first
PySpark + delta-spark**, clone-and-run (`pip install -r requirements.txt` →
`python run_pipeline.py`), with the same code a Databricks workspace would run. See
ADR-0001.

## 3. Synthetic Data Model

Data is generated **deterministically** in **plain single-threaded Python** (explicit
`random.Random(SEED)`, not PySpark — see §11), committed as **CSV fixtures**, then read by
the Spark pipeline. Naming follows `CONTEXT.md`.

**Core tables (Bronze):**

**customers**
| column | type | notes |
|---|---|---|
| customer_id | string | PK |
| plan_id | string | FK to plans |
| signup_date | date | |
| status | string | active / cancelled / suspended |

**ledger_entries** — unified credit/debit ledger, no double-count by construction
| column | type | notes |
|---|---|---|
| entry_id | string | PK |
| customer_id | string | FK |
| type | string | payment / credit (independent credit) / invoice_debit |
| amount | decimal | positive for credit types, negative for invoice_debit |
| effective_date | date | |
| source | string | overpayment-not-a-row; only independent sources (refund / promo / goodwill) |

**invoice_allocations** — the deliberately-imperfect per-invoice matching layer
| column | type | notes |
|---|---|---|
| allocation_id | string | PK |
| entry_id | string | FK to ledger_entries |
| invoice_id | string | FK to invoices |
| amount_allocated | decimal | |
| allocated_date | date | |

**invoices**
| column | type | notes |
|---|---|---|
| invoice_id | string | PK |
| customer_id | string | FK |
| statement_period_start | date | |
| statement_period_end | date | |
| due_date | date | drives the naive notice rule |
| amount_due | decimal | |
| issued_date | date | |

**credits are NOT a separate additive ledger** — overpayment is an emergent balance state,
never written as a row. Independent credits (refund/promo/goodwill) are `ledger_entries`
of type `credit`.

**Counted defect injection** (a separate, counted step — not "~5–8% of a random
population") plants defects with **exact target counts** on the seeded data, each recorded
in the **ground-truth manifest** (§11). Defect families and target counts (non-round by
design):

| family | count | feeds rule |
|---|---|---|
| allocation (unswept credit) | 37 | #1 wrongful notice |
| stale unswept credit | 12 | #2 orphaned credit |
| statement gap | 9 | #3 |
| duplicate invoice | 6 | #5 |
| dishonour without cause | 4 | #4 |

A small tracked-overlap intersection (~3 customers) exercises rule independence.

## 4. Detection Rules (Silver→Gold)

Each rule is its own transformation, independently testable (§12). `flagged_accounts` is
Gold — one row per anomaly instance.

1. **Arrears-while-in-credit / wrongful notice (core bug):** a *computed* notice (see §5)
   whose account **running balance as of `sent_date` ≥ 0**. This is a reconciliation
   mismatch: allocation state says unpaid while the true balance is in credit.
2. **Orphaned credit (leading indicator):** a `ledger_entries` credit/payment whose
   allocated total is less than its own amount, gap persisting beyond N days. Distinct
   from a benign unapplied residual.
3. **Statement continuity break — gaps only:** per customer, flag a **gap** between
   consecutive invoices when `gap_days > gap_threshold_days` (default 3). Faithful
   reproducer of the 2020→2024 complaint defect.
4. **Dishonour-without-cause:** failed direct debit where the running balance (pre-debit)
   was sufficient to cover the attempted amount.
5. **Duplicate invoice — overlaps only:** two invoices for the same customer with
   **overlapping** statement periods and near-identical `amount_due`
   (`abs(Δ) ≤ max($1, 1%)`). Deliberately leaves overlap-with-clearly-different-amount
   unflagged (correction/reissue signature, scoped out).

Rules #3 and #5 are disjoint by construction (gap vs. overlap partition).

## 5. Pipeline Architecture (medallion)

- **Bronze:** committed CSV fixtures, ingested into Delta tables by a Spark stage.
- **Silver:** cleaned/derived tables. One row per invoice with **running balance** (event
  ordering by `effective_date`), **allocated_total** (summed from `invoice_allocations`),
  and **computed arrears notices** via the **naive dunning rule**: for each invoice,
  evaluate at `due_date + grace_days` (default 7, configurable); if `allocated_total <
  amount_due` at that point, emit a notice. Notices are **computed**, not planted.
- **Gold:** `flagged_accounts` — one row per anomaly instance, with `rule_id`, `severity`
  (§6), and a plain-language `reason` string (deterministic template, §7).

Orchestrated by `run_pipeline.py` with named `--stage` flags
(`generate | bronze | silver | gold | dashboard`); no flag = full end-to-end run. Each
Spark-writing stage uses explicit `mode="overwrite"` (idempotent re-run) and a
**pre-flight upstream-table check** with a human-readable failure message (§13).

## 6. Severity

Assigned per anomaly instance by the **escalation principle**: magnitude-escalation
applies only where a rule's dollar amount is well-defined and variable; bounded,
categorical, or not-yet-materialised harm stays static.

| Rule | Base | Escalation |
|---|---|---|
| #1 wrongful notice | high | → critical past `notice_escalation_threshold` ($100 default) |
| #2 orphaned credit | warning | static (leading indicator) |
| #3 statement gap | medium | static |
| #4 dishonour-without-cause | critical | static (categorical, fee-bounded) |
| #5 duplicate invoice | medium | → high past `duplicate_escalation_threshold` ($100 default) |

Escalation thresholds default to $100 — flagged honestly as **not** anchored to provider
evidence (unlike `grace_days`), purely configurable defaults.

## 7. Output Layer

- A `flagged_accounts` Gold table, queryable via SQL.
- A **single committed `dashboard.html`** (Plotly) showing flags by rule type, trend over
  time, and a flagged-customer drill-down table with evidence. Deterministic: fixed
  `div_id` per chart, fixed order, `include_plotlyjs` **inline** (no network at view-time),
  regenerated as the pipeline's final stage, covered by the regenerate-and-diff CI check.
  Streamlit available only behind an explicit `--dashboard` flag, never the default.

## 8. Validation

Since ground truth is synthetic, it is **controlled and counted**. Two tables:

1. **Detection** — injected anomalies vs. flagged anomalies per rule (precision/recall).
2. **Severity** — expected vs. assigned severity, computed by applying the escalation
   thresholds to the manifest's `injected_amount`.

Plus two structural checks:
- **Simulation sanity** — the naive notice rule fires zero (or near-zero) notices on the
  clean majority of accounts.
- **Rule independence** — the tracked-overlap group is flagged by the correct rules
  without conflation or suppression.

## 9. Stretch Goal (AI tie-in)

A plain-English explanation per flagged account, in **two layers**:
1. **Deterministic template (always on):** per-rule template filled from the evidence row.
2. **LLM paraphrase (optional):** NVIDIA NIM (free dev tier), OpenAI-compatible endpoint,
   key from env var (never committed), model ID pinned as a config constant, hard-capped
   calls, excluded from validation/CI. Rephrases the deterministic draft only — never
   invents facts. Without a key, the pipeline emits the deterministic version.

## 10. Portfolio Presentation (README)

Open with the real Whirlpool complaint, explain the target defect (statement-date
discontinuity → reconciliation failure), and state explicitly that the data is synthetic,
built to reproduce a *publicly documented* failure mode — not implying access to real
Superloop data. Note honestly: PySpark here is a **stack-fidelity** choice (the dataset is
~1,000 synthetic customers, well within pandas' reach) and the NIM free tier is
dev/evaluation-only.

## 11. Reproducibility

- **Deterministic generation** in plain Python/pandas with an explicit `random.Random(SEED)`
  — never PySpark, which is only deterministic given fixed partitioning (host core count
  varies). Committed fixtures are **CSV** (git-diffable, inspectable).
- **Counted injection** with exact target counts (§3) written into a **ground-truth
  manifest** (`defect_id, defect_family, customer_id, affected_invoice_id, expected_rule,
  expected_severity, injected_amount`).
- **Regenerate-and-diff CI check:** re-runs the plain-Python generator from the fixed seed
  and diffs byte-for-byte against the committed fixture; divergence = build failure.

## 12. Testing

Three layers under `pytest`, all writing to `tmp_path` (never the committed outputs):
1. **Silver-plumbing unit tests** — running-balance accumulation, grace-period notice
   boundary, allocation-join sums (catches computation errors).
2. **Per-rule unit tests** — crafted mini-Datasets per defect vs. clean rows (catches
   misinterpretation of correct data).
3. **Gold-vs-manifest integration test** — full pipeline against the manifest, detection
   and severity asserted separately (catches wiring/composition errors).

## 13. Runtime / Config

`run_pipeline.py --stage <name>` with per-stage pre-flight checks and `mode="overwrite"`
on all Spark writes. Config constants (seeded, thresholds, model ID) centralised:
`grace_days` (7), `gap_threshold_days` (3), `amount_tolerance` (`max($1, 1%)`),
`notice_escalation_threshold` ($100), `duplicate_escalation_threshold` ($100),
`SEED`, population = 1,000 customers. `requirements.txt` pinned.

## 14. Tech Stack

PySpark + `delta-spark` (local, standalone — no account/cluster), Delta tables, SQL for
Silver→Gold transforms written as `.sql` files, Plotly for the dashboard. Optional: Great
Expectations or a custom assertion layer for data-quality checks under rule #3.
