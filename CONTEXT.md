# Context — Superloop Billing Anomaly Detector

> Glossary only. No implementation detail. These are the canonical terms for the
> domain; resolve all naming against this file before the spec or code introduce
> anything new.

## Account / Balance

- **Account balance** — the cumulative net position of a single customer's billing
  ledger at a point in time: total credit events minus total debit events.
  Bank-style running balance; the source of truth for how much the customer owes
  overall. Keyed by `(customer_id, effective_date)`.

- **Running ledger** — the ordered sequence of ledger entries that yields the account
  balance at any effective date. Credit events raise the balance; debit events lower it.

## Ledger entries

- **Ledger entry** — a single economic event on a customer's account. Exactly one of:
  **payment**, **credit** (independent credit), or **invoice debit**. A unified record —
  summing all entries of one customer gives the true account balance with no
  double-counting by construction.

- **Credit (independent credit)** — a credit event that is genuinely separate from any
  payment: refund issued without reversing a payment, promotional credit, goodwill
  adjustment. NOT an overpayment.

- **Overpayment** — NOT an economic event and never written as a ledger row. It is an
  *emergent state*: the account balance is positive after netting payments against
  invoices. If support needs to see it, it is materialised as a computed view.

## Allocation layer

- **Invoice allocation** — the per-invoice matching of a ledger entry (payment or
  independent credit) against a specific invoice: how much of that entry covers that
  invoice. A many-to-many relationship, because one payment can cover multiple invoices
  and one invoice can be covered by multiple entries.

- **Allocation state** — the separate, per-invoice record of whether that specific
  invoice has been matched against payments/credits. Deliberately allowed to be wrong or
  stale: modelling its staleness is the entire point of the project.

- **Allocation defect / reconciliation failure** — a disagreement between the true
  account-level balance and the (imperfect) per-invoice allocation state. The documented
  arrears bug is a reconciliation failure: an invoice's allocation never swept in an
  available credit, so the arrears trigger (which reads allocation state) fired even
  though the account balance was in credit.

- **Unapplied (entry)** — a ledger entry whose allocated total across all invoices is
  less than its own amount: the unallocated residual of a credit or payment.

- **Orphaned (stale unapplied) credit** — an unapplied credit whose unallocated gap has
  persisted beyond the N-day window. A leading indicator: a credit *at risk* of causing a
  future reconciliation failure. Distinct from "unapplied", which is a benign residual.

## Invoicing

- **Statement period** — the dated interval an invoice covers, from `period_start` to
  `period_end`. For a single customer, consecutive invoices' statement periods should
  join seamlessly (prior `period_end` → next `period_start`) with no gap and no overlap.

- **Statement continuity break** — a discontinuity between consecutive invoices'
  statement periods for the same customer: either a **gap** or an **overlap**. The
  date-sequence defect from the motivating complaint.

- **Gap (statement period)** — the next invoice's `period_start` falls after the prior
  invoice's `period_end`: a *missing* statement period. The source complaint's
  2020→2024 jump is a gap. A discontinuity.

- **Overlap (statement period)** — the next invoice's `period_start` falls before the
  prior invoice's `period_end`: a *duplicated* statement period. A discontinuity.

- **Discontinuity partition** — gap and overlap are mutually exclusive by construction:
  a pair of adjacent invoices is gapped, overlapping, or contiguous — never two at once.
  Rule #3 (statement continuity) targets gaps; Rule #5 (duplicate invoice) targets
  overlaps. Together they partition the discontinuity space without double-flagging.

- **Gap threshold** — the minimum `gap_days` (default 3, configurable) before a gap is
  flagged. Tolerates legitimate calendar shifts (28 vs 30-day months, weekend-adjusted
  issue dates) without treating routine noise as a defect.

- **Duplicate amount tolerance** — "near-identical" amount defined concretely as
  `abs(amount_A − amount_B) <= max($1, 1% of amount_due)`, configurable. Deliberately
  leaves *overlap-with-clearly-different-amount* unflagged: that is the signature of a
  legitimate correction/reissue or prorated mid-cycle change, which the project scopes
  out by design.

- **Due date** — the date by which an invoice's allocated total must reach its
  `amount_due`, after which late payment applies. Part of the naive billing rule.

- **Grace period** — a number of days after the due date during which an under-allocated
  invoice is *not* yet sent to arrears. The naive notice-generator evaluates each invoice
  at `due_date + grace_days`, and only fires a notice if still under-allocated then.

## Notice generation

- **Naive notice rule / dunning rule** — the (deliberately unsophisticated) rule the
  pipeline uses to *compute* arrears notices from allocation state: for each invoice,
  evaluate at `due_date + grace_days`; if `allocated_total < amount_due` at that point,
  emit a notice. Reproduces how a simple billing system decides "unpaid → arrears".

- **Arrears notice (computed)** — a Silver-layer derived record, not planted Bronze data.
  Produced by applying the naive notice rule to the allocation state as of each invoice's
  evaluation date.

- **Wrongful notice** — a computed notice whose account running balance was ≥ 0 at
  `sent_date`. The precise target of rule #1. Distinguished from a **legitimate notice**
  (a credit that arrived *after* the `due_date + grace_days` window closed — bad timing,
  not a defect).

## Validation

- **Simulation sanity check** — verifying the naive notice-generator itself: on the clean
  (no injected defect) majority of accounts, it should fire zero (or near-zero) notices.
  Tests the synthetic simulation, not the detector.

- **Detector validation** — verifying rule #1 recovers exactly the notices caused by
  planted allocation defects. Meaningful only because "wrongful notice" has a precise
  definition (running balance ≥ 0 at `sent_date`).

## Explanation

- **Explanation (reason string)** — the plain-language human-readable account of *why* a
  flagged anomaly was flagged, emitted on the `flagged_accounts` row. Two layers:

- **Deterministic explanation** — a hand-written per-rule template filled from the
  evidence row (e.g. *"a $42.10 credit from March was never applied to the April invoice,
  which triggered a $X arrears notice in error"*). Always on, offline, deterministic,
  testable. The source of truth for what happened.

- **LLM paraphrase** — an optional, key-gated rephrasing of the deterministic explanation
  into a warmer support-agent voice. The LLM **never invents facts**; it only rewords the
  already-verified evidence. Gated behind an env-var API key; without a key the pipeline
  emits the deterministic version. Excluded from the validation/CI path.

- **LLM provider (NVIDIA NIM)** — the concrete default for the optional paraphrase layer:
  free dev tier (signup + phone verification), OpenAI-compatible endpoint
  (`https://integrate.api.nvidia.com/v1`), ~40 requests/min shared across models on a key
  (so calls must be hard-capped), model ID pinned as a named config constant. Free tier is
  explicitly dev/testing/research/evaluation-only, which matches this project's nature.

## Reproducibility

- **Deterministic generation** — the synthetic generator (both organic-noise and
  counted-injection passes) runs in **plain single-threaded Python/pandas**, not PySpark,
  using an explicitly instantiated `random.Random(SEED)` (never the global `random`
  module). PySpark enters only at the pipeline stage (Bronze→Silver→Gold), reading the
  already-fixed fixture. Reason: Spark's `F.rand(seed=...)` is only deterministic given a
  fixed partition count, and local PySpark partitions scale with the host's core count —
  a reviewer's machine could silently diverge.

- **Committed fixture** — the generated Bronze tables are committed as **CSV** (not
  Parquet) so they are `git diff`-able and inspectable without tooling. A reviewer compares
  against exactly the numbers the README publishes.

- **Counted injection** — defects are planted by a separate, counted step with **exact
  target counts** per defect family (not "~5–8% of a random population"), so every cell of
  the validation table has real numbers. Non-round counts signal the data was not
  hand-tuned.

- **Ground-truth manifest** — a committed table, one row per injected defect, schema:
  `defect_id, defect_family (allocation | statement_gap | duplicate_invoice | dishonour),
  customer_id, affected_invoice_id, expected_rule, expected_severity, injected_amount`.
  The last column is what lets severity-escalation thresholds be asserted exactly.

- **Regenerate-and-diff check** — a CI job re-runs the plain-Python generator from the
  fixed seed and diffs its output byte-for-byte against the committed fixture. Divergence =
  build failure, not silent drift. This is what earns the "seeded" claim.

## Dashboard

- **Dashboard artifact** — a single committed `dashboard.html`, rendered with Plotly,
  showing flags by rule type, trend over time, and a flagged-customer drill-down table with
  evidence. Generated as the pipeline's final stage, so a re-run regenerates the same
  artifact.

- **Deterministic chart output** — `plotly.io.write_html` is non-deterministic by default
  (auto-generated `div_id` per chart). The dashboard uses fixed `div_id`s per chart in a
  fixed order, in a single file, and `include_plotlyjs` pinned to **inline/embedded** so the
  chart renders with zero network calls at view-time (a CDN include would reopen the
  "reviewer needs internet" problem at view-time). Covered by the regenerate-and-diff CI
  check.

- **Dashboard text source** — whatever text the drill-down shows (deterministic template,
  or an LLM paraphrase if generated with a key) is baked into the static HTML at generation
  time; the dashboard never calls anything at view-time. Streamlit is available only behind
  an explicit flag, never the default.

## Testing

- **Three test layers** — each catching a different class of bug:
  1. **Silver-plumbing unit tests** (`test_silver_transforms.py`) — verify the Silver
     derivations directly: running-balance accumulation across a sequence of
     payments/credits/invoices, the due-date+grace-period notice boundary, allocation-join
     partial sums. Catches *computation* errors. This is the layer the rule tests otherwise
     assume away.
  2. **Per-rule unit tests** — crafted mini-Datasets encoding each defect vs. clean rows,
     fed to a single rule transformation. Catches *misinterpretation* of correct data.
  3. **Gold-vs-manifest integration test** — run the whole pipeline, assert `flagged_accounts`
     against the ground-truth manifest, with detection (`expected_rule`) and severity
     (`expected_severity`) asserted as separate axes. Catches *wiring/composition* errors.

- **Test output isolation** — tests write to `tmp_path` (pytest), never to the repo's
  committed Gold fixture or `dashboard.html`, so running the suite cannot overwrite what a
  reviewer is looking at.

## Orchestration

- **Staged pipeline** — `run_pipeline.py` exposes named `--stage` flags
  (generate | bronze | silver | gold | dashboard); no flag = full end-to-end run. Each
  stage independently re-runnable; `generate` is decoupled from the Spark stages (generate
  once, commit; pipeline reads the committed fixture).

- **Idempotent stage** — every Spark-writing stage uses explicit `mode="overwrite"` (never
  Spark's default append), so a re-run replaces rather than silently doubling rows.

- **Stage pre-flight check** — each stage verifies its required upstream table exists and
  fails with a one-line, human-readable message (e.g. "Silver table not found — run
  `--stage silver` first") rather than a raw `AnalysisException` stack trace.

## Defect families

The set of injectable failure modes, each tied to the detection rule(s) it feeds:
**allocation** (→ rule #1 wrongful notice), **stale-unswept credit** (→ rule #2 orphaned
credit), **statement gap** (→ rule #3), **duplicate invoice** (→ rule #5), **dishonour
without cause** (→ rule #4). A small tracked-overlap intersection also exercises rule
independence.

## Severity

- **Severity** — the urgency tier on a flagged anomaly; what a support team sorts on when
  they open `flagged_accounts`. Assigned per anomaly instance.

- **Severity escalation principle** — magnitude-escalation applies only where a rule's
  dollar amount is both **well-defined and variable enough to matter**. Rules whose harm
  is bounded, categorical, or not-yet-materialised stay static. A rule about the rules,
  not a per-rule special case.

- **Dollar amount at risk** — the well-defined, variable figure that drives escalation for
  the rules where it applies. Where it does not apply, severity is static by principle.

Per-rule severities (by the escalation principle):

| Rule | Base | Escalation |
|---|---|---|
| #1 wrongful notice | high | → critical past `notice_escalation_threshold` (default $100) |
| #2 orphaned credit | warning | static — leading indicator, no harm materialised |
| #3 statement gap | medium | static — no natural dollar figure |
| #4 dishonour-without-cause | critical | static — harm is categorical and fee-bounded |
| #5 duplicate invoice | medium | → high past `duplicate_escalation_threshold` (default $100) |

Escalation thresholds default to $100, honestly flagged as **not** anchored to source
provider evidence (unlike `grace_days`), purely a configurable default.
