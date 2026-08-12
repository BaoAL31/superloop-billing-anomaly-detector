"""Gold transforms (PySpark): the five anomaly rules, severity assignment, and
deterministic per-rule explanations.

Each rule is a pure function over DataFrames and returns rows with a **common
schema**: ``flag_id, rule_id, customer_id, invoice_id, amount, severity, explanation``.
Severity follows the escalation principle (spec §6): magnitude-escalation only where
the dollar-at-risk is well-defined (#1 high->critical, #5 medium->high); the rest are
static. Explanations are deterministic templates — the always-on layer (spec §7).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from . import config
from .silver import DATE, balance_at

COMMON = [
    "flag_id",
    "rule_id",
    "customer_id",
    "invoice_id",
    "amount",
    "severity",
    "flag_date",
    "explanation",
]


def severity_for(rule_id: int, escalation_amount: float) -> str:
    """Escalation principle: only rules with well-defined dollar-at-risk escalate."""
    base = config.SEVERITY_BASE[rule_id]
    if rule_id == 1 and escalation_amount > config.NOTICE_ESCALATION_THRESHOLD:
        return "critical"
    if rule_id == 5 and escalation_amount > config.DUPLICATE_ESCALATION_THRESHOLD:
        return "high"
    return base


def _severity_col(rule_id: int, escalation_expr):
    base = config.SEVERITY_BASE[rule_id]
    if rule_id == 1:
        return F.when(escalation_expr > config.NOTICE_ESCALATION_THRESHOLD, "critical").otherwise(base)
    if rule_id == 5:
        return F.when(escalation_expr > config.DUPLICATE_ESCALATION_THRESHOLD, "high").otherwise(base)
    return F.lit(base)


# --------------------------------------------------------------------------- rules

def rule1_wrongful_notice(notices: DataFrame) -> DataFrame:
    """Arrears-while-in-credit: a computed notice whose balance at sent_date >= 0."""
    rows = notices.filter(F.col("in_credit"))
    shortfall = F.col("amount_due") - F.col("allocated_total_at_sent")
    return rows.select(
        F.concat(F.lit("R1_"), F.col("invoice_id")).alias("flag_id"),
        F.lit(1).alias("rule_id"),
        F.col("customer_id"),
        F.col("invoice_id"),
        F.col("amount_due").alias("amount"),
        _severity_col(1, shortfall).alias("severity"),
        F.date_format(F.col("sent_date"), "yyyy-MM-dd").alias("flag_date"),
        F.concat_ws(
            "",
            F.lit("Wrongful arrears notice: invoice "),
            F.col("invoice_id"),
            F.lit(" shown "),
            shortfall.cast("decimal(12,2)"),
            F.lit(" under-allocated at "),
            F.col("sent_date"),
            F.lit(" while account balance "),
            F.col("balance_at_sent").cast("decimal(12,2)"),
            F.lit(" was in credit."),
        ).alias("explanation"),
    )


def rule2_orphaned_credit(
    entries: DataFrame, allocations: DataFrame, asof: str, orphan_days: int = config.ORPHAN_DAYS
) -> DataFrame:
    """A credit/payment whose allocated total < its amount, unapplied > N days."""
    alloc_tot = allocations.groupBy("entry_id").agg(
        F.sum("amount_allocated").alias("allocated_total")
    )
    candidates = entries.filter(
        F.col("type").isin("payment", "credit") & (F.col("status") != "dishonoured")
    )
    joined = candidates.join(alloc_tot, "entry_id", "left")
    orphaned = joined.withColumn(
        "allocated_total", F.coalesce(F.col("allocated_total"), F.lit(0.0))
    )
    orphaned = orphaned.filter(F.col("allocated_total") < F.col("amount"))
    orphaned = orphaned.withColumn(
        "unapplied_days",
        F.datediff(F.lit(asof), F.to_date(F.col("effective_date"), DATE)),
    )
    orphaned = orphaned.filter(F.col("unapplied_days") > orphan_days)
    return orphaned.select(
        F.concat(F.lit("R2_"), F.col("entry_id")).alias("flag_id"),
        F.lit(2).alias("rule_id"),
        F.col("customer_id"),
        F.lit(None).cast("string").alias("invoice_id"),
        F.col("amount"),
        _severity_col(2, F.lit(0.0)).alias("severity"),
        F.col("effective_date").alias("flag_date"),
        F.concat_ws(
            "",
            F.lit("Orphaned credit "),
            F.col("entry_id"),
            F.lit(": "),
            F.col("amount").cast("decimal(12,2)"),
            F.lit(" unapplied for "),
            F.col("unapplied_days"),
            F.lit(" days."),
        ).alias("explanation"),
    )


def rule3_statement_gap(invoices: DataFrame, gap_threshold: int = config.GAP_THRESHOLD_DAYS) -> DataFrame:
    """A continuity break: gap between consecutive statements exceeds threshold."""
    inv = invoices.select(
        "invoice_id",
        "customer_id",
        "statement_period_start",
        "statement_period_end",
    ).withColumn("ps", F.to_date("statement_period_start", DATE)).withColumn(
        "pe", F.to_date("statement_period_end", DATE)
    )
    w = Window.partitionBy("customer_id").orderBy("ps", "invoice_id")
    inv = inv.withColumn("prev_end", F.lag("pe").over(w)).withColumn(
        "gap_days", F.datediff(F.col("ps"), F.col("prev_end"))
    )
    rows = inv.filter(F.col("gap_days") > gap_threshold)
    return rows.select(
        F.concat(F.lit("R3_"), F.col("invoice_id")).alias("flag_id"),
        F.lit(3).alias("rule_id"),
        F.col("customer_id"),
        F.col("invoice_id"),
        F.lit(0.0).alias("amount"),
        _severity_col(3, F.lit(0.0)).alias("severity"),
        F.date_format(F.col("ps"), "yyyy-MM-dd").alias("flag_date"),
        F.concat_ws(
            "",
            F.lit("Statement continuity break: "),
            F.col("gap_days"),
            F.lit(" day gap before invoice "),
            F.col("invoice_id"),
            F.lit("."),
        ).alias("explanation"),
    )


def rule4_dishonour_without_cause(
    entries: DataFrame, running_balance: DataFrame
) -> DataFrame:
    """A dishonoured debit while the account was in credit (no billing cause)."""
    dish = (
        entries.filter(F.col("status") == "dishonoured")
        .select("entry_id", "customer_id", "amount", "effective_date")
        .alias("dish")
    )
    targets = dish.select(
        F.col("customer_id"), F.col("effective_date").alias("date"), F.col("entry_id")
    )
    bal = balance_at(targets, running_balance).alias("bal")
    final = dish.join(
        bal,
        (F.col("dish.customer_id") == F.col("bal.customer_id"))
        & (F.col("dish.effective_date") == F.col("bal.date")),
        "left",
    )
    flagged = final.filter(F.coalesce(F.col("bal.running_balance"), F.lit(0.0)) >= 0)
    return flagged.select(
        F.concat(F.lit("R4_"), F.col("dish.entry_id")).alias("flag_id"),
        F.lit(4).alias("rule_id"),
        F.col("dish.customer_id"),
        F.regexp_extract(F.col("dish.entry_id"), r"ENT_(INV_.*)_P", 1).alias(
            "invoice_id"
        ),
        F.col("dish.amount"),
        _severity_col(4, F.lit(0.0)).alias("severity"),
        F.col("dish.effective_date").alias("flag_date"),
        F.concat_ws(
            "",
            F.lit("Dishonoured debit "),
            F.col("dish.entry_id"),
            F.lit(" ("),
            F.col("dish.amount").cast("decimal(12,2)"),
            F.lit(") with no cause: balance "),
            F.coalesce(F.col("bal.running_balance"), F.lit(0.0)).cast("decimal(12,2)"),
            F.lit(" pre-debit."),
        ).alias("explanation"),
    )


def rule5_duplicate_invoice(
    invoices: DataFrame,
    tolerance_ratio: float = config.AMOUNT_TOLERANCE_RATIO,
    tolerance_min: float = config.AMOUNT_TOLERANCE_MIN_DOLLARS,
) -> DataFrame:
    """Two statements with overlapping periods and near-identical amounts."""
    inv = invoices.select(
        "invoice_id",
        "customer_id",
        "statement_period_start",
        "statement_period_end",
        "amount_due",
    ).withColumn("ps", F.to_date("statement_period_start", DATE)).withColumn(
        "pe", F.to_date("statement_period_end", DATE)
    )
    a = inv.alias("a")
    b = inv.alias("b")
    tol = F.greatest(F.lit(tolerance_min), F.lit(tolerance_ratio) * F.col("a.amount_due"))
    joined = a.join(
        b,
        (F.col("a.customer_id") == F.col("b.customer_id"))
        & (F.col("a.invoice_id") < F.col("b.invoice_id"))
        & (F.col("a.ps") <= F.col("b.pe"))
        & (F.col("b.ps") <= F.col("a.pe"))
        & (F.abs(F.col("a.amount_due") - F.col("b.amount_due")) <= tol),
    )
    return joined.select(
        F.concat(F.lit("R5_"), F.col("b.invoice_id")).alias("flag_id"),
        F.lit(5).alias("rule_id"),
        F.col("b.customer_id"),
        F.col("b.invoice_id"),
        F.col("b.amount_due").alias("amount"),
        _severity_col(5, F.col("b.amount_due")).alias("severity"),
        F.date_format(F.col("b.ps"), "yyyy-MM-dd").alias("flag_date"),
        F.concat_ws(
            "",
            F.lit("Duplicate invoice "),
            F.col("b.invoice_id"),
            F.lit(" overlaps "),
            F.col("a.invoice_id"),
            F.lit(" with near-identical amount."),
        ).alias("explanation"),
    )


def run_gold(silver, bronze_tables) -> DataFrame:
    """Assemble all five rule outputs into a single flagged_accounts DataFrame."""
    invoices = bronze_tables["invoices"]
    entries = bronze_tables["ledger_entries"]
    allocations = bronze_tables["invoice_allocations"]

    asof = invoices.selectExpr("max(due_date) as d").collect()[0]["d"]

    frames = [
        rule1_wrongful_notice(silver["silver_notices"]),
        rule2_orphaned_credit(entries, allocations, asof),
        rule3_statement_gap(invoices),
        rule4_dishonour_without_cause(entries, silver["silver_running_balance"]),
        rule5_duplicate_invoice(invoices),
    ]
    from functools import reduce

    return reduce(lambda a, b: a.unionByName(b), frames).select(*COMMON)
