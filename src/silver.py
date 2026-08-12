"""Silver transforms (PySpark): ledger plumbing that the Gold rules run against.

Everything here is a pure function over Spark DataFrames, which is the pre-agreed
test seam. Date columns are ISO strings ('yyyy-MM-dd'); each function parses them.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

from . import config

DATE = "yyyy-MM-dd"
GRACE = config.GRACE_DAYS


def compute_running_balance(entries: DataFrame) -> DataFrame:
    """Cumulative balance per customer over time.

    Dishonoured payments are excluded (they never moved money), so a running balance
    at a dishonoured payment's date naturally equals the pre-debit balance.
    Returns ``customer_id, effective_date, entry_id, running_balance``.
    """
    valid = entries.filter(F.col("status") != "dishonoured")
    w = Window.partitionBy("customer_id").orderBy(
        F.to_date("effective_date", DATE), "entry_id"
    )
    return valid.withColumn("running_balance", F.sum("amount").over(w)).select(
        "customer_id",
        "effective_date",
        "entry_id",
        "running_balance",
    )


def compute_allocated_at_sent(
    invoices: DataFrame, allocations: DataFrame, grace_days: int = GRACE
) -> DataFrame:
    """Per-invoice allocated total, counting only allocations dated <= sent_date.

    ``sent_date = due_date + grace_days`` is the instant the naive dunning rule fires.
    Returns ``invoice_id, customer_id, amount_due, sent_date, allocated_total_at_sent``.
    """
    inv = invoices.withColumn("sent_date", F.date_add(F.to_date("due_date", DATE), grace_days))
    al = allocations.withColumn("alloc_date", F.to_date("allocated_date", DATE))
    joined = inv.alias("i").join(
        al.alias("a"),
        F.col("a.invoice_id") == F.col("i.invoice_id"),
        "left",
    )
    # Sum only allocations dated <= sent_date, but always keep the invoice row
    # (even when its only allocation is late) so it can still be a notice.
    on_time = F.when(
        F.col("a.invoice_id").isNull() | (F.col("a.alloc_date") <= F.col("i.sent_date")),
        F.col("a.amount_allocated"),
    )
    return joined.groupBy(
        "i.invoice_id", "i.customer_id", "i.amount_due", "i.sent_date"
    ).agg(F.coalesce(F.sum(on_time), F.lit(0.0)).alias("allocated_total_at_sent"))


def balance_at(targets: DataFrame, running_balance: DataFrame) -> DataFrame:
    """Running balance of each customer at each target date (asof join).

    ``targets`` must have ``customer_id, date`` (ISO). Returns
    ``customer_id, date, running_balance`` (Null when no entry precedes the date).
    """
    t = targets.alias("t")
    rb = running_balance.alias("b")
    joined = t.join(
        rb,
        (F.col("t.customer_id") == F.col("b.customer_id"))
        & (F.to_date("b.effective_date", DATE) <= F.to_date("t.date", DATE)),
        "left",
    )
    w = Window.partitionBy("t.customer_id", "t.date").orderBy(
        F.to_date("b.effective_date", DATE).desc(), F.col("b.entry_id").desc()
    )
    latest = joined.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return latest.select(
        F.col("t.customer_id"), F.col("t.date"), F.col("b.running_balance")
    )


def compute_naive_notices(
    invoices: DataFrame,
    allocations: DataFrame,
    running_balance: DataFrame,
    grace_days: int = GRACE,
) -> DataFrame:
    """Naive dunning notices: invoices under-allocated at ``due_date + grace_days``.

    Returns notice rows with ``in_credit`` = running balance at sent_date >= 0,
    the signal that the notice was issued while the account was in credit (rule #1).
    """
    alloc_at = compute_allocated_at_sent(invoices, allocations, grace_days)
    under = alloc_at.filter(
        F.col("allocated_total_at_sent") < F.col("amount_due")
    ).alias("under")

    targets = under.selectExpr("customer_id", "sent_date as date")
    bal = balance_at(targets, running_balance).alias("bal")

    final = under.join(
        bal,
        (F.col("under.customer_id") == F.col("bal.customer_id"))
        & (F.col("under.sent_date") == F.col("bal.date")),
        "left",
    )
    return final.select(
        F.col("under.customer_id").alias("customer_id"),
        F.col("under.invoice_id").alias("invoice_id"),
        F.col("under.amount_due").alias("amount_due"),
        F.col("under.sent_date").alias("sent_date"),
        F.col("under.allocated_total_at_sent").alias("allocated_total_at_sent"),
        F.coalesce(F.col("bal.running_balance"), F.lit(0.0)).alias("balance_at_sent"),
        (F.coalesce(F.col("bal.running_balance"), F.lit(0.0)) >= 0).alias("in_credit"),
    )


def build_silver(
    spark, tables: dict[str, DataFrame] | dict[str, object]
) -> dict[str, DataFrame]:
    """Assemble all Silver tables from the four Bronze tables.

    ``tables`` values may be pandas DataFrames (converted here) or Spark DataFrames.
    """
    from pyspark.sql import DataFrame as SDF

    def as_sdf(t):
        return t if isinstance(t, SDF) else spark.createDataFrame(t)

    invoices = as_sdf(tables["invoices"])
    allocations = as_sdf(tables["invoice_allocations"])
    entries = as_sdf(tables["ledger_entries"])

    running_balance = compute_running_balance(entries)
    invoices_aug = compute_allocated_at_sent(invoices, allocations, GRACE)
    notices = compute_naive_notices(invoices, allocations, running_balance, GRACE)

    return {
        "silver_running_balance": running_balance,
        "silver_invoices": invoices_aug,
        "silver_notices": notices,
    }
