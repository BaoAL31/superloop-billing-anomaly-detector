"""Layer 1: Silver plumbing transforms — balance accumulation, grace-period notice
boundary, allocation join with timing, and the wrongful/in-credit signal."""
from datetime import date, timedelta

from src import config
from src.silver import (
    balance_at,
    compute_allocated_at_sent,
    compute_naive_notices,
    compute_running_balance,
)
from tests.conftest import make_df

INV = ["invoice_id", "customer_id", "amount_due", "due_date", "issued_date"]
ENT = ["entry_id", "customer_id", "type", "amount", "effective_date", "source", "status"]
ALL = ["allocation_id", "entry_id", "invoice_id", "amount_allocated", "allocated_date"]


def test_running_balance_accumulates_and_excludes_dishonoured(spark):
    rows = [
        {"entry_id": "e1", "customer_id": "A", "type": "invoice_debit", "amount": -100,
         "effective_date": "2024-01-01", "source": "invoice", "status": "settled"},
        {"entry_id": "e2", "customer_id": "A", "type": "payment", "amount": 100,
         "effective_date": "2024-01-05", "source": "direct_debit", "status": "settled"},
        {"entry_id": "e3", "customer_id": "A", "type": "payment", "amount": 50,
         "effective_date": "2024-01-20", "source": "direct_debit", "status": "settled"},
        {"entry_id": "e4", "customer_id": "A", "type": "payment", "amount": 100,
         "effective_date": "2024-01-25", "source": "direct_debit", "status": "dishonoured"},
    ]
    rb = compute_running_balance(make_df(spark, rows, ENT))
    got = {
        r["entry_id"]: r["running_balance"]
        for r in rb.orderBy("effective_date").collect()
    }
    assert got == {"e1": -100, "e2": 0, "e3": 50}
    # e4 dishonoured must be excluded entirely (never moved money)


def test_balance_at_returns_latest_prior_balance(spark):
    rb = make_df(
        spark,
        [
            {"entry_id": "e1", "customer_id": "A", "type": "d", "amount": -100,
             "effective_date": "2024-01-01", "source": "x", "status": "settled"},
            {"entry_id": "e2", "customer_id": "A", "type": "p", "amount": 100,
             "effective_date": "2024-01-05", "source": "x", "status": "settled"},
        ],
        ENT,
    )
    running = compute_running_balance(rb)
    targets = make_df(
        spark,
        [{"customer_id": "A", "date": "2024-01-03"}, {"customer_id": "A", "date": "2024-01-10"}],
        ["customer_id", "date"],
    )
    bal = {r["date"]: r["running_balance"] for r in balance_at(targets, running).collect()}
    assert bal == {"2024-01-03": -100, "2024-01-10": 0}


def _inv(spark, rows):
    return make_df(spark, rows, INV)


def test_allocated_at_sent_excludes_late_allocations(spark):
    invoices = _inv(
        spark,
        [
            {"invoice_id": "I1", "customer_id": "A", "amount_due": 100.0,
             "due_date": "2024-01-10", "issued_date": "2024-01-01"},
        ],
    )
    allocations = make_df(
        spark,
        [
            {"allocation_id": "a1", "entry_id": "p1", "invoice_id": "I1",
             "amount_allocated": 100.0, "allocated_date": "2024-01-09"},
        ],
        ALL,
    )
    got = compute_allocated_at_sent(invoices, allocations, 7).collect()[0]
    assert got["sent_date"] == date(2024, 1, 17)  # due + grace
    assert got["allocated_total_at_sent"] == 100.0

    # payment AFTER sent must not count -> allocated_total_at_sent == 0
    late = make_df(
        spark,
        [
            {"allocation_id": "a1", "entry_id": "p1", "invoice_id": "I1",
             "amount_allocated": 100.0, "allocated_date": "2024-01-20"},
        ],
        ALL,
    )
    got = compute_allocated_at_sent(invoices, late, 7).collect()[0]
    assert got["allocated_total_at_sent"] == 0.0


def test_naive_notice_fires_only_when_under_allocated_at_sent(spark):
    invoices = _inv(
        spark,
        [
            {"invoice_id": "PAID", "customer_id": "A", "amount_due": 100.0,
             "due_date": "2024-01-10", "issued_date": "2024-01-01"},
            {"invoice_id": "UNPAID", "customer_id": "B", "amount_due": 50.0,
             "due_date": "2024-02-10", "issued_date": "2024-02-01"},
            {"invoice_id": "LATE", "customer_id": "C", "amount_due": 30.0,
             "due_date": "2024-03-10", "issued_date": "2024-03-01"},
        ],
    )
    allocations = make_df(
        spark,
        [
            # PAID: full on time -> no notice
            {"allocation_id": "a1", "entry_id": "p1", "invoice_id": "PAID",
             "amount_allocated": 100.0, "allocated_date": "2024-01-09"},
            # LATE: paid but only after sent -> notice still fires (legitimate)
            {"allocation_id": "a3", "entry_id": "p3", "invoice_id": "LATE",
             "amount_allocated": 30.0, "allocated_date": "2024-03-20"},
        ],
        ALL,
    )
    entries = make_df(
        spark,
        [
            # B has NO money at all (debit only) -> in_credit False
            {"entry_id": "d2", "customer_id": "B", "type": "invoice_debit", "amount": -50,
             "effective_date": "2024-02-01", "source": "invoice", "status": "settled"},
            # A has a payment covering PAID
            {"entry_id": "p1", "customer_id": "A", "type": "payment", "amount": 100,
             "effective_date": "2024-01-05", "source": "direct_debit", "status": "settled"},
            {"entry_id": "d1", "customer_id": "A", "type": "invoice_debit", "amount": -100,
             "effective_date": "2024-01-01", "source": "invoice", "status": "settled"},
        ],
        ENT,
    )
    rb = compute_running_balance(entries)
    notices = compute_naive_notices(invoices, allocations, rb, 7)
    by_inv = {r["invoice_id"]: r for r in notices.collect()}
    assert set(by_inv) == {"UNPAID", "LATE"}
    assert by_inv["UNPAID"]["in_credit"] is False  # genuinely in arrears
