"""Layer 2: per-rule Gold detection + severity on crafted mini-fixtures."""
from src import config
from src.gold import (
    rule1_wrongful_notice,
    rule2_orphaned_credit,
    rule3_statement_gap,
    rule4_dishonour_without_cause,
    rule5_duplicate_invoice,
    severity_for,
)
from src.silver import compute_running_balance
from tests.conftest import make_df

INV = ["invoice_id", "customer_id", "statement_period_start", "statement_period_end",
       "due_date", "amount_due", "issued_date"]
ENT = ["entry_id", "customer_id", "type", "amount", "effective_date", "source", "status"]
ALL = ["allocation_id", "entry_id", "invoice_id", "amount_allocated", "allocated_date"]
NOTICE = ["customer_id", "invoice_id", "amount_due", "sent_date", "allocated_total_at_sent",
          "balance_at_sent", "in_credit"]


# ---- rule #1 wrongful notice ---------------------------------------------------

def test_rule1_flags_only_in_credit_notices(spark):
    notices = make_df(spark, [
        {"customer_id": "A", "invoice_id": "I1", "amount_due": 100.0, "sent_date": "2024-01-17",
         "allocated_total_at_sent": 0.0, "balance_at_sent": 0.0, "in_credit": True},
        {"customer_id": "B", "invoice_id": "I2", "amount_due": 50.0, "sent_date": "2024-02-17",
         "allocated_total_at_sent": 0.0, "balance_at_sent": -50.0, "in_credit": False},
    ], NOTICE)
    rows = rule1_wrongful_notice(notices).collect()
    assert [r.invoice_id for r in rows] == ["I1"]
    assert rows[0].severity == "high"          # shortfall 100 == threshold, not >


def test_rule1_severity_escalates_past_threshold(spark):
    notices = make_df(spark, [
        {"customer_id": "A", "invoice_id": "I3", "amount_due": 150.0, "sent_date": "2024-01-17",
         "allocated_total_at_sent": 0.0, "balance_at_sent": 0.0, "in_credit": True},
    ], NOTICE)
    row = rule1_wrongful_notice(notices).collect()[0]
    assert row.severity == "critical"          # shortfall 150 > 100


# ---- rule #2 orphaned credit ---------------------------------------------------

def test_rule2_flags_stale_unapplied_credit_only(spark):
    entries = make_df(spark, [
        # stale unapplied credit (should flag)
        {"entry_id": "e1", "customer_id": "A", "type": "credit", "amount": 40.0,
         "effective_date": "2024-01-01", "source": "goodwill", "status": "settled"},
        # fresh unapplied credit (within window -> no flag)
        {"entry_id": "e2", "customer_id": "A", "type": "credit", "amount": 25.0,
         "effective_date": "2025-12-20", "source": "promo", "status": "settled"},
        # fully applied credit (no flag)
        {"entry_id": "e3", "customer_id": "A", "type": "credit", "amount": 30.0,
         "effective_date": "2024-02-01", "source": "goodwill", "status": "settled"},
        # dishonoured payment is never an orphan
        {"entry_id": "e4", "customer_id": "A", "type": "payment", "amount": 10.0,
         "effective_date": "2024-01-15", "source": "direct_debit", "status": "dishonoured"},
    ], ENT)
    allocations = make_df(spark, [
        {"allocation_id": "a3", "entry_id": "e3", "invoice_id": "X",
         "amount_allocated": 30.0, "allocated_date": "2024-02-01"},
    ], ALL)
    rows = rule2_orphaned_credit(entries, allocations, asof="2025-12-31").collect()
    assert [r.flag_id for r in rows] == ["R2_e1"]
    assert rows[0].severity == "warning"


# ---- rule #3 statement gap -----------------------------------------------------

def test_rule3_flags_gap_only(spark):
    invoices = make_df(spark, [
        {"invoice_id": "I1", "customer_id": "A", "statement_period_start": "2024-01-01",
         "statement_period_end": "2024-01-31", "due_date": "2024-02-05",
         "amount_due": 100.0, "issued_date": "2024-01-01"},
        {"invoice_id": "I2", "customer_id": "A", "statement_period_start": "2024-02-01",
         "statement_period_end": "2024-02-29", "due_date": "2024-03-05",
         "amount_due": 100.0, "issued_date": "2024-02-01"},
        # 30-day gap before I3
        {"invoice_id": "I3", "customer_id": "A", "statement_period_start": "2024-03-30",
         "statement_period_end": "2024-04-30", "due_date": "2024-05-05",
         "amount_due": 100.0, "issued_date": "2024-03-30"},
    ], INV)
    rows = rule3_statement_gap(invoices).collect()
    assert [r.invoice_id for r in rows] == ["I3"]
    assert rows[0].severity == "medium"


# ---- rule #4 dishonour ---------------------------------------------------------

def test_rule4_flags_dishonour_with_sufficient_balance(spark):
    entries = make_df(spark, [
        {"entry_id": "d1", "customer_id": "A", "type": "invoice_debit", "amount": -100.0,
         "effective_date": "2024-01-01", "source": "invoice", "status": "settled"},
        {"entry_id": "c1", "customer_id": "A", "type": "credit", "amount": 100.0,
         "effective_date": "2024-01-02", "source": "goodwill", "status": "settled"},
        # dishonour at 2024-01-05: pre-debit balance 0 -> no cause
        {"entry_id": "p1", "customer_id": "A", "type": "payment", "amount": 100.0,
         "effective_date": "2024-01-05", "source": "direct_debit", "status": "dishonoured"},
        # dishonour for customer B who owes money -> has cause
        {"entry_id": "d2", "customer_id": "B", "type": "invoice_debit", "amount": -50.0,
         "effective_date": "2024-01-01", "source": "invoice", "status": "settled"},
        {"entry_id": "p2", "customer_id": "B", "type": "payment", "amount": 50.0,
         "effective_date": "2024-01-06", "source": "direct_debit", "status": "dishonoured"},
    ], ENT)
    rb = compute_running_balance(entries)
    rows = rule4_dishonour_without_cause(entries, rb).collect()
    assert [r.flag_id for r in rows] == ["R4_p1"]
    assert rows[0].severity == "critical"


# ---- rule #5 duplicate invoice -------------------------------------------------

def test_rule5_flags_overlapping_near_identical(spark):
    invoices = make_df(spark, [
        {"invoice_id": "I1", "customer_id": "A", "statement_period_start": "2024-01-01",
         "statement_period_end": "2024-01-31", "due_date": "2024-02-05",
         "amount_due": 100.0, "issued_date": "2024-01-01"},
        # duplicate: same period, same amount -> flag I2
        {"invoice_id": "I2", "customer_id": "A", "statement_period_start": "2024-01-01",
         "statement_period_end": "2024-01-31", "due_date": "2024-02-05",
         "amount_due": 100.0, "issued_date": "2024-01-01"},
        # different month (no overlap) -> no flag
        {"invoice_id": "I3", "customer_id": "A", "statement_period_start": "2024-03-01",
         "statement_period_end": "2024-03-31", "due_date": "2024-04-05",
         "amount_due": 100.0, "issued_date": "2024-03-01"},
        # overlaps I1 but amount differs by > tolerance -> no flag
        {"invoice_id": "I4", "customer_id": "A", "statement_period_start": "2024-01-15",
         "statement_period_end": "2024-02-15", "due_date": "2024-02-20",
         "amount_due": 300.0, "issued_date": "2024-01-15"},
    ], INV)
    rows = rule5_duplicate_invoice(invoices).collect()
    assert [r.invoice_id for r in rows] == ["I2"]
    assert rows[0].severity == "medium"


# ---- severity principle --------------------------------------------------------

def test_severity_principle():
    assert severity_for(1, 50.0) == "high"       # base high, below threshold
    assert severity_for(1, 150.0) == "critical"  # escalates
    assert severity_for(2, 1e6) == "warning"     # never escalates
    assert severity_for(3, 1e6) == "medium"
    assert severity_for(4, 1e6) == "critical"
    assert severity_for(5, 50.0) == "medium"
    assert severity_for(5, 150.0) == "high"      # escalates but never critical
