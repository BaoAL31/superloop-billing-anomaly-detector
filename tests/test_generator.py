"""Layer 0 (foundational): generator determinism, exact counted injection, manifest."""
import hashlib

import pytest

from src import config
from src.generator import generate


def _digest(tables, manifest) -> str:
    h = hashlib.sha256()
    for name in ["customers", "ledger_entries", "invoices", "invoice_allocations"]:
        h.update(tables[name].to_csv(index=False).encode())
    h.update(manifest.to_csv(index=False).encode())
    return h.hexdigest()


def test_population_and_table_shape():
    tables, _ = generate()
    c = config.INJECTION_COUNTS
    assert len(tables["customers"]) == config.POPULATION == 1000
    # entries = 2 per clean invoice + injected credits/payments - deleted gap D+P
    assert len(tables["ledger_entries"]) == (
        2 * 24000
        + c["allocation"]            # sized credits (rule #1)
        + c["stale_unswept_credit"]  # stale credits (rule #2)
        + c["dishonour"]             # sized credits (rule #4)
        + c["duplicate_invoice"]     # dup payments (rule #5)
        - 2 * c["statement_gap"]     # deleted debit+payment per gap
    )
    # allocations = 1 per clean invoice + dup + sized credits - deleted gap alloc.
    # Dishonour removes the payment allocation and adds a credit allocation (net 0).
    assert len(tables["invoice_allocations"]) == (
        24000 + c["duplicate_invoice"] + c["allocation"] - c["statement_gap"]
    )
    assert len(tables["invoices"]) == 24000 + c["duplicate_invoice"] - c["statement_gap"]


def test_exact_counted_injection_in_manifest():
    _, m = generate()
    actual = m["defect_family"].value_counts().to_dict()
    assert actual == config.INJECTION_COUNTS


def test_manifest_maps_family_to_expected_rule():
    _, m = generate()
    by_family = {
        "allocation": 1,
        "stale_unswept_credit": 2,
        "statement_gap": 3,
        "dishonour": 4,
        "duplicate_invoice": 5,
    }
    for family, rule in by_family.items():
        sub = m[m.defect_family == family]
        assert (sub.expected_rule == rule).all(), (family, sub.expected_rule.tolist())


def test_tracked_overlap_customers_flagged_by_two_rules():
    """Rule independence: ~TRACKED_OVERLAP customers appear in BOTH rule #1 and #3."""
    _, m = generate()
    multi = m.groupby("customer_id")["expected_rule"].apply(set)
    overlapped = multi[multi.map(lambda s: {1, 3} <= s)]
    assert len(overlapped) == config.TRACKED_OVERLAP


def test_deterministic_across_runs():
    t1, m1 = generate()
    t2, m2 = generate()
    assert _digest(t1, m1) == _digest(t2, m2)


def test_duplicate_and_dishonour_injections_present():
    tables, m = generate()
    invoices = tables["invoices"]
    assert invoices.invoice_id.str.contains("_DUP").sum() == 6
    entries = tables["ledger_entries"]
    assert (entries.status == "dishonoured").sum() == 4
    # each dishonoured customer has an allocated credit covering the invoice
    assert len(m[m.defect_family == "dishonour"]) == 4
