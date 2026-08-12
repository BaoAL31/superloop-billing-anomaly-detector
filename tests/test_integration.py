"""Layer 3 (integration): end-to-end Gold output vs. the committed ground-truth
manifest — detection recall, severity accuracy, simulation sanity, and rule
independence. The (expensive) full-pipeline run is computed ONCE per session and
reused by all tests."""
import os

import pandas as pd
import pytest

from src import config
from src.generator import write_fixtures
from src.gold import run_gold
from src.silver import build_silver
from tests.conftest import load_fixtures


@pytest.fixture(scope="session")
def result(spark, tmp_path_factory):
    """Regenerate fixtures in a temp dir, run Silver+Gold once, return structures."""
    out = tmp_path_factory.mktemp("intg")
    write_fixtures(out_dir=str(out))
    tables = load_fixtures(spark, str(out))
    silver = build_silver(spark, tables)
    notices = silver["silver_notices"]
    flags = run_gold(silver, tables).collect()
    manifest = pd.read_csv(os.path.join(str(out), "ground_truth_manifest.csv"))
    manifest["affected_invoice_id"] = manifest["affected_invoice_id"].fillna("")

    flagged = [
        {"rule": r.rule_id, "customer": r.customer_id,
         "invoice": r.invoice_id or None, "severity": r.severity,
         "flag_date": r.flag_date}
        for r in flags
    ]
    return {
        "flagged": flagged,
        "notice_invoices": {r.invoice_id for r in notices.collect()},
        "manifest": manifest,
    }


def test_full_detection_recall(result):
    by_cust_rule = {}
    for f in result["flagged"]:
        by_cust_rule.setdefault((f["customer"], f["rule"]), []).append(f["invoice"])

    for m in result["manifest"].to_dict("records"):
        key = (m["customer_id"], int(m["expected_rule"]))
        assert key in by_cust_rule, f"missing flag for manifest {m}"
        inv = m["affected_invoice_id"]
        if inv:
            assert inv in by_cust_rule[key], f"wrong invoice for {m}"


def test_no_false_positives_precision(result):
    per_rule = {}
    for f in result["flagged"]:
        per_rule[f["rule"]] = per_rule.get(f["rule"], 0) + 1
    expected = result["manifest"]["expected_rule"].value_counts().to_dict()
    assert per_rule == expected, (per_rule, expected)


def test_common_schema_flag_date_present(result):
    """Every flagged row carries a non-empty flag_date (dashboard temporal axis)."""
    for f in result["flagged"]:
        assert f["flag_date"] is not None and str(f["flag_date"]).strip(), f
    # only orphaned credits (rule #2) have no invoice id
    assert all(f["invoice"] is not None or f["rule"] == 2 for f in result["flagged"])


def test_severity_matches_manifest(result):
    by_key = {(f["customer"], f["rule"], f["invoice"]): f["severity"] for f in result["flagged"]}
    for m in result["manifest"].to_dict("records"):
        key = (m["customer_id"], int(m["expected_rule"]), m["affected_invoice_id"] or None)
        assert by_key[key] == m["expected_severity"], (m, by_key.get(key))


def test_simulation_sanity_naive_notices_only_on_allocation_defects(result):
    """The naive notice rule fires on the clean majority ZERO times: notices are
    exactly the misallocated (allocation-defect) invoices."""
    alloc_invoices = set(
        result["manifest"].loc[result["manifest"].defect_family == "allocation",
                               "affected_invoice_id"]
    )
    assert result["notice_invoices"] == alloc_invoices


def test_rule_independence_tracked_overlap(result):
    """Customers in the tracked-overlap set are flagged by BOTH rule #1 and #3,
    proving rules fire independently on the same account without conflation."""
    rules_by_customer = {}
    for f in result["flagged"]:
        rules_by_customer.setdefault(f["customer"], set()).add(f["rule"])
    overlapped = [c for c, rules in rules_by_customer.items() if rules == {1, 3}]
    assert len(overlapped) == config.TRACKED_OVERLAP
