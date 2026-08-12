"""Layer 4: dashboard determinism — byte-stable output, fixed div order, inline JS."""
import pandas as pd
import pytest

from src import dashboard
from src.dashboard import DIV_IDS, render

SAMPLE = pd.DataFrame(
    [
        {"customer_id": "C00001", "rule_id": 1, "invoice_id": "INV_C00001_10",
         "amount": 59.0, "severity": "high", "flag_date": "2024-04-10",
         "explanation": "Wrongful arrears notice: invoice INV_C00001_10."},
        {"customer_id": "C00002", "rule_id": 3, "invoice_id": "INV_C00002_13",
         "amount": 0.0, "severity": "medium", "flag_date": "2024-05-10",
         "explanation": "Statement continuity break."},
        {"customer_id": "C00003", "rule_id": 2, "invoice_id": None,
         "amount": 40.0, "severity": "warning", "flag_date": "2024-11-01",
         "explanation": "Orphaned credit e1."},
    ]
)


def test_render_is_byte_deterministic(tmp_path):
    a = tmp_path / "a.html"
    b = tmp_path / "b.html"
    render(SAMPLE, str(a))
    render(SAMPLE, str(b))
    assert a.read_bytes() == b.read_bytes()


def test_fixed_div_ids_in_fixed_order(tmp_path):
    out = tmp_path / "d.html"
    render(SAMPLE, str(out))
    html = out.read_text(encoding="utf-8")
    positions = [html.index(f'id="{d}"') for d in DIV_IDS]
    assert positions == sorted(positions), "div ids out of order"


def test_inline_plotly_js_not_cdn(tmp_path):
    out = tmp_path / "d.html"
    render(SAMPLE, str(out))
    html = out.read_text(encoding="utf-8")
    # No external plotly.js <script src> from the CDN; the bundle is embedded inline.
    assert 'src="https://cdn.plot.ly/' not in html
    # inline plotly.js is embedded (the minified bundle is present in-page)
    assert html.count("plotly.js") >= 1


def test_no_dynamic_timestamp(tmp_path):
    """Generated HTML must not embed a wall-clock timestamp (view-time never runs)."""
    out = tmp_path / "d.html"
    render(SAMPLE, str(out))
    html = out.read_text(encoding="utf-8")
    assert "2026-" not in html  # any fixed current-year would fail once baked
