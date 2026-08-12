"""Deterministic synthetic-data generator (plain Python + pandas, NOT PySpark).

Generates the Bronze source tables plus the ground-truth manifest with **exact
counted injection** (spec §3, §11). Runs in plain single-threaded Python using an
explicitly instantiated ``random.Random(SEED)`` — never the global ``random`` module
and never PySpark — so the output is byte-stable across hosts (Spark's ``F.rand`` is
only deterministic given fixed partitioning, which varies with core count).

The clean majority of accounts is fully self-consistent: every invoice is paid by a
settled payment allocated to it, so the naive notice rule fires nothing and no rule
flags anything. Defects are then planted by a separate counted pass and recorded in
the manifest, so validation has exact ground truth.
"""
from __future__ import annotations

import calendar
import random
from datetime import date, timedelta
from typing import Iterable

import pandas as pd

from . import config

PLANS = [
    ("P1", 59.00),
    ("P2", 75.00),
    ("P3", 89.00),
    ("P4", 99.00),
    ("P5", 129.00),
    ("P6", 149.00),
]
PLAN_WEIGHTS = [6, 5, 4, 3, 2, 1]  # cheaper plans are more common

MONTHS_PER_CUSTOMER = 24


def add_months(d: date, n: int) -> date:
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


class Generator:
    """Builds the Bronze tables and manifest. Call ``tables()`` and ``manifest()``."""

    def __init__(self, seed: int = config.SEED, rng: random.Random | None = None):
        self.seed = seed
        self.rng = rng or random.Random(seed)

        self.customers: list[dict] = []
        self.entries: list[dict] = []       # ledger_entries
        self.invoices: list[dict] = []
        self.allocations: list[dict] = []
        self.manifest: list[dict] = []

        # Customer -> list of invoices, kept in period order for gap detection.
        self._cust_invoices: dict[str, list[dict]] = {}

    # -- build helpers ----------------------------------------------------------

    def _generate_clean_data(self) -> None:
        for i in range(config.POPULATION):
            cust_id = f"C{i:05d}"
            plan_id, fee = self._pick_plan()
            signup = date(2023, 1, 1) + timedelta(days=self.rng.randint(0, 150))
            status = self.rng.choices(
                ["active", "cancelled", "suspended"], weights=[9, 1, 0.5], k=1
            )[0]
            self.customers.append(
                {
                    "customer_id": cust_id,
                    "plan_id": plan_id,
                    "signup_date": signup.isoformat(),
                    "status": status,
                }
            )
            self._emit_subscription(cust_id, fee, signup)

    def _pick_plan(self) -> tuple[str, float]:
        idx = self.rng.choices(range(len(PLANS)), weights=PLAN_WEIGHTS, k=1)[0]
        return PLANS[idx]

    def _emit_subscription(self, cust_id: str, fee: float, signup: date) -> None:
        invoices = []
        for m in range(MONTHS_PER_CUSTOMER):
            period_start = add_months(signup, m)
            period_end = add_months(signup, m + 1) - timedelta(days=1)
            issued_date = period_start
            due_date = period_end + timedelta(days=5)
            inv_id = f"INV_{cust_id}_{m:02d}"
            inv = {
                "invoice_id": inv_id,
                "customer_id": cust_id,
                "statement_period_start": period_start.isoformat(),
                "statement_period_end": period_end.isoformat(),
                "due_date": due_date.isoformat(),
                "amount_due": fee,
                "issued_date": issued_date.isoformat(),
            }
            self.invoices.append(inv)
            invoices.append(inv)

            # invoice_debit (negative) on issue.
            self.entries.append(
                {
                    "entry_id": f"ENT_{inv_id}_D",
                    "customer_id": cust_id,
                    "type": "invoice_debit",
                    "amount": -fee,
                    "effective_date": issued_date.isoformat(),
                    "source": "invoice",
                    "status": "settled",
                }
            )
            # settled payment just before due date.
            pay_date = due_date - timedelta(days=1)
            pay_entry = f"ENT_{inv_id}_P"
            self.entries.append(
                {
                    "entry_id": pay_entry,
                    "customer_id": cust_id,
                    "type": "payment",
                    "amount": fee,
                    "effective_date": pay_date.isoformat(),
                    "source": "direct_debit",
                    "status": "settled",
                }
            )
            self.allocations.append(
                {
                    "allocation_id": f"ALL_{inv_id}",
                    "entry_id": pay_entry,
                    "invoice_id": inv_id,
                    "amount_allocated": fee,
                    "allocated_date": pay_date.isoformat(),
                }
            )
        self._cust_invoices[cust_id] = invoices

    # -- injection pass ---------------------------------------------------------

    def _inject(self) -> None:
        self._inject_allocation()      # rule #1 wrongful notice  (37)
        self._inject_stale_credit()    # rule #2 orphaned credit  (12)
        self._inject_statement_gap()   # rule #3                   (9)
        self._inject_duplicate()       # rule #5                   (6)
        self._inject_dishonour()       # rule #4                   (4)

    def _pick_customers(self, n: int, exclude: set[str]) -> list[str]:
        """Return n distinct customer_ids not in ``exclude`` (by period order)."""
        avail = [c for c in self._cust_invoices if c not in exclude]
        self.rng.shuffle(avail)
        chosen = avail[:n]
        assert len(chosen) == n, "not enough distinct customers for injection"
        return chosen

    def _mid_subscription_invoice(self, cust_id: str, offset: int = 10) -> dict:
        """Pick a mid-subscription invoice (has a later sibling for misallocation)."""
        invs = self._cust_invoices[cust_id]
        return invs[offset]

    def _inject_allocation(self) -> None:
        """Rule #1 wrongful notice: an invoice left under-allocated while the account
        is in credit.

        Mechanics:
        1. Misallocate invoice I's payment to a later invoice J, leaving I with no
           allocation (so the naive notice fires at due_date + grace_days).
        2. Add an unallocated-size credit C **allocated to J** (so it is not also an
           orphaned credit under rule #2), dated just before the evaluation instant and
           sized so the running balance at sent_date is clearly >= 0.

        Result: the notice fires (I under-allocated) but the account is in credit,
        which is exactly the wrongful-arse arrears signal rule #1 detects. The payment
        and credit are both fully allocated, keeping rule #1 disjoint from rule #2.
        """
        chosen = self._pick_customers(config.INJECTION_COUNTS["allocation"], set())
        self._allocation_customers = set(chosen)          # membership only
        self._allocation_customer_list = list(chosen)     # deterministic order
        for k, cust_id in enumerate(chosen):
            invs = self._cust_invoices[cust_id]
            i_idx = 10
            inv = invs[i_idx]
            j_idx = i_idx + 1
            if j_idx >= len(invs):
                j_idx = i_idx - 1
            later = invs[j_idx]
            sent = date.fromisoformat(inv["due_date"]) + timedelta(
                days=config.GRACE_DAYS
            )

            pay_entry = f"ENT_{inv['invoice_id']}_P"
            alloc_id = f"ALL_{inv['invoice_id']}"
            # 1) repoint I's payment allocation to the later invoice.
            for a in self.allocations:
                if a["allocation_id"] == alloc_id:
                    a["invoice_id"] = later["invoice_id"]
                    break
            # Enforce the severity-basis invariant used by rule #1: the misallocated
            # invoice has NO allocation at sent, so the detector's shortfall
            # (amount_due - allocated_total_at_sent) equals amount_due and thus the
            # manifest's expected_severity stays consistent with gold's escalation.
            assert not any(
                a.get("invoice_id") == inv["invoice_id"] for a in self.allocations
            ), f"allocation defect must leave {inv['invoice_id']} fully unallocated"

            # 2) sized credit, allocated to J so it is never an orphaned credit.
            amount, eff = self._sized_credit(cust_id, sent)
            cred_entry = f"ENT_{cust_id}_ALLOC{k}"
            self.entries.append(
                {
                    "entry_id": cred_entry,
                    "customer_id": cust_id,
                    "type": "credit",
                    "amount": amount,
                    "effective_date": eff.isoformat(),
                    "source": "refund",
                    "status": "settled",
                }
            )
            self.allocations.append(
                {
                    "allocation_id": f"ALL_{cust_id}_ALLOC{k}",
                    "entry_id": cred_entry,
                    "invoice_id": later["invoice_id"],
                    "amount_allocated": amount,
                    "allocated_date": eff.isoformat(),
                }
            )

            self._manifest_row(
                defect_id=f"D_allocation_{k:03d}",
                family="allocation",
                customer=cust_id,
                invoice=inv["invoice_id"],
                rule=1,
                amount=inv["amount_due"],
            )

    def _inject_stale_credit(self) -> None:
        """Rule #2 orphaned credit: an unallocated credit unapplied > ORPHAN_DAYS."""
        used = set(getattr(self, "_allocation_customers", set()))
        chosen = self._pick_customers(config.INJECTION_COUNTS["stale_unswept_credit"], used)
        asof = self._asof()
        for k, cust_id in enumerate(chosen):
            amount = round(self.rng.uniform(15.0, 60.0), 2)
            eff = asof - timedelta(days=config.ORPHAN_DAYS * 2)  # > orphan window
            self.entries.append(
                {
                    "entry_id": f"ENT_{cust_id}_STALE{k}",
                    "customer_id": cust_id,
                    "type": "credit",
                    "amount": amount,
                    "effective_date": eff.isoformat(),
                    "source": "goodwill",
                    "status": "settled",
                }
            )
            self._manifest_row(
                defect_id=f"D_stale_{k:03d}",
                family="stale_unswept_credit",
                customer=cust_id,
                invoice=None,
                rule=2,
                amount=amount,
            )

    def _inject_statement_gap(self) -> None:
        """Rule #3 statement gap: shift one invoice's period forward -> a real gap.

        ~TRACKED_OVERLAP gap customers are drawn from the allocation-defect set so a
        few customers are hit by BOTH rule #1 and rule #3 (exercises rule independence).
        """
        # Overlap: draw TRACKED_OVERLAP gap customers deterministically from the
        # ordered allocation-customer list (never from a set: set iteration order
        # depends on Python's per-process hash seed and would break reproducibility).
        overlap = self._allocation_customer_list[: config.TRACKED_OVERLAP]
        new_needed = config.INJECTION_COUNTS["statement_gap"] - len(overlap)
        new_customers = self._pick_customers(new_needed, set(self._allocation_customers))
        chosen = list(overlap) + new_customers
        self._gap_customers = set(chosen)

        for k, cust_id in enumerate(chosen):
            invs = self._cust_invoices[cust_id]
            idx = 12
            gone = invs[idx]
            next_inv = invs[idx + 1] if idx + 1 < len(invs) else invs[idx - 1]
            gone_id = gone["invoice_id"]
            shift = timedelta(days=30)

            # Remove the missing statement entirely (invoice + debit + payment + alloc).
            self.invoices = [i for i in self.invoices if i["invoice_id"] != gone_id]
            removed = {f"ENT_{gone_id}_D", f"ENT_{gone_id}_P"}
            self.entries = [e for e in self.entries if e["entry_id"] not in removed]
            self.allocations = [
                a for a in self.allocations if a.get("invoice_id") != gone_id
            ]
            # Shift every LATER statement forward by G, so a real period gap of ~G days
            # opens before them with no overlap into the next month. Keep the ledger
            # and allocations consistent with the shifted invoice dates (debit at new
            # issued_date, payment at new due_date-1, allocation at the payment date)
            # so no rule that joins invoice dates to ledger dates sees a drift.
            for inv in invs[idx + 1:]:
                inv_id = inv["invoice_id"]
                ps = date.fromisoformat(inv["statement_period_start"]) + shift
                pe = date.fromisoformat(inv["statement_period_end"]) + shift
                new_due = pe + timedelta(days=5)
                new_pay = new_due - timedelta(days=1)
                inv["statement_period_start"] = ps.isoformat()
                inv["statement_period_end"] = pe.isoformat()
                inv["issued_date"] = ps.isoformat()
                inv["due_date"] = new_due.isoformat()
                for e in self.entries:
                    if e["entry_id"] == f"ENT_{inv_id}_D":
                        e["effective_date"] = ps.isoformat()
                    elif e["entry_id"] == f"ENT_{inv_id}_P":
                        e["effective_date"] = new_pay.isoformat()
                for a in self.allocations:
                    if a.get("invoice_id") == inv_id:
                        a["allocated_date"] = new_pay.isoformat()
            self._cust_invoices[cust_id] = [
                i for i in invs if i["invoice_id"] != gone_id
            ]

            self._manifest_row(
                defect_id=f"D_gap_{k:03d}",
                family="statement_gap",
                customer=cust_id,
                invoice=next_inv["invoice_id"],
                rule=3,
                amount=0.0,
            )

    def _inject_duplicate(self) -> None:
        """Rule #5 duplicate invoice: overlapping period, near-identical amount."""
        used = (
            set(getattr(self, "_allocation_customers", set()))
            | set(getattr(self, "_gap_customers", set()))
        )
        chosen = self._pick_customers(config.INJECTION_COUNTS["duplicate_invoice"], used)
        self._dup_customers = set(chosen)
        for k, cust_id in enumerate(chosen):
            invs = self._cust_invoices[cust_id]
            src = invs[10]
            dup_id = f"INV_{cust_id}_DUP{k}"
            dup = {
                "invoice_id": dup_id,
                "customer_id": cust_id,
                "statement_period_start": src["statement_period_start"],
                "statement_period_end": src["statement_period_end"],
                "due_date": src["due_date"],
                "amount_due": src["amount_due"],
                "issued_date": src["issued_date"],
            }
            self.invoices.append(dup)
            # A settled payment allocated to the duplicate keeps it fully allocated,
            # so it produces no notice and no orphaned credit.
            pay_date = date.fromisoformat(src["due_date"]) - timedelta(days=1)
            pay_entry = f"ENT_{dup_id}_P"
            self.entries.append(
                {
                    "entry_id": pay_entry,
                    "customer_id": cust_id,
                    "type": "payment",
                    "amount": src["amount_due"],
                    "effective_date": pay_date.isoformat(),
                    "source": "direct_debit",
                    "status": "settled",
                }
            )
            self.allocations.append(
                {
                    "allocation_id": f"ALL_{dup_id}",
                    "entry_id": pay_entry,
                    "invoice_id": dup_id,
                    "amount_allocated": src["amount_due"],
                    "allocated_date": pay_date.isoformat(),
                }
            )
            self._manifest_row(
                defect_id=f"D_dup_{k:03d}",
                family="duplicate_invoice",
                customer=cust_id,
                invoice=dup_id,
                rule=5,
                amount=src["amount_due"],
            )

    def _inject_dishonour(self) -> None:
        """Rule #4 dishonour-without-cause: a failed debit while account in credit.

        Cover invoice I with an allocated credit C sized so the pre-debit balance is
        >= 0 (the account is in credit when the debit is attempted), and mark I's own
        payment as dishonoured (excluded from balance, no allocation). Because the
        account was in credit, the dishonour had no billing cause.
        """
        used = (
            set(getattr(self, "_allocation_customers", set()))
            | set(getattr(self, "_gap_customers", set()))
            | set(getattr(self, "_dup_customers", set()))
        )
        chosen = self._pick_customers(config.INJECTION_COUNTS["dishonour"], used)
        for k, cust_id in enumerate(chosen):
            invs = self._cust_invoices[cust_id]
            inv = invs[10]
            fee = inv["amount_due"]
            pay_entry = f"ENT_{inv['invoice_id']}_P"
            pay_date = date.fromisoformat(inv["due_date"]) - timedelta(days=1)

            # 1) mark the normal payment dishonoured first (so it drops out of balance)
            for e in self.entries:
                if e["entry_id"] == pay_entry:
                    e["status"] = "dishonoured"
                    break
            self.allocations = [
                a for a in self.allocations if a.get("entry_id") != pay_entry
            ]

            # 2) sized credit, allocated to I, so pre-debit balance is clearly >= 0
            amount, eff = self._sized_credit(cust_id, pay_date)
            cred_entry = f"ENT_{cust_id}_DISHC{k}"
            self.entries.append(
                {
                    "entry_id": cred_entry,
                    "customer_id": cust_id,
                    "type": "credit",
                    "amount": amount,
                    "effective_date": eff.isoformat(),
                    "source": "goodwill",
                    "status": "settled",
                }
            )
            self.allocations.append(
                {
                    "allocation_id": f"ALL_{cust_id}_DISH{k}",
                    "entry_id": cred_entry,
                    "invoice_id": inv["invoice_id"],
                    "amount_allocated": amount,
                    "allocated_date": eff.isoformat(),
                }
            )
            self._manifest_row(
                defect_id=f"D_dish_{k:03d}",
                family="dishonour",
                customer=cust_id,
                invoice=inv["invoice_id"],
                rule=4,
                amount=fee,
            )

    def _manifest_row(self, defect_id, family, customer, invoice, rule, amount):
        self.manifest.append(
            {
                "defect_id": defect_id,
                "defect_family": family,
                "customer_id": customer,
                "affected_invoice_id": invoice or "",
                "expected_rule": rule,
                "expected_severity": self._expected_severity(rule, amount),
                "injected_amount": amount,
            }
        )

    def _expected_severity(self, rule: int, amount: float) -> str:
        base = config.SEVERITY_BASE[rule]
        if rule == 1 and amount > config.NOTICE_ESCALATION_THRESHOLD:
            return "critical"
        if rule == 5 and amount > config.DUPLICATE_ESCALATION_THRESHOLD:
            return "high"
        return base

    def _asof(self) -> date:
        return max(date.fromisoformat(i["due_date"]) for i in self.invoices)

    def _balance_at(self, cust_id: str, d: date) -> float:
        """Running ledger balance of ``cust_id`` at ``d`` (dishonoured excluded)."""
        return sum(
            e["amount"]
            for e in self.entries
            if e["customer_id"] == cust_id
            and e["status"] != "dishonoured"
            and date.fromisoformat(e["effective_date"]) <= d
        )

    def _sized_credit(self, cust_id: str, asof_date: date) -> tuple[float, date]:
        """Return (amount, effective_date) for a credit that puts the account clearly
        in credit at ``asof_date`` (account is otherwise perpetually ~one month in
        arrears because invoices are billed before their payments land)."""
        outstanding = self._balance_at(cust_id, asof_date)
        amount = max(0.0, -outstanding) + 25.0  # margin so balance is clearly >= 0
        return round(amount, 2), asof_date - timedelta(days=1)

    # -- public ---------------------------------------------------------------

    def run(self) -> "Generator":
        self._generate_clean_data()
        self._inject()
        return self

    def tables(self) -> dict[str, pd.DataFrame]:
        cols = {
            "customers": ["customer_id", "plan_id", "signup_date", "status"],
            "ledger_entries": [
                "entry_id",
                "customer_id",
                "type",
                "amount",
                "effective_date",
                "source",
                "status",
            ],
            "invoices": [
                "invoice_id",
                "customer_id",
                "statement_period_start",
                "statement_period_end",
                "due_date",
                "amount_due",
                "issued_date",
            ],
            "invoice_allocations": [
                "allocation_id",
                "entry_id",
                "invoice_id",
                "amount_allocated",
                "allocated_date",
            ],
        }
        out = {
            "customers": pd.DataFrame(self.customers, columns=cols["customers"]),
            "ledger_entries": pd.DataFrame(self.entries, columns=cols["ledger_entries"]),
            "invoices": pd.DataFrame(self.invoices, columns=cols["invoices"]),
            "invoice_allocations": pd.DataFrame(
                self.allocations, columns=cols["invoice_allocations"]
            ),
        }
        return out

    def manifest_df(self) -> pd.DataFrame:
        mcols = [
            "defect_id",
            "defect_family",
            "customer_id",
            "affected_invoice_id",
            "expected_rule",
            "expected_severity",
            "injected_amount",
        ]
        return pd.DataFrame(self.manifest, columns=mcols)


def generate() -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """Convenience: run the generator, return (tables, manifest)."""
    g = Generator().run()
    return g.tables(), g.manifest_df()


def write_fixtures(out_dir: str = config.FIXTURE_DIR, seed: int = config.SEED) -> None:
    """Write the Bronze CSV fixtures + manifest to ``out_dir`` (deterministic)."""
    import os

    tables, manifest = generate()
    os.makedirs(out_dir, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False, lineterminator="\n")
    manifest.to_csv(
        os.path.join(out_dir, config.MANIFEST_PATH), index=False, lineterminator="\n"
    )
