"""
reconciler.py – Gold Card extraction, subtraction maths, and output routing.

Responsibilities (Steps C and E of the pipeline)
--------------------------------------------------
Step C – Dynamic Gold Card extraction
    • Detect gold card codes by known prefixes.
    • Extract the person's name from the code suffix.
    • Append new, previously unseen cards to Discounts/Gold-Cards.csv.
    • Route these rows to Complimentary-Sales.csv with the person's name.

Step E – The Reconciliation & Subtraction Engine
    Route each redeemable row to the correct output bucket:

        WASTAGE code          →  Wastage.csv
        100 % comp / gold card →  Complimentary-Sales.csv
        Partial discount       →  Discounted-Sales.csv

    Then calculate Normal Sales:
        Normal Qty = Master Sales Qty – Σ(Redeemable Qty for same Product/Modifier)

    If Normal Qty < 0 the deficit is logged via Scripts/logger.py and the row
    is clamped to zero (never written with a negative quantity).

Output column schemas
----------------------
Normal-Sales.csv:
    Category, Modifiers, Product, Mixer, Qty,
    Item Value, Modifier Value, Gross Product Sales, Cost Price, Gross Profit

Discounted-Sales.csv:
    Category, Product, Modifier, Mixer, Qty,
    Discount_Code, Discount_Percentage, Discount_Amount_IncVAT

Complimentary-Sales.csv:
    Category, Product, Modifier, Mixer, Qty,
    Discount_Code, Discount_Percentage, Person

Wastage.csv:
    Category, Product, Modifier, Mixer, Qty
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Optional

from .logger import PipelineLogger
from .normalizer import clean_numeric_value, format_numeric_value

# ---------------------------------------------------------------------------
# Gold Card configuration
# ---------------------------------------------------------------------------

# Ordered longest-first to avoid prefix collisions
GOLD_CARD_PREFIXES: list[str] = ["GKGOLDCARD", "GKGOLDCRD", "GKGOLDSM"]

# WASTAGE sentinel value
WASTAGE_CODE: str = "WASTAGE"

# ---------------------------------------------------------------------------
# Gold Card helpers
# ---------------------------------------------------------------------------


def is_gold_card(code: str) -> bool:
    """Return True if *code* starts with any known Gold Card prefix."""
    code_upper = code.upper()
    return any(code_upper.startswith(prefix) for prefix in GOLD_CARD_PREFIXES)


def extract_person_name(code: str) -> str:
    """
    Extract a person's name from a Gold Card code.

    Examples
    --------
    ``GKGOLDCRDIAN``  → ``"Ian"``
    ``GKGOLDSMJOHN``  → ``"John"``
    ``GKGOLDCARDMARY`` → ``"Mary"``
    """
    code_upper = code.upper()
    for prefix in GOLD_CARD_PREFIXES:
        if code_upper.startswith(prefix):
            suffix = code_upper[len(prefix):]
            return suffix.title() if suffix else "(Unknown)"
    return "(Unknown)"


def load_gold_cards(path: Path | str) -> set[str]:
    """
    Load the set of previously known Gold Card codes from Discounts/Gold-Cards.csv.

    Returns
    -------
    set[str]
        Upper-cased card codes.
    """
    path = Path(path)
    if not path.exists():
        return set()

    known: set[str] = set()
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            code = row.get("Code", "").strip().upper()
            if code:
                known.add(code)
    return known


def update_gold_cards(
    gold_cards_path: Path | str,
    code: str,
    person_name: str,
    card_type: str = "Gold Card",
) -> None:
    """
    Append a new Gold Card entry to Discounts/Gold-Cards.csv.

    Parameters
    ----------
    gold_cards_path:
        Path to the Gold-Cards.csv file.
    code:
        The upper-cased card code (e.g. ``"GKGOLDCRDIAN"``).
    person_name:
        Extracted person name (e.g. ``"Ian"``).
    card_type:
        Optional type label (default ``"Gold Card"``).
    """
    path = Path(gold_cards_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    file_exists = path.exists()
    with open(path, "a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["Code", "Person", "Type"])
        if not file_exists or path.stat().st_size == 0:
            writer.writeheader()
        writer.writerow(
            {"Code": code.upper(), "Person": person_name, "Type": card_type})


# ---------------------------------------------------------------------------
# Row builders for each output bucket
# ---------------------------------------------------------------------------

def _discounted_row(row: dict) -> dict:
    return {
        "Category": row.get("Category", ""),
        "Product": row["Product"],
        "Modifier": row.get("Modifier", ""),
        "Mixer": "",          # Redeemables carry no mixer info
        "Qty": row["Qty"],
        "Discount_Code": row["Code"],
        "Discount_Percentage": row.get("Discount_Percentage", ""),
        "Discount_Amount_IncVAT": round(row.get("Discount_Amount_IncVAT", 0.0), 2),
    }


def _comp_row(row: dict, person_name: str) -> dict:
    return {
        "Category": row.get("Category", ""),
        "Product": row["Product"],
        "Modifier": row.get("Modifier", ""),
        "Mixer": "",
        "Qty": row["Qty"],
        "Discount_Code": row["Code"],
        "Discount_Percentage": row.get("Discount_Percentage", 100.0),
        "Person": person_name,
    }


def _wastage_row(row: dict) -> dict:
    return {
        "Category": row.get("Category", ""),
        "Product": row["Product"],
        "Modifier": row.get("Modifier", ""),
        "Mixer": "",
        "Qty": row["Qty"],
    }


# ---------------------------------------------------------------------------
# Normal-sales subtraction logic
# ---------------------------------------------------------------------------

def _scale_sales_row(row: dict, new_qty: int) -> dict:
    """
    Return a copy of a Master Sales row with Qty set to *new_qty* and all
    financial fields scaled proportionally by (new_qty / original_qty).
    """
    original_qty = int(clean_numeric_value(row.get("Qty", "1"))) or 1
    scale = new_qty / original_qty

    result = dict(row)
    result["Qty"] = str(new_qty)

    for field in ("Item Value", "Modifier Value", "Gross Product Sales",
                  "Cost Price", "Gross Profit"):
        original_val = clean_numeric_value(row.get(field, "0"))
        result[field] = format_numeric_value(original_val * scale)

    return result


# ---------------------------------------------------------------------------
# Main reconciliation entry-point
# ---------------------------------------------------------------------------

def reconcile(
    master_rows: list[dict],
    enriched_redeemables: list[dict],
    gold_cards_path: Path | str,
    logger: Optional[PipelineLogger] = None,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Split sales data into four output buckets.

    Parameters
    ----------
    master_rows:
        Cleaned and normalised Master Sales rows (from normalizer).
    enriched_redeemables:
        Redeemable rows after modifier deduction (from modifier_engine).
    gold_cards_path:
        Path to Discounts/Gold-Cards.csv (for new card discovery).
    logger:
        Optional PipelineLogger.

    Returns
    -------
    tuple[list[dict], list[dict], list[dict], list[dict]]
        ``(normal_sales, discounted_sales, complimentary_sales, wastage)``
    """
    gold_cards_path = Path(gold_cards_path)
    known_gold_cards = load_gold_cards(gold_cards_path)

    normal_sales: list[dict] = []
    discounted_sales: list[dict] = []
    complimentary_sales: list[dict] = []
    wastage: list[dict] = []

    # Subtraction tracker: {(product, modifier): total_qty_to_subtract}
    redeemable_qty_by_key: dict[tuple[str, str], int] = defaultdict(int)

    # ------------------------------------------------------------------ #
    # Step 1 – Route each redeemable row                                  #
    # ------------------------------------------------------------------ #
    for row in enriched_redeemables:
        code = row["Code"]
        product = row["Product"]
        modifier = row.get("Modifier", "")
        qty = int(row.get("Qty", 0))
        pct = float(row.get("Discount_Percentage", 0.0))

        if code == WASTAGE_CODE:
            wastage.append(_wastage_row(row))
            # Wastage is still tracked for subtraction from master
            redeemable_qty_by_key[(product, modifier)] += qty

        elif is_gold_card(code):
            person = extract_person_name(code)
            # Register new gold cards
            if code not in known_gold_cards:
                update_gold_cards(gold_cards_path, code, person)
                known_gold_cards.add(code)
                msg = f"New Gold Card registered: {code} → {person}"
                if logger:
                    logger.info(msg)
                else:
                    print(f"  ✓ {msg}")
            complimentary_sales.append(_comp_row(row, person))
            redeemable_qty_by_key[(product, modifier)] += qty

        elif pct >= 100.0:
            # 100 % discount that is NOT a gold card (e.g. staff comp)
            complimentary_sales.append(_comp_row(row, "(Comp)"))
            redeemable_qty_by_key[(product, modifier)] += qty

        else:
            # Partial discount
            discounted_sales.append(_discounted_row(row))
            redeemable_qty_by_key[(product, modifier)] += qty

    # ------------------------------------------------------------------ #
    # Step 2 – Calculate Normal Sales                                     #
    # ------------------------------------------------------------------ #
    # Group master rows by (Product, Modifier) – key for subtraction
    master_groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in master_rows:
        key = (row["Product"], row["Modifiers"])
        master_groups[key].append(row)

    # Any redeemable keys that have NO matching master entry get logged
    for (product, modifier), redeemable_qty in redeemable_qty_by_key.items():
        if (product, modifier) not in master_groups:
            msg = (
                f"Redeemable key ({product} / {modifier}) has no matching "
                f"Master Sales entry. {redeemable_qty} units have no base to "
                f"subtract from."
            )
            if logger:
                logger.warning(msg)

    for (product, modifier), rows in master_groups.items():
        # Total master qty for this product/modifier (summed across all mixers)
        total_master_qty = sum(
            int(clean_numeric_value(r.get("Qty", "0"))) for r in rows
        )
        redeemable_qty = redeemable_qty_by_key.get((product, modifier), 0)
        normal_qty = total_master_qty - redeemable_qty

        if normal_qty < 0:
            msg = (
                f"Negative Normal Sales detected: {product} / {modifier} – "
                f"Master Qty={total_master_qty}, Redeemables Qty={redeemable_qty}. "
                f"Normal Qty clamped to 0."
            )
            if logger:
                logger.warning(msg)
            else:
                print(f"  ⚠  {msg}")
            normal_qty = 0

        if normal_qty == 0:
            continue

        # Distribute normal_qty proportionally across mixer variants
        if len(rows) == 1:
            normal_sales.append(_scale_sales_row(rows[0], normal_qty))
        else:
            allocated = 0
            for i, row in enumerate(rows):
                mixer_qty = int(clean_numeric_value(row.get("Qty", "0")))
                if i == len(rows) - 1:
                    # Last slice gets the remainder to avoid rounding drift
                    proportional = normal_qty - allocated
                else:
                    proportional = round(
                        normal_qty * mixer_qty / total_master_qty)
                    allocated += proportional

                if proportional > 0:
                    normal_sales.append(_scale_sales_row(row, proportional))

    # ------------------------------------------------------------------ #
    # Step 3 – Log summary                                                #
    # ------------------------------------------------------------------ #
    if logger:
        logger.info(
            f"Reconciliation complete – "
            f"Normal={len(normal_sales)}, "
            f"Discounted={len(discounted_sales)}, "
            f"Complimentary={len(complimentary_sales)}, "
            f"Wastage={len(wastage)} rows."
        )

    return normal_sales, discounted_sales, complimentary_sales, wastage
