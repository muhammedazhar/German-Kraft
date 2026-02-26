"""
modifier_engine.py – Calculates base prices and deduces missing modifiers.

How it works (Step D of the pipeline)
---------------------------------------
The Redeemables CSV contains only `(Code, Product, Qty, Discount_Amount)`.
It does NOT carry a modifier (Pint / Half / Mass / etc.).  We reverse-engineer
the modifier using two data sources:

    1.  Discounts/Discounts.csv  →  Code → Discount Percentage
    2.  Menus/<selected>.csv     →  Product + Price → Modifier

Algorithm per redeemable row
------------------------------
    per_item_discount = Discount_Amount_IncVAT / Qty
    percentage        = discounts[UPPERCASE_CODE]      # e.g. 20 for 20 %
    base_price        = per_item_discount / (percentage / 100)
    # For 100 % comps  base_price = per_item_discount  (already the full price)
    modifier          = menu_lookup(product, base_price, abs_tol=0.05)

Safeguards
----------
• Rounding errors are handled via math.isclose() with abs_tol=0.05 (5p).
• The menu lookup is strictly scoped by product name (no cross-category clashes).
• Duplicate entries in the menu (same product appears in multiple sections) are
  de-duplicated by (modifier_normalised, effective_price) key.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Optional

from Scripts.logger import PipelineLogger
from Scripts.normalizer import MODIFIER_NORMALIZATION, CATEGORY_MAPPING

# Tolerance for floating-point price matching (in GBP)
PRICE_MATCH_ABS_TOL: float = 0.05

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

# Each menu entry: (modifier_normalised, effective_price, raw_category)
_MenuEntry = tuple[str, float, str]
# Full menu lookup: {product_name_lower: [_MenuEntry, ...]}
MenuData = dict[str, list[_MenuEntry]]
# Discounts lookup: {uppercase_code: percentage_float}
DiscountsData = dict[str, float]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_modifier_name(raw: str) -> str:
    """
    Convert a menu modifier name (ALL CAPS) to the normalised form used in
    the rest of the pipeline (Title Case + MODIFIER_NORMALIZATION table).
    """
    if not raw:
        return ""
    title = raw.strip().title()
    return MODIFIER_NORMALIZATION.get(title, title)


def _normalise_category(raw: str) -> str:
    """Map a raw menu category name through CATEGORY_MAPPING (case-insensitive)."""
    stripped = raw.strip()
    if stripped in CATEGORY_MAPPING:
        return CATEGORY_MAPPING[stripped]
    lower = stripped.lower()
    for key, val in CATEGORY_MAPPING.items():
        if key.lower() == lower:
            return val
    return stripped


# ---------------------------------------------------------------------------
# Public – load_menu
# ---------------------------------------------------------------------------

def load_menu(menu_path: Path | str) -> MenuData:
    """
    Parse the selected Menu CSV into a fast lookup dictionary.

    The effective price for each modifier item is:
        effective_price = float(price) + float(modifier_value)

    Duplicate (product, modifier, price) triples (arising from the same product
    appearing in multiple menu sections) are silently de-duplicated.

    Returns
    -------
    MenuData
        { product_name_lower: [(modifier_normalised, effective_price, category), ...] }
    """
    path = Path(menu_path)
    if not path.exists():
        raise FileNotFoundError(f"Menu file not found: {path}")

    # Use a set per product to avoid duplicates
    raw: dict[str, set[tuple[str, float, str]]] = {}

    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            product_raw = row.get("product_name", "").strip()
            if not product_raw:
                continue

            price_str = row.get("price", "0").strip()
            mod_name_raw = row.get("modifier_item_name", "").strip()
            mod_val_str = row.get("modifier_value", "0").strip()
            category_raw = row.get("product_category_name", "").strip()

            try:
                base_price = float(price_str) if price_str else 0.0
            except ValueError:
                base_price = 0.0

            try:
                mod_val = float(mod_val_str) if mod_val_str else 0.0
            except ValueError:
                mod_val = 0.0

            effective_price = round(base_price + mod_val, 4)
            modifier_normalised = _normalise_modifier_name(mod_name_raw)
            category_normalised = _normalise_category(category_raw)

            key = product_raw.lower()
            if key not in raw:
                raw[key] = set()
            raw[key].add(
                (modifier_normalised, effective_price, category_normalised))

    # Convert sets to sorted lists (stable order for deterministic deduction)
    result: MenuData = {}
    for key, entries in raw.items():
        result[key] = sorted(entries, key=lambda e: e[1])

    return result


# ---------------------------------------------------------------------------
# Public – load_discounts
# ---------------------------------------------------------------------------

def load_discounts(discounts_path: Path | str) -> DiscountsData:
    """
    Parse Discounts/Discounts.csv into a code→percentage mapping.

    The CSV is expected to have columns: Title, Discount Code, Percentage

    Returns
    -------
    DiscountsData
        { UPPERCASE_CODE: percentage_as_float }   e.g. { "GKFANDF20": 20.0 }
    """
    path = Path(discounts_path)
    if not path.exists():
        raise FileNotFoundError(f"Discounts file not found: {path}")

    discounts: DiscountsData = {}

    with open(path, encoding="utf-8") as fh:
        # skipinitialspace=True handles CSVs exported with spaces after commas
        reader = csv.DictReader(fh, skipinitialspace=True)
        for row in reader:
            code = row.get("Discount Code", row.get(
                " Discount Code", "")).strip().upper()
            pct_raw = row.get("Percentage", row.get(
                " Percentage", "0")).strip()
            if not code:
                continue
            try:
                discounts[code] = float(pct_raw)
            except ValueError:
                discounts[code] = 0.0

    return discounts


# ---------------------------------------------------------------------------
# Public – deduce_modifier
# ---------------------------------------------------------------------------

def deduce_modifier(
    product_name: str,
    target_price: float,
    menu_data: MenuData,
    logger: Optional[PipelineLogger] = None,
) -> tuple[str, str]:
    """
    Find the modifier and category for a product at a given effective price.

    The search is strictly scoped to *product_name* to prevent false matches
    across categories (e.g., confusing a Half with a Pint if their prices
    happen to sit close together for different products).

    Parameters
    ----------
    product_name:
        The normalised product name (e.g. ``"Heinr Zwickel"``).
    target_price:
        The reverse-engineered per-item base price (GBP).
    menu_data:
        Lookup built by :func:`load_menu`.
    logger:
        Optional logger for unresolved lookups.

    Returns
    -------
    tuple[str, str]
        ``(modifier_name, category)`` – both empty strings when no match found.
    """
    lookup_key = product_name.strip().lower()

    # Exact match first
    entries = menu_data.get(lookup_key)

    # Fallback: partial match (e.g. "Schwarzbier" should also match
    # "SCHWARZBIER" even if the normalised key contains "l&g schwarzbier")
    if entries is None:
        candidates = [k for k in menu_data if lookup_key in k]
        if len(candidates) == 1:
            entries = menu_data[candidates[0]]
        elif len(candidates) > 1:
            # Prefer the key that is *exactly* the search term (no prefix)
            exact_candidates = [k for k in candidates if k == lookup_key]
            if exact_candidates:
                entries = menu_data[exact_candidates[0]]
            else:
                entries = menu_data[candidates[0]]

    if not entries:
        msg = (
            f"Menu lookup failed: product '{product_name}' not found. "
            f"Modifier will be left blank."
        )
        if logger:
            logger.warning(msg)
        return "", ""

    # Collect all candidates within price tolerance
    from Scripts.normalizer import MODIFIER_SORT_ORDER

    def _mod_priority(mod_name: str) -> int:
        """Return sort priority for a modifier name (lower = higher priority)."""
        for i, p in enumerate(MODIFIER_SORT_ORDER):
            if mod_name == p or mod_name.startswith(p):
                return i
        return len(MODIFIER_SORT_ORDER)

    candidates: list[tuple[float, int, _MenuEntry]] = []
    for modifier, eff_price, category in entries:
        if math.isclose(eff_price, target_price, abs_tol=PRICE_MATCH_ABS_TOL):
            diff = abs(eff_price - target_price)
            priority = _mod_priority(modifier)
            candidates.append(
                (diff, priority, (modifier, eff_price, category)))

    if candidates:
        # Sort by price difference first, then modifier priority for ties
        candidates.sort(key=lambda c: (c[0], c[1]))
        best_entry = candidates[0][2]
        return best_entry[0], best_entry[2]

    # No match – log a warning with useful diagnostics
    available = [f"{mod}=£{p:.2f}" for mod, p, _ in entries[:6]]
    msg = (
        f"Price match failed for '{product_name}': "
        f"target=£{target_price:.2f}, "
        f"available prices: {', '.join(available)}. "
        f"Modifier will be left blank."
    )
    if logger:
        logger.warning(msg)
    return "", ""


# ---------------------------------------------------------------------------
# Public – process_redeemable_modifiers
# ---------------------------------------------------------------------------

def process_redeemable_modifiers(
    redeemable_rows: list[dict],
    discounts: DiscountsData,
    menu_data: MenuData,
    logger: Optional[PipelineLogger] = None,
) -> list[dict]:
    """
    Enrich each redeemable row with deduced ``Modifier``, ``Category``, and
    ``Base_Price`` fields.

    For Gold Card (100 %) entries the ``discounts`` lookup will miss the code
    (since gold card codes are not in Discounts.csv).  In that case the
    per-item discount amount IS the base price.

    Parameters
    ----------
    redeemable_rows:
        Output of :func:`Scripts.normalizer.normalize_redeemables`.
    discounts:
        Output of :func:`load_discounts`.
    menu_data:
        Output of :func:`load_menu`.
    logger:
        Optional PipelineLogger.

    Returns
    -------
    list[dict]
        Each row is the original dict enriched with keys:
            Modifier, Category, Base_Price, Discount_Percentage
    """
    enriched: list[dict] = []

    for row in redeemable_rows:
        code = row["Code"]
        product = row["Product"]
        qty = row["Qty"]
        disc_inc = row["Discount_Amount_IncVAT"]

        # Per-item discount amount
        per_item = disc_inc / qty if qty else 0.0

        # Look up the discount percentage
        pct = discounts.get(code)

        if pct is None:
            # Not in Discounts.csv – treat as 100 % (gold card / comp)
            pct = 100.0
            base_price = per_item
        elif pct == 0.0:
            base_price = 0.0
        else:
            base_price = per_item / (pct / 100.0)

        base_price = round(base_price, 2)

        modifier, category = deduce_modifier(
            product, base_price, menu_data, logger=logger
        )

        enriched.append({
            **row,
            "Discount_Percentage": pct,
            "Base_Price": base_price,
            "Modifier": modifier,
            "Category": category,
        })

    if logger:
        resolved = sum(1 for r in enriched if r["Modifier"])
        logger.info(
            f"Modifier deduction: {resolved}/{len(enriched)} rows resolved."
        )

    return enriched
