"""
normalizer.py – Data ingestion and cleaning for both Dines CSV exports.

Responsibilities (Step B of the pipeline)
------------------------------------------
• normalize_sales(path)        – Cleans the Master Sales CSV.
• normalize_redeemables(path)  – Cleans the Redeemables by Item CSV.

Both functions return a list[dict] ready for downstream processing.
All configuration constants (category mappings, normalization tables, etc.)
are identical to the original standalone normalizer and are maintained here
as the single source of truth.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

from .logger import PipelineLogger

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

CATEGORY_MAPPING: dict[str, str] = {
    "Draught": "Draught Beer",
    "Draught Beer": "Draught Beer",
    "Spirits (GB/GH)": "Spirits",
    "Spirits (DI/CE)": "Spirits",
    "Gin": "Gin",
    "Wines": "Wines",
    "Guest Beer / Cider": "Guest Draughts",
    "Signature Cocktails": "Signature Cocktails",
    "Classic Cocktails": "Classic Cocktails",
    "Cocktails (GB/GH)": "Cocktails",
    "Cocktails (DI/CE)": "Cocktails",
    "Non-alcoholic Cocktails": "Non-alcoholic Cocktails",
    "Spritz": "Spritz",
    "Bottled Beer / Lager": "Bottled Beers",
    "Bottled Softs": "Bottled Softs",
    "Minerals": "Minerals",
    "Shot Rack": "Shot Rack",
    "Beer & Meal Promo": "Promotions",
    "Student Deals": "Promotions",
}

WINE_SIZE_PATTERNS: dict[str, str] = {
    r"\s+250ml$": "250ml",
    r"\s+175ml$": "175ml",
    r"\s+125ml$": "125ml",
    r"\s+Btl$": "Bottle",
}

SORT_COLUMNS: list[tuple[str, str]] = [
    ("Category", "alpha"),
    ("Modifiers", "custom"),
    ("Product", "custom"),
    ("Mixer", "custom"),
    ("Qty", "numeric"),
]

MODIFIER_SORT_ORDER: list[str] = [
    "Pint", "Half", "Mass", "Shandy", "Shandy Half",
    "Top", "Top Half", "Single", "Double",
    "Bottle", "250ml", "175ml", "125ml",
]

PRODUCT_SORT_ORDER: list[str] = [
    "Heinr Zwickel", "Heidi Helles", "Siggi",
    "Lotte Weissb", "Fritz", "Schwarzbier",
]

MIXER_SORT_ORDER: list[str] = [
    "NO Mixer", "POSTMIX", "F&S", "FEVERTREE", "FRITZ",
]

MIXER_NORMALIZATION: dict[str, str] = {
    "FRANKLIN & Sons Cola": "F&S Cola",
    "FRANKLIN & Sons Ginger Ale": "F&S Ginger Ale",
    "FRANKLIN & Sons Ginger Beer": "F&S Ginger Beer",
    "FRANKLIN & Sons Indian Tonic": "F&S Indian Tonic",
    "FRANKLIN & Sons Lemonade": "F&S Lemonade",
    "FRANKLIN & Sons Soda Water": "F&S Soda Water",
    "DIET Cola": "POSTMIX Diet Cola",
    "FEVERTREE Aromatic": "Fevertree Aromatic Tonic",
    "FEVERTREE Elderflower": "Fevertree Elderflower Tonic",
}

MODIFIER_NORMALIZATION: dict[str, str] = {
    "Mezcal Classic Margarita": "Margarita",
    "Half Shandy": "Shandy Half",
    "Half Top": "Top Half",
}

PRODUCT_NORMALIZATION: dict[str, str] = {
    "Bero Kingston Golden Pils (NON-ALC)": "Bero Golden Pils",
    "Bero Kingston Hazy Ipa": "Bero Hazy IPA",
    "Big Drop Pine Trail Can": "Big Drop Pine Trail",
    "Big Drop Reef Point Can": "Big Drop Reef Point",
    "F&s Ginger Ale": "F&S Ginger Ale",
    "F&s Ginger Beer": "F&S Ginger Beer",
    "F&s Indian Tonic": "F&S Indian Tonic",
    "F&s Light Tonic": "F&S Light Tonic",
    "F&s Lemonade": "F&S Lemonade",
    "F&s Soda Water": "F&S Soda Water",
    "Franklin & Sons Cola": "F&S Cola",
}

PRODUCT_CATEGORY_CORRECTIONS: dict[str, str] = {
    "Bero Kingston Hazy Ipa": "Bottled Beers",
    "Bero Kingston Golden Pils (NON-ALC)": "Bottled Beers",
    "Big Drop Pine Trail Can": "Bottled Beers",
    "Big Drop Reef Point Can": "Bottled Beers",
    "Purity Session Ipa": "Guest Draughts",
    "Caple Road": "Guest Draughts",
    "£5 Pint": "Promotions",
    "Wrap & Pint": "Promotions",
}

PRODUCTS_TO_SWAP: set[str] = {
    "£5 Pint", "Wrap & Pint", "Beer Pitcher", "Beer Flight",
    "Pre-sold Mass", "Pre-sold Pint",
    "Margarita", "Mojito", "Virgin Mojito", "Virgin Collins", "Daiquiri",
}

NUMERIC_FIELDS: list[str] = [
    "Qty", "Item Value", "Modifier Value",
    "Gross Product Sales", "Cost Price", "Gross Profit",
]

OUTPUT_COLUMNS: list[str] = [
    "Category", "Modifiers", "Product", "Mixer",
    "Qty", "Item Value", "Modifier Value",
    "Gross Product Sales", "Cost Price", "Gross Profit",
]

_SORT_EMPTY_VALUE = "zzzzz"
_SORT_EMPTY_PRIORITY = 999
_CUSTOM_SORT_MAPPING = {
    "Mixer": MIXER_SORT_ORDER,
    "Modifiers": MODIFIER_SORT_ORDER,
    "Product": PRODUCT_SORT_ORDER,
}


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def clean_numeric_value(value: object) -> float:
    """Convert a (possibly comma-formatted) string value to float."""
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, AttributeError):
        return 0.0


def format_numeric_value(value: float, as_integer: bool = False) -> str:
    if as_integer:
        return str(int(value))
    return f"{value:,.2f}"


def _normalize(value: str, table: dict[str, str]) -> str:
    if not value:
        return value
    return table.get(value, value)


def _standardize_category(cat: str) -> str:
    return CATEGORY_MAPPING.get(cat, cat)


def _correct_product_category(product: str, cat: str) -> str:
    return PRODUCT_CATEGORY_CORRECTIONS.get(product, cat)


def _swap_product_modifier(product: str, modifier: str) -> tuple[str, str]:
    if product in PRODUCTS_TO_SWAP:
        return modifier, product
    return product, modifier


def _standardize_wine_product(
    product: str, modifier: str, category: str
) -> tuple[str, str]:
    if category.lower() != "wines":
        return product, modifier
    for pattern, size in WINE_SIZE_PATTERNS.items():
        if re.search(pattern, product, re.IGNORECASE):
            cleaned = re.sub(pattern, "", product, flags=re.IGNORECASE).strip()
            updated_mod = f"{modifier},{size}" if modifier and modifier.strip(
            ) else size
            return cleaned, updated_mod
    return product, modifier


def _extract_mixer(modifier: str, category: str) -> tuple[str, str]:
    if not any(cat in category.lower() for cat in ("spirits", "gin")):
        return modifier, ""
    if not modifier or not modifier.strip():
        return modifier, ""
    parts = modifier.split(",")
    if len(parts) == 1:
        return modifier, ""
    return parts[0].strip(), ",".join(parts[1:]).strip()


def _get_custom_sort_key(value: str, order: list[str]) -> tuple[int, str]:
    if not value or not str(value).strip():
        return (_SORT_EMPTY_PRIORITY, _SORT_EMPTY_VALUE)
    v = str(value).strip()
    for priority, pattern in enumerate(order):
        if v == pattern or v.startswith(pattern):
            return (priority, v.lower())
    return (len(order), v.lower())


def _sort_key(row: dict) -> tuple:
    parts: list = []
    for col_name, sort_type in SORT_COLUMNS:
        val = row.get(col_name, "")
        if sort_type == "numeric":
            parts.append(clean_numeric_value(val))
        elif sort_type == "custom":
            order = _CUSTOM_SORT_MAPPING.get(col_name, [])
            parts.append(_get_custom_sort_key(str(val), order))
        else:
            parts.append(str(val).lower() if val else _SORT_EMPTY_VALUE)
    return tuple(parts)


def sort_output_rows(rows: list[dict], modifier_col: str = "Modifiers") -> list[dict]:
    """
    Sort output rows using the same priority order as Normal-Sales.

    *modifier_col* should be ``"Modifiers"`` for Normal-Sales rows and
    ``"Modifier"`` for Discounted, Complimentary, and Wastage rows.
    """
    def _key(row: dict) -> tuple:
        parts: list = []
        for col_name, sort_type in SORT_COLUMNS:
            # "Modifiers" in SORT_COLUMNS maps to whichever column the caller names
            actual_col = modifier_col if col_name == "Modifiers" else col_name
            val = row.get(actual_col, "")
            if sort_type == "numeric":
                parts.append(clean_numeric_value(val))
            elif sort_type == "custom":
                # Use col_name (always "Modifiers") as the mapping key
                order = _CUSTOM_SORT_MAPPING.get(col_name, [])
                parts.append(_get_custom_sort_key(str(val), order))
            else:
                parts.append(str(val).lower() if val else _SORT_EMPTY_VALUE)
        return tuple(parts)

    return sorted(rows, key=_key)


def _merge_duplicate_entries(rows: list[dict]) -> tuple[list[dict], int]:
    """Aggregate numeric fields for rows that share the same key quad."""

    def _key(r: dict) -> tuple[str, str, str, str]:
        return (r.get("Category", ""), r.get("Modifiers", ""),
                r.get("Product", ""), r.get("Mixer", ""))

    grouped: dict[tuple, dict] = defaultdict(
        lambda: {"nums": {f: 0.0 for f in NUMERIC_FIELDS}, "count": 0}
    )
    for row in rows:
        k = _key(row)
        grouped[k]["count"] += 1
        for field in NUMERIC_FIELDS:
            grouped[k]["nums"][field] += clean_numeric_value(
                row.get(field, "0"))

    duplicates_merged = sum(
        v["count"] - 1 for v in grouped.values() if v["count"] > 1)

    merged: list[dict] = []
    for (cat, mod, prod, mix), data in grouped.items():
        n = data["nums"]
        merged.append({
            "Category": cat,
            "Modifiers": mod,
            "Product": prod,
            "Mixer": mix,
            "Qty": format_numeric_value(n["Qty"], as_integer=True),
            "Item Value": format_numeric_value(n["Item Value"]),
            "Modifier Value": format_numeric_value(n["Modifier Value"]),
            "Gross Product Sales": format_numeric_value(n["Gross Product Sales"]),
            "Cost Price": format_numeric_value(n["Cost Price"]),
            "Gross Profit": format_numeric_value(n["Gross Profit"]),
        })

    return merged, duplicates_merged


def _process_sales_row(row: dict) -> dict:
    """Apply all cleaning steps to a single Master Sales row."""
    orig_cat = row.get("Category", "")
    orig_prod = row.get("Product", "")
    orig_mod = row.get("Modifiers", "")

    cat = _standardize_category(orig_cat)
    cat = _correct_product_category(orig_prod, cat)
    prod, mod = _swap_product_modifier(orig_prod, orig_mod)
    prod, mod = _standardize_wine_product(prod, mod, cat)
    mod, mix = _extract_mixer(mod, cat)
    prod = _normalize(prod, PRODUCT_NORMALIZATION)
    mod = _normalize(mod, MODIFIER_NORMALIZATION)
    mix = _normalize(mix, MIXER_NORMALIZATION)

    return {
        "Category": cat,
        "Modifiers": mod,
        "Product": prod,
        "Mixer": mix,
        "Qty": row.get("Qty", ""),
        "Item Value": row.get("Item Value", ""),
        "Modifier Value": row.get("Modifier Value", ""),
        "Gross Product Sales": row.get("Gross Product Sales", ""),
        "Cost Price": row.get("Cost Price", ""),
        "Gross Profit": row.get("Gross Profit", ""),
    }


# ---------------------------------------------------------------------------
# Public API – normalize_sales
# ---------------------------------------------------------------------------

def normalize_sales(
    input_path: Path | str,
    logger: Optional[PipelineLogger] = None,
) -> list[dict]:
    """
    Read and clean the Master Sales CSV.

    Parameters
    ----------
    input_path:
        Path to ``Sales by Product and Modifier.csv``.
    logger:
        Optional PipelineLogger; informational messages are sent here.

    Returns
    -------
    list[dict]
        Cleaned, merged, and sorted rows ready for downstream processing.
        Each row has keys from OUTPUT_COLUMNS.
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Master Sales file not found: {path}")

    raw_rows: list[dict] = []
    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            raw_rows.append(_process_sales_row(row))

    merged, dupes = _merge_duplicate_entries(raw_rows)
    sorted_rows = sorted(merged, key=_sort_key)

    msg = (
        f"Master Sales normalised: {len(raw_rows)} raw rows → "
        f"{len(sorted_rows)} rows after merging {dupes} duplicates."
    )
    if logger:
        logger.info(msg)
    else:
        print(f"  ✓ {msg}")

    return sorted_rows


# ---------------------------------------------------------------------------
# Public API – normalize_redeemables
# ---------------------------------------------------------------------------

# Redeemable types that we actively process
_PROCESSED_REDEEMABLE_TYPES = {"Discount Code", "Comp"}


def normalize_redeemables(
    input_path: Path | str,
    logger: Optional[PipelineLogger] = None,
) -> list[dict]:
    """
    Read and clean the Redeemables by Item CSV.

    Logic
    -----
    • Rows where ``Redeemable Type`` is "Voucher Code" are **skipped** per spec.
    • The ``Code`` field is upper-cased for consistent downstream matching.
    • ``Product`` names are corrected via PRODUCT_NORMALIZATION.
    • Numeric amounts are stored as floats under ``Discount_Amount_ExVAT`` and
      ``Discount_Amount_IncVAT``.

    Returns
    -------
    list[dict]
        Each row contains:
            Code, Redeemable_Type, Product, Qty, Discount_Amount_ExVAT,
            Discount_Amount_IncVAT
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Redeemables file not found: {path}")

    rows: list[dict] = []
    skipped_voucher = 0
    skipped_prepaid = 0
    skipped_other = 0

    with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rtype = row.get("Redeemable Type", "").strip()

            # Skip Voucher Codes per spec
            if rtype.lower() == "voucher code":
                skipped_voucher += 1
                continue

            # Skip Prepaid Tabs for now
            if rtype.lower() == "prepaid tab":
                skipped_prepaid += 1
                continue

            # Warn on unknown types but still process them
            if rtype not in _PROCESSED_REDEEMABLE_TYPES:
                skipped_other += 1
                msg = f"Unknown Redeemable Type '{rtype}' for code {row.get('Code', '?')} – included anyway."
                if logger:
                    logger.warning(msg)

            code = row.get("Code", "").strip().upper()
            product = _normalize(
                row.get("Product", "").strip(), PRODUCT_NORMALIZATION
            )
            qty = int(clean_numeric_value(
                row.get("Qty Sold", row.get("Qty", "0"))))
            disc_ex = clean_numeric_value(
                row.get("Discount Amount (Ex VAT)", "0")
            )
            disc_inc = clean_numeric_value(
                row.get("Discount Amount (Inc VAT)", "0")
            )

            rows.append({
                "Code": code,
                "Redeemable_Type": rtype,
                "Product": product,
                "Qty": qty,
                "Discount_Amount_ExVAT": disc_ex,
                "Discount_Amount_IncVAT": disc_inc,
            })

    msg = (
        f"Redeemables normalised: {len(rows)} rows kept, "
        f"{skipped_voucher} Voucher Codes and {skipped_prepaid} Prepaid Tabs skipped."
    )
    if logger:
        logger.info(msg)
    else:
        print(f"  ✓ {msg}")

    return rows


# ---------------------------------------------------------------------------
# Standalone entry-point (legacy compatibility – processes Master Sales only)
# ---------------------------------------------------------------------------

def process_csv(input_file: str | Path, output_file: str | Path | None = None) -> Path:
    """
    Process the Master Sales CSV and write a cleaned version to *output_file*.

    This function preserves backward-compatibility with the original
    standalone script.  For the modular pipeline, use :func:`normalize_sales`
    instead.
    """
    input_path = Path(input_file)
    if output_file is None:
        output_file = input_path.parent / \
            f"{input_path.stem} Cleaned{input_path.suffix}"
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = normalize_sales(input_path)

    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  ✓ Cleaned file written to: {output_path}")
    return output_path


def main() -> None:
    """Legacy standalone entry-point."""
    input_file = "Datasets/Sales by Product and Modifier.csv"
    print("=" * 60)
    print("Sales Data Normalizer for German Kraft Brewing Limited")
    print("=" * 60)
    print(f"\nInput file: {input_file}")
    print("\nProcessing...\n")
    try:
        process_csv(input_file)
        print("\n" + "=" * 60)
    except FileNotFoundError as exc:
        print(f"\n❌  Error: {exc}")
    except Exception as exc:
        print(f"\n❌  Unexpected error: {exc}")
        raise


if __name__ == "__main__":
    main()
