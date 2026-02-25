"""
main.py – Orchestrator for the Dines → Polaris Sales Splitter pipeline.

Pipeline steps
--------------
A.  Pre-flight checks & file management  (Scripts/file_manager.py)
    1.  Scan Menus/ folder, deduplication, and user menu selection.
    2.  Hash the two Datasets/ CSVs and guard against duplicate runs.

B.  Data ingestion & cleaning           (Scripts/normalizer.py)
    3.  Normalise Master Sales CSV.
    4.  Normalise Redeemables by Item CSV.

C+D. Modifier deduction                 (Scripts/modifier_engine.py)
    5.  Load Discounts and Menu data.
    6.  Enrich each redeemable row with its deduced Modifier and Category.

E.  Reconciliation & routing            (Scripts/reconciler.py)
    7.  Route into four buckets: Normal / Discounted / Complimentary / Wastage.

F.  Output CSV generation
    8.  Write each bucket to Outputs/.

G.  Cleanup                             (Scripts/file_manager.py)
    9.  Delete raw CSVs from Datasets/.
    10. Save dataset fingerprints to prevent future duplicate runs.

Run
---
    python main.py
"""

from __future__ import annotations
from Scripts import file_manager, normalizer, modifier_engine, reconciler
from Scripts.logger import PipelineLogger

import csv
import sys
from pathlib import Path

# Ensure project root is on sys.path so "Scripts" package is importable
sys.path.insert(0, str(Path(__file__).parent))


# ---------------------------------------------------------------------------
# Paths (all relative to this file's directory, i.e. the project root)
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent

DATASETS_DIR = BASE_DIR / "Datasets"
MENUS_DIR = BASE_DIR / "Menus"
DISCOUNTS_DIR = BASE_DIR / "Discounts"
OUTPUTS_DIR = BASE_DIR / "Outputs"

MASTER_SALES_PATH = DATASETS_DIR / "Sales by Product and Modifier.csv"
REDEEMABLES_PATH = DATASETS_DIR / "Redeemables by Item.csv"
DISCOUNTS_PATH = DISCOUNTS_DIR / "Discounts.csv"
GOLD_CARDS_PATH = DISCOUNTS_DIR / "Gold-Cards.csv"
FINGERPRINT_LOG_PATH = BASE_DIR / ".processed_fingerprints.json"

OUTPUT_FILES = {
    "normal": OUTPUTS_DIR / "Normal-Sales.csv",
    "discounted": OUTPUTS_DIR / "Discounted-Sales.csv",
    "complimentary": OUTPUTS_DIR / "Complimentary-Sales.csv",
    "wastage": OUTPUTS_DIR / "Wastage.csv",
}

# ---------------------------------------------------------------------------
# Output column schemas
# ---------------------------------------------------------------------------

NORMAL_COLUMNS = [
    "Category", "Modifiers", "Product", "Mixer",
    "Qty", "Item Value", "Modifier Value",
    "Gross Product Sales", "Cost Price", "Gross Profit",
]

DISCOUNTED_COLUMNS = [
    "Category", "Product", "Modifier", "Mixer",
    "Qty", "Discount_Code", "Discount_Percentage", "Discount_Amount_IncVAT",
]

COMP_COLUMNS = [
    "Category", "Product", "Modifier", "Mixer",
    "Qty", "Discount_Code", "Discount_Percentage", "Person",
]

WASTAGE_COLUMNS = [
    "Category", "Product", "Modifier", "Mixer", "Qty",
]


# ---------------------------------------------------------------------------
# CSV writer helper
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    print()
    print("=" * 65)
    print("  German Kraft – Dines → Polaris Sales Splitter")
    print("=" * 65)

    log = PipelineLogger(OUTPUTS_DIR / "pipeline.log")

    try:
        # ---------------------------------------------------------------- #
        # Step A1 – Menu selection & deduplication                         #
        # ---------------------------------------------------------------- #
        print("\n[A1] Checking Menus/ folder…")
        selected_menu = file_manager.scan_and_select_menu(
            menus_dir=MENUS_DIR, logger=log
        )

        # ---------------------------------------------------------------- #
        # Step A2 – Dataset fingerprint check                              #
        # ---------------------------------------------------------------- #
        print("\n[A2] Verifying dataset fingerprints…")
        fingerprints = file_manager.check_dataset_fingerprints(
            datasets_dir=DATASETS_DIR,
            log_path=FINGERPRINT_LOG_PATH,
            logger=log,
        )
        print("  ✓ No duplicate run detected.")

        # ---------------------------------------------------------------- #
        # Step B – Normalise input data                                    #
        # ---------------------------------------------------------------- #
        print("\n[B]  Normalising input data…")
        master_rows = normalizer.normalize_sales(MASTER_SALES_PATH, logger=log)
        redeemable_rows = normalizer.normalize_redeemables(
            REDEEMABLES_PATH, logger=log)

        # ---------------------------------------------------------------- #
        # Steps C+D – Modifier deduction                                   #
        # ---------------------------------------------------------------- #
        print("\n[C+D] Deducing modifiers for redeemables…")
        discounts = modifier_engine.load_discounts(DISCOUNTS_PATH)
        menu_data = modifier_engine.load_menu(selected_menu)
        log.info(
            f"Loaded {len(discounts)} discount codes and "
            f"{len(menu_data)} menu products."
        )

        enriched = modifier_engine.process_redeemable_modifiers(
            redeemable_rows, discounts, menu_data, logger=log
        )

        # ---------------------------------------------------------------- #
        # Step E – Reconciliation & routing                                #
        # ---------------------------------------------------------------- #
        print("\n[E]  Reconciling and routing rows…")
        normal, discounted, comps, wastage_rows = reconciler.reconcile(
            master_rows=master_rows,
            enriched_redeemables=enriched,
            gold_cards_path=GOLD_CARDS_PATH,
            logger=log,
        )

        # ---------------------------------------------------------------- #
        # Step F – Write output CSVs                                        #
        # ---------------------------------------------------------------- #
        print("\n[F]  Writing output files…")
        _write_csv(OUTPUT_FILES["normal"], normal, NORMAL_COLUMNS)
        _write_csv(OUTPUT_FILES["discounted"], discounted, DISCOUNTED_COLUMNS)
        _write_csv(OUTPUT_FILES["complimentary"], comps, COMP_COLUMNS)
        _write_csv(OUTPUT_FILES["wastage"], wastage_rows, WASTAGE_COLUMNS)

        for key, path in OUTPUT_FILES.items():
            count = {
                "normal": len(normal),
                "discounted": len(discounted),
                "complimentary": len(comps),
                "wastage": len(wastage_rows),
            }[key]
            print(f"  ✓ {path.name:30s}  ({count} rows)")
            log.info(f"Written {path.name}: {count} rows → {path}")

        # ---------------------------------------------------------------- #
        # Step G – Cleanup                                                  #
        # ---------------------------------------------------------------- #
        print("\n[G]  Cleaning up…")
        file_manager.cleanup_datasets(datasets_dir=DATASETS_DIR, logger=log)
        file_manager.save_dataset_fingerprints(
            fingerprints=fingerprints,
            log_path=FINGERPRINT_LOG_PATH,
            logger=log,
        )

    except (FileNotFoundError, ValueError) as exc:
        log.error(str(exc))
        print(f"\n  ✖  {exc}")
        sys.exit(1)

    except RuntimeError as exc:
        # Duplicate-run guard already logged inside file_manager
        print(f"\n  ✖  {exc}")
        sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n  Pipeline interrupted by user.")
        sys.exit(130)

    finally:
        log.summary()
        log.close()

    print()
    print("=" * 65)
    print("  Pipeline complete.  Check Outputs/ for your four CSV files.")
    print("=" * 65)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_pipeline()
