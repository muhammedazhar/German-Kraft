"""
file_manager.py – Pre-flight checks, menu selection, fingerprinting, and cleanup.

Responsibilities
----------------
A.  Menu selection & deduplication
    • Scan the Menus/ folder for CSV files.
    • Identify the latest file by last-modified timestamp.
    • Prompt the user to confirm, pick an older file, or accept the default.
    • Hash every file in Menus/ and silently delete exact mirror duplicates.

B.  Dataset fingerprinting (duplicate-run prevention)
    • SHA-256 hash the two Datasets/ CSVs.
    • Compare against a persistent log (.fingerprints.json).
    • Halt (raise RuntimeError) when a previously processed pair is detected.

C.  Post-run cleanup
    • Delete the raw CSVs from Datasets/ once Outputs are successfully written.
    • Append the fingerprints to the persistent log so the same data is never
      processed twice.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from .logger import PipelineLogger

# Paths are relative to the project root (where main.py lives)
MENUS_DIR = Path("Menus")
DATASETS_DIR = Path("Datasets")
FINGERPRINT_LOG = Path(".fingerprints.json")

DATASET_FILES = [
    "Sales by Product and Modifier.csv",
    "Redeemables by Item.csv",
]


# ---------------------------------------------------------------------------
# Hashing helpers
# ---------------------------------------------------------------------------

def _hash_file(path: Path) -> str:
    """Return the SHA-256 hex digest of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Part A – Menu selection & deduplication
# ---------------------------------------------------------------------------

def scan_and_select_menu(
    menus_dir: Path = MENUS_DIR,
    logger: Optional[PipelineLogger] = None,
) -> Path:
    """
    Scan *menus_dir*, remove exact-mirror duplicates, and return the user's
    chosen menu file path.

    Parameters
    ----------
    menus_dir:
        Directory that holds Menu CSV exports.
    logger:
        Optional PipelineLogger for recording events.

    Returns
    -------
    Path
        The absolute path to the selected menu CSV.

    Raises
    ------
    FileNotFoundError
        If no CSV files are found in *menus_dir*.
    """
    csv_files = sorted(menus_dir.glob("*.csv"), key=os.path.getmtime)
    if not csv_files:
        raise FileNotFoundError(
            f"No menu CSV files found in '{menus_dir}'. "
            "Please export a menu from Dines and place it in that folder."
        )

    # ------------------------------------------------------------------ #
    # 1. Deduplicate exact mirrors                                         #
    # ------------------------------------------------------------------ #
    seen_hashes: dict[str, Path] = {}
    duplicates_removed: list[Path] = []

    for csv_path in list(csv_files):
        digest = _hash_file(csv_path)
        if digest in seen_hashes:
            # This file is an exact copy of an already-seen file – delete it
            original = seen_hashes[digest]
            msg = (
                f"Duplicate menu file removed: '{csv_path.name}' "
                f"is an exact copy of '{original.name}'"
            )
            if logger:
                logger.warning(msg)
            else:
                print(f"  ⚠  {msg}")
            csv_path.unlink()
            duplicates_removed.append(csv_path)
            csv_files.remove(csv_path)
        else:
            seen_hashes[digest] = csv_path

    if not csv_files:
        raise FileNotFoundError(
            "All menu files were detected as duplicates and removed. "
            "Please ensure at least one valid menu export is in the Menus/ folder."
        )

    # ------------------------------------------------------------------ #
    # 2. Identify the latest file                                         #
    # ------------------------------------------------------------------ #
    latest = max(csv_files, key=os.path.getmtime)
    latest_mtime = datetime.fromtimestamp(os.path.getmtime(latest))

    # ------------------------------------------------------------------ #
    # 3. Prompt the user                                                  #
    # ------------------------------------------------------------------ #
    print("\n" + "─" * 60)
    print("Menu Selection")
    print("─" * 60)
    print(
        f"  Latest menu  : {latest.name}  (modified {latest_mtime:%Y-%m-%d %H:%M})")
    print()
    print(f"  [ENTER]  Use the latest menu (default)")

    if len(csv_files) > 1:
        print(f"  [L]      List all available menus and choose manually")

    print("─" * 60)
    choice = input("  Your choice: ").strip().upper()

    if choice in ("L", "LIST") and len(csv_files) > 1:
        print()
        for idx, f in enumerate(sorted(csv_files, key=os.path.getmtime, reverse=True), 1):
            mtime = datetime.fromtimestamp(os.path.getmtime(f))
            flag = "  ← latest" if f == latest else ""
            print(f"  [{idx}]  {f.name}  ({mtime:%Y-%m-%d %H:%M}){flag}")
        print()
        while True:
            raw = input(f"  Enter number (1–{len(csv_files)}): ").strip()
            try:
                n = int(raw)
                if 1 <= n <= len(csv_files):
                    selected = sorted(
                        csv_files, key=os.path.getmtime, reverse=True
                    )[n - 1]
                    break
            except ValueError:
                pass
            print(f"  Please enter a number between 1 and {len(csv_files)}.")
    else:
        selected = latest

    if logger:
        logger.info(f"Menu selected: {selected.name}")
    else:
        print(f"\n  ✓ Using menu: {selected.name}")

    return selected.resolve()


# ---------------------------------------------------------------------------
# Part B – Dataset fingerprinting
# ---------------------------------------------------------------------------

def _load_fingerprint_log(log_path: Path) -> list[dict]:
    """Return the list of previously recorded fingerprint entries."""
    if not log_path.exists():
        return []
    try:
        with open(log_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return []


def check_dataset_fingerprints(
    datasets_dir: Path = DATASETS_DIR,
    log_path: Path = FINGERPRINT_LOG,
    logger: Optional[PipelineLogger] = None,
) -> dict[str, str]:
    """
    Hash the two Datasets CSVs and compare against the persistent log.

    Returns
    -------
    dict[str, str]
        Mapping of filename → SHA-256 hex digest for both dataset files.

    Raises
    ------
    FileNotFoundError
        If a required dataset file is missing.
    RuntimeError
        If the exact same data has already been processed (duplicate run guard).
    """
    fingerprints: dict[str, str] = {}

    for filename in DATASET_FILES:
        fpath = datasets_dir / filename
        if not fpath.exists():
            raise FileNotFoundError(
                f"Required dataset file missing: '{fpath}'\n"
                "Please export from Dines and place both CSVs in Datasets/."
            )
        fingerprints[filename] = _hash_file(fpath)

    # Produce a single combined signature for the pair
    combined_hash = hashlib.sha256(
        "".join(fingerprints[f] for f in DATASET_FILES).encode()
    ).hexdigest()

    history = _load_fingerprint_log(log_path)
    past_hashes = {entry["combined_hash"] for entry in history}

    if combined_hash in past_hashes:
        # Find when it was first processed
        for entry in history:
            if entry["combined_hash"] == combined_hash:
                processed_at = entry.get("processed_at", "unknown date")
                break
        msg = (
            f"⛔  Duplicate data detected!\n"
            f"   These exact datasets were already processed on {processed_at}.\n"
            f"   To prevent double-counting, the pipeline has been halted.\n"
            f"   If you genuinely want to re-process, delete the entry from:\n"
            f"   {log_path.resolve()}"
        )
        if logger:
            logger.error(msg)
        raise RuntimeError(msg)

    if logger:
        logger.info("Dataset fingerprints verified – no duplicates detected.")

    return fingerprints


def save_dataset_fingerprints(
    fingerprints: dict[str, str],
    log_path: Path = FINGERPRINT_LOG,
    logger: Optional[PipelineLogger] = None,
) -> None:
    """
    Append the current run's fingerprints to the persistent log.

    Parameters
    ----------
    fingerprints:
        The dict returned by :func:`check_dataset_fingerprints`.
    log_path:
        Path to the JSON fingerprint log file.
    """
    combined_hash = hashlib.sha256(
        "".join(fingerprints[f] for f in DATASET_FILES).encode()
    ).hexdigest()

    history = _load_fingerprint_log(log_path)
    history.append(
        {
            "combined_hash": combined_hash,
            "file_hashes": fingerprints,
            "processed_at": datetime.now().isoformat(timespec="seconds"),
        }
    )

    with open(log_path, "w", encoding="utf-8") as fh:
        json.dump(history, fh, indent=2)

    if logger:
        logger.info(f"Fingerprints saved to {log_path}.")


# ---------------------------------------------------------------------------
# Part C – Post-run cleanup
# ---------------------------------------------------------------------------

def cleanup_datasets(
    datasets_dir: Path = DATASETS_DIR,
    logger: Optional[PipelineLogger] = None,
) -> None:
    """
    Delete the raw CSV files from *datasets_dir* after a successful run.

    Only the two known dataset files are removed; any other files or
    subdirectories are left untouched.

    Parameters
    ----------
    datasets_dir:
        The Datasets/ directory.
    logger:
        Optional PipelineLogger for recording events.
    """
    for filename in DATASET_FILES:
        fpath = datasets_dir / filename
        if fpath.exists():
            fpath.unlink()
            msg = f"Dataset file deleted after successful processing: {fpath.name}"
            if logger:
                logger.info(msg)
            else:
                print(f"  ✓ {msg}")
        else:
            msg = f"Dataset file not found during cleanup (already removed?): {fpath.name}"
            if logger:
                logger.warning(msg)
