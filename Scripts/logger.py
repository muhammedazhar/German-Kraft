"""
logger.py – Captures and persists pipeline warnings and informational messages.

Warnings are:
  • Printed to the console immediately (with severity prefix).
  • Appended to Docs/Logs/pipeline.log for post-run review.

Usage
-----
    from Scripts.logger import PipelineLogger
    log = PipelineLogger()
    log.warning("Negative sales detected for Heinr Zwickel / Pint")
    log.info("Gold card GKGOLDCRDIAN added: Ian")
    log.summary()          # prints a tally at the end of the run
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Literal

# Default log file location (relative to project root, resolved at runtime)
DEFAULT_LOG_PATH = Path("Docs") / "Logs" / "pipeline.log"

Severity = Literal["INFO", "WARNING", "ERROR"]


class PipelineLogger:
    """Lightweight structured logger for the sales-splitting pipeline."""

    def __init__(self, log_path: Path | str | None = None):
        self._log_path: Path = Path(log_path) if log_path else DEFAULT_LOG_PATH
        self._entries: list[tuple[Severity, str]] = []

        # Ensure the Outputs directory exists
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

        # Open the log file once, in append mode, so each run adds to history
        self._file = self._log_path.open("a", encoding="utf-8")
        timestamp = datetime.datetime.now().isoformat(timespec="seconds")
        self._file.write(f"\n{'=' * 60}\n")
        self._file.write(f"Pipeline run started at {timestamp}\n")
        self._file.write(f"{'=' * 60}\n")
        self._file.flush()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def info(self, message: str) -> None:
        """Record an informational message."""
        self._record("INFO", message)

    def warning(self, message: str) -> None:
        """Record a warning – something unexpected but non-fatal."""
        self._record("WARNING", message)

    def error(self, message: str) -> None:
        """Record a fatal error (caller is responsible for halting)."""
        self._record("ERROR", message)

    def summary(self) -> None:
        """Print a tally of logged entries to stdout."""
        counts: dict[Severity, int] = {"INFO": 0, "WARNING": 0, "ERROR": 0}
        for sev, _ in self._entries:
            counts[sev] += 1

        print("\n" + "─" * 60)
        print("Pipeline log summary")
        print("─" * 60)
        print(f"  INFO     : {counts['INFO']}")
        print(f"  WARNING  : {counts['WARNING']}")
        print(f"  ERROR    : {counts['ERROR']}")
        if counts["WARNING"] + counts["ERROR"] > 0:
            print(f"\n  ⚠  Log saved to: {self._log_path.resolve()}")
        print("─" * 60)

    def warnings(self) -> list[str]:
        """Return all warning messages."""
        return [msg for sev, msg in self._entries if sev == "WARNING"]

    def errors(self) -> list[str]:
        """Return all error messages."""
        return [msg for sev, msg in self._entries if sev == "ERROR"]

    def close(self) -> None:
        """Flush and close the log file."""
        self._file.flush()
        self._file.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record(self, severity: Severity, message: str) -> None:
        timestamp = datetime.datetime.now().isoformat(timespec="seconds")
        line = f"[{timestamp}] {severity:>7}: {message}"

        self._entries.append((severity, message))

        # Console output
        prefix = {"INFO": "ℹ", "WARNING": "⚠", "ERROR": "✖"}[severity]
        print(f"  {prefix}  {message}")

        # Persist to log file
        self._file.write(line + "\n")
        self._file.flush()

    def __del__(self):
        try:
            if not self._file.closed:
                self._file.close()
        except Exception:
            pass
