from __future__ import annotations

import argparse
import json
import yaml
import logging
import os
from datetime import datetime, time, timedelta
from time import sleep
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
from src import sheet_mapper

from openpyxl import load_workbook

from src.extractor_utils import STRATEGY_REGISTRY, extract_data, open_workbooks


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
# Allow dynamic log level via env var (VYKAZY_LOG_LEVEL)
_env_level = os.getenv("VYKAZY_LOG_LEVEL")
if _env_level:
    try:
        logging.getLogger().setLevel(_env_level.upper())
    except Exception:  # pragma: no cover
        logging.warning(f"Invalid VYKAZY_LOG_LEVEL '{_env_level}', keeping default INFO")



# ------------------------------------
# Phase 13: Placeholder Implementations & Utilities
# ------------------------------------

class MappingError(Exception):
    """Raised when critical mapping inconsistencies are detected."""


class WorkbookLockedError(Exception):
    """Raised when the workbook cannot be saved due to a file lock."""


class TimeParseError(Exception):
    """Raised when a time string cannot be parsed with expected formats."""


def parse_filename(filename: str) -> dict:
    """Parse filename for project/month/year hints (best-effort).

    Pattern: <project>_<month|mon>_<year>.*  (flexible, heuristic)
    Returns partial dict; failures are non-fatal.
    """
    base = os.path.basename(filename)
    stem = os.path.splitext(base)[0]
    parts = stem.split('_')
    result: dict[str, Any] = {}
    if len(parts) >= 3:
        try:
            year_candidate = int(parts[-1])
            result['year'] = year_candidate
            result['month_raw'] = parts[-2]
            result['project'] = '_'.join(parts[:-2])
        except ValueError:
            result['project'] = parts[0]
    return result


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the refactored workflow.

    New primary arguments:
      --source-excel
      --target-excel
      --clean-target
      --save-mappings-json
      --activity-mode

    Deprecated (accepted but ignored with warning):
      --source_dir, --source_csv, --project
    """
    parser = argparse.ArgumentParser(description="Update labor report workbook (phase: mapping only)")
    parser.add_argument("--source-excel", required=True, help="Path to source attendance/work log Excel file")
    parser.add_argument("--target-excel", required=True, help="Path to target labor report Excel file")
    parser.add_argument("--extraction-config", default="config/extraction_config.yaml", help="YAML extraction config (future use)")
    parser.add_argument("--output-dir", default="data/output", help="Directory for generated artifacts")
    parser.add_argument("--activity-mode", choices=["infer", "static", "none"], default="infer", help="Strategy for activity description population (future use)")
    parser.add_argument("--month", type=int, default=None, help="Month number override (1-12)")
    parser.add_argument("--year", type=int, default=None, help="Year override (e.g. 2025)")
    parser.add_argument("--dry-run", action="store_true", help="Run mapping & validation only; do not write workbook changes (still exports CSV if --export-csv-dir specified)")
    parser.add_argument("--clean-target", action="store_true", help="Remove unmatched target sheets before processing and use cleaned copy")
    parser.add_argument("--save-mappings-json", nargs="?", const=True, default=False, help="Save runtime mapping JSON (optionally specify output path)")
    parser.add_argument("--activities-json", help="Optional JSON file with per-sheet activity overrides { 'SourceSheet': 'Text' }")
    parser.add_argument("--metadata-json", help="Optional JSON file providing per-sheet metadata { 'SourceSheet': { 'Miesto_Vykonu': 'HomeOffice', ... } }")
    parser.add_argument("--only", help="Comma-separated list or glob patterns of source sheet names to process (others skipped)")
    parser.add_argument("--export-csv-dir", help="If set, write transformed per-sheet CSVs for inspection (directory created if missing)")

    # Deprecated args (soft support to ease transition)
    parser.add_argument("--source_dir", help=argparse.SUPPRESS)
    parser.add_argument("--source_csv", help=argparse.SUPPRESS)
    parser.add_argument("--project", help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Emit deprecation warnings if legacy args used
    for legacy in ("source_dir", "source_csv", "project"):
        if getattr(args, legacy) is not None:
            logging.warning(f"Argument --{legacy} is deprecated and will be ignored in this refactored phase.")

    return args

