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

def build_runtime_mapping(source_excel: str, target_excel: str, clean_target: bool) -> Tuple[Dict[str, str], List[str], List[str], str]:
    """Create sheet name mapping at runtime, optionally clean target workbook.

    Returns:
        (mapping, unmatched_source, unmatched_target, effective_target_path)
    """
    source_sheets = sheet_mapper.extract_sheet_names(source_excel)
    target_sheets = sheet_mapper.extract_sheet_names(target_excel)

    if not source_sheets:
        raise SystemExit(f"No sheets found in source workbook: {source_excel}")
    if not target_sheets:
        raise SystemExit(f"No sheets found in target workbook: {target_excel}")

    source_sheets = sheet_mapper.filter_instruction_sheets(source_sheets)

    mapping, unmatched_source, unmatched_target = sheet_mapper.create_mapping(source_sheets, target_sheets)

    effective_target = target_excel
    if clean_target:
        logging.info("Clean-target flag active. Producing cleaned workbook copy via sheet_mapper...")
        effective_target = sheet_mapper.remove_unmatched_target_sheets(target_excel, unmatched_target)
        logging.info(f"Cleaned workbook: {effective_target}")

    # Log concise summary
    pos_mappings = {k: v for k, v in mapping.items() if v != '-'}
    logging.info(f"Positive mappings: {len(pos_mappings)} | Unmatched source: {len(unmatched_source)} | Unmatched target: {len(unmatched_target)}")

    return mapping, unmatched_source, unmatched_target, effective_target


# ----------------------------- ----------
# Phase 4: Extraction Configuration Logic
# ---------------------------------------
DEFAULT_EXTRACTION_CONFIG = {
    # Attendance-style defaults (works with Perry Soft attendance workbooks)
    "column_indices": [1, 2, 3, 4, 5, 6, 7],
    "header_text": "Dátum",
    "header_row_offset": 2,
    "start_row_strategy": None,
    "stop_condition": None,
    "sheets": "__EACH__",  # Special marker meaning we'll supply sheet names individually
}


def load_extraction_config(path: str) -> Dict[str, Any]:
    """Load YAML extraction config; fall back to defaults if missing.
    
    Returns the raw config dict for direct use with extract_data via STRATEGY_REGISTRY.
    Expected structure mirrors run_extractor.py approach - flat config per task/sheet.
    """
    if not path or not os.path.exists(path):
        logging.warning("Extraction config not found; using default inline config.")
        return {}

    try:
        import yaml
    except ImportError:
        logging.warning("PyYAML not installed; cannot parse config. Using defaults.")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            all_configs = yaml.safe_load(f) or {}
        logging.info(f"Loaded extraction config from {path}")
        return all_configs
    except Exception as e:
        logging.error(f"Failed loading extraction config '{path}': {e}; using defaults")
        return {}


def build_sheet_extraction_args(sheet_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Build extraction arguments for a sheet, merging defaults with sheet-specific config.
    
    Follows the extractor_utils.py pattern for configuration handling.
    """
    # Start with defaults
    extraction_args = DEFAULT_EXTRACTION_CONFIG.copy()
    
    # Look for sheet-specific config or fall back to global
    sheet_config = config.get(sheet_name, config.get("global", {}))
    
    # Merge sheet-specific overrides
    extraction_args.update(sheet_config)
    
    # Always include the sheet name for extract_data
    extraction_args["sheet_name"] = sheet_name
    
    return extraction_args


# ------------------------------------
# Phase 5: Per-Sheet Data Extraction
# ------------------------------------
def extract_sheet_data(source_excel: str, sheet_name: str, extraction_args: Dict[str, Any]) -> List[List[Any]]:
    """Extract raw rows for a single sheet using extract_data API."""
    

    start_strategy_key = extraction_args.get("start_row_strategy")
    stop_condition_key = extraction_args.get("stop_condition")

    start_fn = STRATEGY_REGISTRY.get(start_strategy_key) if isinstance(start_strategy_key, str) else start_strategy_key
    stop_fn = STRATEGY_REGISTRY.get(stop_condition_key) if isinstance(stop_condition_key, str) else stop_condition_key

    kwargs = {
        "file_path": source_excel,
        "column_indices": extraction_args["column_indices"],
        "sheet_name": sheet_name,
    }
    if extraction_args.get("header_text") is not None:
        kwargs["header_text"] = extraction_args.get("header_text")
    if extraction_args.get("header_row_offset") is not None:
        kwargs["header_row_offset"] = extraction_args.get("header_row_offset")
    if start_fn:
        kwargs["start_row_strategy"] = start_fn
    if stop_fn:
        kwargs["stop_condition"] = stop_fn

    try:
        return extract_data(**kwargs)
    except Exception as e:
        logging.error(f"Extraction failed for sheet '{sheet_name}': {e}")
        return []


# ------------------------------------
# Phase 6: Transform to Target Schema
# ------------------------------------
TARGET_COLUMNS = [
    "Datum",
    "Cas_Vykonu_Od",
    "Cas_Vykonu_Do",
    "Prestavka_Trvanie",
    "Popis_Cinnosti",
    "Pocet_Odpracovanych_Hodin",
    "Miesto_Vykonu",
    "PH_Projekt_POO",
    "PH_Riesenie_POO",
    "PH_Mimo_Projekt_POO",
    "SPOLU",
]

def _parse_time(val: Any) -> time | None:
    if val in (None, "", "-"):
        return None
    s = str(val).strip()
    # Support a few extra lenient variants (with trailing spaces or decimal hour e.g. 7.5)
    if s.replace('.', '', 1).isdigit() and '.' in s:
        try:
            hours_float = float(s)
            total_seconds = int(hours_float * 3600)
            h = total_seconds // 3600
            m = (total_seconds % 3600) // 60
            return time(hour=min(h, 23), minute=m)
        except Exception:
            pass
    for fmt in ("%H:%M:%S", "%H:%M", "%H.%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.time()
        except ValueError:
            continue
    return None


def _timedelta_to_hhmmss(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def transform_rows(raw_rows: List[List[Any]], source_sheet: str, activity_mode: str, month: int | None, year: int | None, activities_map: Optional[Dict[str, str]] = None, metadata_map: Optional[Dict[str, Dict[str, Any]]] = None) -> pd.DataFrame:
    """Convert raw extracted rows (list of lists) to standardized target dataframe of 31 days.

    Assumptions (can be refined via configuration later):
      raw column layout (indices):
        0: (ignored / date candidate)
        1: start time
        2: end time
        3: break placeholder (minutes or hh:mm) -> converted to Prestavka_Trvanie
        4: description (coalesced from multiple source cols)
        5: worked hours (hh:mm or hh:mm:ss)
        6: Miesto_Vykonu (optional)
        7: PH_Projekt_POO
        8: PH_Riesenie_POO
        9: PH_Mimo_Projekt_POO
        10: SPOLU (if present) else fallback to worked hours
    """
    df = pd.DataFrame(columns=TARGET_COLUMNS, index=range(31))
    df["Datum"] = [f"{i+1}." for i in range(31)]

    if activities_map and source_sheet in activities_map:
        activity_text = activities_map[source_sheet]
    else:
        if activity_mode == "infer":
            activity_text = _clean_activity_name(source_sheet)
        elif activity_mode == "static":
            activity_text = "Aktivita"
        else:
            activity_text = ""

    # Edge case: empty source sheet -> remain as all-zero rows
    for i in range(31):
        row = raw_rows[i] if i < len(raw_rows) else []
        start_val = row[1] if len(row) > 1 else None
        end_val = row[2] if len(row) > 2 else None
        break_val = row[3] if len(row) > 3 else None
        desc_val = row[4] if len(row) > 4 else ""
        worked_val = row[5] if len(row) > 5 else None
        miesto_val = row[6] if len(row) > 6 else ""
        ph_proj = row[7] if len(row) > 7 else None
        ph_ries = row[8] if len(row) > 8 else None
        ph_mimo = row[9] if len(row) > 9 else None
        spolu_val = row[10] if len(row) > 10 else None

        t_start = _parse_time(start_val)
        t_end = _parse_time(end_val)
        # Compute worked seconds if possible
        worked_seconds = None
        if t_start and t_end:
            start_seconds = t_start.hour * 3600 + t_start.minute * 60 + t_start.second
            end_seconds = t_end.hour * 3600 + t_end.minute * 60 + t_end.second
            if end_seconds >= start_seconds:
                worked_seconds = end_seconds - start_seconds
        # Override with provided worked time if parseable
        if not worked_seconds and worked_val:
            parsed = _parse_time(worked_val)
            if parsed:
                worked_seconds = parsed.hour * 3600 + parsed.minute * 60 + parsed.second
        if worked_seconds is None:
            worked_seconds = 0

        # Break formatting
        break_str = "00:00:00"
        if break_val:
            # Accept numeric minutes or hh:mm[:ss]
            if isinstance(break_val, (int, float)):
                break_str = _timedelta_to_hhmmss(int(break_val) * 60)
            else:
                bt = _parse_time(break_val)
                if bt:
                    break_str = _timedelta_to_hhmmss(bt.hour * 3600 + bt.minute * 60 + bt.second)

        worked_str = _timedelta_to_hhmmss(worked_seconds)
        spolu_str = worked_str if spolu_val in (None, "", "-") else str(spolu_val)

        # Activity description priority: explicit desc if present else activity_text
        # Activity precedence:
        # 1. Explicit override via activities_map ALWAYS wins unless desc_val is substantive non-time text.
        # 2. If no override: desc_val if present else inferred/static/blank.
        override_txt = activities_map.get(source_sheet) if activities_map else None
        def _looks_like_time_fragment(val: str) -> bool:
            import re
            return bool(re.fullmatch(r"\d{1,2}[:.]\d{2}(:\d{2})?", val.strip()) or re.fullmatch(r"\d+(\.\d+)?", val.strip()))
        if override_txt:
            if not desc_val or _looks_like_time_fragment(str(desc_val)):
                final_activity = override_txt
            else:
                final_activity = desc_val
        else:
            final_activity = activity_text if activity_text and not desc_val else (desc_val or "")

        # Metadata overrides (Miesto_Vykonu) - consistent for every row if defined
        if metadata_map and source_sheet in metadata_map:
            meta_entry = metadata_map[source_sheet]
            mv_override = meta_entry.get("Miesto_Vykonu") or meta_entry.get("miesto_vykonu")
            if mv_override:
                miesto_val = mv_override

        df.loc[i, :] = [
            df.loc[i, "Datum"],
            str(start_val) if start_val not in (None, "NaN", "nan") else "",
            str(end_val) if end_val not in (None, "NaN", "nan") else "",
            break_str,
            final_activity,
            worked_str,
            str(miesto_val) if miesto_val not in (None, "NaN", "nan") else "",
            _timedelta_to_hhmmss(0) if ph_proj in (None, "", "-") else str(ph_proj),
            _timedelta_to_hhmmss(0) if ph_ries in (None, "", "-") else str(ph_ries),
            _timedelta_to_hhmmss(0) if ph_mimo in (None, "", "-") else str(ph_mimo),
            spolu_str,
        ]

    return df


# ------------------------------------
# Phase 7: Target Sheet Preparation
# ------------------------------------
DAILY_START_ROW = 26
DAILY_ROW_COUNT = 31
DESCRIPTION_COL_START = 5
DESCRIPTION_COL_END = 8


def prepare_target_sheet(target_wb, target_sheet_name: str) -> Any:
    """Ensure target sheet exists and data area cleared.

    If sheet doesn't exist: duplicate first non-instruction sheet (or active sheet) as template.
    Clears rows DAILY_START_ROW .. DAILY_START_ROW+30 columns 1..14.
    Returns worksheet object.
    """
    created = False
    if target_sheet_name not in target_wb.sheetnames:
        template_name = None
        for name in target_wb.sheetnames:
            if name not in INSTRUCTION_SHEET_NAMES:
                template_name = name
                break
        if not template_name:
            template_name = target_wb.sheetnames[0]
        template_ws = target_wb[template_name]
        new_ws = target_wb.copy_worksheet(template_ws)
        new_ws.title = target_sheet_name
        logging.info(f"Created new sheet '{target_sheet_name}' from template '{template_name}'")
        created = True
    ws = target_wb[target_sheet_name]

    # Unmerge any merges overlapping data region (to avoid write errors)
    merges_to_restore = []
    for rng in list(ws.merged_cells.ranges):
        if rng.min_row >= DAILY_START_ROW and rng.max_row <= DAILY_START_ROW + DAILY_ROW_COUNT and rng.min_col <= DESCRIPTION_COL_END and rng.max_col >= DESCRIPTION_COL_START:
            merges_to_restore.append(rng.coord)
            ws.unmerge_cells(rng.coord)

    # Clear previous data
    for r in range(DAILY_START_ROW, DAILY_START_ROW + DAILY_ROW_COUNT):
        for c in range(1, 15):  # 1..14 inclusive
            ws.cell(row=r, column=c, value=None)

    return ws, created


# ------------------------------------
# Phase 8: Writing Data
# ------------------------------------
COL_MAPPING = {
    "Datum": 1,
    "Cas_Vykonu_Od": 2,
    "Cas_Vykonu_Do": 3,
    "Prestavka_Trvanie": 4,
    "Popis_Cinnosti": 5,  # spans 5-8
    "Pocet_Odpracovanych_Hodin": 9,
    "Miesto_Vykonu": 10,
    "PH_Projekt_POO": 11,
    "PH_Riesenie_POO": 12,
    "PH_Mimo_Projekt_POO": 13,
    "SPOLU": 14,
}


def write_daily_rows(ws, df_target: pd.DataFrame, start_row: int = DAILY_START_ROW):
    for i in range(DAILY_ROW_COUNT):
        row_index = start_row + i
        if i >= len(df_target):
            break
        record = df_target.iloc[i]
        for col_name, base_col in COL_MAPPING.items():
            value = record[col_name]
            if col_name == "Popis_Cinnosti":
                # Merge 5-8 for each row
                merge_range = f"{ws.cell(row=row_index, column=DESCRIPTION_COL_START).coordinate}:{ws.cell(row=row_index, column=DESCRIPTION_COL_END).coordinate}"
                ws.merge_cells(merge_range)
            ws.cell(row=row_index, column=base_col, value=value)
    logging.info(f"Wrote {min(DAILY_ROW_COUNT, len(df_target))} daily rows to sheet '{ws.title}' starting at row {start_row}")


# ------------------------------------
# Phase 9: Summary Recalculation
# ------------------------------------
SUMMARY_ROW = 57  # As per legacy logic


def _parse_duration_to_seconds(val: str) -> int:
    if not val or val in ("00:00", "00:00:00", "0", 0, None, "-"):
        return 0
    s = str(val).strip()
    # Accept hh:mm[:ss]
    parts = s.split(":")
    try:
        if len(parts) == 2:
            h, m = int(parts[0]), int(parts[1])
            return h * 3600 + m * 60
        if len(parts) == 3:
            h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
            return h * 3600 + m * 60 + sec
    except ValueError:
        return 0
    return 0


def recalculate_summary(df_target: pd.DataFrame, ws) -> dict:
    """Compute working days and total hours; write to summary row.

    Working day criteria: Pocet_Odpracovanych_Hodin > 00:00:00 OR Popis_Cinnosti non-empty.
    Writes:
      - total hours string to SUMMARY_ROW, column 14 (SPOLU)
      - working day count to SUMMARY_ROW, column 13 (if empty / safe)
    Returns dict with metrics.
    """
    try:
        hours_col = df_target.get("Pocet_Odpracovanych_Hodin", pd.Series([], dtype=str))
        activity_col = df_target.get("Popis_Cinnosti", pd.Series([], dtype=str))
        working_mask = (hours_col.fillna("00:00:00") != "00:00:00") | (activity_col.fillna("") != "")
        working_days = int(working_mask.sum())
    except Exception as e:
        logging.warning(f"Failed to compute working days: {e}")
        working_days = 0

    total_seconds = 0
    try:
        for val in df_target.get("SPOLU", []):
            total_seconds += _parse_duration_to_seconds(val)
    except Exception as e:
        logging.warning(f"Failed accumulating total hours: {e}")

    total_str = f"{total_seconds // 3600:02d}:{(total_seconds % 3600) // 60:02d}:{total_seconds % 60:02d}"

    try:
        ws.cell(row=SUMMARY_ROW, column=14, value=total_str)
        existing_days_cell = ws.cell(row=SUMMARY_ROW, column=13).value
        if existing_days_cell in (None, "", "-") or isinstance(existing_days_cell, (int, float)):
            ws.cell(row=SUMMARY_ROW, column=13, value=working_days)
        logging.info(f"Summary updated for sheet '{ws.title}': days={working_days}, total={total_str}")
    except Exception as e:
        logging.error(f"Error writing summary to sheet '{ws.title}': {e}")

    return {"working_days": working_days, "total_hours": total_str}


def main():  # noqa: D401
    """Entry point executing plan steps 1 & 2 (arguments + runtime mapping)."""
    args = parse_args()

    try:
        mapping, unmatched_source, unmatched_target, effective_target = build_runtime_mapping(
            args.source_excel, args.target_excel, args.clean_target
        )
    except SystemExit:
        raise
    except Exception as e:  # pragma: no cover
        logging.error(f"Failed during runtime mapping: {e}")
        raise SystemExit(1)

    # Optional activities overrides (Phase 16 groundwork)
    activities_overrides: Dict[str, str] | None = None
    if getattr(args, "activities_json", None):
        if os.path.exists(args.activities_json):
            try:
                with open(args.activities_json, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Accept either {sheet: text} or wrapper {"activities": {...}}
                if isinstance(data, dict) and "activities" in data and isinstance(data["activities"], dict):
                    activities_overrides = {k: str(v) for k, v in data["activities"].items()}
                elif isinstance(data, dict):
                    activities_overrides = {k: str(v) for k, v in data.items()}
                logging.info(f"Loaded {len(activities_overrides)} activity override(s) from {args.activities_json}")
            except Exception as e:
                logging.warning(f"Failed loading activities JSON '{args.activities_json}': {e}")
        else:
            logging.warning(f"Activities JSON not found: {args.activities_json}")

    # Metadata JSON (Phase 16 extended)
    metadata_map: Dict[str, Dict[str, Any]] | None = None
    if getattr(args, "metadata_json", None):
        if os.path.exists(args.metadata_json):
            try:
                with open(args.metadata_json, "r", encoding="utf-8") as f:
                    md_raw = json.load(f)
                if isinstance(md_raw, dict):
                    metadata_map = {k: (v if isinstance(v, dict) else {"value": v}) for k, v in md_raw.items()}
                logging.info(f"Loaded metadata entries: {len(metadata_map or {})}")
            except Exception as e:
                logging.warning(f"Failed loading metadata JSON '{args.metadata_json}': {e}")
        else:
            logging.warning(f"Metadata JSON not found: {args.metadata_json}")

    if args.save_mappings_json:
        sheet_mapper.save_mapping_json(mapping, unmatched_source, unmatched_target, args.output_dir, args.save_mappings_json, activities_overrides, metadata_map)

    # Summary of mapping
    print("\n=== Runtime Sheet Mapping Summary ===")
    for src, tgt in mapping.items():
        print(f"{src} -> {tgt}")
    if unmatched_source:
        print("\nUnmatched source sheets:")
        for u in unmatched_source:
            print(f"  {u}")
    if unmatched_target:
        print("\nUnmatched target sheets:")
        for u in unmatched_target:
            print(f"  {u}")
    print("\nEffective target workbook:", effective_target)

    # Phase 3: Open workbooks & backup
    backup_dir = os.path.join(args.output_dir, "backup")
    try:
        source_wb, target_wb, backup_path = open_workbooks(args.source_excel, effective_target, backup_dir, args.dry_run)
    except SystemExit:
        raise
    except Exception as e:
        logging.error(f"Workbook open failure: {e}")
        raise SystemExit(1)

    # Phase 4: Load extraction configuration
    extraction_cfg = load_extraction_config(args.extraction_config)
    logging.info("Loaded extraction configuration (global + per-sheet overrides applied lazily per sheet).")

    # Phase 5 / 6 / 7 / 8 integrated: extract -> transform -> prepare sheet -> write
    extracted_counts = {}
    transformed_counts = {}

    positive_mappings = {s: t for s, t in mapping.items() if s and t and t != '-'}

    # Apply --only filtering if provided
    if args.only:
        import fnmatch
        raw_filters = [f.strip() for f in args.only.split(',') if f.strip()]
        filtered = {}
        for s, t in positive_mappings.items():
            if any(fnmatch.fnmatch(s, pattern) for pattern in raw_filters):
                filtered[s] = t
        skipped_due_to_only = set(positive_mappings.keys()) - set(filtered.keys())
        for s in skipped_due_to_only:
            logging.info(f"Skipping (filtered by --only): {s}")
        positive_mappings = filtered
        logging.info(f"--only applied. Remaining mappings: {len(positive_mappings)}")
    summary_metrics = {}

    # Log skipped mappings explicitly
    skipped = {s: t for s, t in mapping.items() if t == '-' or not t}
    for s, t in skipped.items():
        logging.info(f"Skipping mapping (no target): {s} -> {t}")

    # Duplicate target detection (edge case handling)
    target_occurrences: Dict[str, int] = {}
    for src, tgt in positive_mappings.items():
        target_occurrences[tgt] = target_occurrences.get(tgt, 0) + 1
    duplicates = {t for t, c in target_occurrences.items() if c > 1}
    if duplicates:
        logging.warning(f"Duplicate target sheets detected: {sorted(list(duplicates))} -- subsequent duplicates will be skipped.")

    def process_sheet(source_sheet: str, target_sheet: str) -> None:
        """Extract, transform, write and summarize for one mapping."""
        logging.info(f"--- START sheet '{source_sheet}' -> '{target_sheet}' ---")

        # Extraction
        sheet_args = build_sheet_extraction_args(source_sheet, extraction_cfg)
        raw_rows = extract_sheet_data(args.source_excel, source_sheet, sheet_args)
        extracted_counts[source_sheet] = len(raw_rows)
        logging.debug(f"Raw rows sample (first 2): {raw_rows[:2] if raw_rows else '[]'}")

        # Transformation
        df_target = transform_rows(
            raw_rows,
            source_sheet,
            args.activity_mode,
            args.month,
            args.year,
            activities_overrides,
            metadata_map,
        )
        transformed_counts[source_sheet] = len(df_target)

        # Optional CSV export (debug/audit) - occurs even in dry-run
        if args.export_csv_dir:
            try:
                os.makedirs(args.export_csv_dir, exist_ok=True)
                def _safe_name(name: str) -> str:
                    import re
                    return re.sub(r'[^A-Za-z0-9_.-]+', '_', name)
                csv_path = os.path.join(args.export_csv_dir, f"{_safe_name(target_sheet)}.csv")
                df_target.to_csv(csv_path, index=False)
                logging.info(f"Exported transformed CSV: {csv_path}")
            except Exception as e:
                logging.warning(f"CSV export failed for sheet '{target_sheet}': {e}")

        if args.dry_run:
            logging.info(
                f"Dry-run: skipping write for '{target_sheet}' (transformed rows: {len(df_target)})"
            )
            logging.info(f"--- END sheet '{source_sheet}' (dry-run) ---")
            return

        # Sheet preparation
        ws, created = prepare_target_sheet(target_wb, target_sheet)
        if created:
            logging.info(f"Sheet '{target_sheet}' created")

        # Writing data rows
        write_daily_rows(ws, df_target, DAILY_START_ROW)

        # Summary recalculation (phase 9)
        metrics = recalculate_summary(df_target, ws)
        summary_metrics[target_sheet] = metrics
        logging.info(
            f"Summary results for '{target_sheet}': days={metrics['working_days']} total={metrics['total_hours']}"
        )
        logging.info(f"--- END sheet '{source_sheet}' -> '{target_sheet}' ---")

    processed_targets: set[str] = set()
    for source_sheet, target_sheet in positive_mappings.items():
        if target_sheet in processed_targets and target_sheet in duplicates:
            logging.info(f"Skipping '{source_sheet}' because target '{target_sheet}' already processed (duplicate target)")
            continue
        process_sheet(source_sheet, target_sheet)
        processed_targets.add(target_sheet)

    # Save workbook if not dry-run
    if not args.dry_run:
        # Primary save overwrite
        try:
            target_wb.save(effective_target)
            logging.info(f"Workbook saved: {effective_target}")
        except PermissionError as e:
            logging.warning(f"Primary save PermissionError: {e}; retrying in 1s...")
            sleep(1)
            try:
                target_wb.save(effective_target)
                logging.info(f"Workbook saved after retry: {effective_target}")
            except PermissionError as e2:
                logging.error(f"Workbook locked, aborting save: {e2}")
                raise WorkbookLockedError(str(e2))
        except Exception as e:
            logging.error(f"Error saving primary workbook '{effective_target}': {e}")

        # Secondary versioned copy
        try:
            version_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            os.makedirs(args.output_dir, exist_ok=True)
            secondary_path = os.path.join(args.output_dir, f"updated_{version_ts}.xlsx")
            # Re-open target to duplicate to maintain current memory state
            target_wb.save(secondary_path)
            logging.info(f"Versioned copy saved: {secondary_path}")
        except Exception as e:
            logging.warning(f"Could not create versioned copy: {e}")
    else:
        logging.info("Dry-run: no workbook changes saved (skipping primary & secondary outputs)")

    # Close workbooks
    try:
        source_wb.close()
        target_wb.close()
    except Exception:  # pragma: no cover
        pass

    # Validation pass (only if not dry-run)
    if not args.dry_run:
        try:
            reopened = load_workbook(effective_target, read_only=True)
            missing_targets = [t for t in positive_mappings.values() if t not in reopened.sheetnames]
            if missing_targets:
                logging.warning(f"Validation: Missing target sheets after save: {missing_targets}")
            else:
                logging.info("Validation: All mapped target sheets present.")
            reopened.close()
        except Exception as e:
            logging.warning(f"Validation open failed: {e}")

    # Report summary
    print("\n=== Processing Summary (Phases 5-8) ===")
    for sheet, count in extracted_counts.items():
        tgt = positive_mappings.get(sheet)
        metrics = summary_metrics.get(tgt, {})
        metrics_str = f" | days={metrics.get('working_days')} total={metrics.get('total_hours')}" if metrics else ""
        print(f"{sheet}: extracted {count} row(s), transformed -> {transformed_counts.get(sheet)} rows (target){metrics_str}")

    # Log unmatched at the very end (already printed earlier, but ensure requirement)
    logging.info(f"Unmatched source sheets count: {len(unmatched_source)}")
    logging.info(f"Unmatched target sheets count: {len(unmatched_target)}")
    if args.dry_run:
        print("\nDry-run completed (no modifications written).")
    else:
        print("\nWorkbook updated successfully (summary recalculation pending in later phase).")


if __name__ == "__main__":
    main()