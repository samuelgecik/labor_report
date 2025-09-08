# Implementation Plan for Updating ronec_vykaz.xlsx

## Comprehensive Implementation Plan for Updating ronec_vykaz.xlsx

This plan synthesizes the analysis from update_analysis.md into a detailed, step-by-step guide for automating the update of the "Vykaz" sheet in `data/input/ronec_vykaz.xlsx` with attendance data from `extracted_source_with_headers.csv` for July 2025. It expands on the proposed mappings, transformations, and challenges, ensuring handling of special cases like vacation ("Dovolenka") and absences ("-"), preservation of Excel formatting, recalculation of the summary row, and configurability for project variations (e.g., ronec vs. sadecky activity descriptions). The plan assumes a Python-based automation script, leveraging the existing pipeline in `src/`. No code is written here; this is purely a blueprint for implementation.

### Prerequisites
- **Environment Setup**: Python 3.x with installed libraries: `pandas` for CSV handling and data transformations, `openpyxl` for Excel read/write to preserve formatting, and `datetime`/`timedelta` from the standard library for date/time parsing and conversions.
- **Input Files**: 
  - `extracted_source_with_headers.csv` (32 rows: header + 31 days of July 2025 attendance data).
  - `ronec_vykaz.xlsx` (target Excel file with "Ing. Simon Ronec" sheet: headers in rows 24-25, rows 26-56 daily data, row 57 summary).
- **Optional References**: `extracted_target_with_headers.csv` and `extracted_sadecky_target_with_headers.csv` for validation and vacation handling examples.
- **Configuration**: Define variables for project-specific settings, e.g., a dictionary of activity templates (`activity_templates = {'ronec': 'Podieľanie sa na realizácii pracovného balíka č. 1 s názvom: Analýza užívateľských potrieb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]', 'sadecky': 'Podieľanie sa na realizácii pracovného balíka č. 1 ... a č. 2 ... - [role-specific]'}`) and standard vacation times (e.g., `vacation_start = '09:00:00'`, `vacation_end = '17:00:00'`).
- **Backup Strategy**: Always create a timestamped backup of the original Excel before saving changes.
- **Assumptions**: Source data is clean and aligned to July 2025; interruptions (`Prerusenie_*`) are ignored as net worked time (`Skutocny_Odpracovany_Cas`) is pre-calculated and trusted. Target sheet structure is fixed (columns A-K for Datum to SPOLU). Actual target structure has headers in rows 24-25 with Slovak labels (e.g., 'Dátum', 'Čas výkonu od'), sheet 'Ing. Simon Ronec'; script configurable for variations.

### Script Structure Suggestions
- **Preferred Approach**: Create a new standalone script `src/update_vykaz.py` for modularity, importing necessary libraries and configurations. This can be run monthly via `python src/update_vykaz.py --project ronec --month July --year 2025` (using argparse for inputs to support variations).
- **Alternative**: Extend `src/transfer_hours.py` if it already handles similar hour transfers—add a new function for reverse mapping (CSV to Excel) after analyzing its existing code for compatibility. Integrate with the extraction pipeline in `src/excel_extractor/main.py` for end-to-end automation (e.g., extract → transform → update).
- **Modularity**: Use functions for each major step (e.g., `read_source()`, `transform_data()`, `update_excel()`) to allow testing and reuse. Include logging for debugging and error tracking.

### Step-by-Step Implementation Plan

- [x] 1. **Setup and Configuration**
  - [x] Import libraries: `import pandas as pd`, `from openpyxl import load_workbook`, `from datetime import datetime, timedelta`, and `import shutil` for backups.
  - [x] Define configuration parameters: Select project template (e.g., `project = 'ronec'`), standard times for vacation, and validation rules (e.g., expected month=7, year=2025).
  - [x] Create paths: Source CSV at root, target Excel at `data/input/ronec_vykaz.xlsx`.
  - [x] Backup the original Excel: Copy to `ronec_vykaz_backup_{timestamp}.xlsx` using `shutil.copy()`.
  - [x] Error Handling: Wrap in try-except for file not found or import errors; log and exit gracefully.

- [x] 2. **Read and Validate Source Data**
  - [x] Load the CSV: `df_source = pd.read_csv('extracted_source_with_headers.csv')`.
  - [x] Parse dates: `df_source['Datum'] = pd.to_datetime(df_source['Datum'])` (ignores time `00:00:00` automatically).
  - [x] Validate data: Check row count (32), confirm all dates in July 2025 (`df_source['Datum'].dt.month == 7` and `dt.year == 2025`), and ensure 31 unique days. Flag mismatches (e.g., wrong month) and halt if invalid.
  - [x] Clean data: Replace any NaN in numeric fields (e.g., `Prestavka_min`) with '-' for consistency.
  - [x] Error Handling: Use try-except for parsing errors (e.g., malformed dates); log invalid rows and suggest manual review.

- [x] 3. **Transform and Map Data**
  - [x] Initialize target DataFrame: Create `df_target` with columns matching the target structure (Datum, Cas_Vykonu_Od, Cas_Vykonu_Do, Prestavka_Trvanie, Popis_Cinnosti, Pocet_Odpracovanych_Hodin, Miesto_Vykonu, PH_Projekt_POO, PH_Riesenie_POO, PH_Mimo_Projekt_POO, SPOLU) and 31 rows (exclude header/summary for now).
  - [x] Extract day numbers: `df_target['Datum'] = df_source['Datum'].dt.day.astype(str) + '.'`.
  - [x] Classify rows: For each row, check `Dochadzka_Prichod`: 'Dovolenka' → vacation, '-' → absent, else → work day.
  - [x] Apply conditional mappings (per proposed field mappings):
    - [x] **Vacation ("Dovolenka")**: Set Cas_Vykonu_Od = '09:00:00', Cas_Vykonu_Do = '17:00:00', Prestavka_Trvanie = '', Popis_Cinnosti = 'DOVOLENKA', Pocet_Odpracovanych_Hodin = df_source['Skutocny_Odpracovany_Cas'] (or default '08:00:00' if '-'), Miesto_Vykonu = '', PH_* = '00:00:00', SPOLU = Pocet_Odpracovanych_Hodin.
    - [x] **Absent ("-")**: Set Cas_Vykonu_Od/Do = '', Prestavka_Trvanie = '00:00:00', Popis_Cinnosti = '', Pocet_Odpracovanych_Hodin = '00:00:00', Miesto_Vykonu = '', PH_* = '00:00:00', SPOLU = '00:00:00'.
    - [x] **Work Day**: Set Cas_Vykonu_Od = df_source['Dochadzka_Prichod'], Cas_Vykonu_Do = df_source['Dochadzka_Odchod'], Prestavka_Trvanie = convert minutes (pd.to_timedelta(df_source['Prestavka_min'], unit='min').dt.strftime('%H:%M:%S') or '00:00:00' if invalid), Popis_Cinnosti = activity_templates[project], Pocet_Odpracovanych_Hodin = df_source['Skutocny_Odpracovany_Cas'], Miesto_Vykonu = 'Bratislava', PH_* = '00:00:00', SPOLU = Pocet_Odpracovanych_Hodin.
  - [x] Handle edge cases: For vacation/absent breaks or times, ensure empty strings where appropriate (not '00:00:00' unless hours). Ignore interruptions unless net time seems incorrect (optional validation: recalculate worked time if needed).
  - [x] Configurability: Use the template dict to swap descriptions for sadecky (e.g., omit package 3, add role-specific text).
  - [x] Error Handling: Try-except for timedelta conversions (e.g., invalid minutes → log and set '00:00:00'); validate time formats (HH:MM:SS).

- [x] 4. **Load and Prepare Target Excel Structure**
  - [x] Load workbook: `wb = load_workbook('data/input/ronec_vykaz.xlsx')`, select sheet `ws = wb['Ing. Simon Ronec']` (confirm sheet name dynamically if needed).
  - [x] Verify structure: Check rows 24-25 for headers with Slovak labels (e.g., 'Dátum', 'Čas výkonu od', ..., 'SPOLU'), ensure row count >=33, note any merged cells or styles in rows 26+ to preserve.
  - [x] Preserve formatting: Do not modify styles; only update cell values. If summary row has formulas, note for recalculation. Mention merged cells/styles preserved.
  - [x] Error Handling: If sheet not found or file locked, log error and suggest manual open.

- [x] 5. **Update Daily Rows in Excel**
  - [x] Align data: For i in range(31): target_row = i + 26 (Excel rows 26-56, after headers in 24-25).
  - [x] Map to cells: Assuming columns A= Dátum (1), B= Čas výkonu od (2), ..., K= SPOLU (11). Set `ws.cell(row=target_row, column=col_num, value=df_target.iloc[i][col_name])`.
  - [x] Overwrite selectively: Clear only data columns (B-K) if existing values conflict, but retain Dátum if unchanged.
  - [x] Handle empties: Use '' for empty strings, '00:00:00' for zero times; ensure time cells are formatted as time if possible (via openpyxl styles, but minimally).
  - [x] Special Considerations: For vacation, ensure Popis_Cinnosti is exactly "DOVOLENKA" (uppercase). Do not touch other sheets.
  - [x] Error Handling: IndexError if row mismatch; validate post-update by reading back values.

- [x] 6. **Recalculate and Update Summary Row**
  - [x] Compute summary: Count non-absent days (`work_days = len(df_target[df_target['SPOLU'] != '00:00:00'])`), sum hours (parse HH:MM:SS to timedelta, sum, format back: e.g., total_days + ', ' + total_hours).
  - [x] Update row 57: Set cells for summary text (e.g., A57 = f'{work_days} days, {total_time}'), potentially update SPOLU total.
  - [x] If original summary has formulas (e.g., SUM), re-enable or recalculate manually to match format like "6 days, 16:00:00".
  - [x] Preserve merged cells/styles in row 57.
  - [x] Error Handling: timedelta parsing errors → default to '0 days, 00:00:00'; log discrepancies.

- [x] 7. **Save Changes and Final Validation**
  - [x] Save workbook: `wb.save('data/output/ronec_vykaz.xlsx')`.
  - [x] Optional: Generate a transformed CSV (`df_target.to_csv('transformed_data.csv')`) for audit.
  - [x] Cleanup: Close workbook.
  - [x] Error Handling: Permission errors on save → suggest closing Excel; rollback to backup if update fails midway.

### Testing Considerations
- **Unit Tests**: Test transformations on sample rows (e.g., work/vacation/absent examples from analysis) using pytest: verify mappings, date extraction, break conversions.
- **Integration Tests**: Run full script on a copy of the Excel with sample source CSV; compare output to expected (e.g., diff against sadecky vacation example).
- **Edge Cases**: Test malformed data (invalid times, wrong month), variations (switch to sadecky template), formatting preservation (open updated Excel, check bold headers/merged cells).
- **Validation Metrics**: Ensure 31 daily rows updated, summary matches summed hours, no data loss. Manually inspect 2-3 rows post-run.
- **Performance**: For 31 rows, script should run <1s; scale for larger months if needed.
- **Manual Fallback**: If script fails, generate transformed CSV manually and paste into Excel, then sum summary.

This plan ensures a robust, configurable update process, integrating seamlessly with the existing codebase while addressing all identified challenges. It can be iterated monthly for new data.