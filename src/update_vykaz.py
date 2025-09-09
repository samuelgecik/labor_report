import argparse
import shutil
import os
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import load_workbook
import logging

logging.basicConfig(level=logging.INFO)

def transform_and_map_data(df_source, project):
    activity_templates = {
        'ronec': 'Podieľanie sa na realizácii pracovného balíka č. 1 s názvom: Analýza užívateľských potrieb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]'
    }
    
    cols = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']
    df_target = pd.DataFrame(columns=cols, index=range(31))

    # Extract day numbers for all 31 days
    df_target['Datum'] = [str(i + 1) + '.' for i in range(31)]

    def get_prestavka(r):
        p_min = r['Prestavka_min']
        if p_min == '-' or pd.isna(p_min):
            return '00:00:00'
        if isinstance(p_min, str) and p_min.isdigit():
            mins = int(p_min)
        elif isinstance(p_min, (int, float)):
            mins = p_min
        else:
            return '00:00:00'
        try:
            td = timedelta(minutes=mins)
            hours = td.seconds // 3600
            mins_part = (td.seconds % 3600) // 60
            return f"{hours:02}:{mins_part:02}:00"
        except:
            return '00:00:00'

    def get_skutocny(r):
        s = r['Skutocny_Odpracovany_Cas']
        if pd.isna(s) or s == '-':
            return '00:00:00'
        return s

    for i in range(31):
        if i < len(df_source):
            row = df_source.iloc[i]
            dochadzka = row['Dochadzka_Prichod']
        else:
            dochadzka = '-'
            row = None  # Not used for '-'

        weekend_fields = ['Dochadzka_Prichod', 'Dochadzka_Odchod', 'Prestavka_min', 'Prerusenie_Odchod', 'Prerusenie_Prichod', 'Skutocny_Odpracovany_Cas']

        if i < len(df_source) and row is not None:
            sodch = str(dochadzka).strip() if not pd.isna(dochadzka) else 'NaN'
            is_weekend = all(pd.isna(row[col]) or str(row[col]).strip() == '-' for col in weekend_fields)
            wd = row['Datum'].weekday()
            logging.info(f"Row {i}: weekday {wd} (0=Mon), date {row['Datum'].date()}, dochadzka='{sodch}', weekend_detect={is_weekend}")

        if i < len(df_source) and row is not None and not pd.isna(row['Datum']) and all(pd.isna(row[col]) or str(row[col]).strip() == '-' for col in weekend_fields):
            # Weekend
            df_target.loc[i, 'Cas_Vykonu_Od'] = ''
            df_target.loc[i, 'Cas_Vykonu_Do'] = ''
            df_target.loc[i, 'Prestavka_Trvanie'] = '00:00:00'
            df_target.loc[i, 'Popis_Cinnosti'] = ''
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = '00:00:00'
            df_target.loc[i, 'Miesto_Vykonu'] = ''
            df_target.loc[i, 'PH_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Riesenie_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Mimo_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = '00:00:00'
            logging.info(f"Handled as weekend row {i}: set empty/00:00:00 for non-date fields")
        elif dochadzka == 'Dovolenka':
            # Vacation
            df_target.loc[i, 'Cas_Vykonu_Od'] = ''
            df_target.loc[i, 'Cas_Vykonu_Do'] = ''
            df_target.loc[i, 'Prestavka_Trvanie'] = ''
            df_target.loc[i, 'Popis_Cinnosti'] = 'DOVOLENKA'
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = '08:00:00'
            df_target.loc[i, 'Miesto_Vykonu'] = ''
            logging.info(f"Vacation row {i}: Miesto set empty")
            df_target.loc[i, 'PH_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Riesenie_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Mimo_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = '08:00:00'
        elif dochadzka == '-':
            # Absent
            df_target.loc[i, 'Cas_Vykonu_Od'] = ''
            df_target.loc[i, 'Cas_Vykonu_Do'] = ''
            df_target.loc[i, 'Prestavka_Trvanie'] = '00:00:00'
            df_target.loc[i, 'Popis_Cinnosti'] = ''
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = '00:00:00'
            df_target.loc[i, 'Miesto_Vykonu'] = ''
            df_target.loc[i, 'PH_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Riesenie_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Mimo_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = '00:00:00'
        else:
            # Work Day
            df_target.loc[i, 'Cas_Vykonu_Od'] = row['Dochadzka_Prichod']
            df_target.loc[i, 'Cas_Vykonu_Do'] = row['Dochadzka_Odchod']
            df_target.loc[i, 'Prestavka_Trvanie'] = get_prestavka(row)
            df_target.loc[i, 'Popis_Cinnosti'] = activity_templates[project]
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = row['Skutocny_Odpracovany_Cas']
            df_target.loc[i, 'Miesto_Vykonu'] = 'Bratislava'
            df_target.loc[i, 'PH_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Riesenie_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Mimo_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = df_target.loc[i, 'Pocet_Odpracovanych_Hodin']

    return df_target

def load_target_excel(target_excel):
    expected_headers = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']
    try:
        wb = load_workbook(target_excel)
        print(f"Available sheets in {target_excel}: {wb.sheetnames}")
        sheet_name = 'Vykaz' if 'ronec' not in target_excel else 'Ing. Simon Ronec'
        try:
            ws = wb[sheet_name]
        except KeyError:
            print(f"Error: Sheet 'Vykaz' not found in {target_excel}. Please ensure the sheet exists and is named 'Vykaz'. Available sheets: {wb.sheetnames}")
            return None, None, None, None

        # Verify structure: Search for 'Dátum' in column 1 across rows 1-33
        header_row = None
        for row_num in range(1, 34):  # Check rows 1 to 33
            cell_value = ws.cell(row=row_num, column=1).value
            if cell_value and isinstance(cell_value, str) and 'Dátum' in cell_value.strip():
                header_row = row_num
                break
            print(f"Row {row_num} column 1: {cell_value}")  # Debug print

        if header_row is None:
            print("Error: 'Dátum' not found in column 1 within first 33 rows.")
            return None, None, None, None

        data_start_row = header_row + 2
        print(f"Headers found in row {header_row}, data starts at row {data_start_row}")

        # Ensure ws.max_row >= data_start_row + 30 (for 31 days)
        if ws.max_row < data_start_row + 30:
            print(f"Error: Expected at least {data_start_row + 30} rows for 31 data entries, but found {ws.max_row}.")
            return None, None, None, None

        # Ensure ws.max_row >= 33
        if ws.max_row < 33:
            print(f"Error: Expected at least 33 rows, but found {ws.max_row}.")
            return None, None

        # Note merged cells for preservation
        if ws.merged_cells:
            print(f"Note: Merged cells in worksheet: {list(ws.merged_cells.ranges)}. Formatting will be preserved during value updates.")

        # Preserve formatting: Only cell values will be updated, not styles.
        # If summary row (33) has formulas, note for later recalculation but do not change yet.
        # Note: This function only loads and verifies; no modifications yet.

        return wb, ws, header_row, data_start_row

    except FileNotFoundError:
        print(f"Error: Target Excel file not found: {target_excel}")
        return None, None, None, None
    except PermissionError:
        print(f"Error: Target Excel file is locked or permission denied: {target_excel}. Please close the file if open and try again.")
        return None, None, None, None
    except Exception as e:
        print(f"Unexpected error loading Excel: {e}")
        return None, None, None, None

def update_daily_rows(ws, df_target, data_start_row):
    col_mappings = {
            'Datum': 1,
            'Cas_Vykonu_Od': 2,
            'Cas_Vykonu_Do': 3,
            'Prestavka_Trvanie': 4,
            'Popis_Cinnosti': 5,
            'Pocet_Odpracovanych_Hodin': 9,
            'Miesto_Vykonu': 10,
            'PH_Projekt_POO': 11,
            'PH_Riesenie_POO': 12,
            'PH_Mimo_Projekt_POO': 13,
            'SPOLU': 14
        }

    # Find the Popis merge range [5-8]
    original_ranges = list(ws.merged_cells.ranges)
    popis_merge_coord = None
    for merged_range in original_ranges:
        if merged_range.min_col == 5 and merged_range.max_col == 8:
            popis_merge_coord = merged_range.coord
            break

    logging.info(f"Merged ranges before unmerge: {len(list(ws.merged_cells.ranges))}")

    unmerged_coords = set()
    try:
        for i in range(31):
            target_row = data_start_row + i
            for merged_range in original_ranges:
                if merged_range.min_row <= target_row <= merged_range.max_row:
                    coord = merged_range.coord
                    if coord not in unmerged_coords:
                        ws.unmerge_cells(coord)
                        unmerged_coords.add(coord)
                        logging.info(f"Unmerging {str(merged_range)} for row {target_row}")

            expected_date = df_target.iloc[i]['Datum']
            current_date = ws.cell(row=target_row, column=1).value
            if current_date != expected_date:
                ws.cell(row=target_row, column=1, value=expected_date)

            if df_target.iloc[i]['Popis_Cinnosti'] == 'DOVOLENKA':
                logging.info(f"Vacation row {i}: fields set empty for Od/Do")

            for col_name, col_num in col_mappings.items():
                if col_name == 'Datum':
                    continue  # already handled
                val = df_target.iloc[i][col_name]
                if pd.isna(val) or val == '-':
                    val = ''
                # Ensure vacation Popis_Cinnosti is uppercase, but it's already set in df_target
                ws.cell(row=target_row, column=col_num, value=val)

                if col_name == 'Popis_Cinnosti':
                    if popis_merge_coord and val != '':
                        for c in [6, 7, 8]:
                            ws.cell(row=target_row, column=c, value='')

        # Re-merge cells that were unmerged
        for coord in unmerged_coords:
            ws.merge_cells(coord)
        logging.info(f"Re-merged {len(unmerged_coords)} ranges")

    except IndexError as e:
        logging.error(f"IndexError during row update at row {target_row}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during row update: {e}")

    # Validation: read back sample rows (26 and 56)
    try:
        sample_indices = [0, 30]
        target_rows = [data_start_row, data_start_row + 30]
        discrepancies = []
        for idx, trow in zip(sample_indices, target_rows):
            actual_vals = {col_name: ws.cell(row=trow, column=col_num).value for col_name, col_num in col_mappings.items()}
            expected_vals = df_target.loc[idx, col_mappings.keys()]
            for col_name in col_mappings:
                col_num = col_mappings[col_name]
                # Skip checks for SPOLU if formula cell
                if col_num == 14 and actual_vals[col_name] is None:
                    discrepancies.append(f"Row {trow}, col {col_name}: formula cell, expected {expected_vals[col_name]}")
                    continue
                # Skip checks if cell is not top-left of merged range
                is_top_left = True
                cell_row, cell_col = trow, col_num
                for merged_range in list(ws.merged_cells.ranges):
                    bounds = merged_range.bounds
                    if bounds[0] <= cell_row <= bounds[2] and bounds[1] <= cell_col <= bounds[3]:
                        if cell_row == bounds[0] and cell_col == bounds[1]:
                            pass  # it's top-left
                        else:
                            is_top_left = False
                        break
                if not is_top_left:
                    continue
                if actual_vals[col_name] != expected_vals[col_name]:
                    discrepancies.append(f"Row {trow}, col {col_name}: expected {expected_vals[col_name]}, got {actual_vals[col_name]}")
        if discrepancies:
            logging.warning(f"Validation discrepancies: {'; '.join(discrepancies)}")
        else:
            logging.info("Validation completed successfully: no discrepancies found.")
    except Exception as e:
        logging.error(f"Validation error: {e}")

def recalculate_summary(df_target, ws):
    # Compute non-absent days
    try:
        work_days = len(df_target[df_target['SPOLU'] != '00:00:00'])
    except Exception as e:
        logging.warning(f"Error counting work days: {e}")
        work_days = 0

    # Sum hours
    total_td = timedelta()
    total_time_str = '00:00:00'
    for value in df_target['SPOLU']:
        if value not in (None, '00:00:00'):
            try:
                h, m, s = map(int, value.split(':'))
                total_td += timedelta(hours=h, minutes=m, seconds=s)
            except (ValueError, AttributeError, TypeError) as e:
                logging.warning(f"Could not parse SPOLU value '{value}': {e}")

    try:
        total_seconds = total_td.total_seconds()
        hours = int(abs(total_seconds) // 3600)
        minutes = int((abs(total_seconds) % 3600) // 60)
        seconds = int(abs(total_seconds) % 60)
        total_time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception as e:
        logging.warning(f"Error formatting total time: {e}")

    summary_text = f"{work_days} days, {total_time_str}"

    # Update cells in row 57
    try:
        ws.cell(row=57, column=1, value=summary_text)
        ws.cell(row=57, column=14, value=total_time_str)
        logging.info(f"Summary updated: {summary_text} in A57, {total_time_str} in N57")
    except Exception as e:
        logging.error(f"Error updating summary cells: {e}")

    return summary_text, total_time_str

def save_and_validate(wb, df_target, backup_path, original_path, project):
    output_path = f'data/output/{project}_vykaz.xlsx'
    os.makedirs('data/output', exist_ok=True)
    try:
        wb.save(output_path)
        logging.info(f"Workbook saved to {output_path}")
        df_target.to_csv('transformed_data.csv', index=False)
        logging.info("Transformed CSV generated for audit")
        wb.close()
        logging.info("Workbook closed successfully")
    except PermissionError as e:
        logging.error(f"Permission error saving workbook: {e}. Please close Excel file and retry.")
        wb.close()
    except Exception as e:
        logging.error(f"Error saving workbook: {e}")
        if os.path.exists(backup_path):
            shutil.copy(backup_path, original_path)
            logging.info(f"Rolled back to backup: {backup_path} -> {original_path}")
        else:
            logging.warning("No backup found for rollback")
        wb.close()
        exit(1)

def main():
    # Parse command line arguments with hardcoded defaults
    parser = argparse.ArgumentParser(description='Update Vykaz Script')
    parser.add_argument('--project', type=str, default='ronec', help='Project name (default: ronec)')
    parser.add_argument('--month', type=str, default='July', help='Month (default: July)')
    parser.add_argument('--year', type=int, default=2025, help='Year (default: 2025)')
    args = parser.parse_args()

    try:
        # Configuration parameters
        project = args.project  # Expected: 'ronec'
        vacation_start = '09:00:00'
        vacation_end = '17:00:00'
        activity_templates = {
            'ronec': 'Podieľanie sa na realizácii pracovného balíka č. 1 s názvom: Analýza užívateľských potrieb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]'
        }

        # Validation rules
        expected_month = 7  # July
        expected_year = 2025

        # Define paths
        source_csv = 'extracted_source_with_headers.csv'
        target_excel = f'data/input/{project}_vykaz.xlsx'

        # Backup original Excel file with timestamp
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f'data/input/{project}_vykaz_backup_{backup_timestamp}.xlsx'

        if shutil.os.path.exists(target_excel):
            shutil.copy(target_excel, backup_path)
            print(f"Backup created: {backup_path}")
        else:
            print(f"Original file not found: {target_excel}. Skipping backup.")

        print("Setup and configuration completed successfully.")

        # Step 2: Read and Validate Source Data
        df_source = pd.read_csv(source_csv)
        # Clean data: replace ' -' with '-'
        df_source['Skutocny_Odpracovany_Cas'] = df_source['Skutocny_Odpracovany_Cas'].astype(str).str.strip().replace(' -', '-', regex=False)
        df_source['Prestavka_min'] = df_source['Prestavka_min'].astype(str).str.strip().replace(' -', '-', regex=False)

        # Parse dates
        df_source['Datum'] = pd.to_datetime(df_source['Datum'])

        # Validation
        row_count = df_source.shape[0]
        if row_count != 31:
            print(f"Validation failed: Expected 31 data rows, got {row_count}")
            exit(1)

        if not (df_source['Datum'].dt.month == expected_month).all() or not (df_source['Datum'].dt.year == expected_year).all():
            print("Validation failed: Not all dates are in July 2025")
            exit(1)

        unique_days = df_source['Datum'].dt.date.nunique()
        if unique_days != 31:
            print(f"Validation failed: Expected 31 unique days, got {unique_days}")
            exit(1)

        # Clean data: replace NaN in numeric fields with '-'
        numeric_cols = df_source.select_dtypes(include=['number']).columns
        df_source[numeric_cols] = df_source[numeric_cols].fillna('-')

        print("Read and Validate Source Data completed successfully.")

        # Step 3: Transform and Map Data
        df_target = transform_and_map_data(df_source, project)
        print("Transform and Map Data completed successfully.")

        # Step 4: Load and Prepare Target Excel Structure
        wb, ws, header_row, data_start_row = load_target_excel(target_excel)
        if wb is None or ws is None or header_row is None or data_start_row is None:
            exit(1)
        print("Load and Prepare Target Excel Structure completed successfully.")

        # Step 5: Update Daily Rows in Excel
        update_daily_rows(ws, df_target, data_start_row)
        print("Update Daily Rows in Excel completed successfully.")

        # Step 6: Recalculate and Update Summary Row
        recalculate_summary(df_target, ws)
        print("Recalculate and Update Summary Row completed successfully.")

        # Step 7: Save Changes and Final Validation
        save_and_validate(wb, df_target, backup_path, target_excel, project)
        print("Save Changes and Final Validation completed successfully.")

    except ImportError as e:
        print(f"Import error: {e}")
        exit(1)
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()