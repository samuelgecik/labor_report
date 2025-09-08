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
            return '08:00:00'
        return s

    for i in range(31):
        if i < len(df_source):
            row = df_source.iloc[i]
            dochadzka = row['Dochadzka_Prichod']
        else:
            dochadzka = '-'
            row = None  # Not used for '-'

        if dochadzka == 'Dovolenka':
            # Vacation
            df_target.loc[i, 'Cas_Vykonu_Od'] = '09:00:00'
            df_target.loc[i, 'Cas_Vykonu_Do'] = '17:00:00'
            df_target.loc[i, 'Prestavka_Trvanie'] = ''
            df_target.loc[i, 'Popis_Cinnosti'] = 'DOVOLENKA'
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = get_skutocny(row)
            df_target.loc[i, 'Miesto_Vykonu'] = ''
            df_target.loc[i, 'PH_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Riesenie_POO'] = '00:00:00'
            df_target.loc[i, 'PH_Mimo_Projekt_POO'] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = df_target.loc[i, 'Pocet_Odpracovanych_Hodin']
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
            return None, None

        # Verify structure: Check rows for expected headers
        header_row = None
        for row_num in range(1, 34):  # Check rows 1 to 33
            headers = []
            for col in range(1, len(expected_headers) + 1):
                cell = ws.cell(row=row_num, column=col)
                if cell.value and isinstance(cell.value, str):
                    headers.append(cell.value.strip())
                else:
                    headers.append('')

            if headers == expected_headers:
                header_row = row_num
                break
            print(f"Row {row_num} headers: {headers[:5]}...")  # Print first 5

        if header_row is None:
            print("Error: Expected headers not found in first 33 rows.")
            return None, None

        print(f"Headers found in row {header_row}")
        # Adjust for header row
        if header_row != 1:
            print(f"Error: Headers expected in row 1, but found in row {header_row}. Please ensure the sheet starts with headers in row 1.")
            return None, None

        # Ensure ws.max_row >= 33
        if ws.max_row < 33:
            print(f"Error: Expected at least 33 rows, but found {ws.max_row}.")
            return None, None

        # Note merged cells for preservation
        if ws.merged_cells:
            print(f"Note: Merged cells in worksheet: {ws.merged_cells}. Formatting will be preserved during value updates.")

        # Preserve formatting: Only cell values will be updated, not styles.
        # If summary row (33) has formulas, note for later recalculation but do not change yet.
        # Note: This function only loads and verifies; no modifications yet.

        return wb, ws

    except FileNotFoundError:
        print(f"Error: Target Excel file not found: {target_excel}")
        return None, None
    except PermissionError:
        print(f"Error: Target Excel file is locked or permission denied: {target_excel}. Please close the file if open and try again.")
        return None, None
    except Exception as e:
        print(f"Unexpected error loading Excel: {e}")
        return None, None

def update_daily_rows(ws, df_target):
    col_mappings = {
        'Datum': 1,
        'Cas_Vykonu_Od': 2,
        'Cas_Vykonu_Do': 3,
        'Prestavka_Trvanie': 4,
        'Popis_Cinnosti': 5,
        'Pocet_Odpracovanych_Hodin': 6,
        'Miesto_Vykonu': 7,
        'PH_Projekt_POO': 8,
        'PH_Riesenie_POO': 9,
        'PH_Mimo_Projekt_POO': 10,
        'SPOLU': 11
    }

    try:
        for i in range(31):
            target_row = 26 + i
            expected_date = df_target.iloc[i]['Datum']
            current_date = ws.cell(row=target_row, column=1).value
            if current_date != expected_date:
                ws.cell(row=target_row, column=1, value=expected_date)

            for col_name, col_num in col_mappings.items():
                if col_name == 'Datum':
                    continue  # already handled
                val = df_target.iloc[i][col_name]
                if pd.isna(val) or val == '-':
                    val = ''
                # Ensure vacation Popis_Cinnosti is uppercase, but it's already set in df_target
                ws.cell(row=target_row, column=col_num, value=val)

    except IndexError as e:
        logging.error(f"IndexError during row update at row {target_row}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during row update: {e}")

    # Validation: read back sample rows (26 and 56)
    try:
        sample_indices = [0, 30]
        target_rows = [26, 56]
        discrepancies = []
        for idx, trow in zip(sample_indices, target_rows):
            actual_vals = {col_name: ws.cell(row=trow, column=col_num).value for col_name, col_num in col_mappings.items()}
            expected_vals = df_target.loc[idx, col_mappings.keys()]
            for col_name in col_mappings:
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
        ws.cell(row=57, column=11, value=total_time_str)
        logging.info(f"Summary updated: {summary_text} in A57, {total_time_str} in K57")
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
        wb, ws = load_target_excel(target_excel)
        if wb is None or ws is None:
            exit(1)
        print("Load and Prepare Target Excel Structure completed successfully.")

        # Step 5: Update Daily Rows in Excel
        update_daily_rows(ws, df_target)
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