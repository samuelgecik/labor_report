import argparse
import shutil
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import load_workbook

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