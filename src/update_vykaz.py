import argparse
import shutil
from datetime import datetime
import pandas as pd
from openpyxl import load_workbook

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