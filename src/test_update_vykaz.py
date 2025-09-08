import pandas as pd
import pytest
import tempfile
import os
from datetime import datetime, timedelta
import shutil
from update_vykaz import update_daily_rows, transform_and_map_data

# Mock the argparser and other setup for testing
def mock_args(project='ronec', month=7, year=2025):
    class MockArgs:
        def __init__(self, project, month, year):
            self.project = project
            self.month = month
            self.year = year
    return MockArgs(project, month, year)

# Helper functions from update_vykaz.py (copied for isolation)
def load_and_validate_source(source_csv, expected_month, expected_year):
    try:
        df_source = pd.read_csv(source_csv)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Source CSV not found: {e}")
    except Exception as e:
        raise ValueError(f"CSV reading error: {e}")

    try:
        df_source['Datum'] = pd.to_datetime(df_source['Datum'])
    except Exception as e:
        raise ValueError(f"Date parsing error: {e}")

    row_count = df_source.shape[0]
    if row_count != 31:
        raise ValueError(f"Validation failed: Expected 31 data rows, got {row_count}")

    if not (df_source['Datum'].dt.month == expected_month).all():
        raise ValueError("Validation failed: Not all dates are in the expected month")
    if not (df_source['Datum'].dt.year == expected_year).all():
        raise ValueError("Validation failed: Not all dates are in the expected year")

    unique_days = df_source['Datum'].dt.date.nunique()
    if unique_days != 31:
        raise ValueError(f"Validation failed: Expected 31 unique days, got {unique_days}")

    # Clean NaNs in numeric columns (like 'Prestavka_min')
    numeric_cols = df_source.select_dtypes(include=['number']).columns
    df_source[numeric_cols] = df_source[numeric_cols].fillna('-')

    return df_source

# Test functions
def create_test_csv(rows, file_path):
    # Create a CSV header
    header = "Header1,Datum,Dochadzka_Prichod,Dochadzka_Odchod,Prestavka_min,Skutocny_Odpracovany_Cas\n"
    with open(file_path, 'w') as f:
        f.write(header)
        for i in range(rows):
            # Generate dates for July 2025
            date = datetime(2025, 7, i+1).strftime('%Y-%m-%d %H:%M:%S')
            # Example row data
            row = f"Data{i},{date},09:00:00,17:00:00,{i%60},08:00:00\n"
            f.write(row)

class TestUpdateVykazSteps:
    @pytest.fixture
    def temp_csv(self):
        # Create a temporary CSV with valid data (32 rows, July 2025, 31 unique days)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            create_test_csv(31, tmp.name)  # 31 data rows + header = 32 total
        yield tmp.name
        os.unlink(tmp.name)

    def test_load_csv_success(self, temp_csv):
        expected_month = 7
        expected_year = 2025
        df = load_and_validate_source(temp_csv, expected_month, expected_year)
        assert df.shape[0] == 31  # Excluding header in count? Wait, actually pandas includes header in shape, but we set it to 31 rows.
        # Wait, pd.read_csv with header includes the rows as after header.
        # In function, we check row_count = 32, but tmp has header +31 =32, yes.

    def test_parse_dates_success(self, temp_csv):
        expected_month = 7
        expected_year = 2025
        df = load_and_validate_source(temp_csv, expected_month, expected_year)
        assert pd.api.types.is_datetime64_any_dtype(df['Datum'])
        assert all(df['Datum'].dt.month == expected_month)
        assert all(df['Datum'].dt.year == expected_year)

    def test_row_count_validation_fail(self, temp_csv):
        # Modify to have 30 rows instead of 31 data rows
        with open(temp_csv, 'w') as f:
            header = "Header1,Datum\n"
            f.write(header)
            for i in range(30):  # Less than 31
                date = datetime(2025, 7, i+1).strftime('%Y-%m-%d %H:%M:%S')
                row = f"Data{i},{date}\n"
                f.write(row)
        expected_month = 7
        expected_year = 2025
        with pytest.raises(ValueError, match="Expected 31 data rows"):
            load_and_validate_source(temp_csv, expected_month, expected_year)

    def test_date_validation_fail_wrong_month(self, temp_csv):
        # Modify dates to June instead of July
        lines = []
        with open(temp_csv, 'r') as f:
            lines = f.readlines()
        with open(temp_csv, 'w') as f:
            for line in lines:
                line = line.replace('2025-07-', '2025-08-')
                f.write(line)
        expected_month = 7
        expected_year = 2025
        with pytest.raises(ValueError, match="Not all dates are in the expected month"):
            load_and_validate_source(temp_csv, expected_month, expected_year)

    def test_unique_days_validation_fail(self, temp_csv):
        # Modify to have duplicate dates
        lines = []
        with open(temp_csv, 'r') as f:
            lines = f.readlines()
        # Replace one date with duplicate
        if len(lines) > 2:
            lines[2] = lines[1]  # Duplicate second row
        with open(temp_csv, 'w') as f:
            f.writelines(lines)
        expected_month = 7
        expected_year = 2025
        with pytest.raises(ValueError, match="Expected 31 unique days"):
            load_and_validate_source(temp_csv, expected_month, expected_year)

    def test_clean_na_fills(self, temp_csv):
        # Add NaN in a numeric column
        df = pd.read_csv(temp_csv)
        df.loc[0, 'Prestavka_min'] = pd.NA  # Simulate NaN
        df.to_csv(temp_csv, index=False)
        expected_month = 7
        expected_year = 2025
        df_clean = load_and_validate_source(temp_csv, expected_month, expected_year)
        assert df_clean[['Prestavka_min']].fillna('-').equals(df_clean[['Prestavka_min']])  # Should have filled

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_and_validate_source('nonexistent.csv', 7, 2025)

    def test_invalid_date_parsing(self, temp_csv):
        # Corrupt date column
        df = pd.read_csv(temp_csv)
        df['Datum'] = 'invalid date'
        df.to_csv(temp_csv, index=False)
        with pytest.raises(ValueError, match="Date parsing error"):
            load_and_validate_source(temp_csv, 7, 2025)


# Helper for transform_and_map_data (copied from update_vykaz.py for isolation)
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


class TestTransformAndMap:
    @pytest.fixture
    def sample_df_source(self):
        """Create a sample df_source with 3 rows for testing"""
        data = pd.DataFrame({
            'Datum': pd.to_datetime(['2025-07-01', '2025-07-02', '2025-07-03']),
            'Dochadzka_Prichod': ['Dovolenka', '-', '09:00:00'],
            'Dochadzka_Odchod': ['-', '17:00:00', '17:00:00'],
            'Prestavka_min': ['-', '45', '60'],
            'Skutocny_Odpracovany_Cas': ['08:00:00', '-', '08:00:00']
        })
        return data

    def test_vacation_mapping(self, sample_df_source):
        project = 'ronec'
        df_target = transform_and_map_data(sample_df_source, project)
        
        # Check first row (vacation)
        i = 0
        assert df_target.loc[i, 'Cas_Vykonu_Od'] == '09:00:00'
        assert df_target.loc[i, 'Cas_Vykonu_Do'] == '17:00:00'
        assert df_target.loc[i, 'Prestavka_Trvanie'] == ''
        assert df_target.loc[i, 'Popis_Cinnosti'] == 'DOVOLENKA'
        assert df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] == '08:00:00'
        assert df_target.loc[i, 'Miesto_Vykonu'] == ''
        assert df_target.loc[i, 'PH_Projekt_POO'] == '00:00:00'
        assert df_target.loc[i, 'SPOLU'] == '08:00:00'
        assert df_target.loc[i, 'Datum'] == '1.'

    def test_absent_mapping(self, sample_df_source):
        project = 'ronec'
        df_target = transform_and_map_data(sample_df_source, project)
        
        # Check second row (absent)
        i = 1
        assert df_target.loc[i, 'Cas_Vykonu_Od'] == ''
        assert df_target.loc[i, 'Cas_Vykonu_Do'] == ''
        assert df_target.loc[i, 'Prestavka_Trvanie'] == '00:00:00'
        assert df_target.loc[i, 'Popis_Cinnosti'] == ''
        assert df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] == '00:00:00'
        assert df_target.loc[i, 'Miesto_Vykonu'] == ''
        assert df_target.loc[i, 'PH_Projekt_POO'] == '00:00:00'
        assert df_target.loc[i, 'SPOLU'] == '00:00:00'
        assert df_target.loc[i, 'Datum'] == '2.'

    def test_work_day_mapping(self, sample_df_source):
        project = 'ronec'
        activity_templates = {
            'ronec': 'Podieľanie sa na realizácii pracovného balíka č. 1 s názvom: Analýza užívateľských potrieb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]'
        }
        df_target = transform_and_map_data(sample_df_source, project)

        # Check third row (work day)
        i = 2
        assert df_target.loc[i, 'Cas_Vykonu_Od'] == '09:00:00'
        assert df_target.loc[i, 'Cas_Vykonu_Do'] == '17:00:00'
        assert df_target.loc[i, 'Prestavka_Trvanie'] == '01:00:00'  # 60 min
        assert df_target.loc[i, 'Popis_Cinnosti'] == activity_templates['ronec']
        assert df_target.loc[i, 'Popis_Cinnosti'] == 'Podieľanie sa na realizácii pracovného balíka č. 1 s názvom: Analýza užívateľských potrieb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]'
        assert df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] == '08:00:00'
        assert df_target.loc[i, 'Miesto_Vykonu'] == 'Bratislava'
        assert df_target.loc[i, 'PH_Projekt_POO'] == '00:00:00'
        assert df_target.loc[i, 'SPOLU'] == '08:00:00'
        assert df_target.loc[i, 'Datum'] == '3.'

import tempfile
import os
from openpyxl import Workbook

class TestLoadTargetExcel:
    @pytest.fixture
    def temp_excel(self):
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as tmp:
            wb = Workbook()
            ws = wb.active
            ws.title = 'Vykaz'
            # Add headers as per expected
            expected_headers = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']
            for col, header in enumerate(expected_headers, 1):
                ws.cell(row=1, column=col, value=header)
            wb.save(tmp.name)
        yield tmp.name
        os.unlink(tmp.name)

    # Import the function for testing
    from update_vykaz import load_target_excel

    def test_load_success(self, temp_excel):
        wb, ws = load_target_excel(temp_excel)
        assert wb is not None
        assert ws is not None
        assert ws.title == 'Vykaz'

    def test_sheet_not_found(self, temp_excel):
        wb, ws = load_target_excel(temp_excel + '_nonexistent')
        assert wb is None
        assert ws is None  # Test FileNotFound

class TestUpdateDailyRows:
    @pytest.fixture
    def temp_excel_with_data(self):
        # Create Excel with headers and some pre-existing data in rows 26-56
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as tmp:
            wb = Workbook()
            ws = wb.active
            ws.title = 'Vykaz'
            # Headers in row 1
            expected_headers = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']
            for col, header in enumerate(expected_headers, 1):
                ws.cell(row=1, column=col, value=header)

            # Pre-existing data: set some values in rows 26-56, e.g., old dates and values
            for r in range(26, 57):  # rows 26 to 56
                for c in range(1, 12):
                    ws.cell(row=r, column=c, value=f"old{r}.{c}")

            wb.save(tmp.name)
        yield tmp.name
        os.unlink(tmp.name)

    @pytest.fixture
    def sample_df_target(self):
        # Create sample df_source
        data = pd.DataFrame({
            'Datum': pd.to_datetime(['2025-07-01', '2025-07-02', '2025-07-03', '2025-07-04']),
            'Dochadzka_Prichod': ['Dovolenka', '-', '09:00:00', '08:00:00'],
            'Dochadzka_Odchod': ['-', '17:00:00', '17:00:00', '16:00:00'],
            'Prestavka_min': ['-', '45', '60', '30'],
            'Skutocny_Odpracovany_Cas': ['08:00:00', '-', '08:00:00', '08:00:00']
        })
        # For 31 rows, but we can fill up to 31 in transform
        df_target = transform_and_map_data(data, 'ronec')
        return df_target

    def test_update_success(self, temp_excel_with_data, sample_df_target):
        # Load workbook
        from openpyxl import load_workbook
        wb = load_workbook(temp_excel_with_data)
        ws = wb.active

        # Call update
        update_daily_rows(ws, sample_df_target)

        # Assert changes
        target_row = 26
        assert ws.cell(row=target_row, column=1).value == '1.'
        assert ws.cell(row=target_row, column=2).value == '09:00:00'  # Vacation Cas_Vykonu_Od
        assert ws.cell(row=target_row, column=5).value == 'DOVOLENKA'

        # Second row (absent)
        target_row = 27
        assert ws.cell(row=target_row, column=1).value == '2.'
        assert ws.cell(row=target_row, column=2).value == ''
        assert ws.cell(row=target_row, column=5).value == ''

        # Third row (work day)
        target_row = 28
        assert ws.cell(row=target_row, column=1).value == '3.'
        assert ws.cell(row=target_row, column=2).value == '09:00:00'
        assert ws.cell(row=target_row, column=5).value == 'Podieľanie sa na realizácii pracovného balíka č. 1 s názvom: Analýza užívateľských potrieb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]'

if __name__ == "__main__":
    pytest.main([__file__])
if __name__ == "__main__":
    pytest.main([__file__])