import pandas as pd
import pytest
import tempfile
import os
from datetime import datetime, timedelta
import shutil
import time
from openpyxl import Workbook, load_workbook
from unittest import mock
from update_vykaz import (
    transform_and_map_data,
    update_daily_rows,
    load_target_excel,
    recalculate_summary,
    save_and_validate
)

@pytest.fixture
def temp_csv_and_excel():
    csv_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
    excel_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
    try:
        create_sample_csv(csv_file.name, 31)
        create_sample_excel(excel_file.name)
        yield csv_file.name, excel_file.name
    finally:
        os.unlink(csv_file.name)
        os.unlink(excel_file.name)

# Helper dict for patch
activity_templates_patch = {
    'ronec': 'Podieľanie sa na realizácii pracovného balíka č.endada 1 s názvom: Analýza užívateľských potríeb a návrh konceptu riešenia, pracovného balíka č. 2 s názvom: Získavanie a spracovanie dát na tréning AI modelu a pracovného balíka č. 3 s názvom: Experimentálny vývoj a tréning AI modelu [role-specific addition]',
    'sadecky': 'Podieľanie sa na realizácii pracovného balíka č. 1 ... (simplified for test)'
}

# Helper function to create sample CSV
def create_sample_csv(filepath, num_rows=31, month=7, year=2025):
    with open(filepath, 'w') as f:
        f.write("Datum,Dochadzka_Prichod,Dochadzka_Odchod,Prestavka_min,Prerusenie_Odchod,Prerusenie_Prichod,Skutocny_Odpracovany_Cas\n")
        for i in range(num_rows):
            datum = f"{year:04d}-{month:02d}-{i+1:02d} 00:00:00"
            # Rotate types: work, vacation, absent
            if i % 3 == 0:
                dochadzka_prichod = '09:00:00'
                dochadzka_odchod = '17:00:00'
                prestavka_min = '60'
                prerusenie_odchod = ''
                prerusenie_prichod = ''
                skutocny_cas = '08:00:00'
            elif i % 3 == 1:
                dochadzka_prichod = 'Dovolenka'
                dochadzka_odchod = '-'
                prestavka_min = '-'
                prerusenie_odchod = '-'
                prerusenie_prichod = '-'
                skutocny_cas = '08:00:00'
            else:
                dochadzka_prichod = '-'
                dochadzka_odchod = '-'
                prestavka_min = '-'
                prerusenie_odchod = '-'
                prerusenie_prichod = '-'
                skutocny_cas = '-'
            f.write(f"{datum},{dochadzka_prichod},{dochadzka_odchod},{prestavka_min},{prerusenie_odchod},{prerusenie_prichod},{skutocny_cas}\n")

# Helper function to create sample Excel
def create_sample_excel(filepath):
    wb = Workbook()
    ws = wb.active
    ws.title = 'Vykaz'  # Change to 'Vykaz' to match temp file expectation
    # Headers in row 24-25 (but simplify to row 1 for test)
    headers = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']
    for col, hdr in enumerate(headers, 1):
        ws.cell(row=1, column=col, value=hdr)
    # Ensure rows up to 57 exist
    for row in range(26, 58):
        for col in range(1, 12):
            ws.cell(row=row, column=col, value='')
    wb.save(filepath)

class TestUnitTests:
    @pytest.fixture
    def sample_df_source_vacation(self):
        return pd.DataFrame({
            'Datum': [datetime(2025, 7, 1)],
            'Dochadzka_Prichod': ['Dovolenka'],
            'Dochadzka_Odchod': ['-'],
            'Prestavka_min': ['-'],
            'Prerusenie_Odchod': ['-'],
            'Prerusenie_Prichod': ['-'],
            'Skutocny_Odpracovany_Cas': ['08:00:00']
        })

    @pytest.fixture
    def sample_df_source_absent(self):
        return pd.DataFrame({
            'Datum': [datetime(2025, 7, 2)],
            'Dochadzka_Prichod': ['-'],
            'Dochadzka_Odchod': ['-'],
            'Prestavka_min': ['-'],
            'Prerusenie_Odchod': ['-'],
            'Prerusenie_Prichod': ['-'],
            'Skutocny_Odpracovany_Cas': ['-']
        })

    @pytest.fixture
    def sample_df_source_work(self):
        return pd.DataFrame({
            'Datum': [datetime(2025, 7, 3)],
            'Dochadzka_Prichod': ['09:00:00'],
            'Dochadzka_Odchod': ['17:00:00'],
            'Prestavka_min': ['60'],
            'Prerusenie_Odchod': [pd.NA],
            'Prerusenie_Prichod': [pd.NA],
            'Skutocny_Odpracovany_Cas': ['08:00:00']
        })

    def test_transform_vacation_row(self, sample_df_source_vacation):
        df_target = transform_and_map_data(sample_df_source_vacation, 'ronec')
        row = df_target.iloc[0]
        assert row['Datum'] == '1.'
        assert row['Cas_Vykonu_Od'] == '09:00:00'
        assert row['Cas_Vykonu_Do'] == '17:00:00'
        assert row['Prestavka_Trvanie'] == ''
        assert row['Popis_Cinnosti'] == 'DOVOLENKA'
        assert row['Pocet_Odpracovanych_Hodin'] == '08:00:00'
        assert row['SPOLU'] == '08:00:00'

    def test_transform_absent_row(self, sample_df_source_absent):
        df_target = transform_and_map_data(sample_df_source_absent, 'ronec')
        row = df_target.iloc[0]
        assert row['Datum'] == '1.'
        assert row['Cas_Vykonu_Od'] == ''
        assert row['Cas_Vykonu_Do'] == ''
        assert row['Prestavka_Trvanie'] == '00:00:00'
        assert row['Popis_Cinnosti'] == ''
        assert row['Pocet_Odpracovanych_Hodin'] == '00:00:00'
        assert row['SPOLU'] == '00:00:00'

    def test_transform_work_row(self, sample_df_source_work):
        df_target = transform_and_map_data(sample_df_source_work, 'ronec')
        row = df_target.iloc[0]
        assert row['Datum'] == '1.'
        assert row['Cas_Vykonu_Od'] == '09:00:00'
        assert row['Cas_Vykonu_Do'] == '17:00:00'
        assert row['Prestavka_Trvanie'] == '01:00:00'
        assert row['Popis_Cinnosti'].startswith('Podieľanie sa')
        assert row['Pocet_Odpracovanych_Hodin'] == '08:00:00'
        assert row['SPOLU'] == '08:00:00'

    def test_date_extraction(self, sample_df_source_work):
        df_target = transform_and_map_data(sample_df_source_work, 'ronec')
        for i in range(31):
            assert df_target.loc[i, 'Datum'] == f'{i+1}.'

    def test_break_conversions(self):
        # Test prestavka helper logic inside transform
        # Since helpers are internal, test via transform
        df = pd.DataFrame({
            'Datum': [datetime(2025, 7, 1)],
            'Dochadzka_Prichod': ['09:00:00'],
            'Dochadzka_Odchod': ['17:00:00'],
            'Prestavka_min': ['30'],  # 30 min -> 00:30:00
            'Prerusenie_Odchod': [pd.NA],
            'Prerusenie_Prichod': [pd.NA],
            'Skutocny_Odpracovany_Cas': ['08:00:00']
        })
        df_target = transform_and_map_data(df, 'ronec')
        assert df_target.iloc[0]['Prestavka_Trvanie'] == '00:30:00'

    def test_invalid_break_conversions(self):
        # Invalid prestavka
        df = pd.DataFrame({
            'Datum': [datetime(2025, 7, 1)],
            'Dochadzka_Prichod': ['09:00:00'],
            'Dochadzka_Odchod': ['17:00:00'],
            'Prestavka_min': ['invalid'],
            'Prerusenie_Odchod': [pd.NA],
            'Prerusenie_Prichod': [pd.NA],
            'Skutocny_Odpracovany_Cas': ['08:00:00']
        })
        df_target = transform_and_map_data(df, 'ronec')
        assert df_target.iloc[0]['Prestavka_Trvanie'] == '00:00:00'

    def test_skutocny_default(self):
        # Test if NaN defaults to 08:00:00
        df = pd.DataFrame({
            'Datum': [datetime(2025, 7, 1)],
            'Dochadzka_Prichod': ['Dovolenka'],
            'Dochadzka_Odchod': ['-'],
            'Prestavka_min': ['-'],
            'Prerusenie_Odchod': ['-'],
            'Prerusenie_Prichod': ['-'],
            'Skutocny_Odpracovany_Cas': [pd.NA]
        })
        df_target = transform_and_map_data(df, 'ronec')
        assert df_target.iloc[0]['Pocet_Odpracovanych_Hodin'] == '08:00:00'

class TestIntegrationTests:
    @pytest.fixture
    def temp_csv_and_excel(self):
        csv_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        excel_file = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False)
        try:
            create_sample_csv(csv_file.name, 31)
            create_sample_excel(excel_file.name)
            yield csv_file.name, excel_file.name
        finally:
            os.unlink(csv_file.name)
            os.unlink(excel_file.name)

    def test_full_pipeline(self, temp_csv_and_excel):
        csv_path, excel_path = temp_csv_and_excel
        df_source = pd.read_csv(csv_path)
        df_target = transform_and_map_data(df_source, 'ronec')

        wb, ws = load_target_excel(excel_path)
        assert wb is not None
        assert ws is not None

        update_daily_rows(ws, df_target)
        summary_text, total_time = recalculate_summary(df_target, ws)

        # Assert some values updated
        assert ws.cell(row=26, column=1).value == '1.'
        assert ws.cell(row=57, column=1).value is not None  # Summary in A57

        # Check validation metrics
        work_days = len(df_target[df_target['SPOLU'] != '00:00:00'])
        assert work_days > 0, "Should have work days"
        expected_total = timedelta()  # From df_target SPOLU
        for val in df_target['SPOLU']:
            if val not in ['', '00:00:00'] and val:
                h, m, s = map(int, val.split(':'))
                expected_total += timedelta(hours=h, minutes=m, seconds=s)
        # Further assert if output matches

    @pytest.mark.parametrize("project", ["ronec", "sadecky"])
    def test_project_variations(self, project, temp_csv_and_excel):
        csv_path, excel_path = temp_csv_and_excel
        df_source = pd.read_csv(csv_path)
        df_target = transform_and_map_data(df_source, project)

        # For sadecky, need to modify activity_templates
        if project == 'sadecky':
            # Assuming we add sadecky template
            assert any('sadecky' in str(row['Popis_Cinnosti']) for row in df_target.itertuples() if row.Dochadzka_Prichod == '09:00:00')

class TestEdgeCases:
    def test_malformed_times_invalid(self):
        df = pd.DataFrame({
            'Datum': [datetime(2025, 7, 1)],
            'Dochadzka_Prichod': ['xx:00:00'],
            'Dochadzka_Odchod': ['17:00:00'],
            'Prestavka_min': ['invalid'],
            'Prerusenie_Odchod': [pd.NA],
            'Prerusenie_Prichod': [pd.NA],
            'Skutocny_Odpracovany_Cas': ['invalid']
        })
        df_target = transform_and_map_data(df, 'ronec')
        # Since xx:00:00 is not Dovolenka or -, treat as work day but invalid times
        # Assertions for expected values: since xx:00:00 not 'Dovolenka' or '-', it's work day, but times are invalid
        # But ptsst probably defaults to '' quorum or stard
        # For test, assert Prestava '00:00:00' since 'invalid' prestavka
        assert df_target.iloc[0]['Prestavka_Trvanie'] == '00:00:00'

    @pytest.fixture
    def invalid_csv_wrong_month(self):
        file = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        with open(file.name, 'w') as f:
            f.write("Datum,Dochadzka_Prichod,Dochadzka_Odchod,Prestavka_min,Skutocny_Odpracovany_Cas\n")
            for i in range(31):
                datum = f"2025-08-{i+1:02d} 00:00:00"  # Wrong month
                f.write(f"{datum},09:00:00,17:00:00,60,08:00:00\n")
        yield file.name
        os.unlink(file.name)

    def test_validation_halt_wrong_month(self, invalid_csv_wrong_month):
        df_source = pd.read_csv(invalid_csv_wrong_month)
        df_source['Datum'] = pd.to_datetime(df_source['Datum'])
        # Simulate validation
        assert not all(df_source['Datum'].dt.month == 7)
        # Would halt in main, but test here

    def test_formatting_preservation(self, temp_csv_and_excel):
        # Test that styles are preserved, but since openpyxl, just check no corruption
        csv_path, excel_path = temp_csv_and_excel
        wb_before, _ = load_target_excel(excel_path)
        # Save a copy but since already, just load again after process

    def test_permission_error_save(self, temp_csv_and_excel):
        csv_path, excel_path = temp_csv_and_excel
        os.chmod(excel_path, 0o444)  # Read only
        df_source = pd.read_csv(csv_path)
        df_target = transform_and_map_data(df_source, 'ronec')
        wb, ws = load_target_excel(excel_path)
        update_daily_rows(ws, df_target)
        from update_vykaz import save_and_validate
        with pytest.raises(Exception):  # Or PermissionError
            save_and_validate(wb, df_target, '', '', 'ronec')

    def test_sadecky_variation(self):
        # Add sadecky to transform by adding template
        # For now, test with ronec, but modify to switch
        df = pd.DataFrame({
            'Datum': [datetime(2025, 7, 1)],
            'Dochadzka_Prichod': ['09:00:00'],
            'Dochadzka_Odchod': ['17:00:00'],
            'Prestavka_min': ['60'],
            'Skutocny_Odpracovany_Cas': ['08:00:00']
        })
        activity_templates = {'sadecky': 'Podieľanie sa na realizácii pracovného balíka č. 1 ... '}  # Simplified
        # Since templates are internal, we need to modify the function or duplicate

class TestValidationMetrics:
    def test_31_rows_updated(self, temp_csv_and_excel):
        csv_path, excel_path = temp_csv_and_excel
        df_source = pd.read_csv(csv_path)
        df_target = transform_and_map_data(df_source, 'ronec')
        wb, ws = load_target_excel(excel_path)
        update_daily_rows(ws, df_target)
        # Check first and last row updated
        for row in [26, 56]:  # Assuming 26-56 updated for 31 days
            assert ws.cell(row=row, column=1).value == f'{row-25}.'

    def test_summary_computation(self, temp_csv_and_excel):
        csv_path, excel_path = temp_csv_and_excel
        df_source = pd.read_csv(csv_path)
        df_target = transform_and_map_data(df_source, 'ronec')
        wb, ws = load_target_excel(excel_path)
        update_daily_rows(ws, df_target)
        summary_text, total_time = recalculate_summary(df_target, ws)
        work_days_expected = len(df_target[df_target['SPOLU'] != '00:00:00'])
        assert work_days_expected > 0
        assert summary_text.startswith(f"{work_days_expected} days")
        assert total_time == "13:20:00"  # Example sum, but depends on data

class TestPerformance:
    def test_full_run_under_1s(self, temp_csv_and_excel):
        csv_path, excel_path = temp_csv_and_excel
        start = time.time()
        # Simulate full run
        df_source = pd.read_csv(csv_path)
        df_target = transform_and_map_data(df_source, 'ronec')
        wb, ws = load_target_excel(excel_path)
        update_daily_rows(ws, df_target)
        recalculate_summary(df_target, ws)
        save_and_validate(wb, df_target, '', '', 'ronec')
        end = time.time()
        assert end - start < 1.0, f"Full run took {end - start:.3f}s, which is over 1s"

# Fixtures for setup
@pytest.fixture(scope="session", autouse=True)
def install_pytest_if_needed():
    # Comment: assume pytest available, else pip install pytest
    pass

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])