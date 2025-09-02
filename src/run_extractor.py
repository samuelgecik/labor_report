# run_extractor.py

import csv

# 1. Import the extract_data function
from excel_extractor.main import extract_data

# --- Source File Extraction (ronec_dochadzka.xlsx) ---
print("--- Extracting from ronec_dochadzka.xlsx ---")

# Define column mappings for the source file.
# 'date' corresponds to 'Dátum' and 'hours' to 'odpracovaný čas'.
source_mappings = {'date': ['Dátum'], 'hours': ['odpracovaný čas']}

# Define new headers for CSV
headers = ['Datum', 'Dochadzka_Prichod', 'Dochadzka_Odchod', 'Prestavka_min', 'Prerusenie_Odchod', 'Prerusenie_Prichod', 'Skutocny_Odpracovany_Cas']

# The data starts 2 rows below the header row.
header_row_offset = 2

# Call the extraction function with the specified parameters.
source_data = extract_data(
    file_path='ronec_dochadzka.xlsx',
    column_mappings=source_mappings,
    header_row_offset=header_row_offset,
    stop_condition=lambda val: isinstance(val, str),
    extract_range=True
)

# Write to CSV
with open('extracted_source_with_headers.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers)
    for row in source_data:
        writer.writerow(row.values())

# Print the extracted data from the source file.
print(f"Extracted {len(source_data)} records and saved to extracted_source_with_headers.csv")
print("-" * 40)


# --- Target File Extraction (ronec_vykaz.xlsx) ---
print("\n--- Extracting from ronec_vykaz.xlsx ---")

# Define column mappings for the target file.
# 'date' corresponds to 'Dátum' and 'total' to 'SPOLU'.
target_mappings = {'date': ['Dátum'], 'total': ['SPOLU']}

# Define new headers for target CSV
target_headers = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Od', 'Prestavka_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'Pocet_Hodin_Projekt_POO', 'Pocet_Hodin_Riesenie_POO', 'Pocet_Hodin_Projekt_POO', 'SPOLU']

# Define a strategy to find the start row.
# In this case, the data always begins at row 26.
start_row_strategy = lambda header_row: 26

# Call the extraction function with the specified parameters.
target_data = extract_data(
    file_path='ronec_vykaz.xlsx',
    column_mappings=target_mappings,
    extract_range=True,
    start_row_strategy=start_row_strategy,
    stop_condition=lambda val: val == 'Čestné vyhlásenie: '
)

# Write to CSV
with open('extracted_target_with_headers.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(target_headers)
    for row in target_data:
        writer.writerow(row.values())

# Print the extracted data from the target file.
print(f"Extracted {len(target_data)} records and saved to extracted_target_with_headers.csv")
print("-" * 40)