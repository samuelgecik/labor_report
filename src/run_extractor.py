# run_extractor.py

import csv

# 1. Import the extract_data function
from excel_extractor.main import extract_data

# --- Source File Extraction (ronec_dochadzka.xlsx) ---
print("--- Extracting from ronec_dochadzka.xlsx ---")


# Define new headers for CSV
headers = ['Datum', 'Dochadzka_Prichod', 'Dochadzka_Odchod', 'Prestavka_min', 'Prerusenie_Odchod', 'Prerusenie_Prichod', 'Skutocny_Odpracovany_Cas']

# The header row is dynamically located by searching for "Dátum".
# Call the extraction function with the specified parameters.
source_data = extract_data(
    file_path='data/input/ronec_dochadzka.xlsx',
    column_indices=[1, 2, 3, 4, 5, 6, 7],
    header_text="Dátum",
    header_row_offset=2
)

# Write to CSV
with open('extracted_source_with_headers.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers)
    for row in source_data:
        writer.writerow(row)

# Print the extracted data from the source file.
print(f"Extracted {len(source_data)} records and saved to extracted_source_with_headers.csv")
print("-" * 40)


# --- Target File Extraction (sadecky_vykaz.xlsx) ---
print("\n--- Extracting from sadecky_vykaz.xlsx ---")

# Define new headers for target CSV
target_headers = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']

# Define a strategy to find the start row.
# In this case, the data always begins at row 26.
start_row_strategy = lambda header_row: 26

# Define stop condition to halt extraction before including the "Spolu:" row
def stop_for_spolu(row_data):
    return len(row_data) > 4 and row_data[4] and "Spolu:" in str(row_data[4])

# Call the extraction function with the specified parameters.
target_data = extract_data(
    file_path='data/input/sadecky_vykaz.xlsx',
    column_indices=[1, 2, 3, 4, [5, 6, 7, 8], 9, 10, 11, 12, 13, 14],
    start_row_strategy=start_row_strategy,
    stop_condition=stop_for_spolu
)

# Write to CSV

# Write to CSV
with open('extracted_sadecky_target_with_headers.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(target_headers)
    for row in target_data:
        writer.writerow(row)

# Print the extracted data from the target file.
print(f"Extracted {len(target_data)} records and saved to extracted_sadecky_target_with_headers.csv")
print("-" * 40)