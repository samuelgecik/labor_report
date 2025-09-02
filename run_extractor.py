# run_extractor.py

# 1. Import the extract_data function
from excel_extractor.main import extract_data

# --- Source File Extraction (ronec_dochadzka.xlsx) ---
print("--- Extracting from ronec_dochadzka.xlsx ---")

# Define column mappings for the source file.
# 'date' corresponds to 'Dátum' and 'hours' to 'odpracovaný čas'.
source_mappings = {'date': ['Dátum'], 'hours': ['odpracovaný čas']}

# The data starts 2 rows below the header row.
header_row_offset = 2

# Call the extraction function with the specified parameters.
source_data = extract_data(
    file_path='ronec_dochadzka.xlsx',
    column_mappings=source_mappings,
    header_row_offset=header_row_offset,
    stop_condition=lambda val: isinstance(val, str)
)

# Print the extracted data from the source file.
print(f"Extracted {len(source_data)} records from ronec_dochadzka.xlsx")
print("-" * 40)


# --- Target File Extraction (ronec_vykaz.xlsx) ---
print("\n--- Extracting from ronec_vykaz.xlsx ---")

# Define column mappings for the target file.
# 'date' corresponds to 'Dátum' and 'hours' to 'Počet odpracovaných hodín'.
target_mappings = {'date': ['Dátum'], 'hours': ['Počet odpracovaných hodín']}

# Define a strategy to find the start row.
# In this case, the data always begins at row 26.
start_row_strategy = lambda header_row: 26

# Call the extraction function with the specified parameters.
target_data = extract_data(
    file_path='ronec_vykaz.xlsx',
    column_mappings=target_mappings,
    start_row_strategy=start_row_strategy,
    stop_condition=lambda val: val == 'Čestné vyhlásenie: '
)

# Print the extracted data from the target file.
print(f"Extracted {len(target_data)} records from ronec_vykaz.xlsx")
print("-" * 40)