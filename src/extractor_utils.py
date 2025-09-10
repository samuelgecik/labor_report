"""
Reusable utility functions for extracting data from Excel workbooks.
"""

from typing import Dict, List, Any
from .excel_extractor.main import extract_data


# Strategy Registry for callable functions
STRATEGY_REGISTRY = {
    "fixed_26": lambda header_row: 26,
    "stop_for_spolu": lambda row_data: len(row_data) > 4 and row_data[4] and "Spolu:" in str(row_data[4])
}


def extract_from_workbook(config: Dict[str, Any]) -> Dict[str, List[List[Any]]]:
    """
    Extracts data from multiple sheets in a workbook based on configuration.

    Args:
        config: Dictionary containing extraction configuration with keys:
            - file_path: str - Path to the Excel file
            - sheets: str | list - Either "__ALL__" for all sheets,
                                    or a list of specific sheet names
            - column_indices: List of column specifications
            - header_text: str (optional) - Text to search for header row
            - header_row_offset: int (optional) - Offset from header row to start data
            - start_row_strategy: str (optional) - Key into STRATEGY_REGISTRY
            - stop_condition: str (optional) - Key into STRATEGY_REGISTRY
            - All other parameters supported by extract_data function

    Returns:
        Dict[str, List[List]]: Dictionary where keys are sheet names and values
                               are the extracted data for each sheet as list of lists
    """
    results = {}

    # Load the workbook to get sheet names
    from openpyxl import load_workbook
    wb = load_workbook(config['file_path'], read_only=True)

    # Determine which sheets to process
    sheets_to_process = []
    if config.get('sheets') == "__ALL__":
        sheets_to_process = wb.sheetnames
    elif isinstance(config['sheets'], list):
        sheets_to_process = config['sheets']
    else:
        # Single sheet name
        sheets_to_process = [config['sheets']]

    # Process each sheet
    for sheet_name in sheets_to_process:
        # Resolve strategy functions from registry
        start_strategy = STRATEGY_REGISTRY.get(config.get('start_row_strategy'))
        stop_condition = STRATEGY_REGISTRY.get(config.get('stop_condition'))

        # Prepare the arguments for extract_data
        extract_args = {
            'file_path': config['file_path'],
            'column_indices': config['column_indices'],
            'sheet_name': sheet_name
        }

        # Add optional parameters if provided
        if 'header_text' in config:
            extract_args['header_text'] = config['header_text']
        if 'header_row_offset' in config:
            extract_args['header_row_offset'] = config['header_row_offset']
        if start_strategy:
            extract_args['start_row_strategy'] = start_strategy
        if stop_condition:
            extract_args['stop_condition'] = stop_condition

        # Extract data from this sheet
        data = extract_data(**extract_args)

        # Store the results
        results[sheet_name] = data

    wb.close()  # Close the readonly workbook
    return results


def save_extraction_results(results: Dict[str, List[List[Any]]], config: Dict[str, Any]) -> None:
    """
    Saves extraction results to CSV files.

    Args:
        results: Dictionary of sheet names to extracted data
        config: Configuration dictionary containing 'output_prefix' for naming
    """
    import csv
    import os

    output_prefix = config.get('output_prefix', 'extracted_data')
    headers = config.get('headers', [])

    for sheet_name, data in results.items():
        # Create a safe filename from sheet name
        safe_sheet_name = sheet_name.replace(' ', '_').replace('/', '_')
        filename = f"{output_prefix}_{safe_sheet_name}.csv"
        output_path = os.path.join('data', 'output', filename)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if headers:
                writer.writerow(headers)
            writer.writerows(data)