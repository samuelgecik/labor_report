from openpyxl import load_workbook

def _find_cell_by_text(sheet, search_texts):
    """
    Finds the first cell containing any of the provided search texts.
    
    Args:
        sheet: The openpyxl sheet object.
        search_texts: List of strings to search for.
    
    Returns:
        Tuple (row, col) 1-based if found, else None.
    """
    for row in sheet.iter_rows():
        for cell in row:
            if cell.value:
                cell_str = str(cell.value).lower()
                for text in search_texts:
                    if text.lower() in cell_str:
                        return (cell.row, cell.column)
    return None

def _get_real_cell_value(sheet, row, col):
    """
    Gets the value of a cell, handling merged cells correctly.
    
    Args:
        sheet: The openpyxl sheet object.
        row, col: 1-based indices.
    
    Returns:
        The cell value or the value from the top-left of merged range.
    """
    cell = sheet.cell(row, col)
    for merged_range in sheet.merged_cells.ranges:
        if (merged_range.min_row <= row <= merged_range.max_row and
            merged_range.min_col <= col <= merged_range.max_col):
            return sheet.cell(merged_range.min_row, merged_range.min_col).value
    return cell.value

def extract_data(
    file_path: str,
    header_text: str | None = None,
    start_row_strategy=None,
    header_row_offset: int = 1,
    stop_condition: callable = None,
    extract_range: bool = False,
    num_columns: int | None = None
) -> list[dict]:
    """
    Extracts data from an Excel sheet.

    This function extracts all columns using the header values from the header row as dictionary keys.
    The header row can be dynamically located by searching for a specific text.

    Args:
        file_path (str): The path to the Excel file.
        header_text (str, optional): Text to search for to locate the header row.
            If provided, the function finds the row containing this text and uses it as the header row.
            If not provided, defaults to row 1.
        start_row_strategy (callable, optional): A function that takes the
            header row number as input and returns the starting row for data
            extraction. If None, the default strategy is to start from the
            row immediately after the header row.
        header_row_offset (int, optional): The number of rows to skip after
            the header row to find the start of the data. Defaults to 1.
        stop_condition (callable, optional): A function that takes the value
            of the cell in the column identified as 'date' (or first column if not found)
            as input and returns True if the extraction loop should terminate.
            Defaults to None.
        extract_range (bool, optional): If True, extracts all columns between
            the minimum and maximum found, with keys as column indices ('0', '1', ...).
            If False, uses header values as keys. Defaults to False.
        num_columns (int, optional): Number of columns to extract. If provided,
            extracts from min_col to min_col + num_columns. If None, extracts
            all columns to sheet.max_column. Defaults to None.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary
            represents a row of data with header values as keys if extract_range is False,
            otherwise with keys as column index strings ('0', '1', etc.).
    """
    wb = load_workbook(file_path, data_only=True)
    sheet = wb.active

    pos = None

    # Determine header_row
    if header_text:
        pos = _find_cell_by_text(sheet, [header_text])
        print(f"DEBUG: _find_cell_by_text('{header_text}') result: {pos}")
        if pos:
            header_row = pos[0]
            print(f"DEBUG: header_row set to {header_row}")
        else:
            header_row = 1  # fallback if text not found
            print(f"DEBUG: '{header_text}' not found, defaulted header_row to 1")
    else:
        header_row = 1
    column_headers = []
    date_column_idx = 1
    if pos:
        date_column_idx = pos[1]
    for col_idx in range(1, sheet.max_column + 1):
        header = sheet.cell(header_row, col_idx).value
        if header and 'date' in str(header).lower() and not pos:
            date_column_idx = col_idx
        key = str(header).strip() if header else f'col_{col_idx}'
        column_headers.append(key)

    # Determine start row
    if start_row_strategy:
        start_row = start_row_strategy(header_row)
        print(f"DEBUG: start_row set via strategy: {start_row}")
    else:
        start_row = header_row + header_row_offset
        print(f"DEBUG: start_row calculated as header_row ({header_row}) + offset ({header_row_offset}) = {start_row}")

    # Extract data
    data = []
    row = start_row
    while row <= sheet.max_row:
        row_data = {}
        valid_row = False
        if extract_range:
            min_col = pos[1] if pos else 1
            if num_columns is None:
                max_col = sheet.max_column
            else:
                max_col = min_col + num_columns - 1
            print(f"DEBUG: For row {row}, starting column loop from index {min_col} to {max_col}")
            for col_idx in range(min_col, max_col + 1):
                value = _get_real_cell_value(sheet, row, col_idx)
                key = str(col_idx - min_col)
                row_data[key] = value
                if value is not None:
                    valid_row = True
        else:
            for col_idx in range(1, sheet.max_column + 1):
                value = _get_real_cell_value(sheet, row, col_idx)
                key = column_headers[col_idx - 1]
                row_data[key] = value
                if value is not None:
                    valid_row = True
        # Check stopping condition
        date_val = _get_real_cell_value(sheet, row, date_column_idx) if date_column_idx else None
        print(f"DEBUG: row {row}, date_val: {date_val} type: {type(date_val)}")
        if not date_val or (stop_condition and stop_condition(date_val)):
            print(f"DEBUG: Break condition met at row {row}")
            break
        if valid_row:
            data.append(row_data)
            if row == start_row:
                print(f"DEBUG: Extracted row_data keys: {list(row_data.keys())}")
        row += 1

    return data