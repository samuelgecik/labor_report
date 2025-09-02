# Knowledge Base: Excel Data Analysis and Transfer Process

## 1. Project Overview

This project involves the analysis and automated transfer of employee attendance data from a source Excel file (`ronec_dochadzka.xlsx`) to a target structured timesheet template (`ronec_vykaz.xlsx`). The focus is on extracting work hours and date information for the month of July 2025.

**Key Participants:**
- Employee: Simon Ronec
- Company: PERRY SOFT a.s.

**Objective:**
- Analyze file structures and data distributions
- Identify mapping relationships between source and target data
- Implement automated transfer of July attendance data
- Handle mixed data types and formatting issues

**Output Files:**
- Source: `ronec_dochadzka.xlsx` (attendance data)
- Target: `ronec_vykaz.xlsx` (formal timesheet template)
- Updated: `ronec_vykaz_updated.xlsx` (populated template)

## 2. File Structure Analysis

### Source File (`ronec_dochadzka.xlsx`)
- **Dimensions:** 50 rows × 8 columns
- **Language:** Slovak
- **Content Type:** Raw attendance records with timestamps and calculations
- **Period:** July 2025 (focused on dates indicating July)

**Key Elements Identified:**
- Company information (PERRY SOFT a.s.)
- Employee details (Simon Ronec)
- Date records with arrival times ("Príchod")
- Actual worked time calculations ("Skutočný odpracovaný čas")

### Target File (`ronec_vykaz.xlsx`)
- **Dimensions:** 72 rows × 14 columns
- **Language:** Slovak
- **Content Type:** Formal template form (appears as "Príloha č. 3")
- **Purpose:** Structured work hours reporting template
- **Initial State:** Mostly empty cells requiring population

### File Analysis Script Findings (`analyze_sheets.py`)
- Uses pandas for Excel file analysis
- Reads first sheet of each file
- Provides shape (rows/columns), column headers, first 5 rows, and data types
- Helps identify structural differences between source and target

## 3. Data Mapping Details

### Column Identification (`identify_columns.py`)
Developed a search script to locate key columns by text patterns:

**Source File (`ronec_dochadzka.xlsx`):**
- Searches for "Dátum" → Column B (used for date extraction)
- Searches for "odpracovaný čas" → Column H (used for hours extraction)

**Target File (`ronec_vykaz.xlsx`):**
- Searches for "Dátum" → Column A (used for date placement)
- Searches for "Počet odpracovaných hodín*" → Column I (used for hours placement)

### Transfer Mapping (`transfer_hours.py`)
**Source Data Location:**
- Data starts from row 7 (after headers in row 6)
- Column B (2): Date values
- Column H (8): Hours values

**Target Data Location:**
- Population begins at row 26 because rows 24-25 are merged cells
- Column A (1): Date placement
- Column I (9): Hours placement

**Data Flow:**
- Extract date/hours pairs from source rows 7+
- Filter for July 2025 records
- Map sequentially to target rows 26, 27, 28... (starting at row 26)

## 4. Technical Issues Identified

### Mixed Data Types (`debug_transfer.py`)
**Date Fields:**
- **String Format:** Some dates are text strings containing '.7.' (e.g., '15.7.2025')
- **DateTime Format:** Some dates are Python `datetime` objects
- **Mixed Representation:** Need to handle both formats uniformly for July filtering

**Hours Fields:**
- Various data types depending on original Excel formatting
- Inconsistent type representation across records

### Data Filtering Requirements
- **July Selection:** Must distinguish July records from other months
- **String Method:** Check for '.7.' substring in string dates
- **DateTime Method:** Check `datetime.month == 7` for datetime objects
- **Boundary Handling:** Non-continuous data rows requiring proper termination

### Excel Compatibility Issues
- openpyxl library used for Python-Excel integration
- Need to handle Slovak text encoding
- Date format conversions between string and datetime types

## 5. Solutions Implemented

### Automated Transfer Script (`transfer_hours.py`)
**Core Implementation:**
1. Load source and target workbooks using `openpyxl`
2. Iterate through rows starting from row 7
3. Extract date (column B) and hours (column H) values
4. Apply July filter using string matching ('.7.' in date)
5. Store filtered data pairs for transfer

**Data Transfer Process:**
1. Map each filtered record to target sheet
2. Date goes to Column A at target row (26 + record_index)
3. Hours goes to Column I at same target row
4. Maintains sequential mapping without gaps

**Error Handling & Validation:**
- Checks for missing date or hours values
- Terminates reading on empty cells (handles end-of-data)
- July filtering prevents incorrect record inclusion

### Debugging Script (`debug_transfer.py`)
**Purpose:** Detailed data type analysis
- Prints types for date and hours values
- Verifies July filtering logic
- Provides context around data start positions (rows 4-7 headers, 6+ data)
- Confirms column positions and value formats

### File Identification (`identify_columns.py`)
**Automatic Column Detection:**
- Searches for Slovak text patterns in both files
- Returns row/column coordinates for key fields
- Handles case where exact matches may vary between files
- Provides human-readable column letters (A, B, H, I)

## 6. Current Status

**Implementation Status:** ✅ Complete
- All analysis scripts developed and documented
- Automated transfer script fully functional
- Debug findings integrated into final solution
- Mixed data type issues resolved (July filtering implemented)

**Files Created:**
- `analyze_sheets.py`: Structure analysis tool
- `identify_columns.py`: Column mapping identifier
- `debug_transfer.py`: Data type debugging script
- `transfer_hours.py`: Production transfer implementation
- `ronec_vykaz_updated.xlsx`: Populated target file

**Key Achievements:**
- Successful handling of mixed string/datetime date formats
- Reliable July 2025 data filtering across data types
- Automated mapping from unstructured attendance data to formal template
- Preservation of existing target template structure during population

**Remaining Considerations:**
- Open questions from initial analysis still apply (date formats, employee mapping rules, transformation requirements)
- Template structure may need validation for complete compliance
- Potential future enhancements: time format standardization, totals calculation, validation rules

**Execution:**
Ready for production use. Running `transfer_hours.py` will generate updated timesheet with July attendance data transferred from source to target file.
## 7. Recent Modifications

### August 31, 2025: Direct Value Copy Enhancement in transfer_hours.py

**Modification Date:** August 31, 2025

**Description:** Modified transfer_hours.py to directly copy values from "odpracovaný čas" column to "Počet odpracovaných hodín*" column without using formulas

**Technical Details:**  
- Removed process_hours() call in data collection loop  
- Now directly appends raw hours value from source cell  
- Values are transferred as-is (e.g., time formats like 08:00:00) to target cells in ronec_vykaz_updated.xlsx  

**Test Results:** Script runs successfully, transfers 31 records with hours, direct value copy verified working