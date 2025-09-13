import argparse
import shutil
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
from openpyxl import load_workbook

from src.extractor_utils import extract_from_workbook, open_workbooks
from src import sheet_mapper

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for updating labor report workbook."""
    parser = argparse.ArgumentParser(description="Update labor report workbook")
    parser.add_argument("--source-excel", required=True, 
                       help="Path to source attendance/work log Excel file")
    parser.add_argument("--target-excel", required=True, 
                       help="Path to target labor report Excel file")
    parser.add_argument("--month", type=str, default=None, 
                       help="Month name in Slovak (e.g., 'júl')")
    parser.add_argument("--year", type=int, default=None, 
                       help="Year (e.g., 2025)")
    parser.add_argument("--activity-text", type=str, default=None,
                       help="Activity description text to use")
    parser.add_argument("--work-location", type=str, default="Bratislava",
                       help="Work location (default: Bratislava)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Run validation only; do not write changes")
    parser.add_argument("--output-dir", default="data/output", 
                       help="Directory for output files")
    parser.add_argument("--no-clean-target", action="store_true", default=False,
                       help="Skip removing unmatched target sheets")
    parser.add_argument("--no-sort-target", action="store_true", default=False,
                       help="Skip sorting target sheets based on source sheet order")
    return parser.parse_args()


def extract_source_data(source_excel: str, sheet_name: str = None) -> pd.DataFrame:
    """Extract source data from Excel file using extractor_utils."""
    # Use the source strategy directly from STRATEGY_REGISTRY
    from src.extractor_utils import STRATEGY_REGISTRY
    
    source_strategy = STRATEGY_REGISTRY["source"]
    config = {
        'file_path': source_excel,
        'sheets': [sheet_name] if sheet_name else "__ALL__",  # Extract from specific sheet or all sheets
        'column_indices': source_strategy["column_indices"],
        'header_text': source_strategy["header_text"],
        'header_row_offset': source_strategy["header_row_offset"],
        'start_row_strategy': source_strategy["start_row_strategy"],
        'stop_condition': source_strategy["stop_condition"]
    }
    
    results = extract_from_workbook(config)
    
    # For now, use the first sheet's data (or specified sheet)
    if not results:
        raise ValueError(f"No data extracted from {source_excel}")
    
    # Get the target sheet's data
    if sheet_name and sheet_name in results:
        data = results[sheet_name]
    else:
        sheet_name = list(results.keys())[0]
        data = results[sheet_name]
    
    # Convert to DataFrame with expected column names
    columns = ['Datum', 'Dochadzka_Prichod', 'Dochadzka_Odchod', 'Prestavka_min', 
               'Prerusenie_Odchod', 'Prerusenie_Prichod', 'Skutocny_Odpracovany_Cas']
    
    df = pd.DataFrame(data, columns=columns)
    
    # Clean and process data
    df['Skutocny_Odpracovany_Cas'] = df['Skutocny_Odpracovany_Cas'].astype(str).str.strip().replace(' -', '-', regex=False)
    df['Prestavka_min'] = df['Prestavka_min'].astype(str).str.strip().replace(' -', '-', regex=False)
    
    # Parse dates
    df['Datum'] = pd.to_datetime(df['Datum'], errors='coerce')
    
    # Clean NaN values
    numeric_cols = df.select_dtypes(include=['number']).columns
    df[numeric_cols] = df[numeric_cols].fillna('-')
    
    return df


def source_to_target(df_source: pd.DataFrame, activity_text: str, work_location: str) -> pd.DataFrame:
    """Transform source data to target format."""
    cols = ['Datum', 'Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Prestavka_Trvanie', 
            'Popis_Cinnosti', 'Pocet_Odpracovanych_Hodin', 'Miesto_Vykonu', 
            'PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO', 'SPOLU']
    
    df_target = pd.DataFrame(columns=cols, index=range(31))
    
    # Extract day numbers for all 31 days
    df_target['Datum'] = [str(i + 1) + '.' for i in range(31)]
    
    def get_prestavka(row):
        p_min = row['Prestavka_min']
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
    
    # Default activity text if none provided
    if not activity_text:
        activity_text = "Pracovná činnosť"
    
    for i in range(31):
        if i < len(df_source):
            row = df_source.iloc[i]
            dochadzka = row['Dochadzka_Prichod']
        else:
            dochadzka = '-'
            row = None
        
        # Compact template-based approach
        templates = {
            'vacation': {'Popis_Cinnosti': 'DOVOLENKA', 'Pocet_Odpracovanych_Hodin': {row['Skutocny_Odpracovany_Cas']}, 'SPOLU': {row['Skutocny_Odpracovany_Cas']}},
            'absent': {'Prestavka_Trvanie': '00:00:00'},
            'weekend': {'Prestavka_Trvanie': '00:00:00'}
        }
        
        # Set defaults for all non-work days
        zero_fields = ['PH_Projekt_POO', 'PH_Riesenie_POO', 'PH_Mimo_Projekt_POO']
        empty_fields = ['Cas_Vykonu_Od', 'Cas_Vykonu_Do', 'Miesto_Vykonu', 'Popis_Cinnosti']
        
        # Determine day type
        if dochadzka == 'Dovolenka':
            day_type = 'vacation'
        elif (dochadzka == '-' or pd.isna(dochadzka) or 
              (row is not None and not pd.isna(row['Datum']) and 
               all(pd.isna(row[col]) or str(row[col]).strip() == '-' 
                   for col in ['Dochadzka_Prichod', 'Dochadzka_Odchod', 'Prestavka_min', 
                               'Prerusenie_Odchod', 'Prerusenie_Prichod', 'Skutocny_Odpracovany_Cas']))):
            day_type = 'weekend' if (row is not None and not pd.isna(row['Datum'])) else 'absent'
        else:
            day_type = 'work'
        
        if day_type != 'work':
            # Apply non-work template
            for field in zero_fields:
                df_target.loc[i, field] = '00:00:00'
            for field in empty_fields:
                df_target.loc[i, field] = ''
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = '00:00:00'
            # Apply specific template overrides
            for field, value in templates.get(day_type, {}).items():
                df_target.loc[i, field] = value
            logging.info(f"Applied {day_type} template to row {i}")
        else:
            # Work day
            worked_hours = row['Skutocny_Odpracovany_Cas']
            df_target.loc[i, 'Cas_Vykonu_Od'] = row['Dochadzka_Prichod']
            df_target.loc[i, 'Cas_Vykonu_Do'] = row['Dochadzka_Odchod']
            df_target.loc[i, 'Prestavka_Trvanie'] = get_prestavka(row)
            df_target.loc[i, 'Popis_Cinnosti'] = activity_text
            df_target.loc[i, 'Pocet_Odpracovanych_Hodin'] = worked_hours
            df_target.loc[i, 'Miesto_Vykonu'] = work_location
            for field in zero_fields:
                df_target.loc[i, field] = '00:00:00'
            df_target.loc[i, 'SPOLU'] = worked_hours
            logging.info(f"Applied work template to row {i}")
    
    return df_target


def update_daily_rows(ws, df_target: pd.DataFrame, data_start_row: int):
    """Update daily rows in the target worksheet."""
    col_mappings = {
        'Datum': 1,
        'Cas_Vykonu_Od': 2,
        'Cas_Vykonu_Do': 3,
        'Prestavka_Trvanie': 4,
        'Popis_Cinnosti': 5,
        'Pocet_Odpracovanych_Hodin': 9,
        'Miesto_Vykonu': 10,
        'PH_Projekt_POO': 11,
        'PH_Riesenie_POO': 12,
        'PH_Mimo_Projekt_POO': 13,
        'SPOLU': 14
    }
    
    # Store original merged ranges to restore later
    original_ranges = list(ws.merged_cells.ranges)
    unmerged_coords = set()
    
    try:
        for i in range(31):
            target_row = data_start_row + i
            
            # Unmerge cells for this row if needed
            for merged_range in original_ranges:
                if merged_range.min_row <= target_row <= merged_range.max_row:
                    coord = merged_range.coord
                    if coord not in unmerged_coords:
                        ws.unmerge_cells(coord)
                        unmerged_coords.add(coord)
                        logging.debug(f"Unmerging {coord} for row {target_row}")
            
            # Update cells
            for col_name, col_num in col_mappings.items():
                val = df_target.iloc[i][col_name]
                if pd.isna(val) or val == '-':
                    val = ''
                ws.cell(row=target_row, column=col_num, value=val)
                
                # Clear merged cells for description if it has content
                if col_name == 'Popis_Cinnosti' and val != '':
                    for c in [6, 7, 8]:
                        ws.cell(row=target_row, column=c, value='')
        
        # Re-merge cells that were unmerged
        for coord in unmerged_coords:
            ws.merge_cells(coord)
        logging.info(f"Re-merged {len(unmerged_coords)} ranges")
        
    except Exception as e:
        logging.error(f"Error during row update: {e}")


def recalculate_summary(df_target: pd.DataFrame, ws):
    """Recalculate and update summary row."""
    # Count work days
    try:
        work_days = len(df_target[df_target['SPOLU'] != '00:00:00'])
    except Exception as e:
        logging.warning(f"Error counting work days: {e}")
        work_days = 0
    
    # Sum total hours
    total_td = timedelta()
    for value in df_target['SPOLU']:
        if value not in (None, '00:00:00'):
            try:
                h, m, s = map(int, str(value).split(':'))
                total_td += timedelta(hours=h, minutes=m, seconds=s)
            except (ValueError, AttributeError, TypeError) as e:
                logging.warning(f"Could not parse SPOLU value '{value}': {e}")
    
    # Format total time
    try:
        total_seconds = total_td.total_seconds()
        hours = int(abs(total_seconds) // 3600)
        minutes = int((abs(total_seconds) % 3600) // 60)
        seconds = int(abs(total_seconds) % 60)
        total_time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except Exception as e:
        logging.warning(f"Error formatting total time: {e}")
        total_time_str = '00:00:00'
    
    # Update summary cell (typically row 57, column 14)
    try:
        ws.cell(row=57, column=14, value=total_time_str)
        logging.info(f"Summary updated: {total_time_str} in N57")
    except Exception as e:
        logging.error(f"Error updating summary cell: {e}")
    
    return f"{work_days} days, {total_time_str}", total_time_str


def save_and_validate(wb, df_target: Optional[pd.DataFrame], backup_path: str, output_dir: str, dry_run: bool):
    """Save workbook and generate output files."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamp for output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"updated_{timestamp}.xlsx")
    
    if dry_run:
        logging.info("Dry run: skipping workbook save")
        wb.close()
        return
    
    try:
        wb.save(output_path)
        logging.info(f"Workbook saved to {output_path}")
        
        # Save CSV for audit only if df_target is provided
        if df_target is not None:
            csv_path = os.path.join(output_dir, f"transformed_data_{timestamp}.csv")
            df_target.to_csv(csv_path, index=False)
            logging.info(f"Transformed CSV saved to {csv_path}")
        
        wb.close()
        logging.info("Workbook closed successfully")
        
    except PermissionError as e:
        logging.error(f"Permission error saving workbook: {e}. Please close Excel file and retry.")
        wb.close()
    except Exception as e:
        logging.error(f"Error saving workbook: {e}")
        if backup_path and os.path.exists(backup_path):
            logging.info(f"Backup available at: {backup_path}")
        wb.close()
        raise


def main():
    """Main function to orchestrate the update process."""
    args = parse_args()
    
    try:
        logging.info("Starting vykaz update process")
        
        # Step 1: Create sheet mappings between source and target
        logging.info("Creating sheet mappings...")
        source_sheets = sheet_mapper.extract_sheet_names(args.source_excel)
        source_sheets = sheet_mapper.filter_instruction_sheets(source_sheets)
        target_sheets = sheet_mapper.extract_sheet_names(args.target_excel)
        
        if not source_sheets:
            raise ValueError(f"Could not extract sheet names from source file: {args.source_excel}")
        if not target_sheets:
            raise ValueError(f"Could not extract sheet names from target file: {args.target_excel}")
        
        mapping, unmatched_source, unmatched_target = sheet_mapper.create_mapping(source_sheets, target_sheets)
        logging.info(f"Created mappings for {len(mapping)} sheets")
        
        if unmatched_source:
            logging.warning(f"Unmatched source sheets: {unmatched_source}")
        if unmatched_target:
            logging.warning(f"Unmatched target sheets: {unmatched_target}")
        
        # Step 1.5: Clean target workbook by removing unmatched sheets
        cleaned_target_path = args.target_excel
        if not args.no_clean_target and unmatched_target:
            logging.info("Cleaning target workbook by removing unmatched sheets...")
            cleaned_target_path = sheet_mapper.remove_unmatched_target_sheets(args.target_excel, unmatched_target)
            logging.info(f"Cleaned target file saved to: {cleaned_target_path}")
        elif unmatched_target:
            logging.info("Skipping target cleaning (disabled by --no-clean-target)")
        
        # Step 1.6: Sort target sheets based on source sheet order
        target_file_to_process = cleaned_target_path
        if not args.no_sort_target:
            logging.info("Sorting target sheets based on source sheet order...")
            sorted_target_path = sheet_mapper.sort_target_sheets_by_source_order(
                args.source_excel, 
                cleaned_target_path, 
                mapping, 
                save_sorted=True
            )
            if sorted_target_path:
                logging.info(f"Sorted target workbook saved to: {sorted_target_path}")
                # Use the sorted file as our target for processing
                target_file_to_process = sorted_target_path
        else:
            logging.info("Skipping target sorting (disabled by --no-sort-target)")
        
        # Step 2: Create backup of target file
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join(os.path.dirname(target_file_to_process), 'backup')
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, f"backup_{backup_timestamp}.xlsx")
        
        if not args.dry_run and os.path.exists(target_file_to_process):
            shutil.copy(target_file_to_process, backup_path)
            logging.info(f"Backup created: {backup_path}")
        else:
            backup_path = None
            if args.dry_run:
                logging.info("Dry run: skipping backup creation")
        
        # Step 3: Load target workbook once
        logging.info("Loading target Excel...")
        wb = load_workbook(target_file_to_process)
        
        # Step 4: Process each mapped sheet
        processed_sheets = 0
        for source_sheet, target_sheet in mapping.items():
            if target_sheet == '-':
                logging.info(f"Skipping unmapped source sheet: {source_sheet}")
                continue
                
            logging.info(f"Processing sheet mapping: {source_sheet} -> {target_sheet}")
            
            try:
                # Extract source data for this specific sheet
                logging.info(f"Extracting source data from sheet: {source_sheet}")
                df_source = extract_source_data(args.source_excel, source_sheet)
                logging.info(f"Extracted {len(df_source)} rows from {source_sheet}")
                
                # Transform data
                logging.info(f"Transforming data for sheet: {target_sheet}")
                df_target = source_to_target(
                    df_source, 
                    args.activity_text, 
                    args.work_location
                )
                logging.info("Data transformation completed")
                
                # Get target worksheet
                if target_sheet not in wb.sheetnames:
                    logging.error(f"Target sheet '{target_sheet}' not found in workbook")
                    continue
                    
                ws = wb[target_sheet]
                
                # Find data start row using the target strategy
                from src.extractor_utils import STRATEGY_REGISTRY
                target_strategy = STRATEGY_REGISTRY["target"]
                data_start_row = target_strategy["start_row_strategy"](None)  # Uses fixed row 26
                logging.info(f"Using data start row: {data_start_row}")
                
                # Update month if provided
                if args.month:
                    try:
                        ws['E13'] = args.month
                        logging.info(f"Updated cell E13 with month: {args.month}")
                    except Exception as e:
                        logging.warning(f"Could not update month in E13: {e}")
                
                # Update daily rows
                logging.info(f"Updating daily rows in sheet: {target_sheet}")
                update_daily_rows(ws, df_target, data_start_row)
                
                # Recalculate summary
                logging.info(f"Recalculating summary for sheet: {target_sheet}")
                summary_text, total_time = recalculate_summary(df_target, ws)
                logging.info(f"Summary for {target_sheet}: {summary_text}")
                
                processed_sheets += 1
                
            except Exception as e:
                logging.error(f"Error processing sheet {source_sheet} -> {target_sheet}: {e}")
                continue
        
        logging.info(f"Successfully processed {processed_sheets} sheets")
        
        # Step 5: Save and validate
        logging.info("Saving workbook...")
        save_and_validate(wb, None, backup_path, args.output_dir, args.dry_run)
        logging.info("Process completed successfully")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    main()

