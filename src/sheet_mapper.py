import openpyxl
import difflib
import unicodedata
import argparse
import json
import os
from datetime import date

source_path = 'data/input/Dochádzka_JUL_2025_Perry_soft_.xlsx'
target_path = 'data/input/09I05-03-V04_Príloha č. 3 Pracovné výkazy_04-2025_cleaned.xlsx'

def extract_sheet_names(path):
    try:
        wb = openpyxl.load_workbook(path)
        sheets = wb.sheetnames
        wb.close()
        return sheets
    except FileNotFoundError:
        print(f"File not found: {path}")
        return []
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return []

def _remove_titles(name):
    prefixes = ['Ing.', 'Bc.', 'Mgr.', 'PhD.', 'prof.', 'MUDr.', 'RNDr.']
    for prefix in prefixes:
        if name.startswith(prefix + ' '):
            name = name[len(prefix) + 1:]
            break
    return name

def _normalize_name(name):
    name = _remove_titles(name)
    name = unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode('ascii')
    return name.strip().lower()

def create_mapping(source_sheets, target_sheets):
    mapping = {}
    unmatched_source = []
    norm_targets = [_normalize_name(t) for t in target_sheets]
    used_targets = set()
    for source in source_sheets:
        norm_source = _normalize_name(source)
        if norm_source in norm_targets:
            matched = target_sheets[norm_targets.index(norm_source)]
            used_targets.add(matched)
        else:
            close = difflib.get_close_matches(norm_source, norm_targets, n=1, cutoff=0.8)
            if close:
                matched = target_sheets[norm_targets.index(close[0])]
                used_targets.add(matched)
            else:
                matched = '-'
                unmatched_source.append(f"{source} -> -")
        mapping[source] = matched
    unmatched_target = [t for t in target_sheets if t not in used_targets and t != '-']
    unmatched_target = [f"{t} -> -" for t in unmatched_target]
    return mapping, unmatched_source, unmatched_target

def remove_unmatched_target_sheets(target_path, unmatched_target):
    wb = openpyxl.load_workbook(target_path)
    sheets_to_remove = [name.split(" -> -")[0] for name in unmatched_target]
    for sheet_name in sheets_to_remove:
        if sheet_name in wb.sheetnames:
            wb.remove(wb[sheet_name])
    base, ext = os.path.splitext(target_path)
    cleaned_path = base + '_cleaned' + ext
    wb.save(cleaned_path)
    wb.close()
    return cleaned_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Map sheet names from source to target Excel files.')
    parser.add_argument('--source', default=source_path, help='Path to source Excel file')
    parser.add_argument('--target', default=target_path, help='Path to target Excel file')
    parser.add_argument('--clean-target', action='store_true', default=False, help='Remove unmatched target sheets and save cleaned file')
    args = parser.parse_args()

    source_sheets = extract_sheet_names(args.source)
    source_sheets = [sheet for sheet in source_sheets if sheet != "Inštrukcie k vyplneniu PV"]
    target_sheets = extract_sheet_names(args.target)

    if not source_sheets:
        print(f"Could not extract sheet names from source file: {args.source}")
        exit(1)
    if not target_sheets:
        print(f"Could not extract sheet names from target file: {args.target}")
        exit(1)

    mapping, unmatched_source, unmatched_target = create_mapping(source_sheets, target_sheets)

    print("Sheet name mappings:")
    for source, target in mapping.items():
        print(f"{source} -> {target}")
    if unmatched_source:
        print("Unmatched source sheets:")
        for unmatched in unmatched_source:
            print(unmatched)
    if unmatched_target:
        print("Unmatched target sheets:")
        for unmatched in unmatched_target:
            print(unmatched)
    if args.clean_target and unmatched_target:
        cleaned_file = remove_unmatched_target_sheets(args.target, unmatched_target)
        print(f"Cleaned target file saved to {cleaned_file}")
    
    data = {
        "mappings": mapping,
        "unmatched_source": unmatched_source,
        "unmatched_target": unmatched_target
    }
    filename = f'data/mappings_{date.today().isoformat()}.json'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Mappings saved to {filename}")