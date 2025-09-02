import pandas as pd

def analyze_excel_file(filename, file_description):
    """Read and analyze the structure of an Excel file."""
    print(f"\n{'='*50}")
    print(f"Analyzing: {filename} ({file_description})")
    print(f"{'='*50}")
    
    try:
        # Read the first sheet of the Excel file
        df = pd.read_excel(filename, sheet_name=0)
        
        print(f"\nFile: {filename}")
        print(f"Shape: {df.shape} (rows, columns)")
        print(f"\nColumn names:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        
        print(f"\nFirst 5 rows:")
        print(df.head())
        
        print(f"\nData types:")
        print(df.dtypes)
        
    except Exception as e:
        print(f"Error reading {filename}: {e}")

def main():
    print("Excel Files Structure Analysis")
    print("="*50)
    
    # Analyze source attendance data file
    analyze_excel_file("ronec_dochadzka.xlsx", "source attendance data")
    
    # Analyze target timesheet file
    analyze_excel_file("ronec_vykaz.xlsx", "target timesheet")
    
    print(f"\n{'='*50}")
    print("Analysis complete!")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()