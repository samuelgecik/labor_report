# run_extractor.py

"""
Config-driven Excel data extraction utility.
Run extraction tasks defined in a YAML configuration file.
"""

import argparse
import sys
import os

# Add parent directory to path for relative imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    import yaml
except ImportError:
    print("Error: PyYAML library not found. Install with 'pip install pyyaml'")
    sys.exit(1)

from src.extractor_utils import extract_from_workbook, save_extraction_results


def main():
    parser = argparse.ArgumentParser(
        description="Config-driven Excel data extractor.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.run_extractor --task ronec_source
  python -m src.run_extractor --task perry_soft_source
  python -m src.run_extractor --task all_perry_sheets --config config/custom_config.yaml
        """
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/extraction_config.yaml',
        help='Path to the extraction config YAML file (default: config/extraction_config.yaml)'
    )
    parser.add_argument(
        '--task',
        type=str,
        required=True,
        help='The name of the extraction task to run from the config file'
    )

    args = parser.parse_args()

    # Load the configuration file
    if not os.path.exists(args.config):
        print(f"Error: Configuration file '{args.config}' not found.")
        sys.exit(1)

    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            all_configs = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration file: {e}")
        sys.exit(1)

    # Look for the specified task
    config = all_configs.get(args.task)
    if not config:
        available_tasks = ', '.join(all_configs.keys())
        print(f"Error: Task '{args.task}' not found in configuration.")
        print(f"Available tasks: {available_tasks}")
        sys.exit(1)

    print(f"Starting extraction task: '{args.task}'")
    print(f"Processing file: {config['file_path']}")
    print(f"Output prefix: {config.get('output_prefix', 'extracted_data')}")

    try:
        # Extract data from the workbook
        results = extract_from_workbook(config)

        # Save the results
        save_extraction_results(results, config)

        # Report results
        print("Extraction completed successfully!")
        print(f"Processed {len(results)} sheet(s):")
        for sheet_name, data in results.items():
            print(f"  - {sheet_name}: {len(data)} records")

    except Exception as e:
        print(f"Error during extraction: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()