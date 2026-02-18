import sys
import os
import argparse

# Add the project root to sys.path to allow imports from mingo
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mingo.coverage import run_coverage_analysis

def main():
    parser = argparse.ArgumentParser(description="Calculate genome coverage from ONT reports and sample sheets.")
    parser.add_argument("csv_path", help="Path to the sample sheet CSV file.")
    parser.add_argument("--json", help="Path to the sequencing report JSON file.")
    parser.add_argument("--summary", help="Path to the sequencing summary TXT file.")
    parser.add_argument("--below", type=int, help="Only output lines where coverage is below this value.")
    parser.add_argument("--threshold", type=int, default=7000, help="Read length threshold for binned stats (default: 7000).")
    parser.add_argument("--csv", action="store_true", help="Output in CSV format.")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"Error: CSV file not found: {args.csv_path}")
        sys.exit(1)
    
    run_coverage_analysis(
        args.csv_path, 
        json_path=args.json, 
        summary_path=args.summary, 
        filter_below_coverage=args.below, 
        output_csv=args.csv, 
        threshold=args.threshold
    )

if __name__ == "__main__":
    main()
