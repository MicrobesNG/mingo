#!/usr/bin/env python3
import json
import csv
import sys
import os
import argparse

def calculate_coverage(csv_path, json_path, filter_below_coverage=None, output_csv=False):
    # 1. Parse CSV
    samples = {}
    csv_exp_id = None
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                barcode = row['barcode']
                samples[barcode] = {
                    'alias': row['alias'],
                    'genome_size_mb': float(row['cntn_cf_genomeSizeMb']) if row['cntn_cf_genomeSizeMb'] else 0,
                    'experiment_id': row['experiment_id']
                }
                if csv_exp_id is None:
                    csv_exp_id = row['experiment_id']
                elif csv_exp_id != row['experiment_id']:
                    print(f"Warning: Multiple experiment IDs found in CSV: {csv_exp_id}, {row['experiment_id']}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    # 2. Parse JSON
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading JSON: {e}")
        sys.exit(1)
    
    json_exp_id = data.get('protocol_run_info', {}).get('user_info', {}).get('protocol_group_id')
    
    # Validation
    if csv_exp_id != json_exp_id:
        print(f"Error: Run mismatch. CSV experiment_id ({csv_exp_id}) != JSON protocol_group_id ({json_exp_id})")
        sys.exit(1)

    # Extract Yields
    yields = {}
    for acq in data.get('acquisitions', []):
        for out in acq.get('acquisition_output', []):
            if out.get('type') == 'SplitByBarcode':
                plot_data = out.get('plot', [])
                if not plot_data: continue
                
                barcode_plot_snapshots = plot_data[0].get('snapshots', [])
                for barcode_entry in barcode_plot_snapshots:
                    barcode_name = None
                    for filt in barcode_entry.get('filtering', []):
                        if 'barcode_name' in filt:
                            barcode_name = filt['barcode_name']
                            break
                    if not barcode_name: continue
                    
                    snaps = barcode_entry.get('snapshots', [])
                    if snaps:
                        last_snap = snaps[-1]
                        bases_str = last_snap.get('yield_summary', {}).get('basecalled_pass_bases', "0")
                        bases = int(bases_str)
                        yields[barcode_name] = yields.get(barcode_name, 0) + bases

    # 3. Calculate and Output
    if output_csv:
        writer = csv.writer(sys.stdout)
        writer.writerow(['barcode_alias', 'barcode_name', 'total_reads_mb', 'expected_genome_mb', 'coverage'])
    else:
        header = f"{'barcode_alias':<20} {'native barcode name':<20} {'total reads (Mb)':<20} {'expected genome (Mb)':<20} {'total coverage':<15}"
        print(header)
        print("-" * 95)
    
    for barcode, info in sorted(samples.items()):
        total_bases = yields.get(barcode, 0)
        total_mb = total_bases / 1_000_000
        genome_mb = info['genome_size_mb']
        
        coverage = total_mb / genome_mb if genome_mb > 0 else 0
        
        # Filtering
        if filter_below_coverage is not None and coverage >= filter_below_coverage:
            continue
            
        if output_csv:
            writer.writerow([info['alias'], barcode, f"{total_mb:.2f}", f"{genome_mb:.2f}", f"{coverage:.2f}"])
        else:
            # Truncate alias for better table formatting
            alias = info['alias']
            if len(alias) > 17:
                alias = alias[:14] + "..."
                
            cov_str = f"{coverage:.1f}"
            
            print(f"{alias:<20} {barcode:<20} {total_mb:>20.2f} {genome_mb:>20.2f} {cov_str:>15}")

def main():
    parser = argparse.ArgumentParser(description="Calculate genome coverage from ONT reports and sample sheets.")
    parser.add_argument("csv_path", help="Path to the sample sheet CSV file.")
    parser.add_argument("json_path", help="Path to the sequencing report JSON file.")
    parser.add_argument("--below", type=int, help="Only output lines where coverage is below this value.")
    parser.add_argument("--csv", action="store_true", help="Output in CSV format.")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_path):
        print(f"Error: CSV file not found: {args.csv_path}")
        sys.exit(1)
    if not os.path.exists(args.json_path):
        print(f"Error: JSON file not found: {args.json_path}")
        sys.exit(1)
        
    calculate_coverage(args.csv_path, args.json_path, filter_below_coverage=args.below, output_csv=args.csv)

if __name__ == "__main__":
    main()
