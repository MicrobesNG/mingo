#!/usr/bin/env python3
import json
import csv
import sys
import os

def calculate_coverage(csv_path, json_path):
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
                # The structure is out['plot'][0]['snapshots'] -> list of barcode objects
                plot_data = out.get('plot', [])
                if not plot_data: continue
                
                barcode_plot_snapshots = plot_data[0].get('snapshots', [])
                for barcode_entry in barcode_plot_snapshots:
                    # Find barcode name in filtering
                    barcode_name = None
                    for filt in barcode_entry.get('filtering', []):
                        if 'barcode_name' in filt:
                            barcode_name = filt['barcode_name']
                            break
                    if not barcode_name: continue
                    
                    # Last snapshot has total yield for this specific barcode in this acquisition
                    snaps = barcode_entry.get('snapshots', [])
                    if snaps:
                        last_snap = snaps[-1]
                        bases_str = last_snap.get('yield_summary', {}).get('basecalled_pass_bases', "0")
                        bases = int(bases_str)
                        # Accumulate in case of multiple SplitByBarcode outputs (e.g. across multiple acquisitions)
                        yields[barcode_name] = yields.get(barcode_name, 0) + bases

    # 3. Calculate and Output
    # Header
    print(f"{'barcode_alias':<20} {'native barcode name':<20} {'total reads (Mb)':<20} {'expected genome (Mb)':<20} {'total coverage':<15}")
    print("-" * 95)
    
    # Sort by barcode name (e.g. barcode01, barcode02...)
    for barcode, info in sorted(samples.items()):
        total_bases = yields.get(barcode, 0)
        total_mb = total_bases / 1_000_000
        genome_mb = info['genome_size_mb']
        
        coverage = total_mb / genome_mb if genome_mb > 0 else 0
        
        # Precision: integer if >= 1, else single precision
        if coverage >= 1:
            cov_str = f"{int(round(coverage))}"
        else:
            cov_str = f"{coverage:.1f}"
            
        print(f"{info['alias']:<20} {barcode:<20} {total_mb:<20.2f} {genome_mb:<20.2f} {cov_str:<15}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: calculate_coverage.py <samples.csv> <report.json>")
        sys.exit(1)
    
    csv_arg = sys.argv[1]
    json_arg = sys.argv[2]
    
    if not os.path.exists(csv_arg):
        print(f"Error: CSV file not found: {csv_arg}")
        sys.exit(1)
    if not os.path.exists(json_arg):
        print(f"Error: JSON file not found: {json_arg}")
        sys.exit(1)
        
    calculate_coverage(csv_arg, json_arg)
