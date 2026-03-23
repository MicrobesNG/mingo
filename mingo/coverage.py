import json
import csv
import sys
import os

def run_coverage_analysis(csv_path, json_path=None, summary_path=None, filter_below_coverage=None, output_csv=False, bin_threshold=7000, no_low_material=False):
    # 1. Parse CSV
    samples = {}
    csv_exp_id = None
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                barcode = row['barcode']
                # Determine boolean value for low material, defaulting to False if missing or malformed
                low_mat_val = str(row.get('cntn_cf_lowMaterial', '')).strip().lower() == 'true'
                
                samples[barcode] = {
                    'alias': row['alias'],
                    'genome_size_mb': float(row['cntn_cf_genomeSizeMb']) if row['cntn_cf_genomeSizeMb'] else 0,
                    'experiment_id': row['experiment_id'],
                    'low_material': low_mat_val
                }
                if csv_exp_id is None:
                    csv_exp_id = row['experiment_id']
                elif csv_exp_id != row['experiment_id']:
                    print(f"Warning: Multiple experiment IDs found in CSV: {csv_exp_id}, {row['experiment_id']}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    yields = {} # Stores base counts and read distribution
    # Structure: { barcode: {'bases': 0, 'reads': 0, 'short_bases': 0, 'short_reads': 0, 'long_bases': 0, 'long_reads': 0} }

    # 2. Parse Reports
    max_start_time = 0.0
    if summary_path:
        try:
            with open(summary_path, 'r') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    barcode = row.get('barcode_arrangement')
                    if not barcode: continue
                    
                    try:
                        start_time = float(row.get('start_time', 0))
                        if start_time > max_start_time:
                            max_start_time = start_time
                    except ValueError:
                        pass

                    if row.get('passes_filtering') != 'TRUE':
                        continue
                    
                    try:
                        length = int(row.get('sequence_length_template', 0))
                    except ValueError:
                        length = 0

                    if barcode not in yields:
                        yields[barcode] = {'bases': 0, 'reads': 0, 'short_bases': 0, 'short_reads': 0, 'long_bases': 0, 'long_reads': 0}
                    
                    yields[barcode]['bases'] += length
                    yields[barcode]['reads'] += 1
                    
                    if length < bin_threshold:
                        yields[barcode]['short_bases'] += length
                        yields[barcode]['short_reads'] += 1
                    else:
                        yields[barcode]['long_bases'] += length
                        yields[barcode]['long_reads'] += 1
        except Exception as e:
            print(f"Error reading summary: {e}")
            sys.exit(1)

    elif json_path:
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            
            json_exp_id = data.get('protocol_run_info', {}).get('user_info', {}).get('protocol_group_id')
            if csv_exp_id and json_exp_id and csv_exp_id != json_exp_id:
                print(f"Error: Run mismatch. CSV experiment_id ({csv_exp_id}) != JSON protocol_group_id ({json_exp_id})")
                sys.exit(1)

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
                                bases = int(last_snap.get('yield_summary', {}).get('basecalled_pass_bases', "0"))
                                reads = int(last_snap.get('yield_summary', {}).get('basecalled_pass_read_count', "0"))
                                
                                if barcode_name not in yields:
                                    yields[barcode_name] = {'bases': 0, 'reads': 0, 'short_bases': 0, 'short_reads': 0, 'long_bases': 0, 'long_reads': 0}
                                
                                yields[barcode_name]['bases'] += bases
                                yields[barcode_name]['reads'] += reads
        except Exception as e:
            print(f"Error reading JSON: {e}")
            sys.exit(1)
    else:
        print("Error: Either --json or --summary must be provided.")
        sys.exit(1)

    # 3. Calculate and Output
    show_eta = bool(summary_path and filter_below_coverage is not None)
    
    cols = ['alias', 'barcode', 'low_mat', 'total_mb', 'genome_mb', 'coverage']
    if show_eta:
        cols.append('eta')
        
    if summary_path:
        cols += ['avg_len', 'short_total_mb', 'short_avg_len', 'long_total_mb', 'long_avg_len']

    thresh_kb = f"{bin_threshold/1000:g}kb"
    if output_csv:
        writer = csv.writer(sys.stdout)
        csv_cols = [c if c not in ['short_total_mb', 'short_avg_len', 'long_total_mb', 'long_avg_len'] else 
                    c.replace('short', f'<{thresh_kb}').replace('long', f'>={thresh_kb}') for c in cols]
        writer.writerow(csv_cols)
    else:
        header_map = {
            'alias': f"{'alias':<15}",
            'barcode': f"{'barcode':<12}",
            'low_mat': f"{'low_mat':<8}",
            'total_mb': f"{'yield (Mb)':>10}",
            'genome_mb': f"{'genome (Mb)':>12}",
            'coverage': f"{'cov':>6}",
            'eta': f"{'eta':>11}",
            'avg_len': f"{'avg_len':>8}",
            'short_total_mb': f"{f'<{thresh_kb} yield':>12}",
            'short_avg_len': f"{f'<{thresh_kb} avg':>10}",
            'long_total_mb': f"{f'>={thresh_kb} yield':>12}",
            'long_avg_len': f"{f'>={thresh_kb} avg':>10}"
        }
        print(" ".join([header_map[c] for c in cols]))
        print("-" * (128 if summary_path else 68))
    
    for barcode, info in sorted(samples.items()):
        # Low material filter
        if no_low_material and info['low_material']:
            continue
            
        y = yields.get(barcode, {'bases': 0, 'reads': 0, 'short_bases': 0, 'short_reads': 0, 'long_bases': 0, 'long_reads': 0})
        
        total_mb = y['bases'] / 1_000_000
        genome_mb = info['genome_size_mb']
        coverage = total_mb / genome_mb if genome_mb > 0 else 0
        
        if filter_below_coverage is not None and coverage >= filter_below_coverage:
            continue
            
        eta_str = ""
        if show_eta:
            target_mb = filter_below_coverage * genome_mb
            remaining_mb = target_mb - total_mb
            if max_start_time > 0 and total_mb > 0:
                rate_mb_per_sec = total_mb / max_start_time
                eta_secs = remaining_mb / rate_mb_per_sec
                eta_hrs, eta_rem = divmod(eta_secs, 3600)
                eta_mins = eta_rem // 60
                eta_str = f"{int(eta_hrs)}h {int(eta_mins)}m"
            else:
                eta_str = "N/A"
                
        row_data = {
            'alias': info['alias'][:12] + "..." if len(info['alias']) > 14 else info['alias'],
            'barcode': barcode,
            'low_mat': "Y" if info['low_material'] else " ",
            'total_mb': f"{total_mb:.2f}",
            'genome_mb': f"{genome_mb:.2f}",
            'coverage': f"{coverage:.1f}",
            'eta': eta_str,
            'avg_len': f"{int(y['bases']/y['reads'])}" if y['reads'] > 0 else "0",
            'short_total_mb': f"{y['short_bases']/1_000_000:.2f}",
            'short_avg_len': f"{int(y['short_bases']/y['short_reads'])}" if y['short_reads'] > 0 else "0",
            'long_total_mb': f"{y['long_bases']/1_000_000:.2f}",
            'long_avg_len': f"{int(y['long_bases']/y['long_reads'])}" if y['long_reads'] > 0 else "0"
        }

        if output_csv:
            writer.writerow([row_data[c] for c in cols])
        else:
            fmt_map = {
                'alias': f"{row_data['alias']:<15}",
                'barcode': f"{row_data['barcode']:<12}",
                'low_mat': f"{row_data['low_mat']:<8}",
                'total_mb': f"{row_data['total_mb']:>10}",
                'genome_mb': f"{row_data['genome_mb']:>12}",
                'coverage': f"{row_data['coverage']:>6}",
                'eta': f"{row_data['eta']:>11}",
                'avg_len': f"{row_data['avg_len']:>8}",
                'short_total_mb': f"{row_data['short_total_mb']:>12}",
                'short_avg_len': f"{row_data['short_avg_len']:>10}",
                'long_total_mb': f"{row_data['long_total_mb']:>12}",
                'long_avg_len': f"{row_data['long_avg_len']:>10}"
            }
            print(" ".join([fmt_map[c] for c in cols]))
