#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import json
import glob

try:
    from slims import SlimsClient
    from samplesheet import SampleSheetGenerator
    from minknow_utils import MinKNOWClient
except ImportError:
    # Ensure local modules can be imported if running from same directory
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from slims import SlimsClient
    from samplesheet import SampleSheetGenerator
    from minknow_utils import MinKNOWClient

# Mock Classes
class MockSlimsClient:
    def __init__(self, url, user, password):
        pass
    def fetch_queued_runs(self):
        return [
            {"pk": 1, "xprn_name": "MOCK_RUN_01", "xprn_status": "Ready"},
            {"pk": 2, "xprn_name": "MOCK_RUN_02", "xprn_status": "Ready"}
        ]
    def fetch_run_details(self, run_pk):
        return {
            "run": {"pk": run_pk, "xprn_name": f"MOCK_RUN_{run_pk:02d}"},
            "inputs": [
                {
                    "cntn_id": "SAMPLE_A", 
                    "barcode_i7": "NB01", 
                    "cntn_cf_taxon": "Escherichia coli",
                    "cntn_cf_genomeSizeMb": 5.1
                },
                {
                    "cntn_id": "SAMPLE_B", 
                    "barcode_i7": "NB02",
                    "cntn_cf_taxon": "Staphylococcus aureus",
                    "cntn_cf_genomeSizeMb": 2.8
                }
            ]
        }

class MockMinKNOWClient:
    def get_positions(self):
        return [
            {"name": "1A", "status": "Ready", "running": False, "flow_cell_id": "SIM_MOCK_1"},
            {"name": "1B", "status": "Running", "running": True, "flow_cell_id": "SIM_MOCK_2"}
        ]

    def start_run(self, position_name, protocol_id, sample_sheet_path, run_name, settings=None, samples=None):
        print(f"[MOCK] Started run {run_name} on {position_name} using {protocol_id}")
        if settings:
            print(f"[MOCK] Applied settings from template: {settings.get('script', {}).get('name', 'custom')}")
            if settings.get('customBarcodesSelection'):
                print(f"[MOCK] Dynamic Barcode Selection: {settings['customBarcodesSelection']}")
        
        if settings and settings.get("barcodingExpansionKits"):
             print(f"[MOCK] Barcoding Kits: {settings['barcodingExpansionKits']}")
 
        if samples:
            print(f"[MOCK] Barcode Info Mapping ({len(samples)} samples):")
            for sample in samples:
                alias = sample.get('cntn_id', '')
                barcode = sample.get('barcode_i7', '')
                barcode_name = barcode
                if barcode and barcode[:2] in ['NB', 'BC']:
                    try:
                        idx = int(barcode[2:])
                        barcode_name = f"barcode{idx:02d}"
                    except ValueError:
                        pass
                print(f"   - {alias} -> {barcode_name}")
             
        # Show full settings in mock if debug is on
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            print(f"[DEBUG][MOCK] Full Protocol Settings: {json.dumps(settings, indent=2)}")

def get_input(prompt, options=None):
    while True:
        user_input = input(f"\n{prompt}\n> ").strip()
        if options:
            if user_input in options:
                return user_input
            print("Invalid option, please try again.")
        else:
            return user_input

def main():
    parser = argparse.ArgumentParser(description="ONT Run Manager")
    parser.add_argument("--mock", action="store_true", help="Run in mock mode without connecting to external systems.")
    parser.add_argument("--host", default="localhost", help="MinKNOW host (default: localhost)")
    parser.add_argument("--port", type=int, default=None, help="MinKNOW port (optional)")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging and show detailed protocol parameters.")
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Also update the logger in minknow_utils if it was already initialized
    from minknow_utils import logger as minknow_logger
    minknow_logger.setLevel(log_level)

    print("\n--- ONT Run Manager ---\n")
    
    if args.mock:
        print("!! RUNNING IN MOCK MODE !!")
        slims = MockSlimsClient("mock_url", "mock", "mock")
        minknow = MockMinKNOWClient()
    else:
        # 1. Initialize Clients
        slims_url = os.environ.get('SLIMS_URL')
        slims_user = os.environ.get('SLIMS_USER')
        slims_pass = os.environ.get('SLIMS_PASSWORD')
        
        if not all([slims_url, slims_user, slims_pass]):
            print("Error: SLIMS credentials not found in environment variables.")
            print("Please set SLIMS_URL, SLIMS_USER, and SLIMS_PASSWORD.")
            sys.exit(1)

        slims = SlimsClient(slims_url, slims_user, slims_pass)
        minknow = MinKNOWClient(host=args.host, port=args.port) # Use provided host/port

    # 2. Select Position
    print("Checking sequencer positions...")
    positions = minknow.get_positions()
    if not positions:
        print("No sequencing positions found. Ensure MinKNOW is running.")
        sys.exit(1)
        
    print(f"\nFound {len(positions)} positions:")
    for idx, pos in enumerate(positions):
        status = pos.get('status', 'Unknown')
        fc_id = pos.get('flow_cell_id', 'Unknown')
        print(f" - {idx + 1}) {pos['name']}, Flowcell ID: {fc_id}, Status: {status}")

    while True:
        pos_choice = get_input("Please confirm which position the library is loaded into (number) or type 'q' to quit:", 
                              options=[str(i+1) for i in range(len(positions))] + ['q'])
        
        if pos_choice == 'q':
            sys.exit(0)
            
        selected_pos = positions[int(pos_choice) - 1]
        if selected_pos.get('running'):
            print(f"!! Error: Position {selected_pos['name']} is already running a protocol. Please choose an idle position.")
            continue
        break
    
    # 3. Select Kit
    print("\nAvailable Kits:")
    kits = [
        {"name": "RAPID 96", "code": "SQK-RBK114-96"},
        {"name": "NATIVE 96", "code": "SQK-NBD114-96"}
    ]
    for idx, kit in enumerate(kits):
        print(f" - {idx + 1}) {kit['name']} - {kit['code']}")
        
    kit_choice = get_input("Which kit does this run use? (number) or 'q' to quit:", 
                          options=[str(i+1) for i in range(len(kits))] + ['q'])

    if kit_choice == 'q':
        sys.exit(0)

    selected_kit = kits[int(kit_choice) - 1]

    # 4. Select SLIMS Run
    print("\nFetching queued runs from SLIMS...")
    queued_runs = slims.fetch_queued_runs()
    
    if not queued_runs:
        print("No queued runs found in SLIMS.")
        sys.exit(0)
        
    for idx, run in enumerate(queued_runs):
        print(f" - {idx + 1}) {run.get('xprn_name', 'Unnamed Run')} (Experiment ID: {run.get('pk')})")
        
    run_choice = get_input("Please choose a run (number) or 'q' to quit:", 
                          options=[str(i+1) for i in range(len(queued_runs))] + ['q'])

    if run_choice == 'q':
        sys.exit(0)

    selected_run = queued_runs[int(run_choice) - 1]
    
    # 5. Fetch Run Details & Generate Sample Sheet
    print(f"\nFetching details for run: {selected_run.get('xprn_name')}...")
    details = slims.fetch_run_details(selected_run['pk'])
    samples = details['inputs']
    
    print(f" - Found {len(samples)} samples.")
    
    generator = SampleSheetGenerator()
    csv_content = generator.generate(
        run_metadata=selected_run,
        samples=samples,
        flow_cell_id=selected_pos.get('flow_cell_id', 'UNKNOWN_FC'),
        position_id=selected_pos['name'],
        kit=selected_kit['code']
    )
    
    # Save sample sheet
    run_name = selected_run.get('xprn_name', 'run')
    filename = f"{run_name}.csv"
    filepath = os.path.abspath(filename)
    
    print(f" - Writing sample sheet to {filepath}")
    with open(filepath, 'w') as f:
        f.write(csv_content)
        
    # 6. Select Settings Template
    print("\nSettings Templates:")
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
    template_files = glob.glob(os.path.join(template_dir, "*.json"))
    
    if not template_files:
        print(f" - No templates found in {template_dir}. Using defaults.")
        selected_settings = {}
    else:
        for idx, fpath in enumerate(template_files):
            print(f" - {idx + 1}) {os.path.basename(fpath)}")
        
        tpl_choice = get_input("Choose a settings template (number) or 'n' for none:", 
                              options=[str(i+1) for i in range(len(template_files))] + ['n'])
        
        if tpl_choice == 'n':
            selected_settings = {}
        else:
            with open(template_files[int(tpl_choice) - 1], 'r') as f:
                selected_settings = json.load(f)

    # 7. Dynamic Barcode Detection
    if samples and all('barcode_i7' in s for s in samples):
        barcodes = []
        for s in samples:
            b = s.get('barcode_i7', '')
            # Extract number from NB01, BC01, etc.
            if b and (b.startswith('NB') or b.startswith('BC')):
                try:
                    barcodes.append(int(b[2:]))
                except ValueError:
                    pass
        
        if barcodes:
            barcodes.sort()
            # If they are contiguous, use range, else list
            if len(barcodes) > 1 and all(barcodes[i] == barcodes[i-1] + 1 for i in range(1, len(barcodes))):
                barcode_range = f"{barcodes[0]}-{barcodes[-1]}"
            else:
                barcode_range = ",".join(map(str, barcodes))
            
            print(f" - Detected barcode range: {barcode_range}")
            selected_settings['customBarcodesSelection'] = barcode_range
            selected_settings['barcodingEnabled'] = True

    # 8. Confirm and Start
    print(f"\nReady to start run '{run_name}' on {selected_pos['name']} using kit {selected_kit['code']}.")
    print("Parameters:")
    print(f" - Samples: {len(samples)}")
    print(f" - Sample Sheet: {filepath}")
    if selected_settings.get('customBarcodesSelection'):
        print(f" - Barcodes: {selected_settings['customBarcodesSelection']}")
    
    confirm = get_input("Please type 'Y' to confirm and start the run, or anything else to abort.")
    
    if confirm.upper() == 'Y':
        print("\nStarting run...")
        if args.mock:
             minknow.start_run(
                position_name=selected_pos['name'],
                protocol_id="MOCK_PROTOCOL",
                sample_sheet_path=filepath,
                run_name=run_name,
                settings=selected_settings,
                samples=samples,
                kit=selected_kit['code']
            )
        else:
             # Real implementation: Get protocol from settings or let user choose
             selected_proto = selected_settings.get('script', {}).get('identifier')
             
             if not selected_proto:
                 print("Fetching available protocols...")
                 protocols = minknow.list_protocols(selected_pos['name'])
                 if not protocols:
                     print("No protocols found for this position.")
                     sys.exit(1)
                 
                 print(f"\nFound {len(protocols)} protocols:")
                 for idx, p in enumerate(protocols):
                     print(f" - {idx + 1}) {p}")
                 
                 proto_choice = get_input("Please choose a protocol (number) or 'q' to quit:", 
                                         options=[str(i+1) for i in range(len(protocols))] + ['q'])
                 
                 if proto_choice == 'q':
                     sys.exit(0)
                 
                 selected_proto = protocols[int(proto_choice) - 1]
             else:
                 print(f"Using protocol from template: {selected_proto}")
             
             minknow.start_run(
                position_name=selected_pos['name'],
                protocol_id=selected_proto,
                sample_sheet_path=filepath,
                run_name=run_name,
                settings=selected_settings,
                samples=samples,
                kit=selected_kit['code']
            )
        print("Run successfully started!")
    else:
        print("Aborted.")

if __name__ == "__main__":
    main()
