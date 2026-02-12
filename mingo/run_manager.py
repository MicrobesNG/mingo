#!/usr/bin/env python3
import os
import sys
import argparse
import logging

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

    def start_run(self, position_name, protocol_id, sample_sheet, run_name):
        print(f"[MOCK] Started run {run_name} on {position_name} using {protocol_id}")

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
    args = parser.parse_args()

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

    pos_choice = get_input("Please confirm which position the library is loaded into (number) or type 'q' to quit:", 
                          options=[str(i+1) for i in range(len(positions))] + ['q'])
    
    if pos_choice == 'q':
        sys.exit(0)
        
    selected_pos = positions[int(pos_choice) - 1]
    
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
        
    # 6. Confirm and Start
    print(f"\nReady to start run '{run_name}' on {selected_pos['name']} using kit {selected_kit['code']}.")
    print("Parameters:")
    print(f" - Samples: {len(samples)}")
    print(f" - Sample Sheet: {filepath}")
    # Add more parameters verification here
    
    confirm = get_input("Please type 'Y' to confirm and start the run, or anything else to abort.")
    
    if confirm.upper() == 'Y':
        print("\nStarting run...")
        if args.mock:
             minknow.start_run(
                position_name=selected_pos['name'],
                protocol_id="MOCK_PROTOCOL",
                sample_sheet=filepath,
                run_name=run_name
            )
        else:
             # Real implementation: List available protocols and let user choose
             print("Fetching available protocols...")
             protocols = minknow.list_protocols(selected_pos['name'])
             if not protocols:
                 print("No protocols found for this position.")
                 sys.exit(1)
             
             # Filter or search for a sequencing protocol if needed
             # For now, let the user choose from the list
             print(f"\nFound {len(protocols)} protocols:")
             for idx, p in enumerate(protocols):
                 print(f" - {idx + 1}) {p}")
             
             proto_choice = get_input("Please choose a protocol (number) or 'q' to quit:", 
                                     options=[str(i+1) for i in range(len(protocols))] + ['q'])
             
             if proto_choice == 'q':
                 sys.exit(0)
             
             selected_proto = protocols[int(proto_choice) - 1]
             
             minknow.start_run(
                position_name=selected_pos['name'],
                protocol_id=selected_proto,
                sample_sheet=filepath,
                run_name=run_name
            )
        print("Run successfully started!")
    else:
        print("Aborted.")

if __name__ == "__main__":
    main()
