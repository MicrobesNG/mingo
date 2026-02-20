#!/bin/env python3

import argparse
import csv
import minknow_api
import sys
from minknow_api.manager import Manager


def main():
    # Define the command-line arguments.  The parser module will extract the user's values and place them in
    # variables named after the options.
    parser = argparse.ArgumentParser(
        description="""
        Show protocol status.
        """
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="IP address of the machine running MinKNOW (defaults to localhost)",
    )
    parser.add_argument(
        "--port",
        help="Port to connect to on host (defaults to standard MinKNOW port)",
    )
    parser.add_argument(
        "--api-token",
        default=None,
        help="Specify an API token to use, should be returned from the sequencer as a developer API token. This can only be left unset if there is a local token available.",
    )
    args = parser.parse_args()

    # Try and connect to the minknow-core manager passing the host, port and developer-api token.  If the Python code
    # can't connect it will throw, catch the exception and exit with an error message.
    manager = Manager(
        host=args.host, port=args.port, developer_api_token=args.api_token
    )
    for pos in manager.flow_cell_positions():
        if pos.running:
            protocol = pos.connect().protocol
            run = protocol.get_run_info()
            run = pos.connect().protocol.get_run_info()
            if run.state == protocol._pb.PROTOCOL_RUNNING:
                if run.user_info.protocol_group_id.value != "no_group":
                    print(f"{run.device.device_id}: {run.user_info.protocol_group_id.value} RUNNING")
                else:
                    print(f"{run.device.device_id}: check {run.protocol_id} RUNNING")
                    
            elif run.state == protocol._pb.PROTOCOL_COMPLETED:
                if run.user_info.protocol_group_id.value != "no_group":
                    print(f"{run.device.device_id}: {run.user_info.protocol_group_id.value} COMPLETED")
                else:
                    print(f"{run.device.device_id}: check COMPLETED")
            else:
                print(f"{run.device.device_id}: {protocol._pb.ProtocolState.Name(run.state)}")
            prev_runs = list(protocol.list_protocol_runs().run_ids)[-5:]
            prev_runs.reverse()
            print("  Previous runs:")
            for run_id in prev_runs:
                prev_run = protocol.get_run_info(run_id=run_id)
                print(f"  - {prev_run.protocol_id} / {prev_run.user_info.protocol_group_id.value}")


if __name__ == "__main__":
    main()
