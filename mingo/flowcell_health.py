import argparse
import datetime

# minknow_api.manager supplies "Manager" a wrapper around MinKNOW's Manager gRPC API with utilities for
# querying sequencing positions + offline basecalling tools.
from minknow_api.manager import Manager
from minknow_api.protocol_pb2 import FilteringInfo


def to_datetime(date_str):
    if date_str is None:
        return None
    return datetime.datetime.strptime(date_str, "%Y-%m-%d")


def main():
    """Main entrypoint for list_flow_cell_check example"""
    parser = argparse.ArgumentParser(
        description="List historical flow cell checks on a host."
    )
    parser.add_argument(
        "--host",
        default=["localhost"],
        nargs="+",
        help="Specify which host(s) to connect to.",
    )
    parser.add_argument(
        "--port", default=None, help="Specify which port to connect to."
    )
    parser.add_argument(
        "--position", default=None, help="Specify which position to connect to."
    )
    parser.add_argument(
        "--api-token",
        default=None,
        help="Specify an API token to use, should be returned from the sequencer as a developer API token.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Specify a start date to filter results by. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Specify a end date to filter results by. Format: YYYY-MM-DD",
    )
    parser.add_argument(
        "--flow-cell-id",
        default=[],
        nargs="+",
        help="Specify one or more flow cell IDs to filter results by.",
    )

    args = parser.parse_args()

    start_date_filter = to_datetime(args.start_date)
    end_date_filter = to_datetime(args.end_date)

    results = {}

    for host in args.host:
        try:
            # Construct a manager using the host + port provided.
            manager = Manager(
                host=host, port=args.port, developer_api_token=args.api_token
            )

            # Iterate all sequencing positions:
            found_position = False
            for pos in manager.flow_cell_positions():
                if not pos.running:
                    continue

                # Ignore positions if requested:
                if args.position and args.position != pos.name:
                    continue

                # Dump all pqc protocols run on the position:
                found_position = True
                pos_connection = pos.connect()
                time_filter = FilteringInfo.TimeFilter()
                if start_date_filter:
                    time_filter.start_range.FromDatetime(start_date_filter)
                if end_date_filter:
                    time_filter.end_range.FromDatetime(end_date_filter)
                protocols = pos_connection.protocol.list_protocol_runs(
                    filter_info=FilteringInfo(
                        pqc_filter=FilteringInfo.PlatformQcFilter(),
                        experiment_start_time=time_filter,
                    )
                )
                print(f"Searching position {pos.name} on {host} - {len(protocols.run_ids)} protocols")
                for run_id in protocols.run_ids:
                    # Get the detailed run info (containing device info and qc results):
                    run_info = pos_connection.protocol.get_run_info(run_id=run_id)

                    flow_cell_id = run_info.flow_cell.flow_cell_id or run_info.flow_cell.user_specified_flow_cell_id
                    if args.flow_cell_id and flow_cell_id not in args.flow_cell_id:
                        continue

                    # Ignore the protocol if it didn't store a platform qc result:
                    if run_info.pqc_result:
                        run_start_time = run_info.start_time.ToDatetime()
                        
                        if flow_cell_id not in results or run_start_time > results[flow_cell_id]["timestamp"]:
                            results[flow_cell_id] = {
                                "host": host,
                                "passed": run_info.pqc_result.passed,
                                "total_pore_count": run_info.pqc_result.total_pore_count,
                                "position": pos.name,
                                "product_code": run_info.flow_cell.product_code
                                or run_info.flow_cell.user_specified_product_code,
                                "timestamp": run_start_time,
                            }
        except Exception as e:
            print(f"Could not connect to host {host}: {e}")


    for flow_cell_id, result in results.items():
        print(f"Flow Cell ID: {flow_cell_id}")
        print(f"  Host: {result['host']}")
        print(f"  Position: {result['position']}")
        print(f"  Product Code: {result['product_code']}")
        print(f"  Passed: {result['passed']}")
        print(f"  Total Pore Count: {result['total_pore_count']}")
        print(f"  Timestamp: {result['timestamp']}")

    if not results and args.flow_cell_id:
        print("No results found for the specified flow cell IDs.")


if __name__ == "__main__":
    main()
