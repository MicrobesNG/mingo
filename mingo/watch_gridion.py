#!/bin/env python3

import argparse
import csv
import minknow_api
import sys
from minknow_api.manager import Manager
from minknow_api import protocol_pb2
from slack_sdk.webhook import WebhookClient
from google.protobuf.json_format import (
    MessageToDict,
)
from os import environ

SLACK_HOOK = environ['SLACK_HOOK']

ERROR_STATES = {
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR: "Error",
    protocol_pb2.PROTOCOL_FINISHED_WITH_DEVICE_ERROR: "Device Error",
    protocol_pb2.PROTOCOL_FINISHED_UNABLE_TO_SEND_TELEMETRY: "Unable to send Telemetry (Error)",
    protocol_pb2.PROTOCOL_FINISHED_WITH_FLOW_CELL_DISCONNECT: "Flow cell disconnected",
    protocol_pb2.PROTOCOL_FINISHED_WITH_DEVICE_DISCONNECT: "Device disconnected",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_CALIBRATION: "Calibration error",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_BASECALL_SETTINGS: "Basecall error",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_TEMPERATURE_REQUIRED: "Temperature too low",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_NO_DISK_SPACE: "No disk space",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_TEMPERATURE_HIGH: "Temperature too high",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_BASECALLER_COMMUNICATION: "Error communicating with basecall service",
    protocol_pb2.PROTOCOL_FINISHED_WITH_NO_FLOWCELL_FOR_ACQUISITION: "No flowcell!",
    protocol_pb2.PROTOCOL_FINISHED_WITH_ERROR_BASECALLER_UNAVAILABLE: "No basecaller found",
}

OK_STATES = {
    protocol_pb2.PROTOCOL_RUNNING: "Protocol started",
    protocol_pb2.PROTOCOL_COMPLETED: "Protocol completed",
    protocol_pb2.PROTOCOL_STOPPED_BY_USER: "Protocol stopped by user",
}


def main():
    parser = argparse.ArgumentParser(
        description="""
            Service to track protocol changes on all positions on an ONT sequencer
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
            with pos.connect() as connection:
                protocol = connection.protocol
                device = connection.device
                for msg in protocol.watch_current_protocol_run():
                    dmsg: Dict = MessageToDict(msg)
                    print("\n")
                    print("--------------------------------")
                    print("\n") 
                    print(dmsg)
                    print(dmsg.get("state"), dmsg.get("device",{}).get("device_id"),
                          dmsg.get("run_id"), dmsg.get("protocol_id"), dmsg.get("phase_history",{}).get("phase"))
                    if msg.state in ERROR_STATES:
                        phase = "error"
                        report = ERROR_STATES[msg.state]
                    elif msg.state in OK_STATES:
                        if msg.state == protocol_pb2.PROTOCOL_RUNNING:
                            phase = "starting"
                        else:
                            phase = "finished"
                        report = OK_STATES[msg.state]
                    else:
                        continue
                    slackit(phase, report)

def slackit(phase, msg):
    webhook = WebhookClient(SLACK_HOOK)
    match phase:
        case "starting":
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Run started with: {msg}"
                    },
                    "accessory": {
                        "type": "image",
                        "image_url": "https://pbs.twimg.com/tweet_video_thumb/GbfXKIxawAA0e96.jpg",
                        "alt_text": "yeeeeaaaahhh"
                    }
                }
            ]
        case "finished":
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Run finished with: {msg}"
                    },
                    "accessory": {
                        "type": "image",
                        "image_url": "https://pbs.twimg.com/tweet_video_thumb/GbfXKIxawAA0e96.jpg",
                        "alt_text": "yeeeeaaaahhh"
                    }
                }
            ]

        case "error":
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Run errored with: {msg}"
                    },
                    "accessory": {
                        "type": "image",
                        "image_url": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcREgSDZuZBRAm0ASuRQrpvb91kTrFsbfQDgqw&s",
                        "alt_text": "yeeeeaaaahhh"
                    }
                }
            ]
    response = webhook.send(text=msg, blocks=blocks)
    assert response.status_code == 200
    assert response.body == "ok"

def run_state(run):
    if run.state == protocol_pb2.PROTOCOL_RUNNING:
        if run.user_info.protocol_group_id.value != "no_group":
            print(f"{run.device.device_id}: {run.user_info.protocol_group_id.value} RUNNING")
        else:
            print(f"{run.device.device_id}: check {run.protocol_id} RUNNING")
            
    elif run.state == protocol_pb2.PROTOCOL_COMPLETED:
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
