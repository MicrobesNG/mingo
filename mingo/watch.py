import logging
import threading
import time
from typing import Optional
from minknow_api.manager import Manager
from slack_sdk.webhook import WebhookClient
from minknow_api import protocol_pb2

logger = logging.getLogger(__name__)

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

class MinKNOWWatcher:
    """Daemon class to hook into MinKNOW positions and stream protocol events."""
    def __init__(self, host: str = "localhost", port: Optional[int] = None, 
                 level: str = "normal", slack_webhook_url: Optional[str] = None):
        if port is None:
            self.manager = Manager(host=host)
        else:
            self.manager = Manager(host=host, port=port)
            
        self.level = level  # 'normal', 'info', 'debug'
        self.slack_client = WebhookClient(slack_webhook_url) if slack_webhook_url else None
        self.threads = []
        self._stop_event = threading.Event()

    def send_slack_notification(self, phase: str, msg: str):
        if not self.slack_client:
            return
            
        blocks = []
        if phase in ["starting", "info_starting"]:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Run started: {msg}"
                    }
                }
            ]
        elif phase in ["finished", "info_finished"]:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Run finished: {msg}"
                    }
                }
            ]
        elif phase == "error":
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Run errored: {msg}"
                    },
                    "accessory": {
                        "type": "image",
                        "image_url": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcREgSDZuZBRAm0ASuRQrpvb91kTrFsbfQDgqw&s",
                        "alt_text": "Error"
                    }
                }
            ]

        try:
            response = self.slack_client.send(text=msg, blocks=blocks)
            if response.status_code != 200:
                logger.error(f"Failed to send Slack message. Error: {response.body}")
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

    def log_and_notify(self, phase: str, message: str, is_user_protocol: bool):
        # Determine if we should emit based on level and is_user_protocol
        should_emit = False
        if self.level == "debug":
            should_emit = True
        elif self.level == "info":
            should_emit = phase in ["starting", "finished", "error", "info_starting", "info_finished"]
        elif self.level == "normal":
            # Normal only emits for user protocols
            if is_user_protocol and phase in ["starting", "finished", "error"]:
                should_emit = True

        if should_emit:
            logger.info(message)
            if phase in ["starting", "finished", "error", "info_starting", "info_finished"]:
                self.send_slack_notification(phase, message)

    def _watch_position(self, pos):
        logger.info(f"Started watching position {pos.name}")
        is_first_message = True
        last_announced_state = None
        
        while not self._stop_event.is_set():
            try:
                with pos.connect() as connection:
                    for msg in connection.protocol.watch_current_protocol_run():
                        if self._stop_event.is_set():
                            break

                        current_signature = (msg.run_id, msg.state)
                        
                        if is_first_message:
                            last_announced_state = current_signature
                            is_first_message = False
                            continue
                            
                        if current_signature == last_announced_state:
                            continue
                            
                        last_announced_state = current_signature

                        state_name = connection.protocol._pb.ProtocolState.Name(msg.state)
                        
                        run_info = None
                        try:
                            # Try to get run info to determine if it's a user protocol
                            run_info = connection.protocol.get_run_info(run_id=msg.run_id)
                        except Exception:
                            pass # Information might not be fully available
                            
                        is_user_protocol = False
                        experiment_id = "Unknown"
                        protocol_id = "Unknown"
                        
                        if run_info:
                            group_id = run_info.user_info.protocol_group_id.value
                            is_user_protocol = bool(group_id and group_id != "no_group")
                            experiment_id = group_id if is_user_protocol else "Internal Check"
                            protocol_id = run_info.protocol_id
                        
                        if self.level == "debug":
                            logger.debug(f"[{pos.name}] State change: {state_name}, run_id={msg.run_id}, user_protocol={is_user_protocol}")

                        message = f"[{pos.name}] {protocol_id} (Run: {experiment_id})"
                        
                        if msg.state == protocol_pb2.PROTOCOL_RUNNING:
                            phase = "starting" if is_user_protocol else "info_starting"
                            self.log_and_notify(phase, f"{message} started.", is_user_protocol)
                            
                        elif msg.state == protocol_pb2.PROTOCOL_COMPLETED:
                            # Calculate duration if possible
                            duration_str = ""
                            if run_info and run_info.start_time and run_info.end_time:
                                duration_seconds = run_info.end_time.seconds - run_info.start_time.seconds
                                hours, remainder = divmod(duration_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                duration_str = f" after {hours}h {minutes}m {seconds}s"
                                
                            phase = "finished" if is_user_protocol else "info_finished"
                            self.log_and_notify(phase, f"{message} finished{duration_str}.", is_user_protocol)
                            
                        elif msg.state in ERROR_STATES:
                            phase = "error"
                            report = ERROR_STATES[msg.state]
                            self.log_and_notify(phase, f"{message} stopped with error: {report}", is_user_protocol)
                            
                        elif msg.state == protocol_pb2.PROTOCOL_STOPPED_BY_USER:
                            # Calculate duration for stopped runs too
                            duration_str = ""
                            if run_info and run_info.start_time and run_info.end_time:
                                duration_seconds = run_info.end_time.seconds - run_info.start_time.seconds
                                hours, remainder = divmod(duration_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                duration_str = f" after {hours}h {minutes}m {seconds}s"

                            phase = "finished" if is_user_protocol else "info_finished"
                            self.log_and_notify(phase, f"{message} was stopped by user{duration_str}.", is_user_protocol)
                            
            except Exception as e:
                # If connection fails or stream drops, wait a bit and reconnect
                if not self._stop_event.is_set():
                    logger.debug(f"[{pos.name}] Connection error or stream ended: {e}. Reconnecting in 5s...")
                    time.sleep(5)
                    
        logger.info(f"Stopped watching position {pos.name}")

    def start(self):
        try:
            positions = self.manager.flow_cell_positions()
        except Exception as e:
            logger.error(f"Failed to connect to MinKNOW manager: {e}")
            raise
            
        for pos in positions:
            t = threading.Thread(target=self._watch_position, args=(pos,), daemon=True)
            self.threads.append(t)
            t.start()
            
    def stop(self):
        self._stop_event.set()
        for t in self.threads:
            t.join(timeout=2.0)
