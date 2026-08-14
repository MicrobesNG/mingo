import logging
import threading
import time
import os
import socket
from typing import Optional
from minknow_api.manager import Manager
from slack_sdk.webhook import WebhookClient
from minknow_api import protocol_pb2
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from .coverage import run_coverage_analysis, find_coverage_inputs

logger = logging.getLogger(__name__)

def resolve_display_hostname(host: Optional[str]) -> str:
    """Resolve a clean, friendly display hostname."""
    if not host or host in ["localhost", "127.0.0.1", "::1", "0.0.0.0"]:
        return socket.gethostname().split('.')[0]
    # Return short name if FQDN was provided (e.g. pancake.local -> pancake), or IP/host as given
    if '.' in host and not host.replace('.', '').isdigit():
        return host.split('.')[0]
    return host

def is_remote_host(host: Optional[str]) -> bool:
    """Determine if target MinKNOW host is a remote machine."""
    if not host or host in ["localhost", "127.0.0.1", "::1", "0.0.0.0"]:
        return False
    try:
        local_hostname = socket.gethostname()
        local_fqdn = socket.getfqdn()
        if host.lower() in [local_hostname.lower(), local_fqdn.lower()]:
            return False
        host_ip = socket.gethostbyname(host)
        local_ips = ["127.0.0.1", "::1"]
        try:
            local_ips.append(socket.gethostbyname(local_hostname))
        except Exception:
            pass
        if host_ip in local_ips:
            return False
    except Exception:
        pass
    return True

class BatchFileHandler(FileSystemEventHandler):
    def __init__(self, pos_name, log_callback):
        self.pos_name = pos_name
        self.log_callback = log_callback
        
    def on_created(self, event):
        if not event.is_directory:
            ext = os.path.splitext(event.src_path)[1].lower()
            if ext in [".fastq", ".gz", ".pod5"]:
                filename = os.path.basename(event.src_path)
                self.log_callback(self.pos_name, filename)

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
        self.host = host
        self.port = port
        self.host_name = resolve_display_hostname(host)
        self.is_remote = is_remote_host(host)

        if port is None:
            self.manager = Manager(host=host)
        else:
            self.manager = Manager(host=host, port=port)
            
        self.level = level  # 'normal', 'info', 'debug'
        self.slack_client = WebhookClient(slack_webhook_url) if slack_webhook_url else None
        self.threads = []
        self._stop_event = threading.Event()
        self.file_observers = {}  # pos.name -> (Observer, path)
        self.coverage_threads = {} # pos.name -> (Thread, Event)

    def log_data_file(self, pos_name: str, filename: str):
        logger.info(f"[{self.host_name}:{pos_name}] Wrote data file: {filename}")

    def _start_directory_watcher(self, pos_name: str, path: str):
        current_observer = self.file_observers.get(pos_name)
        if current_observer and current_observer[1] == path:
            return  # Already watching this exact path
            
        self._stop_directory_watcher(pos_name)
        if not path:
            return

        if self.is_remote:
            if not os.path.exists(path):
                logger.warning(
                    f"[{self.host_name}:{pos_name}] MinKNOW host '{self.host}' is remote and output path '{path}' does not exist locally. "
                    f"Filesystem watcher skipped (mount the remote directory locally to enable file watching)."
                )
                return
            else:
                logger.warning(
                    f"[{self.host_name}:{pos_name}] MinKNOW host '{self.host}' is remote. "
                    f"Watching mounted directory '{path}'. Note: inotify file events may not trigger over some network mounts."
                )

        try:
            os.makedirs(path, exist_ok=True)
            observer = Observer()
            handler = BatchFileHandler(pos_name, self.log_data_file)
            observer.schedule(handler, path, recursive=True)
            observer.start()
            self.file_observers[pos_name] = (observer, path)
            logger.debug(f"[{self.host_name}:{pos_name}] Started filesystem watcher on {path}")
        except Exception as e:
            logger.error(f"[{self.host_name}:{pos_name}] Failed to start filesystem watcher on {path}: {e}")

    def _stop_directory_watcher(self, pos_name: str):
        if pos_name in self.file_observers:
            observer, path = self.file_observers.pop(pos_name)
            try:
                observer.stop()
                observer.join(timeout=1.0)
                logger.debug(f"[{self.host_name}:{pos_name}] Stopped filesystem watcher on {path}")
            except Exception:
                pass
                
    def _coverage_thread_func(self, pos_name: str, output_path: str, stop_event: threading.Event):
        logger.info(f"[{self.host_name}:{pos_name}] Started async coverage monitoring on {output_path}")
        target_coverage = 55.0
        alerted_50 = False
        alerted_100 = False
        last_hourly_alert = time.time()
        
        while not stop_event.is_set():
            # Wait 5 minutes between checks, polling effectively every second for rapid exits
            for _ in range(600):
                if stop_event.is_set():
                    break
                time.sleep(1)
                
            if stop_event.is_set():
                break
                
            try:
                csv_path, summary_path, json_path = find_coverage_inputs(output_path, quick=False)
                if not summary_path and not json_path:
                    continue
                    
                results = run_coverage_analysis(
                    csv_path=csv_path, 
                    summary_path=summary_path, 
                    json_path=json_path,
                    filter_below_coverage=None, 
                    output_csv=False,
                    quiet=True
                )
                
                max_cov = 0.0
                for row in results:
                    if row.get('type') == 'negative_control':
                        continue
                    try:
                        cov = float(row.get('coverage_float', 0.0))
                    except (ValueError, TypeError):
                        cov = 0.0
                        
                    if cov > max_cov:
                        max_cov = cov
                        
                msgs = []
                if max_cov >= (target_coverage * 0.5) and not alerted_50:
                    alerted_50 = True
                    msgs.append(f"[{self.host_name}:{pos_name}] 🚀 First non-control sample hit 50% target ({max_cov:.1f}x / {target_coverage}x)")
                    
                if max_cov >= target_coverage and not alerted_100:
                    alerted_100 = True
                    msgs.append(f"[{self.host_name}:{pos_name}] 🎉 First non-control sample hit 100% target ({max_cov:.1f}x / {target_coverage}x)")
                    
                now = time.time()
                if now - last_hourly_alert >= 3600:
                    last_hourly_alert = now
                    msgs.append(f"[{self.host_name}:{pos_name}] ⏱️ Hourly explicit update: Max sample coverage is {max_cov:.1f}x (Target: {target_coverage}x)")
                    
                for m in msgs:
                    logger.info(m)
                    self.send_slack_notification("coverage", m)
                    
            except Exception as e:
                logger.debug(f"[{self.host_name}:{pos_name}] Coverage auto-discovery not ready or failed: {e}")

    def _start_coverage_watcher(self, pos_name: str, path: str):
        self._stop_coverage_watcher(pos_name)
        if not path:
            return
        if self.is_remote and not os.path.exists(path):
            logger.warning(
                f"[{self.host_name}:{pos_name}] MinKNOW host '{self.host}' is remote and output path '{path}' does not exist locally. "
                f"Async coverage monitoring skipped."
            )
            return
        stop_event = threading.Event()
        t = threading.Thread(target=self._coverage_thread_func, args=(pos_name, path, stop_event), daemon=True)
        self.coverage_threads[pos_name] = (t, stop_event)
        t.start()
        
    def _stop_coverage_watcher(self, pos_name: str):
        if pos_name in self.coverage_threads:
            t, stop_event = self.coverage_threads.pop(pos_name)
            stop_event.set()
            t.join(timeout=2.0)
            logger.debug(f"[{self.host_name}:{pos_name}] Stopped async coverage monitoring")

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
                        "text": f"🟢 *Run Started*: {msg}"
                    }
                }
            ]
        elif phase in ["attached", "info_attached"]:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🔗 *Attached to in-progress run*: {msg}"
                    }
                }
            ]
        elif phase in ["finished", "info_finished"]:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🏁 *Run Finished*: {msg}"
                    }
                }
            ]
        elif phase == "error":
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"❌ *Run Errored*: {msg}"
                    },
                    "accessory": {
                        "type": "image",
                        "image_url": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcREgSDZuZBRAm0ASuRQrpvb91kTrFsbfQDgqw&s",
                        "alt_text": "Error"
                    }
                }
            ]
        elif phase == "coverage":
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📊 *Coverage Update*\n{msg}"
                    }
                }
            ]

        try:
            response = self.slack_client.send(text=msg, blocks=blocks, username=f"MiNGo ({self.host_name})")
            if response.status_code != 200:
                logger.error(f"Failed to send Slack message. Error: {response.body}")
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

    def log_and_notify(self, phase: str, message: str, is_user_protocol: bool):
        should_emit = False
        if self.level == "debug":
            should_emit = True
        elif self.level == "info":
            should_emit = phase in ["starting", "finished", "error", "info_starting", "info_finished", "attached", "info_attached"]
        elif self.level == "normal":
            # Normal only emits for user protocols
            if is_user_protocol and phase in ["starting", "finished", "error", "attached"]:
                should_emit = True

        if should_emit:
            logger.info(message)
            if phase in ["starting", "finished", "error", "info_starting", "info_finished", "coverage", "attached", "info_attached"]:
                self.send_slack_notification(phase, message)

    def _extract_run_info(self, connection, run_id: str, msg=None):
        """Extract protocol details and output path from MinKNOW run info."""
        run_info = None
        try:
            run_info = connection.protocol.get_run_info(run_id=run_id)
        except Exception:
            pass

        is_user_protocol = False
        experiment_id = "Unknown"
        protocol_id = "Unknown"
        output_path = None

        if run_info:
            group_id = run_info.user_info.protocol_group_id.value
            is_user_protocol = bool(group_id and group_id != "no_group")
            experiment_id = group_id if is_user_protocol else "Internal Check"
            protocol_id = run_info.protocol_id
            output_path = getattr(run_info, 'output_path', None)

        if not output_path and msg is not None:
            output_path = getattr(msg, 'output_path', None)

        return run_info, is_user_protocol, experiment_id, protocol_id, output_path

    def _watch_position(self, pos):
        logger.info(f"[{self.host_name}:{pos.name}] Started watching position")
        is_first_message = True
        last_announced_state = None
        
        current_run_id = None
        announced_start = False
        
        while not self._stop_event.is_set():
            try:
                with pos.connect() as connection:
                    for msg in connection.protocol.watch_current_protocol_run():
                        if self._stop_event.is_set():
                            break

                        if msg.run_id != current_run_id:
                            current_run_id = msg.run_id
                            announced_start = False

                        current_signature = (msg.run_id, msg.state)
                        
                        if is_first_message:
                            last_announced_state = current_signature
                            is_first_message = False

                            # If attaching to an already running protocol on startup
                            if msg.state == protocol_pb2.PROTOCOL_RUNNING:
                                announced_start = True
                                run_info, is_user_protocol, experiment_id, protocol_id, output_path = (
                                    self._extract_run_info(connection, msg.run_id, msg)
                                )
                                message = f"*{experiment_id}* (`{self.host_name}` | `{pos.name}` | {protocol_id})"

                                if output_path and is_user_protocol:
                                    self._start_directory_watcher(pos.name, output_path)
                                    self._start_coverage_watcher(pos.name, output_path)

                                phase = "attached" if is_user_protocol else "info_attached"
                                self.log_and_notify(phase, f"{message} attached to in-progress run.", is_user_protocol)
                            continue
                            
                        if current_signature == last_announced_state:
                            continue
                            
                        last_announced_state = current_signature

                        state_name = connection.protocol._pb.ProtocolState.Name(msg.state)
                        
                        run_info, is_user_protocol, experiment_id, protocol_id, output_path = (
                            self._extract_run_info(connection, msg.run_id, msg)
                        )
                        
                        if self.level == "debug":
                            logger.debug(f"[{self.host_name}:{pos.name}] State change: {state_name}, run_id={msg.run_id}, user_protocol={is_user_protocol}")

                        message = f"*{experiment_id}* (`{self.host_name}` | `{pos.name}` | {protocol_id})"
                        
                        if msg.state == protocol_pb2.PROTOCOL_RUNNING:
                            # Start watcher when running starts
                            if output_path and is_user_protocol:
                                self._start_directory_watcher(pos.name, output_path)
                                self._start_coverage_watcher(pos.name, output_path)

                        phase = "starting" if is_user_protocol else "info_starting"
                        if not announced_start:
                            self.log_and_notify(phase, f"{message} started.", is_user_protocol)
                            announced_start = True
                            
                        elif msg.state == protocol_pb2.PROTOCOL_COMPLETED:
                            # Calculate duration if possible
                            duration_str = ""
                            if run_info and run_info.start_time and run_info.end_time:
                                duration_seconds = run_info.end_time.seconds - run_info.start_time.seconds
                                hours, remainder = divmod(duration_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                duration_str = f" after {hours}h {minutes}m {seconds}s"
                                
                            phase = "finished" if is_user_protocol else "info_finished"
                            self._stop_directory_watcher(pos.name)
                            self._stop_coverage_watcher(pos.name)
                            self.log_and_notify(phase, f"{message} finished{duration_str}.", is_user_protocol)
                            
                        elif msg.state in ERROR_STATES:
                            phase = "error"
                            self._stop_directory_watcher(pos.name)
                            self._stop_coverage_watcher(pos.name)
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
                            self._stop_directory_watcher(pos.name)
                            self._stop_coverage_watcher(pos.name)
                            self.log_and_notify(phase, f"{message} was stopped by user{duration_str}.", is_user_protocol)
                            
            except Exception as e:
                # If connection fails or stream drops, wait a bit and reconnect
                if not self._stop_event.is_set():
                    logger.debug(f"[{self.host_name}:{pos.name}] Connection error or stream ended: {e}. Reconnecting in 5s...")
                    time.sleep(5)
                    
        logger.info(f"[{self.host_name}:{pos.name}] Stopped watching position")

    def start(self):
        try:
            positions = self.manager.flow_cell_positions()
        except Exception as e:
            logger.error(f"[{self.host_name}] Failed to connect to MinKNOW manager: {e}")
            raise
            
        for pos in positions:
            t = threading.Thread(target=self._watch_position, args=(pos,), daemon=True)
            self.threads.append(t)
            t.start()
            
    def stop(self):
        self._stop_event.set()
        for pos_name in list(self.file_observers.keys()):
            self._stop_directory_watcher(pos_name)
        for pos_name in list(self.coverage_threads.keys()):
            self._stop_coverage_watcher(pos_name)
        for t in self.threads:
            t.join(timeout=2.0)
