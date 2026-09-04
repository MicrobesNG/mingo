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
from .coverage import run_coverage_analysis, find_coverage_inputs, resolve_run_dirs, compute_cohort_stats

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
        
    def _check_and_log(self, file_path):
        filename = os.path.basename(file_path)
        name_lower = filename.lower()
        if (name_lower.endswith(".fastq.gz") or 
            name_lower.endswith(".fq.gz") or 
            name_lower.endswith(".fastq") or 
            name_lower.endswith(".fq") or 
            name_lower.endswith(".pod5") or 
            name_lower.endswith(".fast5")):
            self.log_callback(self.pos_name, filename)

    def on_created(self, event):
        if not event.is_directory:
            self._check_and_log(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._check_and_log(event.dest_path)

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

def make_progress_bar(current: float, target: float, length: int = 10) -> str:
    """Renders a text-based progress bar for Slack."""
    if target <= 0:
        pct = 1.0
    else:
        pct = min(max(current / target, 0.0), 1.0)
    filled = int(round(pct * length))
    bar = "▰" * filled + "▱" * (length - filled)
    return f"`{bar}` *{pct * 100:.1f}%* ({current:.1f}x / {target:.1f}x)"

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
                
    def _coverage_thread_func(self, pos_name: str, output_path: str, stop_event: threading.Event,
                             experiment_id: Optional[str] = None, protocol_id: Optional[str] = None):
        logger.info(f"[{self.host_name}:{pos_name}] Started async coverage monitoring on {output_path}")
        alerted_lead_50 = False
        alerted_lead_100 = False
        alerted_cohort_50 = False
        alerted_cohort_90 = False
        last_hourly_alert = time.time()
        
        # Initial short delay (15s) before first evaluation to let directory settle
        for _ in range(15):
            if stop_event.is_set():
                return
            time.sleep(1)

        while not stop_event.is_set():
            try:
                run_dirs = resolve_run_dirs(output_path)
                if not run_dirs:
                    raise FileNotFoundError(f"No run directories found in '{output_path}'")
                
                target_run_dir = run_dirs[-1]
                csv_path, summary_path, json_path = find_coverage_inputs(target_run_dir, quick=False)
                if not summary_path and not json_path:
                    logger.info(f"[{self.host_name}:{pos_name}] Coverage check: Sample sheet found ({os.path.basename(csv_path)}), waiting for summary or report...")
                else:
                    input_source = os.path.basename(summary_path) if summary_path else os.path.basename(json_path)
                    results = run_coverage_analysis(
                        csv_path=csv_path, 
                        summary_path=summary_path, 
                        json_path=json_path,
                        filter_below_coverage=None, 
                        output_csv=False,
                        quiet=True
                    )
                    
                    stats = compute_cohort_stats(results)
                    total_viable = stats['total_viable']
                    met_count = stats['met_count']
                    pct_met = stats['pct_met']
                    lead = stats['leading_sample']
                    
                    current_exp_id = experiment_id or (results[0].get('experiment_id') if results else None) or "Unknown"
                    current_proto_id = protocol_id
                    
                    if total_viable > 0 and lead is not None:
                        lead_cov = float(lead.get('coverage_float', 0.0))
                        lead_target = float(lead.get('target_coverage_float', 55.0))
                        lead_alias = lead.get('full_alias') or lead.get('alias', 'Unknown')
                        lead_barcode = lead.get('barcode', '')
                        lead_pct = (lead_cov / lead_target) if lead_target > 0 else 0.0

                        logger.info(
                            f"[{self.host_name}:{pos_name}] Coverage check: Cohort {met_count}/{total_viable} met target ({pct_met:.1f}%), "
                            f"Median {stats['median_cov']:.1f}x/{stats['median_target']:.1f}x ({stats['median_pct']:.1f}%), "
                            f"Lead {lead_alias} at {lead_pct * 100:.1f}% ({lead_cov:.1f}x/{lead_target:.1f}x), "
                            f"Lagging (<50%): {stats['lagging_count']} (evaluated from {input_source})"
                        )

                        cohort_progress = make_progress_bar(met_count, total_viable)
                        lagging_txt = f"⚠️ *{stats['lagging_count']}* sample(s)" if stats['lagging_count'] > 0 else "None"

                        extra_fields = [
                            ("Cohort Target Met", f"🎯 *{met_count}/{total_viable}* ({pct_met:.1f}%)\n{cohort_progress}"),
                            ("Cohort Median", f"📊 *{stats['median_cov']:.1f}x* / {stats['median_target']:.1f}x (*{stats['median_pct']:.1f}%*)"),
                            ("Leading Sample", f"🚀 `{lead_alias}` ({lead_barcode})\n📈 *{lead_cov:.1f}x* / {lead_target:.1f}x (*{lead_pct * 100:.1f}%*)"),
                            ("Lagging (<50%)", lagging_txt),
                        ]

                        if lead_pct >= 0.5 and not alerted_lead_50:
                            alerted_lead_50 = True
                            msg_text = f"[{self.host_name}:{pos_name}] 🚀 First sample reached 50% target ({lead_alias}: {lead_cov:.1f}x / {lead_target:.1f}x)"
                            logger.info(msg_text)
                            self.send_slack_notification(
                                "coverage_50", 
                                msg_text,
                                experiment_id=current_exp_id,
                                pos_name=pos_name,
                                protocol_id=current_proto_id,
                                extra_fields=extra_fields
                            )
                            
                        if lead_pct >= 1.0 and not alerted_lead_100:
                            alerted_lead_100 = True
                            msg_text = f"[{self.host_name}:{pos_name}] 🎉 First sample reached 100% target ({lead_alias}: {lead_cov:.1f}x / {lead_target:.1f}x)"
                            logger.info(msg_text)
                            self.send_slack_notification(
                                "coverage_100", 
                                msg_text,
                                experiment_id=current_exp_id,
                                pos_name=pos_name,
                                protocol_id=current_proto_id,
                                extra_fields=extra_fields
                            )

                        if pct_met >= 50.0 and not alerted_cohort_50:
                            alerted_cohort_50 = True
                            msg_text = f"[{self.host_name}:{pos_name}] 📊 Half of cohort reached target ({met_count}/{total_viable} samples met)"
                            logger.info(msg_text)
                            self.send_slack_notification(
                                "cohort_50",
                                msg_text,
                                experiment_id=current_exp_id,
                                pos_name=pos_name,
                                protocol_id=current_proto_id,
                                extra_fields=extra_fields
                            )

                        if pct_met >= 90.0 and not alerted_cohort_90:
                            alerted_cohort_90 = True
                            msg_text = f"[{self.host_name}:{pos_name}] 🏁 Cohort quorum reached: {met_count}/{total_viable} samples ({pct_met:.1f}%) met target"
                            logger.info(msg_text)
                            self.send_slack_notification(
                                "cohort_quorum",
                                msg_text,
                                experiment_id=current_exp_id,
                                pos_name=pos_name,
                                protocol_id=current_proto_id,
                                extra_fields=extra_fields
                            )
                            
                        now = time.time()
                        if now - last_hourly_alert >= 3600:
                            last_hourly_alert = now
                            msg_text = f"[{self.host_name}:{pos_name}] ⏱️ Hourly update: {met_count}/{total_viable} samples met target ({pct_met:.1f}%)"
                            logger.info(msg_text)
                            self.send_slack_notification(
                                "coverage_hourly", 
                                msg_text,
                                experiment_id=current_exp_id,
                                pos_name=pos_name,
                                protocol_id=current_proto_id,
                                extra_fields=extra_fields
                            )
                        
            except FileNotFoundError as e:
                logger.info(f"[{self.host_name}:{pos_name}] Coverage check waiting for run files: {e}")
            except Exception as e:
                logger.warning(f"[{self.host_name}:{pos_name}] Coverage check encountered an issue: {e}")

            # Wait 5 minutes (300s) between checks, checking stop_event every second
            for _ in range(300):
                if stop_event.is_set():
                    break
                time.sleep(1)

    def _start_coverage_watcher(self, pos_name: str, path: str,
                                experiment_id: Optional[str] = None,
                                protocol_id: Optional[str] = None):
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
        t = threading.Thread(
            target=self._coverage_thread_func,
            args=(pos_name, path, stop_event, experiment_id, protocol_id),
            daemon=True
        )
        self.coverage_threads[pos_name] = (t, stop_event)
        t.start()
        
    def _stop_coverage_watcher(self, pos_name: str):
        if pos_name in self.coverage_threads:
            t, stop_event = self.coverage_threads.pop(pos_name)
            stop_event.set()
            t.join(timeout=2.0)
            logger.debug(f"[{self.host_name}:{pos_name}] Stopped async coverage monitoring")

    def _build_slack_blocks(self, phase: str, msg: str,
                            experiment_id: Optional[str] = None,
                            pos_name: Optional[str] = None,
                            protocol_id: Optional[str] = None,
                            duration_str: Optional[str] = None,
                            error_detail: Optional[str] = None,
                            extra_fields: Optional[list] = None):
        """Constructs rich Slack Block Kit attachments with consistent branding and status styling."""
        now_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        host = self.host_name

        phase_config = {
            "starting": ("#2EB67D", "🟢 Sequencing Run Started"),
            "info_starting": ("#2EB67D", "🟢 Routine / Internal Run Started"),
            "attached": ("#4A90E2", "🔗 Attached to In-Progress Run"),
            "info_attached": ("#4A90E2", "🔗 Attached to Routine Run"),
            "finished": ("#27AE60", "🏁 Sequencing Run Finished"),
            "info_finished": ("#27AE60", "🏁 Routine Run Finished"),
            "error": ("#E01E5A", "❌ Sequencing Run Error"),
            "stopped": ("#ECB22E", "⏹️ Run Stopped by User"),
            "coverage_50": ("#ECB22E", "🚀 Coverage Milestone (First Sample 50%)"),
            "coverage_100": ("#00B894", "🎉 Coverage Milestone (First Sample 100%)"),
            "cohort_50": ("#3498DB", "📊 Cohort Milestone (50% Met Target)"),
            "cohort_quorum": ("#27AE60", "🏁 Cohort Quorum Met (90%+ Met Target)"),
            "coverage_hourly": ("#0984E3", "⏱️ Hourly Coverage Update"),
            "coverage": ("#0984E3", "📊 Coverage Update"),
        }

        color, header_text = phase_config.get(phase, ("#4A90E2", "🔔 MinKNOW Notification"))

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header_text,
                    "emoji": True
                }
            }
        ]

        fields = []
        if experiment_id:
            fields.append({"type": "mrkdwn", "text": f"*Experiment Group:*\n`{experiment_id}`"})
        if host and pos_name:
            fields.append({"type": "mrkdwn", "text": f"*Sequencer & Slot:*\n💻 `{host}` | 📍 `{pos_name}`"})
        elif host:
            fields.append({"type": "mrkdwn", "text": f"*Host:*\n💻 `{host}`"})

        if protocol_id:
            fields.append({"type": "mrkdwn", "text": f"*Protocol:*\n`{protocol_id}`"})

        if duration_str:
            clean_duration = duration_str.strip().replace("after ", "")
            fields.append({"type": "mrkdwn", "text": f"*Duration:*\n⏱️ `{clean_duration}`"})

        if error_detail:
            fields.append({"type": "mrkdwn", "text": f"*Error:*\n⚠️ *{error_detail}*"})

        if extra_fields:
            for title, value in extra_fields:
                fields.append({"type": "mrkdwn", "text": f"*{title}:*\n{value}"})

        if fields:
            blocks.append({
                "type": "section",
                "fields": fields
            })
        else:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": msg
                }
            })

        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*MiNGo Watcher* • {now_str}"
                }
            ]
        })

        return color, blocks

    def send_slack_notification(self, phase: str, msg: str, **kwargs):
        if not self.slack_client:
            return
            
        color, blocks = self._build_slack_blocks(phase, msg, **kwargs)

        try:
            response = self.slack_client.send(
                text=msg,
                attachments=[
                    {
                        "color": color,
                        "blocks": blocks
                    }
                ]
            )
            if response.status_code != 200:
                logger.error(f"Failed to send Slack message. Error: {response.body}")
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

    def log_and_notify(self, phase: str, message: str, is_user_protocol: bool, **slack_kwargs):
        should_emit = False
        if self.level == "debug":
            should_emit = True
        elif self.level == "info":
            should_emit = phase in ["starting", "finished", "error", "stopped", "info_starting", "info_finished", "attached", "info_attached"]
        elif self.level == "normal":
            if is_user_protocol and phase in ["starting", "finished", "error", "stopped", "attached"]:
                should_emit = True

        if should_emit:
            logger.info(message)
            if phase in ["starting", "finished", "error", "stopped", "info_starting", "info_finished", "coverage", "attached", "info_attached"]:
                self.send_slack_notification(phase, message, **slack_kwargs)

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
                                    self._start_coverage_watcher(
                                        pos.name, 
                                        output_path, 
                                        experiment_id=experiment_id, 
                                        protocol_id=protocol_id
                                    )

                                phase = "attached" if is_user_protocol else "info_attached"
                                self.log_and_notify(
                                    phase, 
                                    f"{message} attached to in-progress run.", 
                                    is_user_protocol,
                                    experiment_id=experiment_id,
                                    pos_name=pos.name,
                                    protocol_id=protocol_id
                                )
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
                                self._start_coverage_watcher(
                                    pos.name, 
                                    output_path, 
                                    experiment_id=experiment_id, 
                                    protocol_id=protocol_id
                                )

                        phase = "starting" if is_user_protocol else "info_starting"
                        if not announced_start:
                            self.log_and_notify(
                                phase, 
                                f"{message} started.", 
                                is_user_protocol,
                                experiment_id=experiment_id,
                                pos_name=pos.name,
                                protocol_id=protocol_id
                            )
                            announced_start = True
                            
                        elif msg.state == protocol_pb2.PROTOCOL_COMPLETED:
                            duration_str = ""
                            if run_info and run_info.start_time and run_info.end_time:
                                duration_seconds = run_info.end_time.seconds - run_info.start_time.seconds
                                hours, remainder = divmod(duration_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                duration_str = f" after {hours}h {minutes}m {seconds}s"
                                
                            phase = "finished" if is_user_protocol else "info_finished"
                            self._stop_directory_watcher(pos.name)
                            self._stop_coverage_watcher(pos.name)
                            self.log_and_notify(
                                phase, 
                                f"{message} finished{duration_str}.", 
                                is_user_protocol,
                                experiment_id=experiment_id,
                                pos_name=pos.name,
                                protocol_id=protocol_id,
                                duration_str=duration_str
                            )
                            
                        elif msg.state in ERROR_STATES:
                            self._stop_directory_watcher(pos.name)
                            self._stop_coverage_watcher(pos.name)
                            report = ERROR_STATES[msg.state]
                            self.log_and_notify(
                                "error", 
                                f"{message} stopped with error: {report}", 
                                is_user_protocol,
                                experiment_id=experiment_id,
                                pos_name=pos.name,
                                protocol_id=protocol_id,
                                error_detail=report
                            )
                            
                        elif msg.state == protocol_pb2.PROTOCOL_STOPPED_BY_USER:
                            duration_str = ""
                            if run_info and run_info.start_time and run_info.end_time:
                                duration_seconds = run_info.end_time.seconds - run_info.start_time.seconds
                                hours, remainder = divmod(duration_seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                duration_str = f" after {hours}h {minutes}m {seconds}s"

                            phase = "stopped" if is_user_protocol else "info_finished"
                            self._stop_directory_watcher(pos.name)
                            self._stop_coverage_watcher(pos.name)
                            self.log_and_notify(
                                phase, 
                                f"{message} was stopped by user{duration_str}.", 
                                is_user_protocol,
                                experiment_id=experiment_id,
                                pos_name=pos.name,
                                protocol_id=protocol_id,
                                duration_str=duration_str
                            )
                            
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
