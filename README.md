# Mingo

Run manager and status tools for Oxford Nanopore P2 Solo sequencers.

## Contents

 - `mingo/run_manager.py` - interactive CLI to start runs from SLIMS
 - `mingo/gridion_status.py` - show the current run status of all of the active flowcell positions in a local gridion (requires local guest mode enabled)
 - `mingo/flowcell_health.py` - show latest pore count for a flowcell and on which hosts it has been checked
 - `bin/mingo-watch` - dynaminc monitoring daemon for sequencer protocol statuses
 - `bin/mingo-coverage` - calculate genome coverage and read distribution from JSON reports or sequencing summaries

## Get Hacking

We use `direnv` and `pip-tools` for environment management.

1. Ensure `python 3.12` is available.
2. `direnv allow` to set up the venv in `.direnv/python-3.12`.
3. `pip install -r requirements.txt`
4. Run the manager in mock mode to test:
   ```bash
   ./.direnv/python-3.12/bin/python3 mingo/run_manager.py --mock
   ```
## Testing

You can simulate a flowcell using the MinKnow API, which is quite fun!

First, install MinKnow, best done on (ONT offer packages for Ubuntu and MacOS).

Configure RPC access. If you have asecure env you can do:

```
/opt/ont/minknow/bin/config_editor --conf user --filename /opt/ont/minknow/conf/user_conf --set network_security.guest_rpc_enabled=enabled
/opt/ont/minknow/bin/config_editor --conf user --filename /opt/ont/minknow/conf/user_conf --set network_security.local_connection_only=all_open
```

Add a fake device

```
# set up venv - feel free to do it differently, e.g. I use direnv for this
python -m venv .env
. .env/bin/activate
pip install -r requirements.txt

```bash
# create fake device
python -m minknow_api.examples.manage_simulated_devices --prom --host <host|localhost> --port 9502 --add S0

# list fake devices
python -m minknow_api.examples.manage_simulated_devices --host <host|localhost> --port 9502 --list

```

## Coverage Calculation

Calculate genome coverage and read distribution stats from sequencing data. Per-sample target coverage can optionally be specified via the `target_coverage` column in the sample sheet (defaults to 55x).

### Parameters

* `--auto`: Automatically find sample sheet and sequencing summary (or JSON report fallback) in an ONT run directory. This supports live-tracking temporary summaries (`.txt.tmp`) directly from MinKNOW during active runs.
* `--quick`: When using `--auto`, force the use of the (simplified) JSON run report instead of the sequencing summary.
* `--json`: Specify a JSON run report (basic yield only).
* `--summary`: Specify a sequencing summary (recommended for detailed stats).
* `--bin-threshold`: Custom read length threshold (default 7000bp) for sequencing summaries.
* `--no-low-material`: Exclude samples marked as low material in the sample sheet.
* `--below` <coverage>: show samples which miss coverage target of <coverage>x. (Displays real-time ETA when evaluating live sequencing summaries).
* `--csv`: output in csv format (default is human readable format)

### Examples

```bash

# enter run folder
cd NSR_xxxx_timestamp_run_id

# Automatically find sample sheet and sequence summary (or JSON) in an ONT run directory
mingo-coverage --auto

# Fast-track coverage by parsing only the basic JSON report, bypassing full summary sweeps
mingo-coverage --auto --quick

# Exclude low material samples and show barcodes which miss the coverage target of 55x (includes ETA to completion if summary found)
mingo-coverage --auto --no-low-material --below 55

# Using a sequencing summary (recommended for detailed stats)
mingo-coverage samples.csv --summary summary.txt

# Using a custom read length threshold (default 7000bp) for sequencing summaries
mingo-coverage samples.csv --summary summary.txt --bin-threshold 5000

```
## Upload Tool

Calculate and manage S3 uploads of sequenced FASTQ and active POD5 files using `bin/mingo-upload`. It streams files to reduce disk I/O, utilizes a smart directory structure based on the sample sheet, generates JSON manifests with public URLs, and supports upload resumption.

These must be in your `.bashrc`or equivalent.

```bash
export AWS_ACCESS_KEY_ID=XXXX
export AWS_SECRET_ACCESS_KEY=XXXX
export SLACK_WEBHOOK_URL=XXXXXX
export S3_FASTQ_BUCKET=microbesng-data/gridion_run
export S3_POD5_BUCKET=ont-raw-archive/projects
```

### Parameters
* `--fastq-only` / `--pod5-only`: Process only specific file types.
* `--s3-fastq-bucket` / `--s3-pod5-bucket`: Destination S3 Bucket for the upload. **Note: These accept an optional prefix path** (e.g. `my-bucket/path/to/my/folder`). They default to ENV vars or standard routes based on NSR/PSR run types.
* `--s3-root-folder`: Optional root folder placed inside the POD5 bucket structure.
* `--s3-endpoint-url`: Custom S3 endpoint URL for local testing (e.g., MinIO).
* `--slack-webhook`: Slack Webhook URL for status notifications.

### Examples

```bash
# General use in an ONT run directory
cd /data/NSR_xxxxxxxx_RUN_XX
mingo-upload

# Only upload FASTQs to a custom bucket and prefix
mingo-upload --fastq-only

# Only upload POD5s to a custom bucket and prefix
mingo-upload --pod5-only
```

### Testing

```
# Test locally against MinIO
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export SLACK_WEBHOOK_URL=XXXXXX
export S3_FASTQ_BUCKET=microbesng-data/gridion_run
export S3_POD5_BUCKET=ont-raw-archive/projects
export S3_ENDPOINT_URL=http://localhost:9000
# use podman wrapped minio to test as a fake s3
scripts/start_minio_dev.sh
cd example_runs/NSR_UploadTest
mingo-upload
```

## Run Watcher

A daemon (`bin/mingo-watch`) that uses MinKNOW's gRPC stream to hook into all flow cells and report ongoing status transitions. It deduplicates MinKNOW states to avoid notification spam, distinguishes between internal hardware routines and user protocols, and automatically includes the sequencer hostname in all logs and Slack notifications.

During active sequencing, it monitors coverage progression across the cohort:
* Tracks individual sample targets alongside overall cohort statistics (met count, median coverage, leading sample, and lagging samples).
* Emits Slack notifications when the first sample hits 50%/100% target, when 50% of the cohort meets target, when a 90%+ quorum is reached (actionable operator stopping point), and in periodic hourly digests.
* If started while a run is already underway, it attaches cleanly and launches local directory/coverage watchers with an `attached` notice rather than a duplicate `started` alert. When monitoring a remote MinKNOW instance, it logs a warning if the remote run directory is not mounted locally.

### Parameters
* `--host` / `--port`: Address of the sequencer running MinKNOW (default: `localhost` - port can be omitted).
* `--level`: Verbosity of the monitoring stream.
  * `normal` (default): Only emits messages and Slack notifications when standard User Protocols start, attach, error out, or finish smoothly. Discards calibration runs.
  * `info`: Additionally broadcasts MinKNOW internal tests including flow cell pings and hardware checks.
  * `debug`: All underlying events
* `--notify-slack`: Enable Slack webhooks for any of the above thresholds.
* `--slack-webhook`: Slack Webhook URL for status notifications (defaults to `SLACK_WEBHOOK_URL` ENV).

### Example Service Configuration

You can daemonize the script using systemd to start on boot and restart on failure. An example `.service` file is available at `examples/mingo-watch.service`.

1. Copy `examples/mingo-watch.service` to `/etc/systemd/system/mingo-watch.service`.
2. Edit the file to fit your deployment directory and Python interpreter:
```ini
WorkingDirectory=/opt/mng/mingo
Environment="SLACK_WEBHOOK_URL=https://hooks.slack.com/services/..."
Environment="MINKNOW_TRUSTED_CA=/data/rpc-certs/minknow/ca.crt"
ExecStart=/opt/mng/mingo/.venv/bin/python3 /opt/mng/mingo/bin/mingo-watch --host localhost --level normal --notify-slack
```
3. Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now mingo-watch
```
