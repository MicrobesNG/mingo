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

Calculate genome coverage and read distribution stats from sequencing data.

### Parameters

* `--auto`: Automatically find sample sheet and JSON report in an ONT run directory.
* `--json`: Specify a JSON report (basic yield only).
* `--summary`: Specify a sequencing summary (recommended for detailed stats).
* `--bin-threshold`: Custom read length threshold (default 7000bp) for sequencing summaries.
* `--hide-low-material`: Hide low material samples
* `--below` <coverage>: show samples which miss coverage target of <coverage>x.
* `--csv`: output in csv format (default is human readable format)

### Examples

```bash

# enter run folder
cd NSR_xxxx_timestamp_run_id

# Automatically find sample sheet and JSON report in an ONT run directory
mingo-coverage --auto

# Specifying a JSON report (basic yield only)
mingo-coverage samples.csv --json report.json

# Hide low material samples and show samples which miss coverage target of 55x
mingo-coverage --auto --hide-low-material --below 55

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

A daemon (`bin/mingo-watch`) that uses MinKNOW's gRPC stream to hook into all flow cells and report ongoing status transitions. It deduplicates MinKNOW states to avoid notification spam and distinguishes between internal hardware routines and user protocols (i.e. runs).

### Parameters
* `--host` / `--port`: Address of the sequencer running MinKNOW (default: `localhost` - port can be omitted).
* `--level`: Verbosity of the monitoring stream.
  * `normal` (default): Only emits messages and Slack notifications when standard User Protocols start, error out, or finish smoothly. Discards calibration runs.
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
