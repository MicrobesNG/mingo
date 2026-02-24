# Mingo

Run manager and status tools for Oxford Nanopore P2 Solo sequencers.

## Contents

 - `mingo/run_manager.py` - interactive CLI to start runs from SLIMS
 - `mingo/gridion_status.py` - show the current run status of all of the active flowcell positions in a local gridion (requires local guest mode enabled)
 - `mingo/flowcell_health.py` - show latest pore count for a flowcell and on which hosts it has been checked
 - `mingo/watch_gridion.py` - dynamic monitoring of sequencer status
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
