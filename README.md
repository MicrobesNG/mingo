# Mingo

Run manager and status tools for Oxford Nanopore P2 Solo sequencers.

## Contents

 - `mingo/run_manager.py` - interactive CLI to start runs from SLIMS
 - `mingo/gridion_status.py` - show the current run status of all of the active flowcell positions in a local gridion (requires local guest mode enabled)
 - `mingo/flowcell_health.py` - show latest pore count for a flowcell and on which hosts it has been checked
 - `mingo/watch_gridion.py` - dynamic monitoring of sequencer status
 - `bin/calculate_coverage.py` - calculate genome coverage and read distribution from JSON reports or sequencing summaries

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

```bash
# Using a sequencing summary (recommended for detailed stats)
python3 bin/calculate_coverage.py samples.csv --summary summary.txt

# Using a JSON report (basic yield only)
python3 bin/calculate_coverage.py samples.csv --json report.json

# Using a custom read length threshold (default 7000bp)
python3 bin/calculate_coverage.py samples.csv --summary summary.txt --threshold 5000
```
