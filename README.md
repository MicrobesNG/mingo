# Lab tools

Various little bits to help out the production team at MicrobesNG

## Contents

 - `status/gridion_status.py` - show the current run status of all of the active flowcell positions in a local gridion (requires local guest mode enabled)
 - `status/flowcell_health.py` - show latest pore count for a flowcell and on which hosts it has been checked

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

# create fake device
python -m minknow_api.examples.manage_simulated_devices --prom --host <host|localhost> --port 9502 --add S0

# list fake devices
python -m minknow_api.examples.manage_simulated_devices --host <host|localhost> --port 9502 --list

```
