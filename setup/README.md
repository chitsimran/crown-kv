# CROWN-KV VM Automation

These scripts mirror the setup style from `sjsj0/crown/setup`, but run this
repo's current `metadata_store`, `server`, and REPL client.

## Configure

```bash
cp setup/.env.example setup/.env
$EDITOR setup/.env
```

`setup/prod_hosts.csv` is the authoritative VM list. By default it contains:

```text
sp26-cs525-1201.cs.illinois.edu,sp26-cs525-1202.cs.illinois.edu,sp26-cs525-1203.cs.illinois.edu
```

Set `METADATA_HOST` in `setup/.env` to choose the VM that runs
`metadata_store`:

```bash
METADATA_HOST=sp26-cs525-1201.cs.illinois.edu
```

That VM is metadata-only. It is excluded from data-node membership and no
`server` process is started there. Every non-metadata host in
`setup/prod_hosts.csv` runs one `server`, and the hostname is used as that
node's ID.

## Deploy

```bash
./setup/vm_setup.bash setup
./setup/vm_setup.bash start
```

`start` clones or updates the repo, builds with CMake, starts metadata on
`METADATA_HOST`, then starts node servers on the remaining hosts in tmux.

## REPL

Print the exact command:

```bash
./setup/vm_setup.bash repl-info
```

Then run the REPL from a machine reachable by the servers:

```bash
build/client --metadata sp26-cs525-1201.cs.illinois.edu:50050 \
  --listen <reachable-client-host>:6000 \
  --repl
```

Useful REPL commands:

```text
members
put a hello
get a
use CHAIN
use CRAQ
use CROWN
bench 100 key value
quit
```

## Restart / Stop

```bash
./setup/vm_setup.bash rerun
./setup/vm_setup.bash kill
```

tmux sessions:

- `crown_metadata_50050`
- `crown_node_50051`

Logs and pid files live under `run/shared/` in the remote project checkout.
