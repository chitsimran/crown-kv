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
bench 50000 setup/generated_kv_dataset/all_kv_pairs.csv
quit
```

Generate a balanced KV file or a hot-key workload file:

```bash
python setup/generate_kv_dataset.py --total-pairs 20000
python setup/generate_kv_dataset.py --total-pairs 50000 --nodes 3
python setup/generate_kv_dataset.py --total-pairs 20000 --hot-share 20
python setup/generate_kv_dataset.py --total-pairs 20000 --hot-share 40
python setup/generate_kv_dataset.py --total-pairs 20000 --hot-share 60
python setup/generate_kv_dataset.py --total-pairs 20000 --hot-share 80
python setup/generate_kv_dataset.py --total-pairs 20000 --file-name hot_20.csv --hot-share 20
```

`--hot-share` controls what percentage of the rows are drawn from the hot key set.
`--hot-set-share` controls how many of the unique keys belong to that hot set.
`--nodes` balances and prints the expected key-to-head distribution for that ring size.
`--file-name` writes into `setup/generated_kv_dataset/` using just the filename.
`--output-file` still accepts a full custom path.
The default output file is `setup/generated_kv_dataset/all_kv_pairs.csv`.

The benchmark precomputes each key's target node before timing begins, then
prints how many writes it issued to each target node. It defaults to closed-loop
mode capped by `max-outstanding` (default 1000), which is best for failure and
recovery plots. Add `open` or `--open-loop` for open-loop total-throughput runs.
During a benchmark, transport failures or `WRONG_NODE` responses trigger a
metadata refresh and route rebuild for future writes.

## Restart / Stop

```bash
./setup/vm_setup.bash rerun
./setup/vm_setup.bash kill
```

tmux sessions:

- `crown_metadata_50050`
- `crown_node_50051`

Logs and pid files live under `run/shared/` in the remote project checkout.

Useful log commands:

```bash
# metadata host
tail -f /home/crown-kv/run/shared/metadata_50050.log

# data node host
tail -f /home/crown-kv/run/shared/server_50051.log
```

Each log starts with the launch timestamp, host, tmux session, and exact command.
