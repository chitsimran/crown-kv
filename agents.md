# CROWN-KV: Implementation Agent Specification

This document is the authoritative implementation guide for the CROWN-KV system. It is written for coding agents and should be followed precisely. All ambiguities should be resolved by referencing the invariants and open questions flagged below.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Terminology & Invariants](#2-terminology--invariants)
3. [Codebase Layout & Existing Infrastructure](#3-codebase-layout--existing-infrastructure)
4. [Protocol: Write Path](#4-protocol-write-path)
5. [Protocol: Read Path](#5-protocol-read-path)
6. [Protocol: Reconfiguration & Failure Handling](#6-protocol-reconfiguration--failure-handling)
7. [Metadata Store Responsibilities](#7-metadata-store-responsibilities)
8. [gRPC / Proto Additions Required](#8-grpc--proto-additions-required)
9. [Threading Model](#9-threading-model)
10. [State Machine: Node Lifecycle](#10-state-machine-node-lifecycle)
11. [Error Handling & Edge Cases](#11-error-handling--edge-cases)
12. [Testing Checklist](#13-testing-checklist)

---

## 1. System Overview

CROWN (Chain Replication with Orchestrated Write Nodes) is a ring-based replication protocol. Replicas form a directed ring instead of a linear chain. Each node has exactly one **predecessor** and one **successor**. For any given key, a deterministic hash function selects a **head** node; the head's predecessor in the ring is the **tail** for that key.

### Key Properties

- **Per-key linearizability**: all writes to a key pass through one fixed head → version ordering is established at the head.
- **Ring topology**: after the tail, the ring wraps back to the head. There is no "first" or "last" node globally.
- **Distributed heads/tails**: different keys map to different head-tail pairs, eliminating the single-node write bottleneck of classic Chain Replication.
- **Read path**: CROWN uses CRAQ-style reads — any replica may serve a read. If the replica's latest version for the key is clean, it responds immediately (1-RTT). If dirty, it queries the key's tail for the committed version number before responding (see §5).
- **Failure handling**: a Paxos-based `metadata_store` (master) monitors liveness, coordinates FREEZE/drain/reconfiguration, and distributes new ring membership.

### Protocols Implemented

The codebase supports three modes: `CHAIN`, `CRAQ`, and `CROWN`. Replication mechanics differ by mode; the infrastructure (gRPC, CMake, proto transport) is shared.

---

## 2. Terminology & Invariants

| Term | Definition |
|---|---|
| `head(key)` | Node at index `fnv1a_64(key) % ring_size` in the ordered membership array |
| `tail(key)` | Node at index `(fnv1a_64(key) + ring_size - 1) % ring_size` — the predecessor of `head(key)` in the ring |
| `version` | Monotonically increasing uint64 per key, assigned by the head |
| `clean version` | A version that has been committed (reached the tail) |
| `dirty version` | A version propagating through the ring, not yet committed |
| `committed` | A write is committed when the tail has durably applied it |
| `pending list` | Per-node list of writes forwarded but not yet ACK'd |
| `FREEZE` | Metadata-store-initiated signal to stop accepting new client writes |
| `Inflight message` | A sentinel message sent around the ring to drain all pending writes |
| `reconfiguration` | A new membership view delivered by `metadata_store` to all nodes |

### Invariants That Must Never Be Violated

1. **I1 — Version monotonicity**: The head must assign `version = prev_version_for_key + 1` atomically (under a per-key mutex or equivalent). No two writes to the same key may share a version.
2. **I2 — Single commit point**: A write is externally visible only after the tail has applied it and sent an ACK to the client. The head's local acceptance does not constitute commitment.
3. **I3 — Tail-only reads (CHAIN only)**: In CHAIN mode only, only the tail for a key may serve a read response. Non-tail nodes must reject with `WRONG_NODE`.
4. **I4 — Clean reads only (CRAQ and CROWN)**: In both CRAQ and CROWN mode, a node may serve a read from its own store only if the latest stored version for that key is clean. If dirty, it must query the tail for the committed version before responding.
5. **I5 — No version rollback**: A node must silently drop any incoming write whose version ≤ its locally committed version for that key (stale retransmission).
6. **I6 — FREEZE is durable before drain**: No node may re-open to client writes until it has received an explicit UNFREEZE (bundled with the new configuration).

---

## 3. Codebase Layout & Existing Infrastructure

```
crown-kv/
├── CMakeLists.txt
├── agents.md               ← this file
├── proto/
│   └── replication.proto   ← shared proto for all modes
└── src/
    ├── kv_store.*          ← in-memory key-value store (clean versions)
    ├── chain_server.*      ← CHAIN protocol implementation
    ├── craq_server.*       ← CRAQ protocol implementation
    ├── crown_server.*      ← CROWN protocol implementation  ← primary focus
    ├── metadata_store.*    ← master: membership, heartbeats, reconfig
    └── client.*            ← benchmark / test client
```

All three server implementations share the gRPC service defined in `replication.proto`. The protocol-specific behavior is selected at startup via a `--mode` flag or config.

### Existing Infrastructure (Do Not Modify Unless Noted)

**`KVStore`** (`kv_store/kv_store.h`) — do not change the public interface:
- `store_`: `unordered_map<string, Record>` where `Record = {uint64_t version, string value}`. Holds only the latest **clean** committed record per key. O(1) read and write.
- `dirty_store_`: `unordered_map<string, unordered_map<uint64_t, Record>>`. Outer key is the string key; inner key is the version number. O(1) dirty-version existence check (`dirty_store_[key].count(version)`). Holds all in-flight dirty versions for a key until they are ACK'd.
- `mutex_store_`: single `std::mutex` guarding `store_`. Held only for the duration of a single map operation.
- `mutex_dirty_store_`: single `std::mutex` guarding `dirty_store_`. Held only for the duration of a single map operation.
- `put(key, value, version)`: if `version` is `nullopt`, this is a client-originated write — the store assigns `current_version + 1`. If `version` is set, only writes if the provided version is strictly greater than the current version in `store_`. Inserts into `dirty_store_` (not `store_`) for CROWN/CRAQ.
- `get(key)`: returns the value from `store_` (clean committed state only). O(1).
- `get_latest_version(key)`: returns the maximum version across both `store_` and `dirty_store_` for that key. Used by the head to assign the next version.
- `mark_clean(key, version)`: moves the record at `dirty_store_[key][version]` into `store_` if `version > store_[key].version`, then erases it from `dirty_store_`. All versions ≤ the given version are also erased from `dirty_store_` (they are superseded). O(1) amortised.

**`Replication`** (`replication.h`) — base class for all three protocol implementations:
- `pending_acks`: `unordered_map<int64_t, PutRequest>` keyed by `request_id`. O(1) insert, lookup, and erase.
- `key_mapping`: `unordered_map<char, Stub>` — maps a key to its **head** node stub.
- `key_tail_mapping`: `unordered_map<char, Stub>` — maps a key to its **tail** node stub. Used by CROWN/CRAQ replicas to send `VersionQuery` RPCs.
- `forward_put(request, next_stub)`: asynchronously forwards to the successor and inserts into `pending_acks`.
- `send_ack(request_id, prev_stub)`: if `prev_stub` is `nullopt`, sends `CommitAck` to the client address in the request; otherwise sends `WriteAck` to the previous node.
- Subclasses must implement `handle_put`, `handle_get`, and `handle_ack`.

**`Node`** (`node.h`) — per-node identity and ring position:
- `node_id`: the node's index in the ordered membership array, set at startup and updated on every `Reconfigure`.
- `ring_size`: total number of live nodes in the current membership view, updated on every `Reconfigure`.
- `prev` / `next`: `optional<Stub>` for predecessor and successor links. Either may be `nullopt` during startup before membership is received.
- Role for a given key is determined at the call site using `fnv1a_64(key) % ring_size` — see §4.1. Do not add role-check helpers to `Node`.

**`CMakeLists.txt`**: already configured for gRPC/protobuf. New `.cc` files must be added to the `add_executable` target.

**gRPC threading**: each server uses a `grpc::ServerBuilder` with a thread pool. Replica-to-replica forwarding must run off the request thread via `forward_put` (see §9).

---

## 4. Protocol: Write Path

### 4.1 Determining Role for a Given Key

Role determination uses the `fnv1a_64` hash function directly in protocol handlers. **Do not delegate this arithmetic to a helper in `Node`.**

```cpp
// Shared utility — place in a header included by all protocol files.
inline uint64_t fnv1a_64(const std::string& key) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : key) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return hash;
}

// Role checks at a node with node_id `my_index` in a ring of `ring_size` nodes:
bool is_head = (fnv1a_64(key) % ring_size == my_index);
bool is_tail = ((fnv1a_64(key) % ring_size + ring_size - 1) % ring_size == my_index);
```

`ring_size` is the length of the current membership array, delivered by `metadata_store` on startup and on every `Reconfigure`. `my_index` is the node's position in that array, also delivered by `metadata_store`. Both are stored as plain integers on `Node` and updated atomically (under the node's reconfiguration mutex) when `Reconfigure` arrives.

A node may simultaneously be head for some keys and tail for others (they will be different keys in the ring). A node that is neither head nor tail for a key is a **middle replica** for it.

### 4.2 Client → Head Write (version == 0 in PutRequest)

`version == 0` in `PutRequest` is the sentinel indicating the request originates from a client (not a replica).

**Steps at the head node:**

1. **Role check**: compute `fnv1a_64(request.key) % ring_size`. If the result is not `my_index`, return `WRONG_NODE` error to client. The client must re-route using the current membership list.
2. **Assign version and apply locally**: call `kv_store.put(request.key, request.value, std::nullopt)`. Passing `nullopt` signals a client-originated write; `KVStore` atomically assigns `version = get_latest_version(key) + 1`, inserts into `dirty_store_` under that version, and returns the assigned version `v`. The `mutex_store_` and `mutex_dirty_store_` inside `KVStore` protect this atomically — no external mutex is needed at this call site.
3. **Send acceptance ACK to client**: return `PutResponse{success: true, version: v}` to the client immediately. This closes the client's `Put` gRPC call. It is **not** a commit — it signals only that the head has accepted and versioned the write.
4. **Add to server pending list**: call `replication.add_to_pending_acks(request)` with `request.version` set to `v` and `request.client_addr` set to the client's `CommitAck` listener address.
5. **Forward to successor** (on a background thread): call `replication.forward_put(request, node.next)`. `forward_put` carries `client_commit_addr` through every hop so the tail can send the final `CommitAck` directly to the client's listener.

**Client-side model** (implement in `client.*`):

The client runs two threads:

- **Sender thread**: issues `Put` RPCs to the head. On receiving the acceptance `PutResponse`, it inserts `{key, version, timestamp}` into a local **client pending list** and immediately issues the next write. The gRPC call is now closed.
- **Commit listener thread**: exposes a lightweight gRPC service (`ClientAckService`) that the tail calls with `CommitAck{key, version}`. On receiving a `CommitAck`, this thread:
  1. Removes the matching `{key, version}` entry from the client pending list.
  2. Records the commit timestamp and increments the completed-write counter.
  3. Computes throughput as completed writes per second (rolling window or total elapsed).

> **Throughput measurement**: throughput is the rate of `CommitAck` messages received by the client listener thread — not the rate of `Put` calls issued. Latency per write is `commit_timestamp - send_timestamp` from the client pending list entry.

### 4.3 Replica → Replica Write (version != 0 in PutRequest)

`version != 0` means the head already assigned a version. No role check needed.

**Steps at a non-head node:**

1. **Idempotency check (I5)**: call `kv_store.get_latest_version(key)`. If `request.version <= latest_version` and the version is already clean in `store_`, drop the request silently and return `OK`. Still propogate the write to the next node so that the client ultimately receives an ack for that key and version. The `unordered_map` lookup in `dirty_store_` is O(1).
2. **Apply locally**: call `kv_store.put(request.key, request.value, request.version)`. Passing an explicit version inserts into `dirty_store_[key][version]` without touching `store_`. Protected by `mutex_dirty_store_` inside `KVStore`.
3. **Check if tail**: compute `(fnv1a_64(request.key) % ring_size + ring_size - 1) % ring_size == my_index`.
   - **If tail**: A node which is a tail for a certain key range should never have anything in its dirty store (for that key range). It immediately adds it to the clean store (if latest_version < version). Then call `replication.send_ack(request.request_id, std::nullopt)` to send `CommitAck` to the client, and `replication.send_ack(request.request_id, node.prev)` to send `WriteAck` backward to the predecessor. Do **not** forward to successor.
   - **If middle replica**: call `replication.add_to_pending_acks(request)` then `replication.forward_put(request, node.next)` on a background thread.

### 4.4 ACK Propagation (Tail → Head)

When the tail commits a write, it calls `replication.send_ack(request_id, node.prev)` which sends `WriteAck{request_id, key, version}` to the predecessor. Each intermediate node handles this in `handle_ack(request_id)`:

1. Looks up `pending_acks[request_id]` (O(1) `unordered_map` lookup) to retrieve the full request.
2. Calls `kv_store.mark_clean(key, version)` — moves the record from `dirty_store_[key][version]` to `store_[key]` if it is the highest version, and evicts superseded dirty entries. O(1).
3. Erases `pending_acks[request_id]`. O(1).
4. Calls `replication.send_ack(request_id, node.prev)` to forward `WriteAck` to its own predecessor.

The head's `handle_ack` does steps 1–3 but does **not** call `send_ack` onward — `node.prev` at the head points back to the tail in the ring, but `WriteAck` must not wrap around. The head detects this by checking `fnv1a_64(key) % ring_size == my_index`.

### 4.5 Retry on No ACK

Each entry in `pending_acks` has an associated **deadline** (e.g., 5000ms from insertion time, stored alongside the `PutRequest`). A background retry thread:

1. Iterates `pending_acks` periodically (every 1000ms), checking deadlines.
2. For entries past their deadline, calls `replication.forward_put(request, node.next)` again with the same `request` (same version — idempotent at the receiver via I5).

---

## 5. Protocol: Read Path

### 5.1 CHAIN Mode

- Compute `tail_index = (fnv1a_64(request.key) % ring_size + ring_size - 1) % ring_size`. If `my_index != tail_index`: return `WRONG_NODE` error. The client must re-route.
- If tail: return `kv_store.get(key)` directly.

### 5.2 CROWN Mode (CRAQ-style any-replica reads)

CROWN uses the same dirty/clean versioning read logic as CRAQ. Any replica may serve a read for any key — there is no tail-only restriction. The dirty version store is mandatory in CROWN (not optional). The logic is identical to §5.3 below; agents must apply it to both `craq_server` and `crown_server`.

### 5.3 CRAQ and CROWN: Any-Replica Read Logic

Any node may serve a read. The node uses `KVStore`'s built-in `dirty_store_` and `store_` to determine whether it can answer immediately or must consult the tail.

```cpp
GetResponse handle_read(string key):

    uint64_t latest_v = kv_store.get_latest_version(key)
    // get_latest_version returns max(store_[key].version, max key in dirty_store_[key])

    // O(1): check if dirty_store_[key] has any entry at all
    bool has_dirty = dirty_store_[key].size() > 0

    if (!has_dirty):
        // latest version is clean — answer directly from store_
        return kv_store.get(key)   // O(1) unordered_map lookup

    // dirty versions exist: query the tail for the committed version number
    // tail index = (fnv1a_64(key) % ring_size + ring_size - 1) % ring_size
    uint64_t committed_v = rpc_call(key_tail_mapping[key], VersionQuery{key})
    // VersionQuery is answered by the tail as: store_[key].version  (O(1))

    // O(1): check dirty_store_[key][committed_v]
    if dirty_store_[key].count(committed_v):
        return dirty_store_[key][committed_v].value

    // committed version already evicted to store_ — check clean store
    auto record = kv_store.get(key)   // O(1)
    if record.version >= committed_v:
        return record.value

    // should not occur in correct execution
    return ERROR_VERSION_NOT_FOUND
```

**`handle_version_request` at the tail** (called via `VersionQuery` RPC):
Returns `VersionQueryResponse{committed_version: store_[key].version}` via `kv_store.get_latest_version(key)` scoped to `store_` only. O(1). No value is transferred — this is a metadata-only call.

**`dirty_store_` data structure** (lives inside `KVStore`, already implemented):

```cpp
// outer key: string key name           → O(1) lookup via unordered_map
// inner key: uint64_t version number   → O(1) dirty-version existence check
unordered_map<string, unordered_map<uint64_t, Record>> dirty_store_;
```

Complexity guarantees that protocol code must rely on:
- **Dirty-version existence check**: `dirty_store_[key].count(version)` — O(1).
- **Latest dirty version**: `dirty_store_[key]` is an unordered map, so finding the maximum version requires O(n) scan over in-flight versions per key. `get_latest_version` handles this; do not reimplement it.
- **Clean read**: `kv_store.get(key)` hits `store_[key]` — O(1).
- **Mark clean**: `kv_store.mark_clean(key, version)` — O(k) where k is the number of dirty versions being evicted (bounded and small in practice).

Lifecycle of a dirty entry:
- **On `put(key, value, version)` with explicit version**: inserted into `dirty_store_[key][version]`. `store_` is not touched.
- **On `mark_clean(key, version)`**: `dirty_store_[key][version]` is moved to `store_[key]` if `version > store_[key].version`. All entries with version ≤ the given version are erased from `dirty_store_[key]`.

> **Key difference from CHAIN**: in CHAIN, `handle_get` rejects non-tail nodes with `WRONG_NODE` (checked via `fnv1a_64`). In CROWN and CRAQ, `handle_get` runs the above logic on every node without any role check. Do not add tail-index guards to the CROWN/CRAQ `handle_get` implementation.

---

## 6. Protocol: Reconfiguration & Failure Handling

Reconfiguration covers two fully specified scenarios:
- **A**: Adding a new node to the ring.
- **B**: Removing a failed node from the ring (node crash detected via missed heartbeats).

Network partition handling and `metadata_store` self-failure are explicitly **out of scope**.

### 6.1 Failure Detection

`metadata_store` sends **heartbeat pings** to each node every `HEARTBEAT_INTERVAL_MS = 500ms`. A node is declared **failed** if it misses `HEARTBEAT_MISS_THRESHOLD = 3` consecutive heartbeats (1500ms timeout).

Upon declaring a node failed, `metadata_store` initiates the reconfiguration protocol immediately.

### 6.2 CROWN Reconfiguration Protocol (Full Detail)

This is a **FREEZE → Drain → Commit** protocol. There are two sub-cases depending on whether it's an addition or a removal.

#### Phase 0: Failure/Addition Detected

`metadata_store` has a new target membership view `V_new` (either `V_old - {failed_node}` or `V_old + {new_node}`).

#### Phase 1: FREEZE

1. `metadata_store` multicasts a `Freeze{epoch: E}` RPC to **all currently live nodes** in `V_old`.
2. Each node, upon receiving `Freeze`:
   - Sets its local state to `FROZEN`.
   - **Stops accepting new client write requests** (returns `SERVICE_UNAVAILABLE` to clients).
   - **Continues forwarding in-progress server-to-server writes** (does not drop the pending list).
   - **Continues serving reads** (CROWN reads are still valid).
   - Sends `FreezeAck{node_id, epoch: E}` back to `metadata_store`.
3. `metadata_store` waits for `FreezeAck` from all live nodes. If a node does not respond within `FREEZE_ACK_TIMEOUT_MS = 2000ms`, treat it as also failed and remove from `V_new`.

#### Phase 2: Inflight Drain

This phase ensures all writes that were in-flight when FREEZE was declared are flushed to completion before reconfiguration is committed.

Each node independently runs the Inflight protocol:

1. After entering `FROZEN` state, each node creates an **Inflight sentinel message**: `Inflight{origin_node_id: my_id, epoch: E}`.
2. The node sends `Inflight` to its **successor** (using the pre-freeze ring topology).
3. As `Inflight` propagates around the ring, each node that receives it:
   a. **Flushes all pending writes**: sends any writes still in its pending list to its successor. Waits until the pending list for epoch ≤ E is empty before forwarding the `Inflight` message.
   b. Forwards `Inflight{origin_node_id, epoch}` to its own successor.
4. When a node receives its **own** `Inflight` message back (i.e., `origin_node_id == my_id`), it knows all writes it originated have traversed the full ring. It sends `DrainAck{node_id, epoch: E}` to `metadata_store`.

> **Key invariant**: A node must not forward `Inflight` until its pending list (for writes with version ≤ current epoch's max version) is empty.

> **Failed node handling during Inflight**: If a node's successor is the failed node, the `Inflight` message is sent directly to the failed node's successor (the next live node). The `Freeze` message includes `v_new_successor: address` so each node knows which address to skip to without needing a separate lookup. Any pending writes that were in the predecessor's pending list and targeted the failed node are **retransmitted by the predecessor to the new successor** once `Reconfigure` arrives (see §11.3). Writes are not considered lost — the predecessor's pending list is the source of truth for replay.

#### Phase 3: Data Sync (Addition Only)

If the reconfiguration is adding a new node `N_new`:

1. `metadata_store` waits for `DrainAck` from all live nodes.
2. `metadata_store` instructs `N_new` to perform a **full state sync**: `SyncRequest{source_node: any_live_node}`.
3. `N_new` calls `StateDump` RPC on the source node to retrieve the full key-value store snapshot.
4. `N_new` applies the snapshot locally.
5. `N_new` sends `SyncComplete{node_id: N_new}` to `metadata_store`.

> **Node removal**: No data sync needed. Skip Phase 3 and proceed directly to Phase 4.

#### Phase 4: Commit New Configuration

1. `metadata_store` has received `DrainAck` from all nodes (and `SyncComplete` from `N_new` if adding).
2. `metadata_store` computes `V_new`: the new ordered ring membership array.

   **For node removal specifically**: the failed node `F` is removed from the array. All keys that previously hashed to `F` (i.e., `fnv1a_64(key) % old_ring_size == F.index`) now hash to a different node because `fnv1a_64(key) % new_ring_size` is recomputed with the new ring size. The `metadata_store` does **not** need to explicitly enumerate or transfer key ranges — the deterministic hash function naturally redistributes affected keys when `ring_size` decrements. However, `metadata_store` must include a **key-range hint** in the `ReconfigureRequest` so that nodes know to expect incoming key-ownership changes:

   ```
   ReconfigureRequest {
     ...
     string removed_node_id = 5;   // set only for removal reconfigs; empty for additions
   }
   ```

3. `metadata_store` multicasts `Reconfigure{epoch: E+1, membership: V_new, removed_node_id}` + `Unfreeze` (bundled in one RPC call) to all nodes in `V_new`.
4. Each node in `V_new`, upon receiving `Reconfigure`:
   - Updates its local membership list to `V_new`.
   - Updates `my_index` and `ring_size` from `V_new`. Recomputes `predecessor` and `successor` stubs.
   - Recomputes role for any in-progress or newly arriving key using `fnv1a_64(key) % new_ring_size`. A node that was not head/tail for a key under `V_old` may now be, and vice versa — all role checks from this point forward use `new_ring_size`.
   - **If this node is the immediate successor of the removed node**: it is now the head/tail for additional keys. It must accept writes and reads for those keys immediately upon entering `NORMAL`. No data migration is needed because all surviving replicas already hold a full copy of all key-value data (full replication).
   - **If this node's predecessor was the failed node**: update predecessor stub to point to the new predecessor (failed node's predecessor in `V_old`). Retry any pending writes to the new successor (see §11.3).
   - Sets state to `NORMAL`.
   - Sends `ReconfigureAck{node_id, epoch: E+1}` to `metadata_store`.
5. `metadata_store` records epoch `E+1` as committed once all `ReconfigureAck`s are received.

### 6.3 CHAIN / CRAQ Reconfiguration Protocol

Chain and CRAQ use a simpler protocol because there is only one head and one tail.

#### Adding a new tail:

1. `metadata_store` sends `Freeze{}` to the **head** only (stops new client writes entering).
2. `metadata_store` sends `NewTailPending{new_tail_addr}` to the **current tail**, so it can forward any remaining writes to `new_tail` as they arrive. The current tail also forwards all existing pending writes to `new_tail`.
3. `new_tail` performs a full state sync from the current tail (`StateDump` RPC).
4. Once `new_tail` confirms sync complete (`SyncComplete` to `metadata_store`):
5. `metadata_store` sends `Reconfigure{new_membership}` + `Unfreeze` to all nodes.

#### Removing a failed node:

- **Failed head**: `metadata_store` promotes the second node to head. Sends `Reconfigure` to all survivors. No drain needed (head was the entry point; any client writes that got no acceptance ACK will be retried by the client).
- **Failed tail**: `metadata_store` promotes the penultimate node to tail. Sends `Reconfigure`. Any writes that were propagating to the failed tail: the predecessor (now new tail) has them in its pending list and will commit them itself (sends an ACK to the client).
- **Failed middle node**: `metadata_store` instructs the failed node's predecessor to link directly to the failed node's successor. Pending writes at the predecessor are retransmitted to the new successor.

---

## 7. Metadata Store Responsibilities

The `metadata_store` is a single logical master (may itself be replicated via Paxos for production; for this implementation a single process is acceptable).

### State Maintained

```
struct ClusterState {
    uint64_t             epoch;               // current configuration epoch
    vector<NodeInfo>     membership;          // ordered ring array
    ProtocolMode         mode;                // CHAIN, CRAQ, or CROWN
    map<string, string>  key_to_head_cache;   // optional: cached routing hints for clients
};

struct NodeInfo {
    string node_id;
    string address;
    int    port;
    bool   alive;
    time_t last_heartbeat;
};
```

### RPCs Exposed by metadata_store

| RPC | Direction | Purpose |
|---|---|---|
| `GetMembership` | client/node → master | Returns current `ClusterState` |
| `Freeze` | master → node | Initiates freeze phase |
| `Reconfigure` | master → node | Delivers new membership + unfreeze |
| `DrainAck` | node → master | Node confirms drain complete |
| `SyncComplete` | new node → master | New node confirms state sync done |
| `FreezeAck` | node → master | Node confirms it is frozen |
| `ReconfigureAck` | node → master | Node confirms new config applied |
| `ReportFailure` | node → master | Node reports suspected peer failure |
| `Heartbeat` | master → node (or node → master) | Liveness ping |

### Heartbeat Mechanism

**Node-push**: Each node sends a `HeartbeatPing{node_id, epoch}` to `metadata_store` every 500ms. If `metadata_store` receives nothing from a node for 1500ms, declare failed.

### Epoch Enforcement

All RPCs between nodes carry an `epoch` field. Nodes reject any RPC with `epoch < my_epoch` (stale message from a node that hasn't received reconfiguration yet). Nodes accept and apply any RPC with `epoch > my_epoch` (they were behind; they should immediately request the latest `GetMembership` from master).

---

## 8. gRPC / Proto Additions Required

The following messages and RPCs need to be added to (or verified as present in) `replication.proto`. Do not break existing CHAIN/CRAQ message signatures.

```protobuf
syntax = "proto3";
package replication;

// ── Existing (verify these exist) ──────────────────────────────────────────

message PutRequest {
  string key        = 1;
  string value      = 2;
  uint64 version    = 3;   // 0 = from client; >0 = replica-to-replica
  string client_addr = 4;  // client address for tail to send commit ACK to
  uint64 epoch      = 5;   // current config epoch; node rejects if stale
}

message PutResponse {
  bool   success = 1;
  string error   = 2;   // e.g. "WRONG_NODE", "FROZEN", "STALE_EPOCH"
  uint64 version = 3;   // assigned version (from head acceptance ACK)
}

message GetRequest {
  string key   = 1;
  uint64 epoch = 2;
}

message GetResponse {
  string value   = 1;
  uint64 version = 2;
  string error   = 3;
}

// ── New: Commit ACK (tail → predecessor chain, and tail → client) ───────────

message WriteAck {
  string key     = 1;
  uint64 version = 2;
  uint64 epoch   = 3;
}

message WriteAckResponse {
  bool success = 1;
}

// Sent by the tail directly to the client's listener service upon commit.
message CommitAck {
  string key     = 1;
  uint64 version = 2;
}

message CommitAckResponse {
  bool success = 1;
}

// ── New: CRAQ version query ─────────────────────────────────────────────────

message VersionQueryRequest {
  string key   = 1;
  uint64 epoch = 2;
}

message VersionQueryResponse {
  uint64 committed_version = 1;
  string error             = 2;
}

// ── New: Reconfiguration messages ───────────────────────────────────────────

message NodeInfo {
  string node_id = 1;
  string address = 2;
  int32  port    = 3;
}

message FreezeRequest {
  uint64            epoch        = 1;
  repeated NodeInfo v_new        = 2;   // new membership (for computing new successor)
}

message FreezeResponse {
  bool   success = 1;
  string node_id = 2;
}

message DrainAckRequest {
  string node_id = 1;
  uint64 epoch   = 2;
}

message DrainAckResponse {
  bool success = 1;
}

message ReconfigureRequest {
  uint64            epoch          = 1;
  repeated NodeInfo membership    = 2;  // new ordered ring
  string            mode          = 3;  // "CHAIN", "CRAQ", "CROWN"
  string            removed_node_id = 4; // non-empty only for node-removal reconfigs
  string            added_node_id   = 5; // non-empty only for node-addition reconfigs
}

message ReconfigureResponse {
  bool   success = 1;
  string node_id = 2;
}

message InflightRequest {
  string origin_node_id = 1;
  uint64 epoch          = 2;
}

message InflightResponse {
  bool success = 1;
}

message StateDumpRequest {
  uint64 epoch = 1;
}

message StateDumpResponse {
  // All committed key-value pairs
  map<string, string> kv_data         = 1;
  map<string, uint64> version_data     = 2;  // key → committed version
  uint64              epoch            = 3;
}

message SyncCompleteRequest {
  string node_id = 1;
  uint64 epoch   = 2;
}

message SyncCompleteResponse {
  bool success = 1;
}

message HeartbeatRequest {
  string node_id = 1;
  uint64 epoch   = 2;
}

message HeartbeatResponse {
  bool   alive = 1;
  uint64 epoch = 2;  // master's current epoch (node can detect if it's behind)
}

message MembershipRequest {}

message MembershipResponse {
  uint64            epoch      = 1;
  repeated NodeInfo membership = 2;
  string            mode       = 3;
}

// ── Services ─────────────────────────────────────────────────────────────────

service ReplicationService {
  // Client-facing
  rpc Put             (PutRequest)          returns (PutResponse);
  rpc Get             (GetRequest)          returns (GetResponse);

  // Replica-to-replica
  rpc ForwardPut      (PutRequest)          returns (PutResponse);
  rpc SendWriteAck    (WriteAck)            returns (WriteAckResponse);
  rpc VersionQuery    (VersionQueryRequest) returns (VersionQueryResponse);
  rpc ForwardInflight (InflightRequest)     returns (InflightResponse);
  rpc StateDump       (StateDumpRequest)    returns (StateDumpResponse);
}

// Exposed by the client process so the tail can push commit notifications back.
service ClientAckService {
  rpc CommitAck (CommitAck) returns (CommitAckResponse);
}

service MetadataService {
  rpc GetMembership   (MembershipRequest)   returns (MembershipResponse);
  rpc Freeze          (FreezeRequest)       returns (FreezeResponse);
  rpc Reconfigure     (ReconfigureRequest)  returns (ReconfigureResponse);
  rpc ReportDrainAck  (DrainAckRequest)     returns (DrainAckResponse);
  rpc ReportSyncDone  (SyncCompleteRequest) returns (SyncCompleteResponse);
  rpc Heartbeat       (HeartbeatRequest)    returns (HeartbeatResponse);
}
```

> **Implementation note**: `Put` is the client-facing RPC. `ForwardPut` is replica-to-replica. They carry the same `PutRequest` message but are on separate RPC endpoints so routing logic can distinguish them trivially without inspecting the `version` field.

---

## 9. Threading Model

Each server node runs the following threads:

| Thread | Responsibility |
|---|---|
| **gRPC server threads** (pool) | Handle incoming RPCs. Must return quickly — no blocking I/O on this thread. |
| **Replication worker thread** | Dequeues entries from the replication queue and calls `ForwardPut` to successor. |
| **ACK worker thread** | Dequeues `WriteAck` messages and calls `SendWriteAck` to predecessor. |
| **Retry thread** | Periodically scans pending list for timed-out writes and retransmits or escalates to `metadata_store`. Runs every 1000ms. |
| **Heartbeat thread** | Sends `HeartbeatRequest` to `metadata_store` every 500ms. |
| **Drain thread** (activated during FREEZE) | Runs the Inflight protocol: waits for pending list to empty, then forwards `Inflight` sentinel. |

The client process runs the following threads:

| Thread | Responsibility |
|---|---|
| **Sender thread** | Issues `Put` RPCs to the head. On receiving acceptance `PutResponse`, inserts `{key, version, send_timestamp}` into the client pending list and immediately issues the next write. |
| **Commit listener thread** | Runs a `ClientAckService` gRPC server. On receiving `CommitAck{key, version}` from the tail: removes the entry from the client pending list, records `commit_timestamp - send_timestamp` as write latency, and increments the completed-write counter for throughput calculation. |

### Synchronization Rules

- **`store_`** is protected by `mutex_store_` (a `std::mutex` inside `KVStore`). All access goes through `KVStore` public methods — do not lock it externally.
- **`dirty_store_`** is protected by `mutex_dirty_store_` (a separate `std::mutex` inside `KVStore`). Same rule — access only via `KVStore` methods.
- **`pending_acks`** (`unordered_map<int64_t, PutRequest>` in `Replication`) must be protected by a dedicated `std::mutex` in the `Replication` subclass. It is accessed from the gRPC handler thread (insert on `forward_put`) and the retry thread (scan) and the ACK handler thread (erase on `handle_ack`).
- The **node state** (`NORMAL`, `FROZEN`, `RECONFIGURING`) is an `std::atomic<NodeState>`. The gRPC handler checks state atomically before processing any client write.
- **`node.prev`, `node.next`, `my_index`, and `ring_size`** are updated atomically under a `std::mutex` during reconfiguration. Callers must acquire this mutex before reading any of these fields if reconfiguration is possible concurrently.
- **No gRPC stubs should be created inside locks**. Stubs are created once at startup (or on reconfiguration) and stored as `shared_ptr`s, swapped in under the stub mutex.

---

## 10. State Machine: Node Lifecycle

```
         ┌──────────────────────────────────────────────┐
         │                                              │
  boot → STARTING → GetMembership → NORMAL ←──── Reconfigure+Unfreeze
                                      │                 │
                                   Freeze          DrainComplete
                                      │                 │
                                    FROZEN ─────────────┘
                                      │
                                  InflightDone
                                      │
                                 DRAINING_WAIT
                                 (waiting for
                               metadata_store to
                               send Reconfigure)
```

### State Descriptions

- **STARTING**: node is booting, fetches membership from `metadata_store`, initializes `my_index`, `ring_size`, and successor/predecessor stubs.
- **NORMAL**: fully operational. Accepts client writes and reads. Forwards replicas.
- **FROZEN**: client writes rejected (`SERVICE_UNAVAILABLE`). Server-to-server forwarding continues. Drain thread is active.
- **DRAINING_WAIT**: Inflight round trip complete; `DrainAck` sent to master; waiting for `Reconfigure`.

---

## 11. Error Handling & Edge Cases

### 11.1 `WRONG_NODE` Response to Client

Clients that receive `WRONG_NODE` must:
1. Call `GetMembership` on `metadata_store` to refresh their routing table.
2. Recompute `head(key) = fnv1a_64(key) % new_ring_size` with the fresh membership list.
3. Retry the write to the correct node.

The server implementation should include the correct `head_address` in the `PutResponse.error` field as a hint (e.g., `"WRONG_NODE:192.168.1.5:50051"`) to avoid an extra round-trip to `metadata_store`.

### 11.2 Stale Epoch on Incoming RPC

If a node receives an RPC with `epoch < my_epoch`:
- Log a warning and return `STALE_EPOCH` error.
- Do **not** apply the write.

If a node receives an RPC with `epoch > my_epoch`:
- Apply the operation tentatively (it is likely valid).
- Asynchronously call `GetMembership` from `metadata_store` to catch up.

### 11.3 Successor Failure During Normal Operation

If `ForwardPut` to successor returns `UNAVAILABLE` or times out:
1. Do **not** remove from the pending list.
2. When `Reconfigure` arrives with a new membership that excludes the failed successor:
   - The node updates `ring_size` and `my_index` from `V_new` and recomputes its successor stub.
   - **All pending entries are immediately retransmitted to the new successor** in version order (ascending). This is the authoritative replay policy — the predecessor is responsible for ensuring these writes complete. Clients do **not** need to retry.
   - After retransmission, entries are removed from the pending list only when a `WriteAck` is received for them from the new successor's downstream.

> **Why the predecessor replays, not the client**: Each pending-list entry carries the full `{key, version, value, client_addr}`. The version was already assigned by the head, so replaying to the new successor with the same version preserves the linearization order. Client-side retry would cause the head to assign a new, higher version — breaking the ordering that was established before the failure.

### 11.4 Double Delivery / Idempotency

All `ForwardPut` calls must be idempotent. The receiving node enforces I5 (version ≤ committed → drop). No additional deduplication logic is needed.

### 11.5 Node Restart / Crash Recovery

Nothing. We use fail-stop model.

> **Design note**: Persistent storage (WAL/disk) is out of scope for this implementation. All state is in-memory. A restarted node always re-syncs from peers.

### 11.6 Concurrent Writes to the Same Key

The per-key mutex at the head serializes concurrent writes to the same key. Only one thread may be in the version-assign + local-apply section at a time. The replication pipeline for a single key is therefore sequential (FIFO order is preserved). Concurrent writes to *different* keys proceed in parallel.

---

## 12. Testing Checklist

Agents should write unit and integration tests covering the following. Each test should be a named test case in a `tests/` directory.

### Write Path Tests

- [ ] `test_head_assigns_version_monotonically`: concurrent writes to same key → versions are 1, 2, 3... with no gaps or duplicates.
- [ ] `test_wrong_node_rejected`: client sends write to a non-head node → `WRONG_NODE` error returned.
- [ ] `test_write_propagates_full_ring`: write at head → all replicas have the value after ACK.
- [ ] `test_stale_write_dropped`: send `ForwardPut` with version ≤ committed → node drops it, returns OK.
- [ ] `test_pending_list_cleared_on_ack`: after tail ACK, head's pending list for that version is empty.

### Read Path Tests

- [ ] `test_chain_read_from_tail_only`: in CHAIN mode, reading from a non-tail node returns `WRONG_NODE`.
- [ ] `test_crown_any_replica_read_accepted`: in CROWN mode, a read issued to any replica (non-tail included) does not return `WRONG_NODE`.
- [ ] `test_crown_clean_read_no_tail_contact`: after a write is fully ACK'd (version is clean on all replicas), any replica answers the read without issuing a `VersionQuery` to the tail.
- [ ] `test_crown_dirty_read_queries_tail_version`: a write is in-flight (dirty) → replica receives a read → replica issues `VersionQuery` to the tail → returns the value at the committed version.
- [ ] `test_craq_clean_read`: same as `test_crown_clean_read_no_tail_contact` but for CRAQ mode.
- [ ] `test_craq_dirty_read_queries_tail`: same as `test_crown_dirty_read_queries_tail_version` but for CRAQ mode.
- [ ] `test_read_during_freeze_succeeds`: while the ring is FROZEN (no client writes accepted), reads from any replica still return the last committed value.

### Reconfiguration Tests

- [ ] `test_freeze_stops_client_writes`: after `Freeze`, client writes return `SERVICE_UNAVAILABLE`.
- [ ] `test_inflight_drains_before_reconfig`: writes in-flight during FREEZE are committed before new config is applied.
- [ ] `test_add_node_state_sync`: new node receives full state after `SyncComplete`; it can serve reads for all keys.
- [ ] `test_remove_node_ring_repaired`: after node F is removed, F's predecessor and F's successor become direct neighbors; reads and writes proceed correctly.
- [ ] `test_remove_node_key_range_moves_to_successor`: keys whose head index changes under the new `ring_size` are correctly handled after reconfiguration; writes to those keys succeed.
- [ ] `test_predecessor_replays_parked_writes_in_version_order`: writes parked during successor failure are retransmitted in ascending version order to the new successor after `Reconfigure`; client receives correct commit ACKs without retrying.
- [ ] `test_no_client_retry_needed_on_successor_failure`: client issues write → successor fails mid-propagation → predecessor replays → client receives commit ACK; client does not time out or need to re-issue.

### Failure Detection Tests

- [ ] `test_metadata_store_declares_failure_after_3_missed_heartbeats`: simulate node silence → master initiates reconfig after 1500ms.
- [ ] `test_successor_fail_escalation`: `ForwardPut` fails 3 times → `ReportFailure` called on master.

### Epoch Tests

- [ ] `test_stale_epoch_rejected`: node rejects RPC with `epoch < my_epoch`.
- [ ] `test_future_epoch_triggers_membership_refresh`: node receiving `epoch > my_epoch` calls `GetMembership`.