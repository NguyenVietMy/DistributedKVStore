# DistributedKVStore

This project is a small three-node Raft key-value store in progress. Each node
persists its Raft term, vote, and log. The Linux `dkv_node` program runs the
election and heartbeat timers and exchanges Raft messages over local TCP.

Build and test:

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Run three nodes in separate terminals, using distinct data directories:

```sh
./build/dkv_node 1 /tmp/dkv-1 9101 9102 9103
./build/dkv_node 2 /tmp/dkv-2 9101 9102 9103
./build/dkv_node 3 /tmp/dkv-3 9101 9102 9103
```

The ports are ordered by node ID. On each terminal, `status` shows the current
term, role, and log progress. Send client requests to the leader's port:

```sh
./build/dkv_client 9101 put x 10
./build/dkv_client 9101 get x
./build/dkv_client 9101 delete x
```

Or give the client all three ports in node-ID order so it can find the leader
and retry during failover:

```sh
./build/dkv_client 9101 9102 9103 put x 10
./build/dkv_client 9101 9102 9103 get x
```

The client reports `committed` only after the entry is applied by the leader.
A follower returns `not leader` with a leader hint if it knows one. If the
leader steps down while a write is pending, the reply says `outcome unknown`:
the write might still be committed later. Each write prints a 32-digit request
ID on stderr. Set `ID` to that value and retry the same command:

```sh
./build/dkv_client 9101 9102 9103 put x 10 "$ID"
```

The leader returns the original committed index if that request already
applied. Reusing an ID for different contents is rejected. IDs and results
are reconstructed from the replicated log after failover or restart. Omitting
the ID generates a new one, so save the printed ID when you may need to retry.
In three-port mode, the client keeps one ID while following leader hints and
retrying transport failures or uncertain replies for up to eight seconds. If
that deadline expires, it reports an unknown write outcome and prints the ID
for a later retry. Single-port mode sends one request and leaves retries to
the caller.

For `get`, the leader commits and applies a no-op barrier before reading its
local state. This costs one log entry per read, but prevents stale replies and
means reads cannot complete without a majority. A follower returns `not leader`.

The node console's `put x 10` and `delete x` commands remain local debugging
proposals: they print a log index without waiting for commitment. An
`applied index N` line means that node learned the entry was committed and
applied it. `inspect x` shows only that node's local applied state; it is not a
linearizable client read. Use `quit` to stop a node.

The current TCP transport is localhost-only. Raft messages use one short-lived
connection each. A client request keeps its connection open until completion,
step-down, or disconnection.
