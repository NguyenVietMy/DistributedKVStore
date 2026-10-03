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
term, role, and log progress. To write through the client protocol, send the
request to the leader's port:

```sh
./build/dkv_client 9101 put x 10
./build/dkv_client 9101 delete x
```

The client reports `committed` only after the entry is applied by the leader.
A follower returns `not leader` with a leader hint if it knows one. If the
leader steps down while a write is pending, the reply says `outcome unknown`:
the write might still be committed later, and retrying can duplicate it.

The node console's `put x 10` and `delete x` commands remain local debugging
proposals: they print a log index without waiting for commitment. An
`applied index N` line means that node learned the entry was committed and
applied it. `inspect x` shows only that node's local applied state; it is not a
linearizable client read. Use `quit` to stop a node.

The current TCP transport is localhost-only. Raft messages use one short-lived
connection each. A client write keeps its connection open until commitment,
step-down, or disconnection. There is no client read protocol or automatic
deduplication yet.
